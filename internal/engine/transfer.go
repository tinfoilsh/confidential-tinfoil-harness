package engine

import (
	"context"
	"encoding/base64"
	"net/http"
	"strconv"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Moving a chat between the cloud and the device keeps its id, as the web
// client always has. A chat on the device holds its attachments' content and
// no server keys, since deleting the cloud copy deletes its blobs.

// ThreadTranscript is a stored chat in the form the device keeps: every
// attachment's content inline and no server bookkeeping. It refuses when any
// attachment cannot be read, so moving the chat never loses content.
func (eng *Engine) ThreadTranscript(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *api.ThreadRequest) (any, error) {
	eng.Live.mu.Lock()
	live := eng.Live.threads[liveKey(p, in.ThreadID)]
	running := live != nil && (live.Active != nil || len(live.Queue) > 0)
	eng.Live.mu.Unlock()
	if running {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "Wait for the reply to finish before moving this chat").WithAction(api.ActionRetry)
	}
	row, err := eng.Store.LoadThread(ctx, p, key, in.ThreadID)
	if err != nil {
		return nil, err
	}
	if err := eng.Store.ApplyPins(ctx, p, key, []*domain.ThreadRow{row}); err != nil {
		return nil, err
	}
	missing := 0
	err = row.EachAttachment(func(a *domain.Attachment) error {
		if a.StorageKey() == "" {
			return nil
		}
		data, err := eng.Attachments.Bytes(ctx, p, a)
		if err == nil && a.IsImage() {
			a.Base64 = base64.StdEncoding.EncodeToString(data)
		} else if err == nil {
			var content attach.DocumentBlob
			if content, err = attach.DecodeDocument(data); err == nil {
				a.TextContent, a.Pages = content.TextContent, content.Pages
			}
		}
		clear(data)
		if attach.Unreachable(err) {
			missing++
			return nil
		}
		if err != nil {
			return err
		}
		a.EncryptionKey, a.Key, a.StorageThreadID = "", "", ""
		return nil
	})
	if err != nil {
		return nil, err
	}
	if missing > 0 {
		return nil, api.Err(409, "ATTACHMENTS_UNAVAILABLE", strconv.Itoa(missing)+" attachments in this chat could not be read")
	}
	row.CheckWidgets()
	row.RecordVersion = domain.RecordVersion
	return struct {
		Thread   domain.DeviceTranscript `json:"thread"`
		Revision string                  `json:"revision"`
	}{domain.DeviceTranscript{Thread: row.Thread}, row.ETag}, nil
}

func (eng *Engine) FinishTransfer(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *api.DeleteRequest) error {
	eng.Live.mu.Lock()
	name := liveKey(p, in.ID)
	s := eng.Live.threads[name]
	if s != nil && (s.deleted || s.Active != nil || len(s.Queue) > 0) {
		eng.Live.mu.Unlock()
		return api.Err(409, "RUN_IN_FLIGHT", "Wait for the reply to finish before moving this chat").WithAction(api.ActionRetry)
	}
	if eng.Live.threads == nil {
		eng.Live.threads = map[string]*threadSession{}
	}
	s = &threadSession{ID: in.ID, owner: p.Scope(), deleted: true}
	eng.Live.threads[name] = s
	eng.Live.mu.Unlock()
	defer func() {
		eng.Live.mu.Lock()
		eng.dropIdle(s)
		eng.Live.mu.Unlock()
	}()
	return eng.Store.RemoveThreadRevision(ctx, p, key, in.ID, in.Revision)
}

// HandleThreadImport stores a chat from the device as a cloud chat under the
// same id. Its attachments are stored under the new chat first, as the control plane
// requires; the chat itself is a create, so an existing cloud chat is never
// overwritten.
func (eng *Engine) HandleThreadImport(w http.ResponseWriter, r *http.Request) {
	p, err := eng.Auth.Authenticate(r, false)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	body, err := api.ReadBody(w, r, domain.MaxTurnBody)
	defer clear(body)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	var in ThreadImportRequest
	defer in.ClearKey()
	if err := api.DecodeBody(body, &in); err != nil {
		api.Respond(w, nil, err)
		return
	}
	key, err := auth.DecodeContentKey(in.Key)
	in.ClearKey()
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer clear(key[:])
	result, err := eng.ImportThread(r.Context(), p, key, &in)
	api.Respond(w, result, err)
}

func (eng *Engine) ImportThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *ThreadImportRequest) (any, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	id := in.ThreadID
	thread, err := domain.ReadDeviceThread(id, in.Thread)
	if err != nil {
		return nil, err
	}
	row := &domain.ThreadRow{Thread: thread}
	if err := eng.Attachments.PlaceAttachments(ctx, p, key, id, &row.Thread); err != nil {
		return nil, err
	}
	attach.StripInlineAttachments(&row.Thread)
	if err := eng.Store.SaveThread(ctx, p, key, row); err != nil {
		if api.AsError(err).Code == "REVISION_CONFLICT" {
			return nil, api.Err(409, "THREAD_EXISTS", "This chat is already in the cloud")
		}
		return nil, err
	}
	if row.Pinned {
		if err := eng.Store.SetPin(ctx, p, key, id, true); err != nil {
			return nil, err
		}
	}
	return domain.Summarize(&row.Thread), nil
}
