package main

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

// threadTranscript is a stored chat in the form the device keeps: every
// attachment's content inline and no server bookkeeping. It refuses when any
// attachment cannot be read, so moving the chat never loses content.
func (eng *engine) threadTranscript(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *api.ThreadRequest) (any, error) {
	eng.live.mu.Lock()
	live := eng.live.threads[liveKey(p, in.ThreadID)]
	running := live != nil && (live.active != nil || len(live.queue) > 0)
	eng.live.mu.Unlock()
	if running {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "Wait for the reply to finish before moving this chat").WithAction(api.ActionRetry)
	}
	row, err := eng.store.LoadThread(ctx, p, key, in.ThreadID)
	if err != nil {
		return nil, err
	}
	if err := eng.store.ApplyPins(ctx, p, key, []*domain.ThreadRow{row}); err != nil {
		return nil, err
	}
	missing := 0
	err = row.EachAttachment(func(a *domain.Attachment) error {
		if a.StorageKey() == "" {
			return nil
		}
		data, err := eng.attachments.Bytes(ctx, p, a, false)
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

func (eng *engine) finishTransfer(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *api.DeleteRequest) error {
	eng.live.mu.Lock()
	name := liveKey(p, in.ID)
	s := eng.live.threads[name]
	if s != nil && (s.deleted || s.active != nil || len(s.queue) > 0) {
		eng.live.mu.Unlock()
		return api.Err(409, "RUN_IN_FLIGHT", "Wait for the reply to finish before moving this chat").WithAction(api.ActionRetry)
	}
	if eng.live.threads == nil {
		eng.live.threads = map[string]*threadSession{}
	}
	s = &threadSession{id: in.ID, owner: p.Scope(), deleted: true}
	eng.live.threads[name] = s
	eng.live.mu.Unlock()
	defer func() {
		eng.live.mu.Lock()
		eng.dropIdle(s)
		eng.live.mu.Unlock()
	}()
	return eng.store.RemoveThreadRevision(ctx, p, key, in.ID, in.Revision)
}

// handleThreadImport stores a chat from the device as a cloud chat under the
// same id. Its attachments are stored under the new chat first, as sync
// requires; the chat itself is a create, so an existing cloud chat is never
// overwritten.
func (eng *engine) handleThreadImport(w http.ResponseWriter, r *http.Request) {
	p, err := eng.auth.Authenticate(r, false)
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
	var in threadImportRequest
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
	result, err := eng.importThread(r.Context(), p, key, &in)
	api.Respond(w, result, err)
}

func (eng *engine) importThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *threadImportRequest) (any, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	id := in.ThreadID
	thread, err := domain.ReadDeviceThread(id, in.Thread)
	if err != nil {
		return nil, err
	}
	row := &domain.ThreadRow{Thread: thread, Restore: true}
	if err := eng.attachments.PlaceAttachments(ctx, p, id, &row.Thread); err != nil {
		return nil, err
	}
	attach.StripInlineAttachments(&row.Thread)
	if err := eng.store.SaveThread(ctx, p, key, row); err != nil {
		if api.AsError(err).Code == "REVISION_CONFLICT" {
			return nil, api.Err(409, "THREAD_EXISTS", "This chat is already in the cloud")
		}
		return nil, err
	}
	if row.Pinned {
		if err := eng.store.SetPin(ctx, p, key, id, true); err != nil {
			return nil, err
		}
	}
	return domain.Summarize(&row.Thread), nil
}
