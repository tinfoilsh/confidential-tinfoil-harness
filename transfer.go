package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"strconv"
)

// Moving a chat between the cloud and the device keeps its id, as the web
// client always has. A chat on the device holds its attachments' content and
// no server keys, since deleting the cloud copy deletes its blobs.

// threadTranscript is a stored chat in the form the device keeps: every
// attachment's content inline and no server bookkeeping. It refuses when any
// attachment cannot be read, so moving the chat never loses content.
func (h *harness) threadTranscript(ctx context.Context, p *principal, key contentKey, in *threadRequest) (any, error) {
	h.live.mu.Lock()
	live := h.live.threads[liveKey(p, in.ThreadID)]
	running := live != nil && (live.active != nil || len(live.queue) > 0)
	h.live.mu.Unlock()
	if running {
		return nil, apiErr(409, "RUN_IN_FLIGHT", "Wait for the reply to finish before moving this chat")
	}
	row, err := h.loadThread(ctx, p, key, in.ThreadID)
	if err != nil {
		return nil, err
	}
	if err := h.applyPins(ctx, p, key, []*threadRow{row}); err != nil {
		return nil, err
	}
	missing := 0
	err = row.eachAttachment(func(a *Attachment) error {
		if a.storageKey() == "" {
			return nil
		}
		data, err := h.attachmentBytes(ctx, p, a, false)
		if err == nil && a.isImage() {
			a.Base64 = base64.StdEncoding.EncodeToString(data)
		} else if err == nil {
			var content documentBlob
			if content, err = decodeDocument(data); err == nil {
				a.TextContent, a.Pages = content.TextContent, content.Pages
			}
		}
		clear(data)
		if unreachable(err) {
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
		return nil, apiErr(409, "ATTACHMENTS_UNAVAILABLE", strconv.Itoa(missing)+" attachments in this chat could not be read")
	}
	row.checkWidgets()
	return struct {
		Thread   deviceTranscript `json:"thread"`
		Revision string           `json:"revision"`
	}{deviceTranscript{Thread: row.Thread}, row.etag}, nil
}

func (h *harness) finishTransfer(ctx context.Context, p *principal, key contentKey, in *deleteThreadRequest) error {
	h.live.mu.Lock()
	name := liveKey(p, in.ID)
	s := h.live.threads[name]
	if s != nil && (s.deleted || s.active != nil || len(s.queue) > 0) {
		h.live.mu.Unlock()
		return apiErr(409, "RUN_IN_FLIGHT", "Wait for the reply to finish before moving this chat")
	}
	if h.live.threads == nil {
		h.live.threads = map[string]*threadSession{}
	}
	s = &threadSession{id: in.ID, owner: p.scope(), deleted: true}
	h.live.threads[name] = s
	h.live.mu.Unlock()
	defer func() {
		h.live.mu.Lock()
		h.dropIdle(s)
		h.live.mu.Unlock()
	}()
	return h.removeThreadRevision(ctx, p, key, in.ID, in.Revision)
}

// handleThreadImport stores a chat from the device as a cloud chat under the
// same id. Its attachments are stored under the new chat first, as sync
// requires; the chat itself is a create, so an existing cloud chat is never
// overwritten.
func (h *harness) handleThreadImport(w http.ResponseWriter, r *http.Request) {
	p, err := h.auth.authenticate(r, false)
	if err != nil {
		respond(w, nil, err)
		return
	}
	body, err := readBody(w, r, maxTurnBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	var in threadImportRequest
	if err := decodeRequest(body, &in); err != nil {
		respond(w, nil, err)
		return
	}
	key, err := decodeContentKey(in.Key)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	result, err := h.importThread(r.Context(), p, key, &in)
	respond(w, result, err)
}

func (h *harness) importThread(ctx context.Context, p *principal, key contentKey, in *threadImportRequest) (any, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}
	id := in.ThreadID
	row := &threadRow{Thread: readDeviceThread(id, in.Thread), restore: true}
	if err := h.placeAttachments(ctx, p, id, &row.Thread); err != nil {
		return nil, err
	}
	stripInlineAttachments(&row.Thread)
	if err := h.saveThread(ctx, p, key, row); err != nil {
		if asAPIError(err).Code == "REVISION_CONFLICT" {
			return nil, apiErr(409, "THREAD_EXISTS", "This chat is already in the cloud")
		}
		return nil, err
	}
	if row.Pinned {
		if err := h.setPin(ctx, p, key, id, true); err != nil {
			return nil, err
		}
	}
	return summarize(&row.Thread), nil
}
