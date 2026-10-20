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
func (h *harness) threadTranscript(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, err
	}
	row, err := h.loadThread(ctx, p, key, id)
	if err != nil {
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
		a.EncryptionKey, a.Key, a.StorageThreadID, a.PageCount = "", "", "", 0
		return nil
	})
	if err != nil {
		return nil, err
	}
	if missing > 0 {
		return nil, apiErr(409, "ATTACHMENTS_UNAVAILABLE", strconv.Itoa(missing)+" attachments in this chat could not be read")
	}
	return struct {
		Thread deviceTranscript `json:"thread"`
	}{deviceTranscript{Thread: row.Thread}}, nil
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
	in, err := decodeJSONWithin(w, r, maxTurnBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	key, err := decodeContentKey(str(in["key"]))
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	result, err := h.importThread(r.Context(), p, key, in)
	respond(w, result, err)
}

func (h *harness) importThread(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, err
	}
	if !validObject(in["thread"]) {
		return nil, apiErr(400, "BAD_REQUEST", "thread must be an object")
	}
	if err := validateLocalThread(obj(in["thread"])); err != nil {
		return nil, err
	}
	row := &threadRow{Thread: readDeviceThread(id, raw(in["thread"])), restore: true}
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
	return summarize(&row.Thread), nil
}
