package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"strconv"

	"github.com/google/uuid"
)

// Moving a chat between the cloud and the device keeps its id, as the web
// client always has. A chat on the device holds its attachments' content and
// no server keys, since deleting the cloud copy deletes its blobs.

// threadTranscript is a stored chat in the form the device keeps: every
// attachment's content inline and no server bookkeeping. It refuses while a
// reply streams or when any attachment cannot be read, so moving the chat
// never loses content.
func (h *harness) threadTranscript(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, err
	}
	row, err := h.storeRows.load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	thread := normalizeThread(row)
	if str(obj(thread["activeRun"])["runId"]) != "" {
		return nil, apiErr(409, "RUN_IN_FLIGHT", "Wait for the reply to finish before moving this chat")
	}
	missing := 0
	for _, item := range arr(thread["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			a := obj(value)
			if attachmentKey(a) == "" {
				continue
			}
			data, err := h.attachmentBytes(ctx, p, a, false)
			if err == nil && isImage(a) {
				a["base64"] = base64.StdEncoding.EncodeToString(data)
			} else if err == nil {
				var content object
				if content, err = decodeDocument(data); err == nil {
					for field, value := range content {
						a[field] = value
					}
				}
			}
			clear(data)
			if unreachable(err) {
				missing++
				continue
			}
			if err != nil {
				return nil, err
			}
			for _, field := range []string{"encryptionKey", "key", "storageThreadId", "pageCount"} {
				delete(a, field)
			}
		}
	}
	if missing > 0 {
		return nil, apiErr(409, "ATTACHMENTS_UNAVAILABLE", strconv.Itoa(missing)+" attachments in this chat could not be read")
	}
	for _, field := range append(serverThreadFields, "clock", "clockVersion", "writer") {
		delete(thread, field)
	}
	return object{"thread": thread}, nil
}

// handleThreadImport stores a chat from the device as a cloud chat under the
// same id. Its attachments are stored under the new chat first, as sync
// requires; the chat itself is a create, so an existing cloud chat is never
// overwritten.
func (h *harness) handleThreadImport(w http.ResponseWriter, r *http.Request) {
	in, err := decodeJSONWithin(w, r, maxTurnBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	p, err := h.auth.authenticate(r, false)
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
	row := localThread(id, obj(in["thread"]))
	if err := h.placeAttachments(ctx, p, id, row.Data); err != nil {
		return nil, err
	}
	stripInlineAttachments(row.Data)
	row.Data["updatedAt"] = timestamp()
	row.restore = true
	stampRow(row, "chat", nil)
	etag, err := h.storeRows.push(ctx, p, key, "chat", row, uuid.NewString())
	if e := asAPIError(err); err != nil && e.Code == "MISSING_ATTACHMENT" {
		if err := h.repairAttachments(ctx, p, row, e.missing); err != nil {
			return nil, err
		}
		etag, err = h.storeRows.push(ctx, p, key, "chat", row, uuid.NewString())
	}
	if err != nil {
		if asAPIError(err).Code == "REVISION_CONFLICT" {
			return nil, apiErr(409, "THREAD_EXISTS", "This chat is already in the cloud")
		}
		return nil, err
	}
	row.ETag = etag
	return threadSummary(normalizeThread(row)), nil
}
