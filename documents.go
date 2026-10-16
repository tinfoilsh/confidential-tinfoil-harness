package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

// Since sync protocol 3 a stored document's blob holds its extracted content
// as JSON, and the chat row keeps only metadata and the blob key. Sync deletes
// any blob a chat stops referencing, so every attachment a row names must be
// stored under that row's chat. Every client reads and writes this format.
const maxDocumentPayload = 48 << 20

var enclaveAttachmentID = regexp.MustCompile(`^[0-9a-f]{36}$`)

func attachmentKey(a object) string { return firstString(a["encryptionKey"], a["key"]) }
func isImage(a object) bool         { return firstString(a["type"], a["kind"]) == "image" }
func documentContent(a object) bool {
	return str(a["textContent"]) != "" || len(arr(a["pages"])) > 0
}

func encodeDocument(a object) ([]byte, error) {
	payload := object{}
	if text := str(a["textContent"]); text != "" {
		payload["textContent"] = text
	}
	pages := []any{}
	for _, item := range arr(a["pages"]) {
		page := obj(item)
		image := str(page["image"])
		if strings.HasPrefix(image, "data:") {
			_, image, _ = strings.Cut(image, ",")
		}
		pages = append(pages, object{"page": max(0, integer(page["page"])), "text": str(page["text"]), "image": image, "is_scanned": boolean(page["is_scanned"], false)})
	}
	if len(pages) > 0 {
		payload["pages"] = pages
	}
	if len(payload) == 0 {
		return nil, apiErr(415, "ATTACHMENT_UNSUPPORTED", "The document has no extracted content")
	}
	encoded := raw(payload)
	if len(encoded) > maxDocumentPayload {
		return nil, apiErr(413, "ATTACHMENT_TOO_LARGE", "The document exceeds the storage limit")
	}
	return encoded, nil
}

func decodeDocument(data []byte) (object, error) {
	if trimmed := bytes.TrimSpace(data); len(trimmed) > 0 && trimmed[0] == '{' {
		var payload struct {
			TextContent *string `json:"textContent"`
			Pages       *[]struct {
				Page      int    `json:"page"`
				Text      string `json:"text"`
				Image     string `json:"image"`
				IsScanned bool   `json:"is_scanned"`
			} `json:"pages"`
		}
		if json.Unmarshal(trimmed, &payload) == nil && (payload.TextContent != nil || payload.Pages != nil) {
			out := object{}
			if payload.TextContent != nil {
				out["textContent"] = *payload.TextContent
			}
			if payload.Pages != nil {
				pages := []any{}
				for _, page := range *payload.Pages {
					pages = append(pages, object{"page": page.Page, "text": page.Text, "image": page.Image, "is_scanned": page.IsScanned})
				}
				out["pages"] = pages
			}
			return out, nil
		}
	}
	return nil, apiErr(422, "ATTACHMENT_UNSUPPORTED", "The stored document cannot be read")
}

func documentText(content object) string {
	if text := str(content["textContent"]); text != "" {
		return text
	}
	parts := []string{}
	for _, item := range arr(content["pages"]) {
		if text := str(obj(item)["text"]); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n")
}

// The web client derives the same key, so either client retrying an upload of
// the same bytes to the same chat reuses one blob.
func attachmentIdempotencyKey(chatID, clientID string, data []byte) string {
	digest := sha256.Sum256(data)
	sum := sha256.Sum256([]byte("attachment:" + chatID + ":" + clientID + ":" + base64.StdEncoding.EncodeToString(digest[:])))
	return hex.EncodeToString(sum[:16])
}

func (h *harness) putAttachment(ctx context.Context, p *principal, chatID, clientID string, data []byte) (string, string, error) {
	in := object{"chat_id": chatID, "plaintext": base64.StdEncoding.EncodeToString(data), "idempotency_key": attachmentIdempotencyKey(chatID, clientID, data)}
	var stored object
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-ctx.Done():
				return "", "", ctx.Err()
			}
		}
		stored = nil
		// A purge of the same chat or a lost index write fails with a 502.
		// The idempotency key makes the retry store the same blob.
		if err = h.syncAPI.call(ctx, p, "/v1/attachment/put", in, &stored); err == nil || asAPIError(err).Status != 502 {
			break
		}
	}
	if err != nil {
		return "", "", err
	}
	id, key := str(stored["id"]), str(stored["att_key"])
	if !enclaveAttachmentID.MatchString(id) || key == "" {
		return "", "", apiErr(502, "UPSTREAM_REFUSED", "The sync service returned an invalid attachment")
	}
	return id, key, nil
}

func unreachable(err error) bool {
	e := asAPIError(err)
	return e.HTTP == 404 || e.Status == 400 || e.Status == 404 || e.Code == "ATTACHMENT_UNSUPPORTED"
}

// placeAttachments stores every attachment of a thread under the thread, in
// the protocol 3 format, before the thread is pushed. It keeps the content
// inline in the given copy, which is the one a prompt is built from.
func (h *harness) placeAttachments(ctx context.Context, p *principal, threadID string, thread object) error {
	for _, item := range arr(thread["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			if err := h.placeAttachment(ctx, p, threadID, obj(value)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *harness) placeAttachment(ctx context.Context, p *principal, threadID string, a object) error {
	id, keyed := str(a["id"]), attachmentKey(a) != ""
	elsewhere := !enclaveAttachmentID.MatchString(id) || firstString(a["storageThreadId"], threadID) != threadID
	var data []byte
	switch {
	case !isImage(a) && documentContent(a):
		// A new upload, or a row the web or iOS app wrote before it stored
		// documents as blobs, keeps the content inline.
		payload, err := encodeDocument(a)
		if err != nil {
			return err
		}
		data = payload
	case isImage(a) && str(a["base64"]) != "" && (!keyed || elsewhere):
		decoded, err := base64.StdEncoding.DecodeString(str(a["base64"]))
		if err != nil {
			return apiErr(400, "ATTACHMENT_UNSUPPORTED", "An image is not valid base64")
		}
		data = decoded
	case keyed && elsewhere:
		stored, err := h.attachmentBytes(ctx, p, a, false)
		if err == nil && !isImage(a) {
			var content object
			content, err = decodeDocument(stored)
			clear(stored)
			if err == nil {
				stored, err = encodeDocument(content)
			}
		}
		if unreachable(err) {
			// Sync rejects a push naming an id that is not an enclave id, so
			// such a reference is retired now. Any other unavailable one fails
			// the push as missing, and repairAttachments then retires it.
			if !enclaveAttachmentID.MatchString(id) {
				retireAttachment(a)
			}
			return nil
		}
		if err != nil {
			return err
		}
		data = stored
	default:
		return nil
	}
	defer clear(data)
	storedID, key, err := h.putAttachment(ctx, p, threadID, id, data)
	if err != nil {
		return err
	}
	if storedID != id {
		a["legacyId"] = id
	}
	a["id"], a["encryptionKey"], a["storageThreadId"] = storedID, key, threadID
	delete(a, "key")
	if pages := arr(a["pages"]); len(pages) > 0 {
		a["pageCount"] = len(pages)
	}
	return nil
}

func unavailableDocument(a object) string {
	return "[The document " + firstString(a["fileName"], "attachment") + " is no longer available.]"
}

// hydrateDocuments reads offloaded documents into a working copy of a thread.
// A document whose blob is gone is described as unavailable rather than
// failing every later turn in its chat.
func (h *harness) hydrateDocuments(ctx context.Context, p *principal, thread object) error {
	for _, item := range arr(thread["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			a := obj(value)
			if isImage(a) || documentContent(a) || attachmentKey(a) == "" {
				continue
			}
			if err := h.inflateDocument(ctx, p, a); err != nil {
				if !unreachable(err) {
					return err
				}
				a["textContent"] = unavailableDocument(a)
			}
		}
	}
	return nil
}

func (h *harness) inflateDocument(ctx context.Context, p *principal, a object) error {
	data, err := h.attachmentBytes(ctx, p, a, false)
	if err != nil {
		return err
	}
	defer clear(data)
	content, err := decodeDocument(data)
	if err != nil {
		return err
	}
	for field, value := range content {
		a[field] = value
	}
	return nil
}

// repairAttachments makes a row pushable when sync names references it does
// not hold under the row's chat: a row the web or iOS app wrote before sync
// checked references may point at blobs filed under another chat or deleted
// since.
func (h *harness) repairAttachments(ctx context.Context, p *principal, row *storedRow, missing []string) error {
	for _, item := range arr(row.Data["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			a := obj(value)
			if !containsString(missing, str(a["id"])) {
				continue
			}
			a["storageThreadId"] = "elsewhere"
			if err := h.placeAttachment(ctx, p, row.ID, a); err != nil {
				return err
			}
			if str(a["storageThreadId"]) != row.ID {
				retireAttachment(a)
			}
		}
	}
	return nil
}

// An image without its blob keeps its thumbnail. A document keeps a note in
// place of its content, since the web client refuses to push an empty one.
func retireAttachment(a object) {
	delete(a, "encryptionKey")
	delete(a, "key")
	delete(a, "storageThreadId")
	if !isImage(a) && !documentContent(a) {
		a["textContent"] = unavailableDocument(a)
	}
}

func containsString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

// stripInlineAttachments keeps only metadata and keys in a row that is about
// to be stored. Content stays inline only for attachments without a blob.
func stripInlineAttachments(thread object) {
	for _, message := range arr(thread["messages"]) {
		for _, value := range arr(obj(message)["attachments"]) {
			a := obj(value)
			for field, v := range a {
				if v == nil {
					delete(a, field)
				}
			}
			delete(a, "legacyId")
			delete(a, "inferenceMimeType")
			if attachmentKey(a) == "" {
				continue
			}
			delete(a, "base64")
			if !isImage(a) {
				delete(a, "textContent")
				delete(a, "pages")
			}
		}
	}
}

// keepDerivedAttachments gives a stored row the ids and attachments the
// harness derives from legacy message fields when reading it, so the blobs
// placed for them are referenced instead of being stored again every turn.
func keepDerivedAttachments(id string, thread object) {
	for i, item := range arr(thread["messages"]) {
		m := obj(item)
		if str(m["id"]) == "" {
			m["id"] = id + "-message-" + jsonNumber(i)
		}
		upgradeAttachments(m)
	}
}

func copyAttachmentDerivatives(source, target object) {
	byID := map[string]object{}
	for _, item := range arr(source["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			a := obj(value)
			byID[firstString(a["legacyId"], a["id"])] = a
		}
	}
	for _, item := range arr(target["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			a := obj(value)
			source := byID[str(a["id"])]
			if source == nil {
				continue
			}
			for _, field := range []string{"id", "description", "encryptionKey", "storageThreadId", "pageCount"} {
				if v, ok := source[field]; ok {
					a[field] = v
				}
			}
			if str(source["encryptionKey"]) != "" {
				delete(a, "key")
			}
		}
	}
}
