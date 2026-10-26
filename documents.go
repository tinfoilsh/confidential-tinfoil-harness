package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"slices"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Since sync protocol 3 a stored document's blob holds its extracted content
// as JSON, and the chat row keeps only metadata and the blob key. Sync deletes
// any blob a chat stops referencing, so every attachment a row names must be
// stored under that row's chat. Every client reads and writes this format.
var enclaveAttachmentID = regexp.MustCompile(`^[0-9a-f]{36}$`)

// documentBlob is a document's extracted content, as its blob holds it.
type documentBlob struct {
	Pages       []DocumentPage `json:"pages,omitempty"`
	TextContent string         `json:"textContent,omitempty"`
}

// content is the extracted content an attachment holds inline.
func (a *Attachment) content() documentBlob {
	return documentBlob{TextContent: a.TextContent, Pages: a.Pages}
}

func (d documentBlob) empty() bool { return d.TextContent == "" && len(d.Pages) == 0 }

func encodeDocument(a *Attachment) ([]byte, error) {
	payload := documentBlob{TextContent: a.TextContent}
	for _, page := range a.Pages {
		image := page.Image
		if strings.HasPrefix(image, "data:") {
			_, image, _ = strings.Cut(image, ",")
		}
		payload.Pages = append(payload.Pages, DocumentPage{Page: max(0, page.Page), Text: page.Text, Image: image, IsScanned: page.IsScanned})
	}
	if payload.empty() {
		return nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The document has no extracted content")
	}
	return api.Raw(payload), nil
}

func decodeDocument(data []byte) (documentBlob, error) {
	if trimmed := bytes.TrimSpace(data); len(trimmed) > 0 && trimmed[0] == '{' {
		var payload struct {
			TextContent *string         `json:"textContent"`
			Pages       *[]DocumentPage `json:"pages"`
		}
		if json.Unmarshal(trimmed, &payload) == nil && (payload.TextContent != nil || payload.Pages != nil) {
			return documentBlob{TextContent: valueOr(payload.TextContent, ""), Pages: valueOr(payload.Pages, []DocumentPage{})}, nil
		}
	}
	return documentBlob{}, api.Err(422, "ATTACHMENT_UNSUPPORTED", "The stored document cannot be read")
}

func documentText(content documentBlob) string {
	if content.TextContent != "" {
		return content.TextContent
	}
	parts := []string{}
	for _, page := range content.Pages {
		if page.Text != "" {
			parts = append(parts, page.Text)
		}
	}
	return strings.Join(parts, "\n\n")
}

func unreachable(err error) bool {
	e := api.AsError(err)
	return e.HTTP == 404 || e.Status == 400 || e.Status == 404 || e.Code == "ATTACHMENT_UNSUPPORTED"
}

// placeAttachments stores every attachment of a thread under the thread, in
// the protocol 3 format, before the thread is pushed. It keeps the content
// inline in the given copy, which is the one a prompt is built from.
func (h *harness) placeAttachments(ctx context.Context, p *principal, threadID string, t *Thread) error {
	return t.eachAttachment(func(a *Attachment) error { return h.placeAttachment(ctx, p, threadID, a) })
}

func (h *harness) placeAttachment(ctx context.Context, p *principal, threadID string, a *Attachment) error {
	keyed := a.storageKey() != ""
	elsewhere := !enclaveAttachmentID.MatchString(a.ID) || cmp.Or(a.StorageThreadID, threadID) != threadID
	var data []byte
	switch {
	case !a.isImage() && a.hasContent():
		// A new upload, or a row the web or iOS app wrote before it stored
		// documents as blobs, keeps the content inline.
		payload, err := encodeDocument(a)
		if err != nil {
			return err
		}
		data = payload
	case a.isImage() && a.Base64 != "" && (!keyed || elsewhere):
		decoded, err := base64.StdEncoding.DecodeString(a.Base64)
		if err != nil {
			return api.Err(400, "ATTACHMENT_UNSUPPORTED", "An image is not valid base64")
		}
		data = decoded
	case keyed && elsewhere:
		stored, err := h.attachmentBytes(ctx, p, a, false)
		if err == nil && !a.isImage() {
			var content documentBlob
			content, err = decodeDocument(stored)
			clear(stored)
			if err == nil {
				stored, err = encodeDocument(&Attachment{TextContent: content.TextContent, Pages: content.Pages})
			}
		}
		if unreachable(err) {
			// Sync rejects a push naming an id that is not an enclave id, so
			// such a reference is retired now. Any other unavailable one fails
			// the push as missing, and repairAttachments then retires it.
			if !enclaveAttachmentID.MatchString(a.ID) {
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
	storedID, key, err := h.putAttachment(ctx, p, threadID, a.ID, data)
	if err != nil {
		return err
	}
	a.ID, a.EncryptionKey, a.StorageThreadID, a.Key = storedID, key, threadID, ""
	return nil
}

func unavailableDocument(a *Attachment) string {
	return "[The document " + cmp.Or(a.FileName, "attachment") + " is no longer available.]"
}

// hydrateDocuments reads offloaded documents into a working copy of a thread.
// A document whose blob is gone is described as unavailable rather than
// failing every later turn in its chat.
func (h *harness) hydrateDocuments(ctx context.Context, p *principal, t *Thread) error {
	return t.eachAttachment(func(a *Attachment) error {
		if a.isImage() || a.hasContent() {
			return nil
		}
		if a.storageKey() == "" {
			a.TextContent = unavailableDocument(a)
			return nil
		}
		if err := h.inflateDocument(ctx, p, a); err != nil {
			if !unreachable(err) {
				return err
			}
			a.TextContent = unavailableDocument(a)
		}
		return nil
	})
}

// inflateDocument reads a document's content from its blob into it.
func (h *harness) inflateDocument(ctx context.Context, p *principal, a *Attachment) error {
	data, err := h.attachmentBytes(ctx, p, a, false)
	if err != nil {
		return err
	}
	defer clear(data)
	content, err := decodeDocument(data)
	if err != nil {
		return err
	}
	a.TextContent, a.Pages = content.TextContent, content.Pages
	return nil
}

// repairAttachments makes a row pushable when sync names references it does
// not hold under the row's chat: a row the web or iOS app wrote before sync
// checked references may point at blobs filed under another chat or deleted
// since.
func (h *harness) repairAttachments(ctx context.Context, p *principal, t *Thread, missing []string) error {
	return t.eachAttachment(func(a *Attachment) error {
		if !slices.Contains(missing, a.ID) {
			return nil
		}
		a.StorageThreadID = "elsewhere"
		if err := h.placeAttachment(ctx, p, t.ID, a); err != nil {
			return err
		}
		if a.StorageThreadID != t.ID {
			retireAttachment(a)
		}
		return nil
	})
}

// An attachment without its blob keeps its metadata, and an image its
// thumbnail.
func retireAttachment(a *Attachment) {
	a.EncryptionKey, a.Key, a.StorageThreadID = "", "", ""
}

// stripInlineAttachments keeps only metadata and keys in a thread that is
// about to be stored. Content stays inline only for attachments without a
// blob.
func stripInlineAttachments(t *Thread) {
	t.eachAttachment(func(a *Attachment) error {
		a.inferenceMimeType = ""
		if a.storageKey() == "" {
			return nil
		}
		a.Base64 = ""
		if !a.isImage() {
			a.TextContent, a.Pages = "", nil
		}
		return nil
	})
}
