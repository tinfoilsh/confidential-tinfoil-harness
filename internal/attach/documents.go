package attach

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Since sync protocol 3 a stored document's blob holds its extracted content
// as JSON, and the chat row keeps only metadata and the blob key. Sync deletes
// any blob a chat stops referencing, so every attachment a row names must be
// stored under that row's chat. Every client reads and writes this format.

// DocumentBlob is a document's extracted content, as its blob holds it.
type DocumentBlob struct {
	Pages       []domain.DocumentPage `json:"pages,omitempty"`
	TextContent string                `json:"textContent,omitempty"`
}

// content is the extracted content an attachment holds inline.
func Content(a *domain.Attachment) DocumentBlob {
	return DocumentBlob{TextContent: a.TextContent, Pages: a.Pages}
}

func (d DocumentBlob) empty() bool { return d.TextContent == "" && len(d.Pages) == 0 }

func encodeDocument(a *domain.Attachment) ([]byte, error) {
	payload := DocumentBlob{TextContent: a.TextContent}
	for _, page := range a.Pages {
		image := page.Image
		if strings.HasPrefix(image, "data:") {
			_, image, _ = strings.Cut(image, ",")
		}
		payload.Pages = append(payload.Pages, domain.DocumentPage{Page: max(0, page.Page), Text: page.Text, Image: image, IsScanned: page.IsScanned})
	}
	if payload.empty() {
		return nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The document has no extracted content")
	}
	return api.Raw(payload), nil
}

func DecodeDocument(data []byte) (DocumentBlob, error) {
	if trimmed := bytes.TrimSpace(data); len(trimmed) > 0 && trimmed[0] == '{' {
		var payload struct {
			TextContent *string                `json:"textContent"`
			Pages       *[]domain.DocumentPage `json:"pages"`
		}
		if json.Unmarshal(trimmed, &payload) == nil && (payload.TextContent != nil || payload.Pages != nil) {
			return DocumentBlob{TextContent: domain.ValueOr(payload.TextContent, ""), Pages: domain.ValueOr(payload.Pages, []domain.DocumentPage{})}, nil
		}
	}
	return DocumentBlob{}, api.Err(422, "ATTACHMENT_UNSUPPORTED", "The stored document cannot be read")
}

func DocumentText(content DocumentBlob) string {
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

// Unreachable is whether an attachment's content is gone or unusable: its
// blob is not held, its key does not open it, or what it holds cannot be
// read as its kind.
func Unreachable(err error) bool {
	e := api.AsError(err)
	return e.HTTP == 404 || e.Code == "ATTACHMENT_UNSUPPORTED"
}

// PlaceAttachments stores every attachment of a thread under the thread, in
// the protocol 3 format, before the thread is pushed. It keeps the content
// inline in the given copy, which is the one a prompt is built from.
func (at *Service) PlaceAttachments(ctx context.Context, p *auth.Principal, threadID string, t *domain.Thread) error {
	return t.EachAttachment(func(a *domain.Attachment) error { return at.placeAttachment(ctx, p, threadID, a) })
}

func (at *Service) placeAttachment(ctx context.Context, p *auth.Principal, threadID string, a *domain.Attachment) error {
	keyed := a.StorageKey() != ""
	elsewhere := !domain.EnclaveAttachmentID.MatchString(a.ID) || cmp.Or(a.StorageThreadID, threadID) != threadID
	var data []byte
	switch {
	case !a.IsImage() && a.HasContent():
		// A new upload, or a row the web or iOS app wrote before it stored
		// documents as blobs, keeps the content inline.
		payload, err := encodeDocument(a)
		if err != nil {
			return err
		}
		data = payload
	case a.IsImage() && a.Base64 != "" && (!keyed || elsewhere):
		decoded, err := base64.StdEncoding.DecodeString(a.Base64)
		if err != nil {
			return api.Err(400, "ATTACHMENT_UNSUPPORTED", "An image is not valid base64")
		}
		data = decoded
	case keyed && elsewhere:
		stored, err := at.Bytes(ctx, p, a)
		if err == nil && !a.IsImage() {
			var content DocumentBlob
			content, err = DecodeDocument(stored)
			clear(stored)
			if err == nil {
				stored, err = encodeDocument(&domain.Attachment{TextContent: content.TextContent, Pages: content.Pages})
			}
		}
		if Unreachable(err) {
			// Sync rejects a push naming an id that is not an enclave id, so
			// such a reference is retired now. Any other unavailable one fails
			// the push as missing, and RepairAttachments then retires it.
			if !domain.EnclaveAttachmentID.MatchString(a.ID) {
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
	storedID, key, err := at.Store.PutAttachment(ctx, p, threadID, a.ID, data)
	if err != nil {
		return err
	}
	a.ID, a.EncryptionKey, a.StorageThreadID, a.Key = storedID, key, threadID, ""
	return nil
}

func UnavailableDocument(a *domain.Attachment) string {
	return "[The document " + cmp.Or(a.FileName, "attachment") + " is no longer available.]"
}

// HydrateDocuments reads offloaded documents into a working copy of a thread.
// A document whose blob is gone is described as unavailable rather than
// failing every later turn in its chat.
func (at *Service) HydrateDocuments(ctx context.Context, p *auth.Principal, t *domain.Thread) error {
	return t.EachAttachment(func(a *domain.Attachment) error {
		if a.IsImage() || a.HasContent() {
			return nil
		}
		if a.StorageKey() == "" {
			a.TextContent = UnavailableDocument(a)
			return nil
		}
		if err := at.InflateDocument(ctx, p, a); err != nil {
			if !Unreachable(err) {
				return err
			}
			a.TextContent = UnavailableDocument(a)
		}
		return nil
	})
}

// InflateDocument reads a document's content from its blob into it.
func (at *Service) InflateDocument(ctx context.Context, p *auth.Principal, a *domain.Attachment) error {
	data, err := at.Bytes(ctx, p, a)
	if err != nil {
		return err
	}
	defer clear(data)
	content, err := DecodeDocument(data)
	if err != nil {
		return err
	}
	a.TextContent, a.Pages = content.TextContent, content.Pages
	return nil
}

// RepairAttachments makes a row pushable when sync names references it does
// not hold under the row's chat: a row the web or iOS app wrote before sync
// checked references may point at blobs filed under another chat or deleted
// since.
func (at *Service) RepairAttachments(ctx context.Context, p *auth.Principal, t *domain.Thread, missing []string) error {
	return t.EachAttachment(func(a *domain.Attachment) error {
		if !slices.Contains(missing, a.ID) {
			return nil
		}
		a.StorageThreadID = "elsewhere"
		if err := at.placeAttachment(ctx, p, t.ID, a); err != nil {
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
func retireAttachment(a *domain.Attachment) {
	a.EncryptionKey, a.Key, a.StorageThreadID = "", "", ""
}

// StripInlineAttachments keeps only metadata and keys in a thread that is
// about to be stored. Content stays inline only for attachments without a
// blob.
func StripInlineAttachments(t *domain.Thread) {
	t.EachAttachment(func(a *domain.Attachment) error {
		a.InferenceMimeType = ""
		if a.StorageKey() == "" {
			return nil
		}
		a.Base64 = ""
		if !a.IsImage() {
			a.TextContent, a.Pages = "", nil
		}
		return nil
	})
}
