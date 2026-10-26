package attach

import (
	"slices"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// PageRequest names a page whose details the metadata service reads.
type PageRequest struct {
	URL string `json:"url"`
}

func (in *PageRequest) Validate() error {
	if strings.TrimSpace(in.URL) == "" {
		return api.Err(400, "BAD_REQUEST", "A page URL is required")
	}
	return nil
}

// speechRequest names a message to read aloud: one of a stored chat, named
// with the content key, or of a device chat its client sends. Text a client
// selected, or a shared chat's message, is sent as it is.
type speechRequest struct {
	domain.ThreadSource
	api.KeyRequest
	ThreadID  string  `json:"threadId"`
	MessageID string  `json:"messageId"`
	Text      *string `json:"text"`
}

func (in *speechRequest) Validate() error {
	if in.MessageID == "" {
		if strings.TrimSpace(domain.ValueOr(in.Text, "")) == "" {
			return api.Err(400, "BAD_REQUEST", "A message or text is required")
		}
		return nil
	}
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if err := api.RequireID("messageId", in.MessageID); err != nil {
		return err
	}
	return in.ThreadSource.Validate()
}

// attachmentRequest downloads an attachment of a stored chat, or one a share
// names by the key its link carries.
type attachmentRequest struct {
	api.KeyRequest
	ID          string                `json:"id"`
	ThreadID    string                `json:"threadId"`
	AttKey      string                `json:"attKey"`
	Type        string                `json:"type"`
	Kind        string                `json:"kind"`
	FileName    string                `json:"fileName"`
	MimeType    string                `json:"mimeType"`
	TextContent string                `json:"textContent"`
	Pages       []domain.DocumentPage `json:"pages"`
}

func (in *attachmentRequest) Validate() error {
	if err := api.RequireID("id", in.ID); err != nil {
		return err
	}
	if in.ThreadID != "" {
		return api.RequireID("threadId", in.ThreadID)
	}
	return nil
}

// Sent is an upload's descriptor as a turn sends it back, or the
// id alone of an attachment the thread already holds.
type Sent struct {
	ID          string                `json:"id"`
	Kind        string                `json:"kind,omitempty"`
	FileName    string                `json:"fileName,omitempty"`
	MimeType    string                `json:"mimeType,omitempty"`
	Size        *int64                `json:"size,omitempty"`
	Thumbnail   *string               `json:"thumbnail,omitempty"`
	TextContent string                `json:"textContent,omitempty"`
	Description string                `json:"description,omitempty"`
	Pages       []domain.DocumentPage `json:"pages,omitempty"`
	Base64      string                `json:"base64,omitempty"`
}

func (a *Sent) Validate() error {
	if err := api.RequireID("id", a.ID); err != nil {
		return err
	}
	if a.Kind != "" && !slices.Contains(domain.AttachmentKinds, a.Kind) {
		return api.Err(400, "BAD_REQUEST", "An attachment kind must be image, document, or audio")
	}
	if a.whole() {
		return a.fitsUpload()
	}
	return nil
}

// whole reports whether the turn sends the attachment's descriptor rather
// than naming one its thread holds.
func (a *Sent) whole() bool { return a.Kind != "" }
