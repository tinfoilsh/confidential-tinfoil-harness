package main

import (
	"cmp"
	"encoding/json"
	"slices"
)

// Thread is a chat in the thread format: what the harness stores in sync,
// what a device keeps, and what a device chat's run returns.
type Thread struct {
	ID               string    `json:"id"`
	Title            string    `json:"title"`
	TitleState       string    `json:"titleState"`
	CreatedAt        string    `json:"createdAt,omitempty"`
	UpdatedAt        string    `json:"updatedAt,omitempty"`
	Model            string    `json:"model"`
	ProjectID        *string   `json:"projectId"`
	PresetID         *string   `json:"presetId"`
	Pinned           bool      `json:"pinned"`
	WebSearchEnabled bool      `json:"webSearchEnabled"`
	Messages         []Message `json:"messages"`
	LastRequestID    string    `json:"lastRequestId,omitempty"`
	// LastRequestKind is the kind of turn that request carried out, which a
	// retry repeats when its reply failed.
	LastRequestKind string        `json:"lastRequestKind,omitempty"`
	ActiveRun       *ActiveRun    `json:"activeRun"`
	ContextUsage    *ContextUsage `json:"contextUsage,omitempty"`
	// HiddenCount is how many leading messages an ask's thread holds as
	// context without showing them.
	HiddenCount int `json:"harnessHiddenCount,omitempty"`
}

// Message is one turn of a chat. Its pointer fields are absent unless set.
type Message struct {
	ID               string          `json:"id"`
	Role             string          `json:"role"`
	CreatedAt        string          `json:"createdAt"`
	Content          string          `json:"content"`
	Quote            string          `json:"quote,omitempty"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	Timeline         []Block         `json:"timeline,omitzero"`
	Attachments      []Attachment    `json:"attachments,omitzero"`
	Annotations      []Annotation    `json:"annotations,omitzero"`
	Usage            json.RawMessage `json:"usage,omitempty"`
	IsError          *bool           `json:"isError,omitempty"`
	IsInterrupted    *bool           `json:"isInterrupted,omitempty"`
}

// Block is one step of a reply: text, reasoning, or a tool call. What tools
// return and what a client resolves a widget with are kept as they came.
type Block struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	Content    string            `json:"content,omitempty"`
	ToolCallID string            `json:"toolCallId,omitempty"`
	Name       string            `json:"name,omitempty"`
	Arguments  string            `json:"arguments,omitempty"`
	Complete   bool              `json:"complete,omitempty"`
	Result     json.RawMessage   `json:"result,omitempty"`
	Metadata   json.RawMessage   `json:"metadata,omitempty"`
	Progress   []json.RawMessage `json:"progress,omitzero"`
	Activity   json.RawMessage   `json:"activity,omitempty"`
	ResolvedAt json.RawMessage   `json:"resolvedAt,omitempty"`
	Resolution json.RawMessage   `json:"resolution,omitempty"`
}

// Attachment is a file a message carries, in the form sync protocol 3
// stores it: metadata and the key of its blob, with its content inline
// only while no blob holds it.
type Attachment struct {
	ID              string         `json:"id"`
	Type            string         `json:"type,omitempty"`
	Kind            string         `json:"kind,omitempty"`
	FileName        string         `json:"fileName,omitempty"`
	MimeType        string         `json:"mimeType,omitempty"`
	FileSize        *int64         `json:"fileSize,omitempty"`
	ThumbnailBase64 string         `json:"thumbnailBase64,omitempty"`
	TextContent     string         `json:"textContent,omitempty"`
	Description     string         `json:"description,omitempty"`
	Pages           []DocumentPage `json:"pages,omitempty"`
	PageCount       int            `json:"pageCount,omitempty"`
	Base64          string         `json:"base64,omitempty"`
	EncryptionKey   string         `json:"encryptionKey,omitempty"`
	// Key is the control plane key of an image stored before sync existed.
	Key             string `json:"key,omitempty"`
	StorageThreadID string `json:"storageThreadId,omitempty"`

	// inferenceMimeType is the type of the copy a model reads, set while a
	// turn's prompt is built.
	inferenceMimeType string
}

// DocumentPage is a converted document page. Its fields are in the order
// a blob has always held them.
type DocumentPage struct {
	Image     string `json:"image"`
	IsScanned bool   `json:"is_scanned"`
	Page      int    `json:"page"`
	Text      string `json:"text"`
}

// Annotation marks a span of a reply as citing a page.
type Annotation struct {
	Type        string       `json:"type"`
	URLCitation *URLCitation `json:"url_citation,omitempty"`
}

type URLCitation struct {
	URL        string `json:"url"`
	Title      string `json:"title"`
	StartIndex int    `json:"start_index"`
	EndIndex   int    `json:"end_index"`
}

// ActiveRun tells clients a reply may be streaming, until the run's
// deadline. It never locks the chat: a run whose process died leaves it.
type ActiveRun struct {
	RunID     string `json:"runId"`
	ExpiresAt string `json:"expiresAt"`
}

// ContextUsage is how much of the model's window the chat's last prompt filled.
type ContextUsage struct {
	UsedTokens  int     `json:"usedTokens"`
	LimitTokens int     `json:"limitTokens"`
	Percentage  float64 `json:"percentage"`
}

// messageUsage is what a reply cost.
type messageUsage struct {
	PromptTokens     int64 `json:"promptTokens"`
	CompletionTokens int64 `json:"completionTokens"`
}

// threadRow is a stored chat and the revision a write must match; a chat
// not yet stored has none.
type threadRow struct {
	Thread
	etag string
	// restore lets a create reuse the id of a chat deleted from sync, as a
	// chat moved to the device and back does.
	restore bool
}

// clone copies a thread deeply enough to change it apart from the original.
// Raw tool output is never changed in place, so copies share it.
func (t *Thread) clone() Thread {
	copy := *t
	copy.Messages = make([]Message, len(t.Messages))
	for i := range t.Messages {
		copy.Messages[i] = t.Messages[i].clone()
	}
	if t.ActiveRun != nil {
		run := *t.ActiveRun
		copy.ActiveRun = &run
	}
	if t.ContextUsage != nil {
		usage := *t.ContextUsage
		copy.ContextUsage = &usage
	}
	return copy
}

func (m *Message) clone() Message {
	copy := *m
	copy.Timeline = clonedSlice(m.Timeline)
	for i := range copy.Timeline {
		copy.Timeline[i].Progress = clonedSlice(copy.Timeline[i].Progress)
	}
	copy.Attachments = clonedSlice(m.Attachments)
	for i := range copy.Attachments {
		copy.Attachments[i].Pages = clonedSlice(copy.Attachments[i].Pages)
	}
	copy.Annotations = clonedSlice(m.Annotations)
	return copy
}

// clonedSlice copies a slice, keeping nil and empty apart.
func clonedSlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	return append(make([]T, 0, len(s)), s...)
}

func (t *Thread) messageIndex(id string) int {
	return slices.IndexFunc(t.Messages, func(m Message) bool { return m.ID == id })
}

// attachment finds an attachment of the thread by id.
func (t *Thread) attachment(id string) *Attachment {
	for i := range t.Messages {
		for j := range t.Messages[i].Attachments {
			if t.Messages[i].Attachments[j].ID == id {
				return &t.Messages[i].Attachments[j]
			}
		}
	}
	return nil
}

// eachAttachment visits every attachment of the thread in place.
func (t *Thread) eachAttachment(visit func(*Attachment) error) error {
	for i := range t.Messages {
		for j := range t.Messages[i].Attachments {
			if err := visit(&t.Messages[i].Attachments[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

// callID is the id a tool call block answers to.
func (b *Block) callID() string {
	if b.ToolCallID != "" {
		return b.ToolCallID
	}
	return b.ID
}

func (a *Attachment) isImage() bool { return a.fileKind() == "image" }

// fileKind is whether an attachment is an image or a document, which old
// rows name in either field.
func (a *Attachment) fileKind() string { return cmp.Or(a.Type, a.Kind) }

// storageKey opens the attachment's blob: a sync key, or a control plane
// key from before sync.
func (a *Attachment) storageKey() string {
	if a.EncryptionKey != "" {
		return a.EncryptionKey
	}
	return a.Key
}

func (a *Attachment) hasContent() bool { return a.TextContent != "" || len(a.Pages) > 0 }

// valueOr is what p points at, or fallback when p is nil.
func valueOr[T any](p *T, fallback T) T {
	if p == nil {
		return fallback
	}
	return *p
}
