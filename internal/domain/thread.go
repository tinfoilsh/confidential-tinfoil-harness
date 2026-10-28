package domain

import (
	"cmp"
	"encoding/json"
	"slices"
)

// RecordVersion is the format of the rows and device records this harness
// writes. A change to what they hold changes it.
const RecordVersion = 1

// Thread is a chat in the thread format: what the harness stores in sync,
// what a device keeps, and what a device chat's run returns.
type Thread struct {
	ID               string        `json:"id"`
	Title            string        `json:"title"`
	TitleState       string        `json:"titleState"`
	CreatedAt        string        `json:"createdAt,omitempty"`
	UpdatedAt        string        `json:"updatedAt,omitempty"`
	Model            string        `json:"model"`
	ProjectID        *string       `json:"projectId"`
	PresetID         *string       `json:"presetId"`
	Pinned           bool          `json:"pinned"`
	WebSearchEnabled bool          `json:"webSearchEnabled"`
	Messages         []Message     `json:"messages"`
	LastRequestID    string        `json:"lastRequestId,omitempty"`
	LastRequestKind  string        `json:"lastRequestKind,omitempty"` // what that request's turn did, which a retry repeats
	ActiveRun        *ActiveRun    `json:"activeRun"`
	ContextUsage     *ContextUsage `json:"contextUsage,omitempty"`
	Flagged          bool          `json:"flagged,omitempty"` // the safeguards flagged the chat, which takes no more turns
	// HiddenCount is how many leading messages an ask's thread holds as
	// context without showing them.
	HiddenCount   int `json:"harnessHiddenCount,omitempty"`
	RecordVersion int `json:"recordVersion,omitempty"`
	// ImportedFrom is set on a stored chat an import made, so the same
	// import run again finds it. No client is sent it.
	ImportedFrom *ImportOrigin `json:"_restore,omitempty"`
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
// return is kept as it came.
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
	Display    *ToolDisplay      `json:"display,omitempty"`
	// StartedAt and EndedAt are when a reasoning or tool step ran, in epoch
	// milliseconds, and Duration is how long a reasoning step thought, in
	// seconds, as the apps have stored them.
	StartedAt float64 `json:"startedAt,omitempty"`
	EndedAt   float64 `json:"endedAt,omitempty"`
	Duration  float64 `json:"duration,omitempty"`
	// InvalidArguments marks a widget call whose arguments the widget's
	// current declaration refuses, so its client offers to regenerate them.
	// It is worked out again for whatever clients are sent, never taken
	// from a row.
	InvalidArguments bool `json:"invalidArguments,omitempty"`
}

// ToolDisplay is what a web search or page fetch shows its client, kept
// current while the call runs.
type ToolDisplay struct {
	Query   string          `json:"query,omitempty"`
	URL     string          `json:"url,omitempty"`
	Status  string          `json:"status"`
	Sources []DisplaySource `json:"sources"`
}

type DisplaySource struct {
	URL     string `json:"url"`
	Title   string `json:"title"`
	Snippet string `json:"snippet,omitempty"`
}

// SearchDisplay is what a web search or page fetch shows, read from its
// arguments and the result its client was sent: a call without a result is
// running, and one whose result is an error failed. Other calls show none.
func SearchDisplay(name, args string, result json.RawMessage) *ToolDisplay {
	if name != "web_search" && name != "web_fetch" {
		return nil
	}
	var arguments struct {
		Query string   `json:"query"`
		URL   string   `json:"url"`
		URLs  []string `json:"urls"`
	}
	readLenient([]byte(HistoryArgs(args)), &arguments)
	var shown struct {
		Query   string          `json:"query"`
		URL     string          `json:"url"`
		Error   json.RawMessage `json:"error"`
		Results []DisplaySource `json:"results"`
		Pages   []DisplaySource `json:"pages"`
	}
	readLenient(result, &shown)
	display := &ToolDisplay{Status: "completed", Sources: []DisplaySource{}}
	switch {
	case len(result) == 0 || string(result) == "null":
		display.Status = "running"
	case shown.Error != nil:
		display.Status = "failed"
	}
	if name == "web_search" {
		display.Query = cmp.Or(shown.Query, arguments.Query)
		display.Sources = append(display.Sources, shown.Results...)
		return display
	}
	display.URL = cmp.Or(shown.URL, arguments.URL)
	if display.URL == "" && len(arguments.URLs) > 0 {
		display.URL = arguments.URLs[0]
	}
	for _, page := range shown.Pages {
		display.Sources = append(display.Sources, DisplaySource{URL: page.URL, Title: page.Title})
	}
	return display
}

// Attachment is a file a message carries, in the one form an upload returns
// it, a turn sends it, a chat stores it and a view shows it. A stored chat
// keeps the key of its blob, with its content inline only while no blob
// holds it; a chat on a device keeps its content.
type Attachment struct {
	ID          string         `json:"id"`
	Kind        string         `json:"kind"` // image, document or audio
	FileName    string         `json:"fileName"`
	MimeType    string         `json:"mimeType"`
	Size        *int64         `json:"size,omitempty"`
	Thumbnail   string         `json:"thumbnail,omitempty"`
	TextContent string         `json:"textContent,omitempty"`
	Description string         `json:"description,omitempty"`
	Pages       []DocumentPage `json:"pages,omitempty"`
	// Base64 is an image's copy that models read.
	Base64 string `json:"base64,omitempty"`
	// EncryptionKey opens the attachment's blob. Sync reads it by this name
	// to copy a forked chat's blobs.
	EncryptionKey string `json:"encryptionKey,omitempty"`
	// Key is the control plane key of an image stored before sync existed.
	Key             string `json:"key,omitempty"`
	StorageThreadID string `json:"storageThreadId,omitempty"`

	// inferenceMimeType is the type of the copy a model reads, set while a
	// turn's prompt is built.
	InferenceMimeType string `json:"-"`
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
	Instance  string `json:"instance"` // the process holding the run
}

// ContextUsage is how much of the model's window the chat's last prompt filled.
type ContextUsage struct {
	UsedTokens  int     `json:"usedTokens"`
	LimitTokens int     `json:"limitTokens"`
	Percentage  float64 `json:"percentage"`
}

// MessageUsage is what a reply cost.
type MessageUsage struct {
	PromptTokens     int64 `json:"promptTokens"`
	CompletionTokens int64 `json:"completionTokens"`
}

// ThreadRow is a stored chat and the revision a write must match; a chat
// not yet stored has none.
type ThreadRow struct {
	Thread
	ETag string `json:"-"`
}

// Clone copies a thread deeply enough to change it apart from the original.
// Raw tool output is never changed in place, so copies share it.
func (t *Thread) Clone() Thread {
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

func (t *Thread) MessageIndex(id string) int {
	return slices.IndexFunc(t.Messages, func(m Message) bool { return m.ID == id })
}

// FindAttachment finds an attachment of the thread by id.
func (t *Thread) FindAttachment(id string) *Attachment {
	for i := range t.Messages {
		for j := range t.Messages[i].Attachments {
			if t.Messages[i].Attachments[j].ID == id {
				return &t.Messages[i].Attachments[j]
			}
		}
	}
	return nil
}

// EachAttachment visits every attachment of the thread in place.
func (t *Thread) EachAttachment(visit func(*Attachment) error) error {
	for i := range t.Messages {
		for j := range t.Messages[i].Attachments {
			if err := visit(&t.Messages[i].Attachments[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

// CallID is the id a tool call block answers to.
func (b *Block) CallID() string {
	if b.ToolCallID != "" {
		return b.ToolCallID
	}
	return b.ID
}

func (a *Attachment) IsImage() bool { return a.Kind == "image" }

// StorageKey opens the attachment's blob: a sync key, or a control plane
// key from before sync.
func (a *Attachment) StorageKey() string {
	if a.EncryptionKey != "" {
		return a.EncryptionKey
	}
	return a.Key
}

func (a *Attachment) HasContent() bool { return a.TextContent != "" || len(a.Pages) > 0 }

// ValueOr is what p points at, or fallback when p is nil.
func ValueOr[T any](p *T, fallback T) T {
	if p == nil {
		return fallback
	}
	return *p
}

// AttachmentKinds are the kinds of attachment a message carries.
var AttachmentKinds = []string{"image", "document", "audio"}
