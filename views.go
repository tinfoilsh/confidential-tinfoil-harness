package main

import (
	"cmp"
	"encoding/json"
	"time"
)

// What the API returns of a thread and its parts.

// threadSummary is a chat as its list shows it.
type threadSummary struct {
	ID               string        `json:"id"`
	Title            string        `json:"title"`
	TitleState       string        `json:"titleState"`
	CreatedAt        *string       `json:"createdAt"`
	UpdatedAt        *string       `json:"updatedAt"`
	ProjectID        *string       `json:"projectId"`
	Model            string        `json:"model"`
	WebSearchEnabled bool          `json:"webSearchEnabled"`
	Pinned           bool          `json:"pinned"`
	ContextUsage     *ContextUsage `json:"contextUsage"`
	ActiveRun        *ActiveRun    `json:"activeRun"`
	MessageCount     int           `json:"messageCount"`
	Flagged          bool          `json:"flagged,omitempty"`
}

func summarize(t *Thread) threadSummary {
	s := threadSummary{ID: t.ID, Title: t.Title, TitleState: t.TitleState, CreatedAt: present(t.CreatedAt), UpdatedAt: present(t.UpdatedAt), ProjectID: t.ProjectID, Model: t.Model,
		WebSearchEnabled: t.WebSearchEnabled, Pinned: t.Pinned, ContextUsage: t.ContextUsage, MessageCount: max(0, len(t.Messages)-t.HiddenCount), Flagged: t.Flagged}
	// The hint lapses with the run's deadline.
	if run := t.ActiveRun; run != nil {
		if expires, err := time.Parse(time.RFC3339Nano, run.ExpiresAt); err == nil && nowUTC().Before(expires) {
			s.ActiveRun = &ActiveRun{RunID: run.RunID, ExpiresAt: run.ExpiresAt}
		}
	}
	return s
}

// present is a text field a view shows as null when it is empty.
func present(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// threadWindow is a chat shown from its latest messages back.
type threadWindow struct {
	threadSummary
	Revision string        `json:"revision"`
	PresetID *string       `json:"presetId"`
	Messages []messageView `json:"messages"`
	HasOlder bool          `json:"hasOlder"`
}

// A chat is shown in windows of its latest messages.
const (
	messageWindow    = 50
	maxMessageWindow = 200
)

// window shows the messages before the one named, or the latest.
func window(row *threadRow, before string, limit int) threadWindow {
	t := &row.Thread
	all := t.Messages[min(len(t.Messages), t.HiddenCount):]
	end := len(all)
	if before != "" {
		for i, m := range all {
			if m.ID == before {
				end = i
				break
			}
		}
	}
	if limit <= 0 || limit > maxMessageWindow {
		limit = messageWindow
	}
	start := max(0, end-limit)
	messages := []messageView{}
	for i := range all[start:end] {
		messages = append(messages, viewMessage(&all[start+i]))
	}
	return threadWindow{threadSummary: summarize(t), Revision: row.etag, PresetID: t.PresetID, Messages: messages, HasOlder: start > 0}
}

// messageView is a message as a client shows it: a question's text or a
// reply's timeline, and its attachments without their content.
type messageView struct {
	ID               string           `json:"id"`
	Role             string           `json:"role"`
	CreatedAt        string           `json:"createdAt"`
	Quote            string           `json:"quote,omitempty"`
	Model            string           `json:"model,omitempty"`
	ModelDisplayName string           `json:"modelDisplayName,omitempty"`
	IsError          *bool            `json:"isError,omitempty"`
	IsInterrupted    *bool            `json:"isInterrupted,omitempty"`
	Usage            json.RawMessage  `json:"usage,omitempty"`
	Annotations      []Annotation     `json:"annotations,omitzero"`
	Timeline         []Block          `json:"timeline,omitzero"`
	Content          *string          `json:"content,omitempty"`
	Attachments      []attachmentView `json:"attachments,omitempty"`
}

func viewMessage(m *Message) messageView {
	view := messageView{ID: m.ID, Role: m.Role, CreatedAt: m.CreatedAt, Quote: m.Quote, Model: m.Model, ModelDisplayName: m.ModelDisplayName,
		IsError: m.IsError, IsInterrupted: m.IsInterrupted, Usage: m.Usage, Annotations: m.Annotations}
	if m.Role == "assistant" {
		view.Timeline = m.Timeline
		if view.Timeline == nil {
			view.Timeline = []Block{}
		}
	} else {
		view.Content = &m.Content
	}
	for i := range m.Attachments {
		view.Attachments = append(view.Attachments, viewAttachment(&m.Attachments[i]))
	}
	return view
}

// attachmentView is an attachment as a client lists it.
type attachmentView struct {
	ID          string  `json:"id"`
	Kind        string  `json:"kind"`
	FileName    *string `json:"fileName"`
	MimeType    *string `json:"mimeType"`
	Size        *int64  `json:"size"`
	Thumbnail   *string `json:"thumbnail"`
	Pages       *int    `json:"pages"`
	Status      string  `json:"status"`
	TextContent string  `json:"textContent,omitempty"`
	// A text-only model reads an image through this. Local clients keep it,
	// so later turns do not ask a vision model to describe the image again.
	Description string `json:"description,omitempty"`
}

func viewAttachment(a *Attachment) attachmentView {
	view := attachmentView{ID: a.ID, Kind: cmp.Or(a.Kind, a.Type), FileName: present(a.FileName), MimeType: present(a.MimeType), Size: a.FileSize, Thumbnail: present(a.ThumbnailBase64),
		Status: "ready", TextContent: a.TextContent, Description: a.Description}
	if len(a.Pages) > 0 {
		view.Pages = new(len(a.Pages))
	} else if a.PageCount > 0 {
		view.Pages = new(a.PageCount)
	}
	return view
}

// uploadedAttachment is an upload's descriptor as its client holds it until
// a turn sends it back: page images included, image bytes not.
type uploadedAttachment struct {
	ID          string         `json:"id"`
	Kind        string         `json:"kind"`
	FileName    string         `json:"fileName"`
	MimeType    string         `json:"mimeType"`
	Size        *int64         `json:"size"`
	Thumbnail   string         `json:"thumbnail,omitempty"`
	TextContent string         `json:"textContent,omitempty"`
	Description string         `json:"description,omitempty"`
	Pages       []DocumentPage `json:"pages,omitempty"`
}

func viewUpload(a *Attachment) uploadedAttachment {
	return uploadedAttachment{ID: a.ID, Kind: cmp.Or(a.Kind, a.Type), FileName: a.FileName, MimeType: a.MimeType, Size: a.FileSize, Thumbnail: a.ThumbnailBase64, TextContent: a.TextContent, Description: a.Description, Pages: a.Pages}
}

// deviceTranscript is a stored chat as its device keeps it, without the
// fields the harness owns.
type deviceTranscript struct {
	Thread
	ActiveRun    *ActiveRun    `json:"activeRun,omitempty"`
	ProjectID    *string       `json:"projectId,omitempty"`
	ContextUsage *ContextUsage `json:"contextUsage,omitempty"`
	HiddenCount  int           `json:"harnessHiddenCount,omitempty"`
}

// sharePayload is what a share link opens, sealed under its key: each
// attachment's thumbnail or text, and the key of its blob, which sync keeps
// for the share.
type sharePayload struct {
	V         int            `json:"v"`
	Title     string         `json:"title"`
	Messages  []shareMessage `json:"messages"`
	CreatedAt int64          `json:"createdAt"`
}

type shareMessage struct {
	Role             string            `json:"role"`
	Content          string            `json:"content"`
	Timestamp        int64             `json:"timestamp"`
	Timeline         []Block           `json:"timeline"`
	ModelDisplayName string            `json:"modelDisplayName,omitempty"`
	IsError          *bool             `json:"isError,omitempty"`
	Quote            string            `json:"quote,omitempty"`
	Attachments      []shareAttachment `json:"attachments,omitempty"`
}

type shareAttachment struct {
	ID              string `json:"id,omitempty"`
	Type            string `json:"type,omitempty"`
	FileName        string `json:"fileName,omitempty"`
	MimeType        string `json:"mimeType,omitempty"`
	ThumbnailBase64 string `json:"thumbnailBase64,omitempty"`
	EncryptionKey   string `json:"encryptionKey,omitempty"`
	TextContent     string `json:"textContent,omitempty"`
	Description     string `json:"description,omitempty"`
}

// sharedThread is an opened share, whose attachments name the key that
// reads each blob.
type sharedThread struct {
	Title     string          `json:"title"`
	CreatedAt *string         `json:"createdAt"`
	Messages  []sharedMessage `json:"messages"`
}

type sharedMessage struct {
	messageView
	Attachments []sharedAttachment `json:"attachments,omitempty"`
}

type sharedAttachment struct {
	attachmentView
	AttKey *string `json:"attKey"`
}

// statePatch is one change a run makes to its client's state.
type statePatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// runMetadata is what a run's last event reports.
type runMetadata struct {
	Messages  []committedReply `json:"messages,omitzero"`
	Thread    *Thread          `json:"thread,omitempty"`
	Usage     *usage           `json:"usage,omitempty"`
	RateLimit *rateLimit       `json:"rateLimit,omitempty"`
	Cancelled *bool            `json:"cancelled,omitempty"`
}

// committedReply is what a run commits to a reply beyond its streamed
// content, so a client that stores the thread itself stores the same.
type committedReply struct {
	ID               string          `json:"id"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	Usage            json.RawMessage `json:"usage,omitempty"`
	IsError          *bool           `json:"isError,omitempty"`
	IsInterrupted    *bool           `json:"isInterrupted,omitempty"`
	Annotations      []Annotation    `json:"annotations,omitempty"`
}

// queueItem is a turn waiting behind its chat's running reply.
type queueItem struct {
	QueueID     string             `json:"queueId"`
	Kind        string             `json:"kind"`
	Content     *string            `json:"content"`
	Quote       *string            `json:"quote"`
	Attachments []queuedAttachment `json:"attachments"`
	CreatedAt   string             `json:"createdAt"`
}

type queuedAttachment struct {
	ID        string  `json:"id,omitempty"`
	Kind      string  `json:"kind,omitempty"`
	FileName  string  `json:"fileName,omitempty"`
	MimeType  string  `json:"mimeType,omitempty"`
	Size      *int64  `json:"size,omitempty"`
	Thumbnail *string `json:"thumbnail,omitempty"`
}

// stateSnapshot is a run's chat, its queue, and the caller's quota.
type stateSnapshot struct {
	Thread    threadSummary `json:"thread"`
	Queue     []queueItem   `json:"queue"`
	RateLimit rateLimit     `json:"rateLimit"`
}

// sessionView is what a client opens the app with.
type sessionView struct {
	User            sessionUser     `json:"user"`
	RateLimit       rateLimit       `json:"rateLimit"`
	Models          []sessionModel  `json:"models"`
	Auto            autoView        `json:"auto"`
	DefaultModel    string          `json:"defaultModel"`
	Widgets         []string        `json:"widgets"`
	ProfileDefaults Profile         `json:"profileDefaults"`
	Composer        composerView    `json:"composer"`
	Presets         []preset        `json:"presets"`
	Features        map[string]bool `json:"features"`
}

type sessionUser struct {
	ID        string `json:"id"`
	Anonymous bool   `json:"anonymous"`
}

// autoView is how a client offers Auto.
type autoView struct {
	Multimodal bool                `json:"multimodal"`
	Default    string              `json:"default"`
	Levels     []intelligenceLevel `json:"levels"`
}
