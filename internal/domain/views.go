package domain

import (
	"encoding/json"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// What the API returns of a thread and its parts.

// ThreadSummary is a chat as its list shows it.
type ThreadSummary struct {
	ID               string        `json:"id"`
	Title            string        `json:"title"`
	TitleState       string        `json:"titleState"`
	CreatedAt        *string       `json:"createdAt"`
	UpdatedAt        *string       `json:"updatedAt"`
	ActivityAt       string        `json:"activityAt"`
	ProjectID        *string       `json:"projectId"`
	Model            string        `json:"model"`
	WebSearchEnabled bool          `json:"webSearchEnabled"`
	Pinned           bool          `json:"pinned"`
	ContextUsage     *ContextUsage `json:"contextUsage"`
	ActiveRun        *ActiveRun    `json:"activeRun"`
	MessageCount     int           `json:"messageCount"`
	Flagged          bool          `json:"flagged,omitempty"`
}

func Summarize(t *Thread) ThreadSummary {
	s := ThreadSummary{ID: t.ID, Title: t.Title, TitleState: t.TitleState, CreatedAt: Present(t.CreatedAt), UpdatedAt: Present(t.UpdatedAt), ProjectID: t.ProjectID, Model: t.Model,
		WebSearchEnabled: t.WebSearchEnabled, Pinned: t.Pinned, ContextUsage: t.ContextUsage, MessageCount: max(0, len(t.Messages)-t.HiddenCount), Flagged: t.Flagged}
	s.ActivityAt = t.activityAt()
	// The hint lapses with the run's deadline. One naming no instance was
	// stored by a process from before runs named theirs, whose runs ended
	// when it did.
	if run := t.ActiveRun; run != nil && run.Instance != "" {
		if expires, err := time.Parse(time.RFC3339Nano, run.ExpiresAt); err == nil && api.Now().Before(expires) {
			s.ActiveRun = &ActiveRun{RunID: run.RunID, ExpiresAt: run.ExpiresAt, Instance: run.Instance}
		}
	}
	return s
}

func (t *Thread) activityAt() string {
	latest, _ := time.Parse(time.RFC3339Nano, t.CreatedAt)
	for _, message := range t.Messages {
		if at, err := time.Parse(time.RFC3339Nano, message.CreatedAt); err == nil && at.After(latest) {
			latest = at
		}
	}
	if latest.IsZero() {
		return t.CreatedAt
	}
	return latest.Format(time.RFC3339Nano)
}

// Present is a text field a view shows as null when it is empty.
func Present(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ThreadWindow is a chat shown from its latest messages back.
type ThreadWindow struct {
	ThreadSummary
	Revision string        `json:"revision"`
	PresetID *string       `json:"presetId"`
	Messages []MessageView `json:"messages"`
	HasOlder bool          `json:"hasOlder"`
}

// A chat is shown in windows of its latest messages.
const (
	MessageWindow    = 50
	MaxMessageWindow = 200
)

// Window shows the messages before the one named, or the latest.
func Window(row *ThreadRow, before string, limit int) ThreadWindow {
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
	if limit <= 0 || limit > MaxMessageWindow {
		limit = MessageWindow
	}
	start := max(0, end-limit)
	messages := []MessageView{}
	for i := range all[start:end] {
		messages = append(messages, ViewMessage(&all[start+i]))
	}
	return ThreadWindow{ThreadSummary: Summarize(t), Revision: row.ETag, PresetID: t.PresetID, Messages: messages, HasOlder: start > 0}
}

// MessageView is a message as a client shows it: a question's text or a
// reply's timeline, and its attachments without their content.
type MessageView struct {
	ID               string          `json:"id"`
	Role             string          `json:"role"`
	CreatedAt        string          `json:"createdAt"`
	Quote            string          `json:"quote,omitempty"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	IsError          *bool           `json:"isError,omitempty"`
	IsInterrupted    *bool           `json:"isInterrupted,omitempty"`
	Usage            json.RawMessage `json:"usage,omitempty"`
	Annotations      []Annotation    `json:"annotations,omitzero"`
	Timeline         []Block         `json:"timeline,omitzero"`
	Content          *string         `json:"content,omitempty"`
	Attachments      []Attachment    `json:"attachments,omitempty"`
}

func ViewMessage(m *Message) MessageView {
	view := MessageView{ID: m.ID, Role: m.Role, CreatedAt: m.CreatedAt, Quote: m.Quote, Model: m.Model, ModelDisplayName: m.ModelDisplayName,
		IsError: m.IsError, IsInterrupted: m.IsInterrupted, Usage: m.Usage, Annotations: m.Annotations}
	if m.Role == "assistant" {
		view.Timeline = CheckedWidgets(m.Timeline)
		if view.Timeline == nil {
			view.Timeline = []Block{}
		}
	} else {
		view.Content = &m.Content
	}
	for i := range m.Attachments {
		view.Attachments = append(view.Attachments, ViewAttachment(&m.Attachments[i]))
	}
	return view
}

// ViewAttachment is an attachment as a client lists it: its metadata,
// thumbnail and any text it holds inline, without its blob's key or its
// bytes. A text-only model reads an image through its description.
func ViewAttachment(a *Attachment) Attachment {
	return Attachment{ID: a.ID, Kind: a.Kind, FileName: a.FileName, MimeType: a.MimeType, Size: a.Size, Thumbnail: a.Thumbnail, TextContent: a.TextContent, Description: a.Description}
}

// DeviceTranscript is a stored chat as its device keeps it, without the
// fields the harness owns.
type DeviceTranscript struct {
	Thread
	ActiveRun    *ActiveRun    `json:"activeRun,omitempty"`
	ProjectID    *string       `json:"projectId,omitempty"`
	ContextUsage *ContextUsage `json:"contextUsage,omitempty"`
	HiddenCount  int           `json:"harnessHiddenCount,omitempty"`
}

// SharePayload is what a share link opens, sealed under its key: each
// attachment's thumbnail or text, and the key of its blob, which sync keeps
// for the share.
type SharePayload struct {
	V         int            `json:"v"`
	Title     string         `json:"title"`
	Messages  []ShareMessage `json:"messages"`
	CreatedAt int64          `json:"createdAt"`
}

type ShareMessage struct {
	Role             string       `json:"role"`
	Content          string       `json:"content"`
	Timestamp        int64        `json:"timestamp"`
	Timeline         []Block      `json:"timeline"`
	ModelDisplayName string       `json:"modelDisplayName,omitempty"`
	IsError          *bool        `json:"isError,omitempty"`
	Quote            string       `json:"quote,omitempty"`
	Annotations      []Annotation `json:"annotations,omitempty"`
	Attachments      []Attachment `json:"attachments,omitempty"`
}

// SharedThread is an opened share, whose attachments name the key that
// reads each blob.
type SharedThread struct {
	Title     string          `json:"title"`
	CreatedAt *string         `json:"createdAt"`
	Messages  []SharedMessage `json:"messages"`
}

type SharedMessage struct {
	MessageView
	Attachments []SharedAttachment `json:"attachments,omitempty"`
}

type SharedAttachment struct {
	Attachment
	AttKey *string `json:"attKey"`
}

// StatePatch is one change a run makes to its client's state.
type StatePatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// RunMetadata is what a run's last event reports.
type RunMetadata struct {
	Messages  []CommittedReply `json:"messages,omitzero"`
	Thread    *Thread          `json:"thread,omitempty"`
	Usage     *api.Usage       `json:"usage,omitempty"`
	RateLimit *api.RateLimit   `json:"rateLimit,omitempty"`
	Cancelled *bool            `json:"cancelled,omitempty"`
}

// CommittedReply is what a run commits to a reply beyond its streamed
// content, so a client that stores the thread itself stores the same.
type CommittedReply struct {
	ID               string          `json:"id"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	Usage            json.RawMessage `json:"usage,omitempty"`
	IsError          *bool           `json:"isError,omitempty"`
	IsInterrupted    *bool           `json:"isInterrupted,omitempty"`
	Annotations      []Annotation    `json:"annotations,omitempty"`
}

// QueueItem is a turn waiting behind its chat's running reply.
type QueueItem struct {
	QueueID     string             `json:"queueId"`
	Kind        string             `json:"kind"`
	Content     *string            `json:"content"`
	Quote       *string            `json:"quote"`
	Attachments []QueuedAttachment `json:"attachments"`
	CreatedAt   string             `json:"createdAt"`
}

type QueuedAttachment struct {
	ID        string  `json:"id,omitempty"`
	Kind      string  `json:"kind,omitempty"`
	FileName  string  `json:"fileName,omitempty"`
	MimeType  string  `json:"mimeType,omitempty"`
	Size      *int64  `json:"size,omitempty"`
	Thumbnail *string `json:"thumbnail,omitempty"`
}

// StateSnapshot is a run's chat, its queue, and the caller's quota.
type StateSnapshot struct {
	Thread    ThreadSummary `json:"thread"`
	Queue     []QueueItem   `json:"queue"`
	RateLimit api.RateLimit `json:"rateLimit"`
}
