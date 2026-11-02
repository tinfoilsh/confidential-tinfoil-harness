package engine

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
)

// A turn whose prompt would not fit the window compacts its chat: every
// message before the turn's question, from the chat's latest compaction
// marker on, is summarized into a new marker before that question, which
// the model reads in their place. A run compacts once, before its first
// model turn or between two of them, and its marker is kept with the turn
// even when the reply then fails.

// compaction is what a run's compaction leaves: its marker, the question it
// sits before, and the context the turn's prompt fills after it.
type compaction struct {
	marker   domain.Message
	beforeID string
	usage    *domain.ContextUsage
}

// errNothingToCompact is a prompt too large whose chat holds no message
// before its question that a summary could replace.
var errNothingToCompact = errors.New("engine: nothing before the question to compact")

// errCompactionFailed refuses a turn whose chat could not be compacted.
func errCompactionFailed() error {
	return api.Err(502, "COMPACTION_FAILED", "The chat could not be compacted to fit the context window")
}

// compact summarizes the messages of req before its question into a marker,
// and reads the chat into req from that marker on.
func (eng *Engine) compact(ctx context.Context, r *Run, req *Request) error {
	messages := req.messages
	question := len(messages) - 1
	for question >= 0 && messages[question].Role != "user" {
		question--
	}
	latest := domain.LatestCompaction(messages)
	if question <= latest+1 {
		return errNothingToCompact
	}
	earlier := ""
	if latest >= 0 {
		earlier = messages[latest].Content
	}
	marker := domain.Message{ID: "msg_" + api.Token(), Role: domain.CompactionRole}
	before := messages[question].ID
	r.out.emit(domain.CompactionStart{MessageID: marker.ID, BeforeMessageID: before})
	started := api.Now()
	var result summarize.Result
	err := errCompactionFailed()
	if eng.Summaries != nil {
		if result, err = eng.Summaries.Summarize(ctx, summarize.Compaction, summarize.Input{Earlier: earlier, Documents: transcript(messages[latest+1:question], req.loc)}); err != nil {
			err = errCompactionFailed()
		}
	}
	if err != nil {
		r.out.emit(domain.CompactionEnd{MessageID: marker.ID, Outcome: domain.CompactionFailed})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	covered := 0
	for _, m := range messages[:question] {
		if m.Role != domain.CompactionRole {
			covered++
		}
	}
	marker.CreatedAt, marker.Content = api.Timestamp(), result.Text
	marker.Compaction = &domain.Compaction{Messages: covered, StartedAt: float64(started.UnixMilli()), EndedAt: float64(api.Now().UnixMilli()), Model: result.Model, Version: summarize.Compaction.Version}
	req.messages = slices.Insert(slices.Clone(messages), question, marker)
	req.render()
	r.compacted = &compaction{marker: marker, beforeID: before, usage: requestContextUsage(req)}
	view := domain.ViewMessage(&marker)
	r.out.emit(domain.CompactionEnd{MessageID: marker.ID, Outcome: domain.CompactionCompleted, Marker: &view})
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	summary := domain.Summarize(&r.row.Thread)
	summary.ContextUsage = r.compacted.usage
	r.out.emit(domain.StateSnapshotEvent{Snapshot: domain.StateSnapshot{Thread: summary, Queue: queueItems(r.Thread), RateLimit: r.credential.Limit}})
	return nil
}

// transcript is what a compaction reads of messages: each question and
// reply with when it was sent, the text of each file attached to a
// question, and what each tool call of a reply did. What a search or page
// returned is left out: the reply says what it drew from them.
func transcript(messages []domain.Message, loc *time.Location) []summarize.Document {
	var docs []summarize.Document
	for i := range messages {
		m := &messages[i]
		attrs := []summarize.Attr{{Name: "from", Value: m.Role}}
		if sent, err := time.Parse(time.RFC3339Nano, m.CreatedAt); err == nil {
			attrs = append(attrs, summarize.Attr{Name: "sent", Value: englishTime(sent.In(loc), loc.String())})
		}
		switch m.Role {
		case "user":
			docs = append(docs, summarize.Document{Kind: "message", Attrs: attrs, Text: questionText(m)})
			for j := range m.Attachments {
				docs = append(docs, attachmentDocument(&m.Attachments[j]))
			}
		case "assistant":
			if domain.ValueOr(m.IsError, false) {
				attrs = append(attrs, summarize.Attr{Name: "state", Value: "failed"})
			} else if domain.ValueOr(m.IsInterrupted, false) {
				attrs = append(attrs, summarize.Attr{Name: "state", Value: "stopped"})
			}
			docs = append(docs, summarize.Document{Kind: "message", Attrs: attrs, Text: replyText(m)})
		}
	}
	return docs
}

func questionText(m *domain.Message) string {
	parts := []string{}
	if len(m.Answers) > 0 {
		parts = append(parts, domain.AnswersPrompt(m.Answers))
	}
	if m.Quote != "" {
		parts = append(parts, "In reply to:\n> "+strings.ReplaceAll(m.Quote, "\n", "\n> "))
	}
	if m.Content != "" {
		parts = append(parts, m.Content)
	}
	return strings.Join(parts, "\n\n")
}

// replyText is a reply's text with a line for each tool call where it ran.
func replyText(m *domain.Message) string {
	var b strings.Builder
	wrote := false
	for _, block := range m.Timeline {
		switch block.Type {
		case "content":
			b.WriteString(block.Content)
			wrote = wrote || block.Content != ""
		case "tool_call":
			b.WriteString("\n[" + callLine(&block) + "]\n")
		}
	}
	if !wrote {
		return strings.TrimSpace(m.Content + "\n" + b.String())
	}
	return strings.TrimSpace(b.String())
}

// callLine says what a tool call did: what a search looked for and found,
// which page was read, or what a widget showed the user.
func callLine(b *domain.Block) string {
	display := b.Display
	if display == nil {
		display = domain.SearchDisplay(b.Name, b.Arguments, b.Result)
	}
	sources := func() string {
		named := []string{}
		for _, s := range display.Sources {
			named = append(named, cmp.Or(s.Title, s.URL)+" <"+s.URL+">")
		}
		return strings.Join(named, "; ")
	}
	switch {
	case b.Name == "web_search" && display != nil:
		return `Searched the web for "` + display.Query + `". Results: ` + cmp.Or(sources(), "none")
	case b.Name == "web_fetch" && display != nil:
		return "Read " + cmp.Or(sources(), display.URL)
	case strings.HasPrefix(b.Name, "render_"):
		return "Showed the user the " + b.Name + " widget with " + domain.HistoryArgs(cmp.Or(b.Arguments, "{}"))
	}
	return "Called " + b.Name
}

// attachmentDocument is a file attached to a question, as a compaction reads
// it: a document's text, or an image's description.
func attachmentDocument(a *domain.Attachment) summarize.Document {
	text := attach.DocumentText(attach.Content(a))
	if a.IsImage() {
		text = cmp.Or(a.Description, "[An image with no description.]")
	}
	return summarize.Document{Kind: "attachment", Attrs: []summarize.Attr{{Name: "name", Value: a.FileName}, {Name: "kind", Value: a.Kind}},
		Text: cmp.Or(text, "[No text was read from this file.]")}
}
