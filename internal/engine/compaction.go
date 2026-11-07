package engine

import (
	"cmp"
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
)

// A turn whose prompt would not fit the window compacts its chat: the
// messages from the chat's latest compaction marker on, up to the latest
// exchanges before the turn's question, are summarized into a new marker
// before the first question it keeps. The model reads the summary in their
// place, and the exchanges after it and the question as they were. A run
// compacts once, before its first model turn or between two of them, and
// its marker is kept with the turn even when the reply then fails. A compact
// turn compacts its chat the same way on request, its last question being
// the turn's, and asks the model nothing.

// recentTokens is how much of the chat before a compacted turn's question
// the model may go on reading as it was, so a follow-up on what was just
// said does not rest on the summary.
const recentTokens = 20000

// recentLimitShareTenths is the share of the prompt budget the kept
// exchanges may fill instead, when that is more, so a model with a larger
// window keeps more of them.
const recentLimitShareTenths = 1

// recentShareTenths is the share of the room left beside the summary that
// the kept exchanges may fill, so the turns after a compaction have room
// before the next one, and a summary longer than its reserve still fits.
const recentShareTenths = 5

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

// errNothingToCompactNow refuses a compact turn whose chat holds no message
// before its last question that a summary could replace.
func errNothingToCompactNow() error {
	return api.Err(400, "NOTHING_TO_COMPACT", "The chat holds nothing before its latest exchange to compact")
}

// lastQuestion is the index of the last question among messages, or -1.
func lastQuestion(messages []domain.Message) int {
	question := len(messages) - 1
	for question >= 0 && messages[question].Role != "user" {
		question--
	}
	return question
}

// compactable reports whether a compaction of messages has something to
// summarize: a message after the latest marker, before the last question.
func compactable(messages []domain.Message) bool {
	return lastQuestion(messages) > domain.LatestCompaction(messages)+1
}

// compactChat carries out a compact turn, whose prompt fills what a turn's
// would besides the chat.
func (eng *Engine) compactChat(ctx context.Context, r *Run, req *Request) error {
	return eng.compact(ctx, r, req, fixedCost(req, req.webSearch, api.Raw(req.rendered)))
}

// compact summarizes the messages of req before its latest exchanges into a
// marker, and reads the chat into req from that marker on. reserved is what
// the turn's prompt fills besides the chat: its system prompt, tools and
// reminder, and the run's exchange so far.
func (eng *Engine) compact(ctx context.Context, r *Run, req *Request, reserved int) error {
	messages := req.messages
	question := lastQuestion(messages)
	latest := domain.LatestCompaction(messages)
	if question <= latest+1 {
		return errNothingToCompact
	}
	room := req.limit() - reserved - summaryReserve
	for i := question; i < len(messages); i++ {
		room -= messageCost(req, &messages[i])
	}
	shown := slices.IndexFunc(messages, func(m domain.Message) bool { return m.ID == r.firstShown })
	if shown < 0 {
		shown = question
	}
	kept := keptFrom(req, latest, question, shown, min(max(recentTokens, req.limit()*recentLimitShareTenths/10), room*recentShareTenths/10))
	earlier := ""
	if latest >= 0 {
		earlier = messages[latest].Content
	}
	marker := domain.Message{ID: "msg_" + api.Token(), Role: domain.CompactionRole}
	before := messages[kept].ID
	r.out.emit(domain.CompactionStart{MessageID: marker.ID, BeforeMessageID: before})
	started := api.Now()
	var result summarize.Result
	err := errCompactionFailed()
	if eng.Summaries != nil {
		input := summarize.Input{Earlier: earlier, Documents: transcript(messages[latest+1:kept], req.loc)}
		result, err = eng.Summaries.Summarize(ctx, summarize.Compaction, input)
		// The caller pays for the summary, so a key that expired during the
		// run is renewed, as a model turn's is.
		var refused *inference.Refused
		if errors.As(err, &refused) && refused.Status == http.StatusUnauthorized && req.refreshAuth != nil {
			if credential, renewErr := req.refreshAuth(ctx, auth.Secret(auth.CallerKey(ctx))); renewErr == nil {
				result, err = eng.Summaries.Summarize(auth.Rekeyed(ctx, string(credential.Key)), summarize.Compaction, input)
			}
		}
		if err != nil {
			err = errCompactionFailed()
		}
	}
	if err != nil {
		r.out.emit(domain.CompactionEnd{MessageID: marker.ID, BeforeMessageID: before, Outcome: domain.CompactionFailed})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	covered := 0
	for _, m := range messages[:kept] {
		if m.Role != domain.CompactionRole {
			covered++
		}
	}
	marker.CreatedAt, marker.Content = api.Timestamp(), result.Text
	marker.Compaction = &domain.Compaction{Messages: covered, StartedAt: float64(started.UnixMilli()), EndedAt: float64(api.Now().UnixMilli()), Model: result.Model, Version: summarize.Compaction.Version}
	req.messages = slices.Insert(slices.Clone(messages), kept, marker)
	req.render()
	req.remind()
	r.compacted = &compaction{marker: marker, beforeID: before, usage: req.usage}
	view := domain.ViewMessage(&marker)
	r.out.emit(domain.CompactionEnd{MessageID: marker.ID, BeforeMessageID: before, Outcome: domain.CompactionCompleted, Marker: &view})
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	summary := domain.Summarize(&r.row.Thread)
	summary.ContextUsage = r.compacted.usage
	r.out.emit(domain.StateSnapshotEvent{Snapshot: domain.StateSnapshot{Thread: summary, Queue: queueItems(r.Thread), RateLimit: r.credential.Limit}})
	return nil
}

// keptFrom is the index of the first message of req a compaction keeps: the
// question that opens the latest whole exchanges before the turn's question
// that fill no more than budget, or else the turn's question. It leaves at
// least one message after the chat's latest marker for the summary, and
// keeps only messages from shown on, which the run's MESSAGES_SNAPSHOT
// showed, so a client can place the marker.
func keptFrom(req *Request, latest, question, shown, budget int) int {
	kept, cost := question, 0
	for i := question - 1; i >= max(latest+2, shown); i-- {
		if cost += messageCost(req, &req.messages[i]); cost > budget {
			break
		}
		if req.messages[i].Role == "user" {
			kept = i
		}
	}
	return kept
}

// messageCost is what a message of req fills of a prompt.
func messageCost(req *Request, m *domain.Message) int {
	cost := 0
	for _, message := range RenderMessage(m, req.vision, req.policy, req.loc) {
		cost += messageTokens(message)
	}
	return cost
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
// which page was read, or what a widget showed the user. A search or read
// that found nothing says so, so its sources are not taken as read.
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
	case b.Name == "web_search" && display != nil && display.Status != "completed":
		return `Tried to search the web for "` + display.Query + `", but ` + unfinished(display)
	case b.Name == "web_search" && display != nil:
		return `Searched the web for "` + display.Query + `". Results: ` + cmp.Or(sources(), "none")
	case b.Name == "web_fetch" && display != nil && display.Status != "completed":
		return "Tried to read " + display.URL + ", but " + unfinished(display)
	case b.Name == "web_fetch" && display != nil:
		return "Read " + cmp.Or(sources(), display.URL)
	case strings.HasPrefix(b.Name, "render_"):
		return "Showed the user the " + b.Name + " widget with " + domain.HistoryArgs(cmp.Or(b.Arguments, "{}"))
	}
	return "Called " + b.Name
}

// unfinished is how a search or read that returned nothing ended.
func unfinished(display *domain.ToolDisplay) string {
	if display.Status == "failed" {
		return "it failed"
	}
	return "it was stopped before it finished"
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
