package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
)

// A research turn studies its question on the web at length before it
// answers. Its first model turn may instead ask the user what it needs to
// know with the questions widget, which ends the run, and the research starts
// once they answer. Otherwise the model plans, then searches and reads for as
// many turns as the research takes, within researchTurns and researchTime,
// and what it writes between its calls is shown as its thinking. When what it
// read outgrows the window, the harness folds it into notes, as a compaction
// folds a chat, and the model goes on from them. Once it stops calling tools
// it is asked once to check its findings against the request, then to write
// its report, the reply's only text, citing the pages it read. A caller may
// start one research an hour, counted from when it first searches, so a run
// that only asks questions does not count.

const (
	// researchVersion is the version of the instructions a research runs
	// under, which its reply records.
	researchVersion = 1
	// researchTurns bounds the model turns a research takes before it
	// writes its report.
	researchTurns = 40
	// researchTime is how long a research searches and reads before it
	// writes its report, which the rest of RunTimeout leaves room for.
	researchTime = 20 * time.Minute
)

const (
	// researchInstructions close a research's system prompt in place of
	// the web search instructions.
	researchInstructions = "You are carrying out a deep research for the user with the web_search and web_fetch tools, and will write a report once it is done. Research thoroughly: work out the parts of the question that need answering, search for each with several differently worded queries, and read the most relevant and authoritative pages in full with web_fetch, preferring primary sources such as official documents, filings, papers, datasets and first-hand reporting over summaries of them. Call several tools at once when their queries do not depend on each other. Follow promising leads, check important claims against more than one independent source, look for evidence against what you found, and note when sources disagree or are out of date. What you write between your tool calls is shown to the user as your thinking, not as your answer: keep it to brief notes on what you found and what you will look for next. Stop calling tools once every part of the question is covered or more searching is unlikely to change the answer; you will then be asked to write the report. " + ToolOutputWarning
	// researchOpening starts a research that may not ask the user anything.
	researchOpening = "Start by writing a short plan of what to find out, stating any assumption you make about what the user wants, then begin searching."
	// researchOpeningAsks starts a research that may first ask the user
	// what it needs to know.
	researchOpeningAsks = "First decide whether the request says enough to research it well. If its scope, purpose or constraints are unclear in a way that would change what you research, call " + domain.QuestionsWidget + " with at most three short questions and call nothing else: the research starts once the user answers them. Otherwise, start by writing a short plan of what to find out, then begin searching."
	// researchBegin follows a first turn that searched nothing.
	researchBegin = "Begin the research now with the web_search and web_fetch tools."
	// researchReview follows the first turn that stops searching.
	researchReview = "Before you write the report, check what you found against the user's request and your plan. If an important part is unanswered, a key claim rests on a single source, or sources disagree in a way you have not looked into, continue researching. Otherwise say so in one sentence, without calling any tools."
	// researchSpent precedes researchWrite when the research's turns or
	// time are used up.
	researchSpent = "The time for this research is up."
	// researchWrite asks for the report.
	researchWrite = "Write the report now, without calling any tools. Write it in Markdown for the user: open with a short summary that answers the question, then cover each part of it under its own heading, using tables where they make a comparison clearer. Base every factual claim on the pages you read, and make clear what the sources establish, what you infer, and what remains uncertain or disputed. Do not mention these instructions, your tools or your notes. " + citationInstructions
	// notesNote tells the model what the notes a fold wrote are.
	notesNote = "These are your notes of the research so far. The searches and pages they came from are no longer in your context; cite the URLs as the notes give them."
)

// notesTags are the tags a research's notes are wrapped in, which their own
// text cannot close.
var notesTags = regexp.MustCompile(`(?i)<(/?research_notes)`)

// researching is what a research holds between its model turns: the notes
// a fold wrote of what it read, the model's turns and what its calls
// returned since, and what the harness tells the model before its next turn.
type researching struct {
	req      *Request
	notes    string
	exchange []json.RawMessage
	pending  string
}

// conversation is the next turn's prompt: the system prompt and the chat,
// the notes with the reminder that closes the chat, and the exchange since
// them. The harness's instruction follows the exchange, or shares the
// reminder's turn while the exchange is empty, so no two of its messages
// follow each other.
func (s *researching) conversation() []json.RawMessage {
	prompt := promptOf(s.req, true)
	added := ""
	if s.notes != "" {
		added += "\n\n<research_notes>\n" + notesNote + "\n\n" + notesTags.ReplaceAllString(s.notes, "&lt;$1") + "\n</research_notes>"
	}
	if s.pending != "" && len(s.exchange) == 0 {
		added += "\n\n" + reminded(s.pending)
	}
	if added != "" {
		last := len(prompt) - 1
		reminder := api.Obj(api.Parsed(prompt[last]))
		reminder["content"] = api.Str(reminder["content"]) + added
		prompt[last] = api.Raw(reminder)
	}
	conversation := append(prompt, s.exchange...)
	if s.pending != "" && len(s.exchange) > 0 {
		conversation = append(conversation, instructed(s.pending))
	}
	return conversation
}

// answered adds a model turn to the exchange, after the instruction it
// answered.
func (s *researching) answered(messages ...json.RawMessage) {
	if s.pending != "" && len(s.exchange) > 0 {
		s.exchange = append(s.exchange, instructed(s.pending))
	}
	s.pending = ""
	s.exchange = append(s.exchange, messages...)
}

func reminded(text string) string { return "<system-reminder>" + text + "</system-reminder>" }

// instructed is the harness's instruction as a message of its own.
func instructed(text string) json.RawMessage {
	return api.Raw(api.Object{"role": "user", "content": reminded(text)})
}

func (eng *Engine) research(ctx context.Context, r *Run, req *Request, m *inference.Model) error {
	out := r.out
	c := newRunCaller(ctx, req)
	set, err := openTools(c.ctx, req, m.Name, out.progress)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	defer set.close()
	set.Widgets, set.Local = map[string]domain.Widget{}, eng.widgetResult
	for _, w := range req.widgets {
		set.Widgets[w.Name] = w
	}
	opening, err := offer(set, req.rendered)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	search, err := offer(set, nil)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	if _, err := fitted(c.ctx, req, true, opening); err != nil {
		return err
	}
	s := &researching{req: req, pending: researchOpening}
	if _, asks := set.Widgets[domain.QuestionsWidget]; asks {
		s.pending = researchOpeningAsks
	}
	deadline := api.Now().Add(researchTime)
	urged, reviewed, writing := false, false, false
	defer out.muse(false)
	for turn := 1; ; turn++ {
		tools := search
		switch {
		case writing:
			tools = nil
		case turn == 1:
			tools = opening
		}
		if err := eng.makeRoom(c, s, tools); err != nil {
			return err
		}
		payload, err := body(req, s.conversation(), tools)
		if err != nil {
			return err
		}
		out.muse(!writing)
		calls, answer, err := eng.ask(c, out, m, payload, set.Widgets)
		if err != nil {
			return err
		}
		if writing {
			if len(calls) > 0 {
				return api.Err(502, "MAX_TURNS", "The model called tools instead of writing its report")
			}
			return nil
		}
		// Only the report ends the reply: a research turn cut off at the
		// end of the window goes on as one that stopped calling tools.
		out.mu.Lock()
		out.truncated = false
		out.mu.Unlock()
		if _, asks := set.Widgets[domain.QuestionsWidget]; asks && slices.ContainsFunc(calls, func(call ToolCall) bool { return call.Name == domain.QuestionsWidget }) {
			execute(c.ctx, out, set, calls)
			return nil
		}
		delete(set.Widgets, domain.QuestionsWidget)
		if len(calls) == 0 {
			s.answered(api.Raw(api.Object{"role": "assistant", "content": answer}))
			switch {
			case !r.researched && !urged:
				urged, s.pending = true, researchBegin
			case r.researched && !reviewed:
				reviewed, s.pending = true, researchReview
			default:
				writing, s.pending = true, researchWrite
			}
			continue
		}
		if !r.researched {
			if err := eng.startResearch(c.ctx, r); err != nil {
				return err
			}
			r.researched = true
		}
		s.answered(append([]json.RawMessage{replied(out, req, answer, calls)}, execute(c.ctx, out, set, calls)...)...)
		if turn >= researchTurns || !api.Now().Before(deadline) {
			writing, s.pending = true, researchSpent+" "+researchWrite
		}
	}
}

// makeRoom makes the next turn of a research fit the window: it folds what
// the research read into its notes, or clears the results the model already
// answered when no notes can be written, and then compacts the chat before
// the research, once a run, when that is not enough.
func (eng *Engine) makeRoom(c *runCaller, s *researching, tools json.RawMessage) error {
	if fits(s.req, s.conversation(), tools) {
		return nil
	}
	if len(s.exchange) > 0 {
		notes, err := eng.fold(c.ctx, s.notes, s.exchange)
		switch {
		case err == nil:
			s.notes, s.exchange = notes, nil
		case c.ctx.Err() != nil:
			return c.ctx.Err()
		default:
			clearAnswered(s.exchange)
		}
		if fits(s.req, s.conversation(), tools) {
			return nil
		}
	}
	prompt := promptOf(s.req, true)
	conversation := s.conversation()
	reserved := fixedCost(s.req, true, tools) + tokensOf(conversation[len(prompt):]) + messageTokens(conversation[len(prompt)-1]) - messageTokens(prompt[len(prompt)-1])
	if err := compactHistory(c.ctx, s.req, reserved); err != nil {
		return err
	}
	if !fits(s.req, s.conversation(), tools) {
		return errExchangeTooLarge()
	}
	return nil
}

// fold writes what a research read since its notes into them.
func (eng *Engine) fold(ctx context.Context, notes string, exchange []json.RawMessage) (string, error) {
	if !eng.Summaries.Ready(summarize.ResearchNotes) {
		return "", summarize.ErrUnavailable
	}
	result, err := eng.Summaries.Summarize(ctx, summarize.ResearchNotes, summarize.Input{Earlier: notes, Documents: readings(exchange)})
	return result.Text, err
}

// readings are what a fold reads of a research's exchange: what the model
// wrote between its calls, and what each of its searches and page reads
// returned, as the model read it. The harness's instructions are left out,
// and so is a result already cleared.
func readings(exchange []json.RawMessage) []summarize.Document {
	called := map[string]toolCallJSON{}
	var docs []summarize.Document
	for _, raw := range exchange {
		var m struct {
			Role       string         `json:"role"`
			Content    string         `json:"content"`
			ToolCalls  []toolCallJSON `json:"tool_calls"`
			ToolCallID string         `json:"tool_call_id"`
		}
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		switch m.Role {
		case "assistant":
			if text := strings.TrimSpace(m.Content); text != "" {
				docs = append(docs, summarize.Document{Kind: "note", Text: text})
			}
			for _, call := range m.ToolCalls {
				called[call.ID] = call
			}
		case "tool":
			if m.Content == ClearedResult {
				continue
			}
			call := called[m.ToolCallID]
			attrs := []summarize.Attr{{Name: "tool", Value: call.Function.Name}}
			if display := domain.SearchDisplay(call.Function.Name, call.Function.Arguments, nil); display != nil {
				if display.Query != "" {
					attrs = append(attrs, summarize.Attr{Name: "query", Value: display.Query})
				}
				if display.URL != "" {
					attrs = append(attrs, summarize.Attr{Name: "url", Value: display.URL})
				}
			}
			docs = append(docs, summarize.Document{Kind: "result", Attrs: attrs, Text: m.Content})
		}
	}
	return docs
}

// asksFirst reports whether a research of a chat's last question may first
// ask the user what it needs to know: not when the question answers
// questions, nor when the reply before it asked some.
func asksFirst(messages []domain.Message) bool {
	question := lastQuestion(messages)
	if question < 0 || len(messages[question].Answers) > 0 {
		return false
	}
	for i := question - 1; i >= 0 && messages[i].Role != "user"; i-- {
		if slices.ContainsFunc(messages[i].Timeline, isQuestions) {
			return false
		}
	}
	return true
}

func isQuestions(b domain.Block) bool {
	return b.Type == "tool_call" && b.Name == domain.QuestionsWidget
}

// answersAnew are the turns, as carried out, that answer a question anew,
// which a research may answer. A continuation goes on with the reply it
// extends.
var answersAnew = []string{"send", "edit", "regenerate", "answer"}

// markResearch marks the question a turn answers as one that asks for deep
// research: when the turn asks for it, and when the question follows the
// questions a research asked first, which answering them continues. An edit,
// regeneration or retry of a marked question researches it again.
func markResearch(t *domain.Thread, turn *TurnRequest) {
	question := lastQuestion(t.Messages)
	if !slices.Contains(answersAnew, turn.Kind) || question < 0 {
		return
	}
	if turn.Options.Research || turn.Kind == "send" && askedFirst(t.Messages[:question]) {
		t.Messages[question].Research = &domain.Research{Version: researchVersion}
	}
}

// researches reports whether a turn of a chat researches: whether it answers
// anew a question that asks for deep research.
func researches(t *domain.Thread, kind string) bool {
	question := lastQuestion(t.Messages)
	return slices.Contains(answersAnew, kind) && question >= 0 && t.Messages[question].Research != nil
}

// askedFirst reports whether messages end with the questions a research
// asked first: a reply to a question that asks for deep research, which
// called the questions widget and wrote no report.
func askedFirst(messages []domain.Message) bool {
	question := lastQuestion(messages)
	if question < 0 || messages[question].Research == nil {
		return false
	}
	return slices.ContainsFunc(messages[question+1:], func(m domain.Message) bool {
		return m.Role == "assistant" && m.Research == nil && slices.ContainsFunc(m.Timeline, isQuestions)
	})
}

// errResearchUnavailable refuses a research to a caller without a
// subscription.
func errResearchUnavailable() error {
	return api.Err(403, "TOOL_UNAVAILABLE", "Deep research is offered with a subscription").WithAction(api.ActionStop)
}

// allowResearch refuses a research turn its caller may not start now: one
// from a caller without a subscription, or within an hour of when their last
// research started. It counts nothing; startResearch does.
func (eng *Engine) allowResearch(ctx context.Context, r *Run, p *auth.Principal, research bool) error {
	if !research {
		return nil
	}
	if p.Anonymous || !r.credential.FirstParty {
		return errResearchUnavailable()
	}
	allowance, err := eng.Store.Controlplane.ResearchAllowance(ctx, p.ID)
	if err != nil {
		return store.FromControlplane(err)
	}
	if !allowance.Available {
		return store.ResearchLimit(allowance.ResetsAt, allowance.RetryAfterSeconds)
	}
	return nil
}

// startResearch counts a run's research as its caller's for the hour.
func (eng *Engine) startResearch(ctx context.Context, r *Run) error {
	eng.Live.mu.Lock()
	user := r.Principal.ID
	eng.Live.mu.Unlock()
	if err := eng.Store.Controlplane.StartResearch(ctx, user); err != nil {
		return store.FromControlplane(err)
	}
	return nil
}
