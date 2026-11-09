package engine

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

const (
	MaxTurns = 16
	// A tool result is bounded twice: what the model reads back, what the caller sees.
	MaxToolOutput = 30000
	maxSSEEvent   = 1 << 22
	// maxToolCalls bounds the calls one model turn may open.
	maxToolCalls = 128

	FinalAnswerInstruction = "You have reached the maximum number of tool iterations. Do not call any more tools. Provide the best answer you can."
	ContinueInstruction    = "Continue your previous response exactly where it left off. Do not repeat, summarize, or restate anything you already wrote, and do not add any preamble."
	// ClearedResult is what the model reads in place of a tool result it
	// already answered, once the run's exchange outgrows the window.
	ClearedResult = "[This result was cleared to make room in the context window. Call the tool again if you need it.]"

	// lengthFinish is the finish reason of a model turn that filled the room
	// its window left it.
	lengthFinish = "length"
)

func (eng *Engine) loop(ctx context.Context, out *Stream, req *Request, m *inference.Model) error {
	c := newRunCaller(ctx, req)
	ctx = c.ctx

	set, err := openTools(ctx, req, m.Name, out.progress)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	defer set.close()
	set.Widgets = map[string]domain.Widget{}
	for _, w := range req.widgets {
		set.Widgets[w.Name] = w
	}
	set.Local = eng.widgetResult

	defs, err := offer(set, req.rendered)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	searching := set.search != nil
	conversation, err := fitted(ctx, req, searching, defs)
	if err != nil {
		return err
	}
	// The run's own exchange follows the history it starts from.
	history := len(conversation)
	for turn := 1; ; turn++ {
		// Past the cap the model answers from what it has, with no tools to call.
		tools := defs
		if turn > MaxTurns {
			tools = nil
			conversation = append(conversation, api.Raw(api.Object{"role": "system", "content": FinalAnswerInstruction}))
		}
		// An exchange that outgrows the window first drops the results the
		// model already answered, then compacts the history before it, if the
		// run has not yet.
		if !fits(req, conversation, tools) && (!clearAnswered(conversation[history:]) || !fits(req, conversation, tools)) {
			exchange := slices.Clone(conversation[history:])
			if err := compactHistory(ctx, req, fixedCost(req, searching, tools)+tokensOf(exchange)); err != nil {
				return err
			}
			conversation = append(promptOf(req, searching), exchange...)
			history = len(conversation) - len(exchange)
			if !fits(req, conversation, tools) {
				return errExchangeTooLarge()
			}
		}
		payload, err := body(req, conversation, tools)
		if err != nil {
			return err
		}
		calls, answer, err := eng.ask(c, out, m, payload, set.Widgets)
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}
		if turn > MaxTurns {
			return api.Err(502, "MAX_TURNS", fmt.Sprintf("The model called tools for %d turns without answering", MaxTurns))
		}
		conversation = append(conversation, replied(out, req, answer, calls))
		conversation = append(conversation, execute(ctx, out, set, calls)...)
	}
}

// runCaller is who a run's model turns are asked for: the caller whose key
// they carry, under the run's usage, and whose request the first turn is.
type runCaller struct {
	ctx   context.Context
	usage usagereporting.Context
	turns int
}

func newRunCaller(ctx context.Context, req *Request) *runCaller {
	usage := usagereporting.Context{
		ContextID:     req.usageID,
		RootRequestID: req.usageID,
		ParentService: "harness",
		Depth:         1,
	}
	return &runCaller{ctx: auth.WithUsage(ctx, usage), usage: usage}
}

// turnCtx is what the caller's next model turn is asked under: the first
// bills the caller's request.
func (c *runCaller) turnCtx() context.Context {
	if c.turns > 1 {
		return c.ctx
	}
	billing := c.usage
	billing.BillCustomerRequest = true
	return auth.WithUsage(c.ctx, billing)
}

// ask streams one model turn for the caller.
func (eng *Engine) ask(c *runCaller, out *Stream, m *inference.Model, payload []byte, widgets map[string]domain.Widget) ([]ToolCall, string, error) {
	c.turns++
	return eng.turn(c.turnCtx(), out, m, payload, widgets)
}

// replied is the message a model turn that called tools adds to its run's
// exchange: its text and calls, and its reasoning when the model's history
// policy replays it.
func replied(out *Stream, req *Request, answer string, calls []ToolCall) json.RawMessage {
	reply := requested(answer, calls)
	if req.policy == "all" || req.policy == "tool-call-only" {
		out.mu.Lock()
		reasoning, field := out.turnReasoning, out.reasoningField
		out.mu.Unlock()
		if reasoning != "" {
			// Echoed in the field the enclave streamed it in, since vLLM versions disagree on it.
			value := api.Obj(api.Parsed(reply))
			value[field] = reasoning
			reply = api.Raw(value)
		}
	}
	return reply
}

// compactHistory compacts the chat before a run's exchange to make room for
// it, once a run. reserved is what the turn's prompt fills besides the chat.
func compactHistory(ctx context.Context, req *Request, reserved int) error {
	compact := req.compact
	req.compact = nil
	if compact == nil {
		return errExchangeTooLarge()
	}
	if err := compact(ctx, reserved); errors.Is(err, errNothingToCompact) {
		return errExchangeTooLarge()
	} else if err != nil {
		return err
	}
	return nil
}

// tokensOf is what messages fill of a prompt.
func tokensOf(messages []json.RawMessage) int {
	tokens := 0
	for _, message := range messages {
		tokens += messageTokens(message)
	}
	return tokens
}

func errExchangeTooLarge() error {
	return api.Err(413, "CONTEXT_TOO_LARGE", "The current tool exchange exceeds the context window")
}

// clearAnswered replaces each tool result in a run's exchange that the model
// already answered, those before its latest turn, with ClearedResult, and
// reports whether it cleared any. A result no longer than ClearedResult, such
// as a widget's or one already cleared, is kept. The reply keeps every result
// it showed.
func clearAnswered(exchange []json.RawMessage) bool {
	latest := len(exchange) - 1
	for latest >= 0 && role(exchange[latest]) != "assistant" {
		latest--
	}
	cleared := false
	for i := range exchange[:max(0, latest)] {
		var m outMessage
		if json.Unmarshal(exchange[i], &m) != nil || m.Role != "tool" || len(m.Content) <= len(quote(ClearedResult)) {
			continue
		}
		exchange[i], cleared = answered(m.ToolCallID, ClearedResult), true
	}
	return cleared
}

func body(req *Request, conversation []json.RawMessage, defs json.RawMessage) ([]byte, error) {
	payload := map[string]any{
		"model":  req.model,
		"stream": true,
		// Without this the stream carries no usage chunk and nothing is metered.
		"stream_options":    map[string]any{"include_usage": true},
		"messages":          conversation,
		"user_cache_secret": req.cacheScope,
	}
	for key, value := range req.params {
		payload[key] = value
	}
	if len(defs) > 0 {
		payload["tools"] = defs
	}
	return json.Marshal(payload)
}

// Search tools come first, so a caller's turns keep hitting the prompt cache.
func offer(set *Toolset, rendered []json.RawMessage) (json.RawMessage, error) {
	var defs []json.RawMessage
	if set.search != nil {
		defs = append(defs, set.search.defs...)
	}
	defs = append(defs, rendered...)
	if len(defs) == 0 {
		return nil, nil
	}
	return json.Marshal(defs)
}

func execute(ctx context.Context, out *Stream, set *Toolset, calls []ToolCall) []json.RawMessage {
	messages := make([]json.RawMessage, len(calls))
	var wg sync.WaitGroup
	for i, call := range calls {
		wg.Go(func() { messages[i] = Invoke(ctx, out, set, call) })
	}
	wg.Wait()
	return messages
}

func Invoke(ctx context.Context, out *Stream, set *Toolset, call ToolCall) (message json.RawMessage) {
	// One tool goroutine's panic is that call's failure, read by the model like
	// any other. Nothing is emitted from here: a frame is what may have panicked.
	defer func() {
		if err := api.Rescued("tool "+call.Name, recover()); err != nil {
			failure, _ := json.Marshal(map[string]string{"error": err.Error()})
			message = answered(call.ID, string(failure))
		}
	}()
	// A widget is drawn by the caller from TOOL_CALL_ARGS; the model just needs a result.
	if !set.searches(call.Name) {
		w, ok := set.Widgets[call.Name]
		if !ok {
			return answered(call.ID, `{"error":"unknown tool"}`)
		}
		// The caller never reports back, so only the model can be told the widget was not drawn.
		if failure := domain.WidgetFailure(w, call.Args); failure != nil {
			out.emit(domain.ToolCallResult{MessageID: "msg_" + api.Token(), ToolCallID: call.ID, Role: "tool", Content: failure})
			if !set.retry(call.Name) {
				return answered(call.ID, `{"status":"executed"}`)
			}
			return answered(call.ID, string(failure))
		}
		content, err := set.Local(ctx, call)
		if err != nil {
			content = api.Object{"error": api.AsError(err).Message}
		}
		out.emit(domain.ToolCallResult{MessageID: "msg_" + api.Token(), ToolCallID: call.ID, Role: "tool", Content: api.Raw(content)})
		if call.Name == domain.QuestionsWidget {
			return answered(call.ID, domain.QuestionsShown)
		}
		return answered(call.ID, "[rendered for the user]")
	}
	out.activity(call)
	content, meta, err := set.call(ctx, call.Name, call.ID, json.RawMessage(call.Args))
	// The model reads search results as cited text, the caller as data.
	read, sources := modelCopy(call.Name, content)
	out.cite(sources)
	var shown any
	if err != nil {
		// The model reads the failure and can retry rather than the turn dying;
		// the caller learns only that the call failed, never the tool's own text.
		failure, _ := json.Marshal(map[string]string{"error": err.Error()})
		read, meta, shown = string(failure), nil, json.RawMessage(domain.ToolError)
	} else {
		shown = domain.NormalizeToolResult(call.Name, call.Args, content)
	}
	out.emit(domain.ToolCallResult{MessageID: "msg_" + api.Token(), ToolCallID: call.ID, Role: "tool", Content: api.Raw(shown), Metadata: meta, Display: domain.SearchDisplay(call.Name, call.Args, api.Raw(shown))})
	return answered(call.ID, clip(read, MaxToolOutput))
}

func clip(out string, limit int) string {
	if len(out) > limit {
		return out[:limit] + "\n[truncated]"
	}
	if out == "" {
		return "[no output]"
	}
	return out
}

func (eng *Engine) turn(ctx context.Context, out *Stream, m *inference.Model, payload []byte, widgets map[string]domain.Widget) ([]ToolCall, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := m.Client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("model request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, "", &api.Refusal{Status: resp.StatusCode, Detail: string(bytes.TrimSpace(detail)), RetryAfter: api.RetryAfter(resp.Header)}
	}

	out.mu.Lock()
	out.modelAccepted = true
	out.mu.Unlock()
	// The router has vLLM attach the usage so far to every chunk, so only
	// the last one a turn streams counts, once the turn ends.
	var usage json.RawMessage
	defer func() {
		if len(usage) > 0 {
			out.Meter(usage)
		}
	}()
	var calls []ToolCall
	ended, cutOff := false, false
	var answer, reasoningText strings.Builder
	reasoningField := "reasoning_content"
	message := "msg_" + api.Token()
	out.mu.Lock()
	if out.replyID == "" {
		out.replyID = message
	}
	message = out.replyID
	out.mu.Unlock()
	defer func() {
		out.mu.Lock()
		out.turnReasoning, out.reasoningField = reasoningText.String(), reasoningField
		out.mu.Unlock()
	}()
	events := bufio.NewScanner(resp.Body)
	events.Buffer(nil, maxSSEEvent)
	for events.Scan() {
		data, ok := strings.CutPrefix(events.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			ended = true
			break
		}
		if data == "" {
			continue
		}
		var chunk struct {
			Model   string          `json:"model"`
			Object  string          `json:"object"`
			Error   json.RawMessage `json:"error"`
			Choices []struct {
				FinishReason string `json:"finish_reason"`
				Delta        struct {
					Content string `json:"content"`
					// Enclaves disagree on the field because of diverging vLLM versions
					Reasoning    string          `json:"reasoning_content"`
					ReasoningAlt string          `json:"reasoning"`
					ToolCalls    []toolCallDelta `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
			Usage json.RawMessage `json:"usage"`
		}
		if json.Unmarshal([]byte(data), &chunk) != nil {
			return nil, answer.String(), api.Err(502, "UPSTREAM_REFUSED", "The model stream contains invalid JSON")
		}
		if len(chunk.Error) > 0 && string(chunk.Error) != "null" || chunk.Object == "error" {
			return nil, answer.String(), api.StreamError([]byte(data))
		}
		if chunk.Model != "" && chunk.Model != "auto" {
			out.mu.Lock()
			out.concreteModel = chunk.Model
			out.mu.Unlock()
		}
		if len(chunk.Usage) > 0 && string(chunk.Usage) != "null" {
			usage = chunk.Usage
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		if reason := chunk.Choices[0].FinishReason; reason != "" {
			ended, cutOff = true, reason == lengthFinish
		}
		delta := chunk.Choices[0].Delta
		// Reasoning first: at the think-close boundary one delta carries the last
		// of the reasoning and the first of the answer.
		if reasoning := cmp.Or(delta.Reasoning, delta.ReasoningAlt); reasoning != "" {
			if delta.Reasoning == "" {
				reasoningField = "reasoning"
			}
			reasoningText.WriteString(reasoning)
			out.reasoning(message, reasoning)
		}
		if delta.Content != "" {
			answer.WriteString(delta.Content)
			out.text(message, delta.Content)
		}
		for _, called := range delta.ToolCalls {
			if called.Index < 0 || called.Index >= maxToolCalls {
				return nil, answer.String(), api.Err(502, "UPSTREAM_REFUSED", "The model exceeded the tool call limit")
			}
			calls = announce(out, message, calls, called, widgets)
		}
	}
	// A cut stream leaves its calls open: TOOL_CALL_END would mark truncated arguments complete.
	if err := events.Err(); err != nil {
		return calls, answer.String(), err
	}
	if !ended {
		return calls, answer.String(), api.Err(502, "UPSTREAM_REFUSED", "The model stream ended before completion")
	}
	// A turn cut off at the end of the window ends the reply there. Its calls
	// stay open, as a cut stream's do, and none of them runs.
	if cutOff {
		out.mu.Lock()
		out.truncated = true
		out.mu.Unlock()
		return nil, answer.String(), nil
	}
	for i := range calls {
		c := &calls[i]
		shown := c.open && !c.held
		if !c.open {
			out.callStart(c.ID, c.Name, parent(message, &answer))
		}
		if c.held {
			c.Args = domain.WidgetArguments(c.Args)
		}
		// The enclave reads absent arguments as none; the caller should too.
		if c.Args == "" {
			out.callArgs(c.ID, "{}")
		} else if !shown {
			out.callArgs(c.ID, c.Args)
		}
		out.callEnd(c.ID)
	}
	return calls, answer.String(), nil
}

type ToolCall struct {
	ID, Name, Args string
	open           bool
	held           bool // a widget: its arguments reach the caller whole, once repaired
}

type toolCallDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// A turn that opens with a tool call has no message to hang it on.
func parent(message string, answer *strings.Builder) string {
	if answer.Len() == 0 {
		return ""
	}
	return message
}

// Arguments that arrive before the name are sent once the call opens.
func announce(out *Stream, message string, calls []ToolCall, delta toolCallDelta, widgets map[string]domain.Widget) []ToolCall {
	for len(calls) <= delta.Index {
		calls = append(calls, ToolCall{ID: "call_" + api.Token()})
	}
	call := &calls[delta.Index]
	if delta.ID != "" {
		call.ID = delta.ID
	}
	call.Name += delta.Function.Name
	call.Args += delta.Function.Arguments
	if !call.open && call.Name != "" {
		_, call.held = widgets[call.Name]
		call.open = true
		out.callStart(call.ID, call.Name, message)
		if !call.held && call.Args != "" {
			out.callArgs(call.ID, call.Args)
		}
	} else if call.open && !call.held && delta.Function.Arguments != "" {
		out.callArgs(call.ID, delta.Function.Arguments)
	}
	return calls
}

func requested(answer string, calls []ToolCall) json.RawMessage {
	made := make([]toolCallJSON, len(calls))
	for i, call := range calls {
		made[i].ID, made[i].Type = call.ID, "function"
		made[i].Function.Name, made[i].Function.Arguments = call.Name, domain.HistoryArgs(call.Args)
	}
	message, _ := json.Marshal(outMessage{Role: "assistant", Content: quote(answer), ToolCalls: made})
	return message
}

func answered(id, content string) json.RawMessage {
	message, _ := json.Marshal(outMessage{Role: "tool", ToolCallID: id, Content: quote(content)})
	return message
}

type toolCallJSON struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type outMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content,omitempty"`
	Name       string          `json:"name,omitempty"`
	ToolCalls  []toolCallJSON  `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}

func quote(s string) json.RawMessage {
	if s == "" {
		return nil
	}
	encoded, _ := json.Marshal(s)
	return encoded
}
