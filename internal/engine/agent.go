package engine

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
)

func (eng *Engine) loop(ctx context.Context, out *Stream, req *Request, m *inference.Model) error {
	apiKey := auth.CallerKey(ctx)
	inRun := usagereporting.Context{
		ContextID:     req.usageID,
		RootRequestID: req.usageID,
		ParentService: "harness",
		APIKeyHash:    usagereporting.HashAPIKey(apiKey),
		Depth:         1,
	}
	billing := inRun
	billing.BillCustomerRequest = true
	ctx = auth.WithUsage(ctx, inRun)
	first := auth.WithUsage(ctx, billing)

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
	conversation, err := BudgetConversation(req, set.search != nil, defs)
	if err != nil {
		return err
	}
	refreshed := false
	for turn := 1; ; turn++ {
		// Past the cap the model answers from what it has, with no tools to call.
		tools := defs
		if turn > MaxTurns {
			tools = nil
			conversation = append(conversation, api.Raw(api.Object{"role": "system", "content": FinalAnswerInstruction}))
		}
		conversation, err = trimToolHistory(req, conversation, tools)
		if err != nil {
			return err
		}
		payload, err := body(req, conversation, tools)
		if err != nil {
			return err
		}
		turnCtx := ctx
		if turn == 1 {
			turnCtx = first
		}
		calls, answer, err := eng.turn(turnCtx, out, m, payload, set.Widgets)
		if err != nil && req.refreshAuth != nil && !refreshed && api.AsError(err).HTTP == http.StatusUnauthorized {
			// A 401 rejected this model request before processing. Replay its
			// exact body once, retaining the run ID and request billing flag.
			refreshed = true
			credential, refreshErr := req.refreshAuth(ctx)
			if refreshErr != nil {
				return refreshErr
			}
			inRun.APIKeyHash = usagereporting.HashAPIKey(string(credential.Key))
			ctx = auth.WithCallerKey(ctx, string(credential.Key))
			ctx = auth.WithUsage(ctx, inRun)
			turnCtx = ctx
			if turn == 1 {
				billing.APIKeyHash = inRun.APIKeyHash
				turnCtx = auth.WithUsage(ctx, billing)
			}
			calls, answer, err = eng.turn(turnCtx, out, m, payload, set.Widgets)
		}
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}
		if turn > MaxTurns {
			return api.Err(502, "MAX_TURNS", fmt.Sprintf("The model called tools for %d turns without answering", MaxTurns))
		}
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
		conversation = append(conversation, reply)
		conversation = append(conversation, execute(ctx, out, set, calls)...)
	}
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
	endpoint := m.Endpoint
	if endpoint == "" {
		endpoint = eng.Models.Gateway + "/v1/chat/completions"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := m.Client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("gateway: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, "", &api.Refusal{Status: resp.StatusCode, Detail: string(bytes.TrimSpace(detail)), RetryAfter: api.RetryAfter(resp.Header)}
	}

	out.mu.Lock()
	out.modelAccepted = true
	out.mu.Unlock()
	var calls []ToolCall
	ended := false
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
		if len(chunk.Usage) > 0 {
			out.Meter(chunk.Usage)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		if chunk.Choices[0].FinishReason != "" {
			ended = true
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
