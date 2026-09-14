package main

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
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

const (
	maxTurns = 16
	// A tool result is bounded twice: what the model reads back, what the caller sees.
	maxToolOutput  = 30000
	maxShownOutput = 1 << 20
	maxSSEEvent    = 1 << 20
)

func (h *harness) loop(ctx context.Context, out *stream, req *request, m *model) error {
	apiKey := callerKey(ctx)
	inRun := usagereporting.Context{
		ContextID:     req.runID,
		RootRequestID: req.runID,
		ParentService: "harness",
		APIKeyHash:    usagereporting.HashAPIKey(apiKey),
		Depth:         1,
	}
	billing := inRun
	billing.BillCustomerRequest = true
	ctx = context.WithValue(ctx, usageContextKey{}, inRun)
	first := context.WithValue(ctx, usageContextKey{}, billing)

	set, err := openTools(ctx, req, out.progress)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	defer set.close()
	if req.managed {
		set.widgets = map[string]widget{}
		for _, offered := range req.rendered {
			declaration := obj(obj(decodeValue(offered))["function"])
			for _, w := range widgetCatalog() {
				if w.Name == str(declaration["name"]) {
					set.widgets[w.Name] = w
				}
			}
		}
		set.local = h.widgetResult
	}

	defs, err := offer(set, req.rendered)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	conversation := append(set.system(), req.messages...)
	if req.managed {
		conversation, err = budgetConversation(req, set, defs)
		if err != nil {
			return err
		}
	}
	refreshed := false
	for turn := 1; ; turn++ {
		if req.managed {
			conversation, err = trimToolHistory(req, conversation, defs)
			if err != nil {
				return err
			}
		}
		payload, err := body(req, m, conversation, defs)
		if err != nil {
			return err
		}
		turnCtx := ctx
		if turn == 1 {
			turnCtx = first
		}
		calls, answer, err := h.turn(turnCtx, out, m, payload)
		if err != nil && req.refreshAuth != nil && !refreshed && asAPIError(err).HTTP == http.StatusUnauthorized {
			// A 401 rejected this model request before processing. Replay its
			// exact body once, retaining the run ID and request billing flag.
			refreshed = true
			credential, refreshErr := req.refreshAuth(ctx)
			if refreshErr != nil {
				return refreshErr
			}
			inRun.APIKeyHash = usagereporting.HashAPIKey(string(credential.Key))
			ctx = context.WithValue(ctx, apiKeyKey{}, string(credential.Key))
			ctx = context.WithValue(ctx, usageContextKey{}, inRun)
			turnCtx = ctx
			if turn == 1 {
				billing.APIKeyHash = inRun.APIKeyHash
				turnCtx = context.WithValue(ctx, usageContextKey{}, billing)
			}
			calls, answer, err = h.turn(turnCtx, out, m, payload)
		}
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}
		if turn == maxTurns {
			return apiErr(502, "MAX_TURNS", fmt.Sprintf("The model called tools for %d turns without answering", maxTurns))
		}
		reply := requested(answer, calls)
		if req.managed && (req.policy == "all" || req.policy == "tool-call-only") {
			out.mu.Lock()
			reasoning := out.turnReasoning
			out.mu.Unlock()
			if reasoning != "" {
				value := obj(decodeValue(reply))
				value["reasoning_content"] = reasoning
				reply = raw(value)
			}
		}
		conversation = append(conversation, reply)
		conversation = append(conversation, execute(ctx, out, set, calls)...)
	}
}

func body(req *request, m *model, conversation []json.RawMessage, defs json.RawMessage) ([]byte, error) {
	payload := map[string]any{
		"model":  m.name,
		"stream": true,
		// Without this the stream carries no usage chunk and nothing is metered.
		"stream_options":    map[string]any{"include_usage": true},
		"messages":          conversation,
		"user_cache_secret": req.cacheScope,
	}
	if req.managed {
		payload["model"] = req.model
		for key, value := range req.params {
			payload[key] = value
		}
	}
	if len(defs) > 0 {
		payload["tools"] = defs
	}
	if req.piiCheck {
		payload["pii_check_options"] = map[string]any{}
	}
	return json.Marshal(payload)
}

// Order follows the family table, so a caller's turns keep hitting the prompt cache.
func offer(set *toolset, rendered []json.RawMessage) (json.RawMessage, error) {
	var defs []json.RawMessage
	for _, s := range set.sessions {
		defs = append(defs, s.defs...)
	}
	defs = append(defs, rendered...)
	if len(defs) == 0 {
		return nil, nil
	}
	return json.Marshal(defs)
}

// declaration refuses a caller tool that shadows one the run itself dials.
func declaration(raw json.RawMessage, dialled map[string]mcp.Meta) error {
	var decl struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &decl); err != nil {
		return fmt.Errorf("tool is not an object: %w", err)
	}
	if decl.Type != "function" || decl.Function.Name == "" {
		return errors.New(`each tool must be {"type":"function","function":{"name":...}}`)
	}
	for _, f := range families {
		if _, on := dialled[f.name]; !on {
			continue
		}
		for _, t := range f.tools {
			if decl.Function.Name == t.as {
				return fmt.Errorf("tool %q is served by this run and cannot be redeclared", t.as)
			}
		}
	}
	return nil
}

func execute(ctx context.Context, out *stream, set *toolset, calls []toolCall) []json.RawMessage {
	messages := make([]json.RawMessage, len(calls))
	var wg sync.WaitGroup
	for _, queue := range schedule(set, calls) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, i := range queue {
				messages[i] = invoke(ctx, out, set, calls[i])
			}
		}()
	}
	wg.Wait()
	return messages
}

// schedule gives serial families one queue in call order, everything else its own.
func schedule(set *toolset, calls []toolCall) [][]int {
	var queues [][]int
	held := map[*session]int{}
	for i, call := range calls {
		s := set.byName[call.name]
		if at, ok := held[s]; ok {
			queues[at] = append(queues[at], i)
			continue
		}
		if s != nil && s.fam.serial {
			held[s] = len(queues)
		}
		queues = append(queues, []int{i})
	}
	return queues
}

func invoke(ctx context.Context, out *stream, set *toolset, call toolCall) (message json.RawMessage) {
	// One tool goroutine's panic is that call's failure, read by the model like
	// any other. Nothing is emitted from here: a frame is what may have panicked.
	defer func() {
		if err := rescued("tool "+call.name, recover()); err != nil {
			failure, _ := json.Marshal(map[string]string{"error": err.Error()})
			message = answered(call.id, string(failure))
		}
	}()
	// A widget is drawn by the caller from TOOL_CALL_ARGS; the model just needs a result.
	if set.byName[call.name] == nil {
		if set.widgets != nil {
			w, ok := set.widgets[call.name]
			if !ok {
				return answered(call.id, `{"error":"unknown tool"}`)
			}
			var args any
			if json.Unmarshal([]byte(call.args), &args) != nil || validateSchema(w.Schema, args) != nil {
				out.result(call.id, `{"error":"invalid widget arguments"}`, nil)
				return answered(call.id, `{"error":"invalid widget arguments"}`)
			}
			content, err := set.local(ctx, call)
			if err != nil {
				content = object{"error": asAPIError(err).Message}
			}
			out.emit(event{Type: "TOOL_CALL_RESULT", MessageID: "msg_" + token(), ToolCallID: call.id, Role: "tool", Content: raw(content)})
			return answered(call.id, "[rendered for the user]")
		}
		out.result(call.id, "[rendered for the user]", nil)
		return answered(call.id, "[rendered for the user]")
	}
	out.activity(call)
	content, meta, err := set.call(ctx, call.name, call.id, json.RawMessage(call.args))
	if err != nil {
		// The model reads the failure and can retry rather than the turn dying.
		failure, _ := json.Marshal(map[string]string{"error": err.Error()})
		content, meta = string(failure), nil
	}
	// The caller's copy is clipped far later than the model's: present renders whole files.
	if set.widgets != nil {
		out.emit(event{Type: "TOOL_CALL_RESULT", MessageID: "msg_" + token(), ToolCallID: call.id, Role: "tool", Content: raw(normalizeToolResult(call, clip(content, maxShownOutput))), Metadata: meta})
	} else {
		out.result(call.id, clip(content, maxShownOutput), meta)
	}
	return answered(call.id, clip(content, maxToolOutput))
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

func (h *harness) turn(ctx context.Context, out *stream, m *model, payload []byte) ([]toolCall, string, error) {
	endpoint := m.endpoint
	if endpoint == "" {
		endpoint = h.gateway + "/v1/chat/completions"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("gateway: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, "", &refusal{status: resp.StatusCode, detail: string(bytes.TrimSpace(detail))}
	}

	out.mu.Lock()
	out.modelAccepted = true
	out.mu.Unlock()
	var calls []toolCall
	ended := false
	var answer, reasoningText strings.Builder
	message := "msg_" + token()
	if out.managed {
		out.mu.Lock()
		if out.replyID == "" {
			out.replyID = message
		}
		message = out.replyID
		out.mu.Unlock()
	}
	defer func() { out.mu.Lock(); out.turnReasoning = reasoningText.String(); out.mu.Unlock() }()
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
			if out.managed {
				return nil, answer.String(), apiErr(502, "UPSTREAM_REFUSED", "The model stream contains invalid JSON")
			}
			continue
		}
		if len(chunk.Error) > 0 && string(chunk.Error) != "null" {
			return nil, answer.String(), downstreamError(502, chunk.Error)
		}
		if chunk.Model != "" && chunk.Model != "auto" {
			out.mu.Lock()
			out.concreteModel = chunk.Model
			out.mu.Unlock()
		}
		if len(chunk.Usage) > 0 {
			out.meter(chunk.Usage)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		if chunk.Choices[0].FinishReason != "" {
			ended = true
		}
		delta := chunk.Choices[0].Delta
		if delta.Content != "" {
			answer.WriteString(delta.Content)
			out.text(message, delta.Content)
		}
		if reasoning := cmp.Or(delta.Reasoning, delta.ReasoningAlt); reasoning != "" {
			reasoningText.WriteString(reasoning)
			out.reasoning(message, reasoning)
		}
		for _, called := range delta.ToolCalls {
			if called.Index < 0 || called.Index >= 128 {
				return nil, answer.String(), apiErr(502, "UPSTREAM_REFUSED", "The model exceeded the tool call limit")
			}
			calls = announce(out, message, calls, called)
		}
	}
	for i := range calls {
		if !calls[i].open {
			out.callStart(calls[i].id, calls[i].name, parent(message, &answer))
		}
		// The enclave reads absent arguments as none; the caller should too.
		if calls[i].args == "" {
			out.callArgs(calls[i].id, "{}")
		}
		out.callEnd(calls[i].id)
	}
	if events.Err() != nil {
		return calls, answer.String(), events.Err()
	}
	if out.managed && !ended {
		return calls, answer.String(), apiErr(502, "UPSTREAM_REFUSED", "The model stream ended before completion")
	}
	return calls, answer.String(), nil
}

type toolCall struct {
	id, name, args string
	open           bool
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

func announce(out *stream, message string, calls []toolCall, delta toolCallDelta) []toolCall {
	for len(calls) <= delta.Index {
		calls = append(calls, toolCall{id: "call_" + token()})
	}
	call := &calls[delta.Index]
	if delta.ID != "" {
		call.id = delta.ID
	}
	call.name += delta.Function.Name
	call.args += delta.Function.Arguments
	if !call.open && call.name != "" {
		call.open = true
		out.callStart(call.id, call.name, message)
	}
	if delta.Function.Arguments != "" {
		out.callArgs(call.id, delta.Function.Arguments)
	}
	return calls
}

func requested(answer string, calls []toolCall) json.RawMessage {
	made := make([]toolCallJSON, len(calls))
	for i, call := range calls {
		made[i].ID, made[i].Type = call.id, "function"
		made[i].Function.Name, made[i].Function.Arguments = call.name, call.args
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

type refusal struct {
	status int
	detail string
}

func (r *refusal) Error() string { return fmt.Sprintf("gateway returned %d: %s", r.status, r.detail) }
