package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxTurns      = 8
	maxToolOutput = 30000
	maxSSEEvent   = 1 << 20
)

func (h *harness) loop(ctx context.Context, out *stream, req *request, m *model) error {
	// No session when the caller turned search off: the enclave is dialled
	// because the model was offered its tools, so not offering them is what
	// leaves it undialled.
	var set *toolset
	if req.webSearch {
		opened, err := openTools(ctx, h.toolsEndpoint, h.toolsClient, out.progress)
		if err != nil {
			return fmt.Errorf("tools: %w", err)
		}
		defer opened.close()
		set = opened
	}
	defs, err := offer(set, req.rendered)
	if err != nil {
		return err
	}

	conversation := slices.Clone(req.messages)
	for turn := 1; ; turn++ {
		payload, err := body(req, m, conversation, defs)
		if err != nil {
			return err
		}
		calls, answer, err := h.turn(ctx, out, m, payload)
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}
		if turn == maxTurns {
			return fmt.Errorf("model called tools for %d turns without answering", maxTurns)
		}
		conversation = append(conversation, requested(answer, calls))
		conversation = append(conversation, execute(ctx, out, set, calls)...)
	}
}

func body(req *request, m *model, conversation []json.RawMessage, defs json.RawMessage) ([]byte, error) {
	payload := map[string]any{
		"model":  m.name,
		"stream": true,
		// Without this the stream carries no usage chunk, so the run reports
		// no tokens and the gateway meters the request as costing nothing.
		"stream_options":    map[string]any{"include_usage": true},
		"messages":          conversation,
		"user_cache_secret": req.cacheScope,
	}
	if len(defs) > 0 {
		payload["tools"] = defs
	}
	if req.piiCheck {
		payload["pii_check_options"] = map[string]any{}
	}
	return json.Marshal(payload)
}

// offer is what the model sees: the tools this run may dial, then the ones the
// caller draws. Order follows the enclave's so a caller's turns keep hitting
// the same prompt cache.
func offer(set *toolset, rendered []json.RawMessage) (json.RawMessage, error) {
	var defs []json.RawMessage
	if set != nil {
		if err := json.Unmarshal(set.defs, &defs); err != nil {
			return nil, fmt.Errorf("enclave published unreadable tool defs: %w", err)
		}
	}
	defs = append(defs, rendered...)
	if len(defs) == 0 {
		return nil, nil
	}
	return json.Marshal(defs)
}

// declaration checks a caller tool is a function declaration, and that it does
// not claim a name the loop attests -- a widget must not shadow web_search.
func declaration(raw json.RawMessage) error {
	var tool struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &tool); err != nil {
		return fmt.Errorf("tool is not an object: %w", err)
	}
	if tool.Type != "function" || tool.Function.Name == "" {
		return errors.New(`each tool must be {"type":"function","function":{"name":...}}`)
	}
	for _, as := range exposed {
		if tool.Function.Name == as {
			return fmt.Errorf("tool %q is served by this agent and cannot be redeclared", as)
		}
	}
	return nil
}

func execute(ctx context.Context, out *stream, set *toolset, calls []toolCall) []json.RawMessage {
	messages := make([]json.RawMessage, len(calls))
	var wg sync.WaitGroup
	for i, call := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			messages[i] = invoke(ctx, out, set, call)
		}()
	}
	wg.Wait()
	return messages
}

func invoke(ctx context.Context, out *stream, set *toolset, call toolCall) json.RawMessage {
	// The caller already has everything it needs from TOOL_CALL_ARGS, and the
	// model needs a result to keep going, so the widget is answered here.
	// Nothing is streamed back: there is no output to show.
	if !attested(set, call.name) {
		return answered(call.id, "[rendered for the user]")
	}
	out.activity(call)
	content, meta, err := set.call(ctx, call.name, call.id, json.RawMessage(call.args))
	if err != nil {
		// The model reads the failure and can retry rather than the turn dying.
		failure, _ := json.Marshal(map[string]string{"error": err.Error()})
		content, meta = string(failure), nil
	}
	content = clip(content)
	out.result(call.id, content, meta)
	return answered(call.id, content)
}

// attested reports whether the loop may dial this tool itself.
func attested(set *toolset, name string) bool {
	if set == nil {
		return false
	}
	for _, as := range exposed {
		if as == name {
			return true
		}
	}
	return false
}

func clip(out string) string {
	if len(out) > maxToolOutput {
		return out[:maxToolOutput] + "\n[truncated]"
	}
	if out == "" {
		return "[no output]"
	}
	return out
}

func (h *harness) turn(ctx context.Context, out *stream, m *model, payload []byte) ([]toolCall, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.gateway+"/v1/chat/completions", bytes.NewReader(payload))
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

	var calls []toolCall
	var answer strings.Builder
	message := "msg_" + token()
	events := bufio.NewScanner(resp.Body)
	events.Buffer(nil, maxSSEEvent)
	for events.Scan() {
		data, ok := strings.CutPrefix(events.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "" || data == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   string          `json:"content"`
					Reasoning string          `json:"reasoning_content"`
					ToolCalls []toolCallDelta `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
			Usage json.RawMessage `json:"usage"`
		}
		if json.Unmarshal([]byte(data), &chunk) != nil {
			continue
		}
		if len(chunk.Usage) > 0 {
			out.meter(chunk.Usage)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		delta := chunk.Choices[0].Delta
		if delta.Content != "" {
			answer.WriteString(delta.Content)
			out.text(message, delta.Content)
		}
		if delta.Reasoning != "" {
			out.reasoning(message, delta.Reasoning)
		}
		for _, called := range delta.ToolCalls {
			calls = announce(out, parent(message, &answer), calls, called)
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
	return calls, answer.String(), events.Err()
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

// parent names the message a tool call belongs to, and only once the caller
// has seen one: a turn that opens with a call has no message to hang it on.
func parent(message string, answer *strings.Builder) string {
	if answer.Len() == 0 {
		return ""
	}
	return message
}

// announce accumulates one tool-call delta and streams it on. Calls are
// addressed by id rather than by position, so parallel ones may interleave. A
// call the model left unnamed still gets an id, so its result correlates.
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

// exposed maps the enclave's tool names to what the model is offered; a tool
// this map does not name is never advertised.
var exposed = map[string]string{"search": "web_search", "fetch": "web_fetch"}

type toolset struct {
	session *mcp.ClientSession
	defs    json.RawMessage
}

func openTools(ctx context.Context, endpoint string, client *http.Client, progress func(string, string, float64)) (*toolset, error) {
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "confidential-tinfoil-harness", Version: "2"}, &mcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			call, _ := req.Params.ProgressToken.(string)
			progress(call, req.Params.Message, req.Params.Progress)
		},
	})
	session, err := mcpClient.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: client}, nil)
	if err != nil {
		return nil, err
	}
	found, err := session.ListTools(ctx, nil)
	if err != nil {
		session.Close()
		return nil, err
	}
	defs, err := advertise(found.Tools)
	if err != nil {
		session.Close()
		return nil, err
	}
	return &toolset{session: session, defs: defs}, nil
}

// Order follows the enclave's, so one caller's turns keep hitting the same
// prompt cache.
func advertise(found []*mcp.Tool) (json.RawMessage, error) {
	defs := []any{}
	for _, tool := range found {
		as, ok := exposed[tool.Name]
		if !ok {
			continue
		}
		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("tool %q published an unreadable schema: %w", tool.Name, err)
		}
		// json.RawMessage, not the []byte json.Marshal returned: a []byte in a
		// map marshals as base64, which the enclave rejects as a non-object
		// parameters field.
		defs = append(defs, map[string]any{"type": "function", "function": map[string]any{
			"name": as, "description": tool.Description, "parameters": json.RawMessage(schema)}})
	}
	if len(defs) == 0 {
		return nil, errors.New("enclave serves neither search nor fetch")
	}
	return json.Marshal(defs)
}

func (t *toolset) call(ctx context.Context, name, token string, args json.RawMessage) (string, json.RawMessage, error) {
	var remote string
	for enclaves, as := range exposed {
		if as == name {
			remote = enclaves
		}
	}
	if remote == "" {
		return "", nil, fmt.Errorf("no such tool %q", name)
	}
	arguments := map[string]any{}
	if len(bytes.TrimSpace(args)) > 0 {
		if err := json.Unmarshal(args, &arguments); err != nil {
			return "", nil, fmt.Errorf("arguments are not a JSON object: %w", err)
		}
	}
	params := &mcp.CallToolParams{Name: remote, Arguments: arguments}
	params.SetProgressToken(token)
	result, err := t.session.CallTool(ctx, params)
	if err != nil {
		return "", nil, err
	}
	out := output(result)
	if result.IsError {
		if out == "" {
			out = "tool call failed"
		}
		return "", nil, errors.New(out)
	}
	var meta json.RawMessage
	if len(result.Meta) > 0 {
		meta, _ = json.Marshal(result.Meta)
	}
	return out, meta, nil
}

func (t *toolset) close() { t.session.Close() }

func output(result *mcp.CallToolResult) string {
	if result.StructuredContent != nil {
		if encoded, err := json.Marshal(result.StructuredContent); err == nil {
			return string(encoded)
		}
	}
	var parts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			parts = append(parts, text.Text)
		}
	}
	return strings.Join(parts, "\n")
}
