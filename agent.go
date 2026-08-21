package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxTurns      = 8
	maxToolOutput = 30000
)

// run drives one request to completion; all of its state arrives in arguments.
func (h *harness) run(ctx context.Context, out *sse, req *request, m *model) error {
	set, err := openTools(ctx, h.toolsEndpoint, h.toolsClient, out.progress)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	defer set.close()

	// The conversation is the loop's, grown a turn at a time from what arrived.
	conversation := slices.Clone(req.messages)
	for turn := 1; ; turn++ {
		payload, err := body(req, m, conversation, set.defs)
		if err != nil {
			return err
		}
		calls, err := h.turn(ctx, out, m, payload, req.apiKey)
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}
		if turn == maxTurns {
			return fmt.Errorf("model called tools for %d turns without answering", maxTurns)
		}
		conversation = append(conversation, requested(calls))
		conversation = append(conversation, execute(ctx, out, set, calls)...)
	}
}

// execute runs a turn's tool calls concurrently, announcing each before
// dispatch; each call then reports itself and fills its own reply.
func execute(ctx context.Context, out *sse, set *toolset, calls []toolCall) []json.RawMessage {
	messages := make([]json.RawMessage, len(calls))
	var wg sync.WaitGroup
	for i, call := range calls {
		id := "ws_" + token()
		out.event(event{ID: id, Status: "in_progress", Tool: call.name, Args: valid(call.args)})
		wg.Add(1)
		go func() {
			defer wg.Done()
			messages[i] = invoke(ctx, out, set, call, id)
		}()
	}
	wg.Wait()
	return messages
}

// invoke runs one call and reports it twice: to the caller as an event, and to
// the model as the message it will read next.
func invoke(ctx context.Context, out *sse, set *toolset, call toolCall, id string) json.RawMessage {
	done := event{ID: id, Status: "completed", Tool: call.name}
	content, meta, err := set.call(ctx, call.name, id, json.RawMessage(call.args))
	done.Meta = meta
	if err != nil {
		// The model reads the failure and can retry rather than the turn dying.
		failure, _ := json.Marshal(map[string]string{"error": err.Error()})
		content, done.Status, done.Error = string(failure), "failed", clip(err.Error())
	}
	out.event(done)
	return answered(call.id, clip(content))
}

// clip bounds one call's context spend; the tail goes so the head still orients.
func clip(out string) string {
	if len(out) > maxToolOutput {
		return out[:maxToolOutput] + "\n[truncated]"
	}
	if out == "" {
		return "[no output]"
	}
	return out
}

// valid forwards arguments verbatim; malformed JSON yields none.
func valid(args string) json.RawMessage {
	if !json.Valid([]byte(args)) {
		return nil
	}
	return json.RawMessage(args)
}

const maxSSEEvent = 1 << 20

// body renders one model turn: the caller's fields plus the loop's.
func body(req *request, m *model, conversation []json.RawMessage, defs json.RawMessage) ([]byte, error) {
	messages, err := json.Marshal(conversation)
	if err != nil {
		return nil, err
	}
	fields := make(map[string]json.RawMessage, len(req.fields)+4)
	maps.Copy(fields, req.fields)
	fields["model"] = quote(m.name)
	fields["stream"] = json.RawMessage("true")
	fields["messages"] = messages
	fields["tools"] = defs
	// Partitions the enclave's prompt cache per caller; a namespace, not a credential.
	fields["user_cache_secret"] = quote(cacheScope(req.apiKey))
	return json.Marshal(fields)
}

func cacheScope(apiKey string) string {
	sum := sha256.Sum256([]byte("confidential-tinfoil-harness cache scope\x00" + apiKey))
	return hex.EncodeToString(sum[:])
}

// turn streams one model turn, forwarding the answer to the caller and keeping
// the tool-call deltas for the loop.
func (h *harness) turn(ctx context.Context, out *sse, m *model, payload []byte, apiKey string) ([]toolCall, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.gateway+"/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	// Metered against the caller: the harness has no key of its own.
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, &refusal{status: resp.StatusCode, detail: string(bytes.TrimSpace(detail))}
	}

	var calls []toolCall
	events := bufio.NewScanner(resp.Body)
	events.Buffer(nil, maxSSEEvent)
	for events.Scan() {
		data, ok := strings.CutPrefix(events.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		// The caller's stream ends when the loop does, not when a turn does.
		if data == "" || data == "[DONE]" {
			continue
		}
		var chunk map[string]json.RawMessage
		if json.Unmarshal([]byte(data), &chunk) != nil {
			out.write(data)
			continue
		}
		var choices []struct {
			Delta struct {
				ToolCalls []toolCallDelta `json:"tool_calls"`
			} `json:"delta"`
			FinishReason string `json:"finish_reason"`
		}
		json.Unmarshal(chunk["choices"], &choices)

		switch {
		case len(choices) == 0:
			// A usage tail is part of the answer only when this turn was it.
			if len(calls) == 0 {
				out.pass(chunk)
			}
		case len(choices[0].Delta.ToolCalls) > 0:
			calls = accumulate(calls, choices[0].Delta.ToolCalls)
		case choices[0].FinishReason == "tool_calls":
		default:
			out.pass(chunk)
		}
	}
	return calls, events.Err()
}

type toolCall struct{ id, name, args string }

type toolCallDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func accumulate(calls []toolCall, deltas []toolCallDelta) []toolCall {
	for _, delta := range deltas {
		for len(calls) <= delta.Index {
			calls = append(calls, toolCall{})
		}
		call := &calls[delta.Index]
		call.id += delta.ID
		call.name += delta.Function.Name
		call.args += delta.Function.Arguments
	}
	return calls
}

// requested records what the model asked for; answered is one tool's reply.
func requested(calls []toolCall) json.RawMessage {
	made := make([]any, len(calls))
	for i, c := range calls {
		made[i] = map[string]any{"id": c.id, "type": "function",
			"function": map[string]any{"name": c.name, "arguments": c.args}}
	}
	message, _ := json.Marshal(map[string]any{"role": "assistant", "tool_calls": made})
	return message
}

func answered(id, content string) json.RawMessage {
	message, _ := json.Marshal(map[string]string{"role": "tool", "tool_call_id": id, "content": content})
	return message
}

// refusal is the gateway's own answer to a request it would not forward.
type refusal struct {
	status int
	detail string
}

func (r *refusal) Error() string { return fmt.Sprintf("gateway returned %d: %s", r.status, r.detail) }

// exposed maps the enclave's tool names to what the model is offered; a tool
// this map does not name is never advertised.
var exposed = map[string]string{"search": "web_search", "fetch": "web_fetch"}

// toolset is one request's tool surface: a session that holds the calls it will
// serve, defs as the model is told about them, and the enclave's own name for
// each tool the model may ask for.
type toolset struct {
	session *mcp.ClientSession
	defs    json.RawMessage
	remote  map[string]string // web_search -> search
}

// openTools gives one request its own session and asks the enclave what it
// serves rather than guessing.
func openTools(ctx context.Context, endpoint string, client *http.Client, progress func(token string, detail json.RawMessage)) (*toolset, error) {
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "confidential-tinfoil-harness", Version: "2"}, &mcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			token, _ := req.Params.ProgressToken.(string)
			if detail, err := json.Marshal(req.Params); err == nil {
				progress(token, detail)
			}
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
	defs, remote, err := advertise(found.Tools)
	if err != nil {
		session.Close()
		return nil, err
	}
	return &toolset{session: session, defs: defs, remote: remote}, nil
}

// advertise renders the enclave's tools as the model is told about them, in the
// order the enclave listed them so that one caller's turns keep hitting the
// same prompt cache.
func advertise(found []*mcp.Tool) (json.RawMessage, map[string]string, error) {
	defs := []any{}
	remote := map[string]string{}
	for _, tool := range found {
		as, ok := exposed[tool.Name]
		if !ok {
			continue
		}
		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			return nil, nil, fmt.Errorf("tool %q published an unreadable schema: %w", tool.Name, err)
		}
		remote[as] = tool.Name
		defs = append(defs, map[string]any{"type": "function", "function": map[string]any{
			"name": as, "description": tool.Description, "parameters": schema}})
	}
	if len(defs) == 0 {
		return nil, nil, errors.New("enclave serves neither search nor fetch")
	}
	encoded, err := json.Marshal(defs)
	return encoded, remote, err
}

// call runs one advertised tool; token names it in the enclave's progress
// notifications. It returns what the model reads, plus Meta for the caller.
func (t *toolset) call(ctx context.Context, name, token string, args json.RawMessage) (string, json.RawMessage, error) {
	remote, ok := t.remote[name]
	if !ok {
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

// output renders a result as the model reads it: structured if any, else text.
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
