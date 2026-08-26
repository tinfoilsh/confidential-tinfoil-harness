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
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxTurns = 16
	// A tool result is bounded twice: what the model reads back, what the caller sees.
	maxToolOutput  = 30000
	maxShownOutput = 1 << 20
	maxSSEEvent    = 1 << 20
)

func (h *harness) loop(ctx context.Context, out *stream, req *request, m *model) error {
	set, err := openTools(ctx, req, out.progress)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	defer set.close()

	defs, err := offer(set, req.rendered)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	conversation := append(set.system(), req.messages...)
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
		// Without this the stream carries no usage chunk and nothing is metered.
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

func invoke(ctx context.Context, out *stream, set *toolset, call toolCall) json.RawMessage {
	// A widget is drawn by the caller from TOOL_CALL_ARGS; the model just needs a result.
	if set.byName[call.name] == nil {
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
	out.result(call.id, clip(content, maxShownOutput), meta)
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
