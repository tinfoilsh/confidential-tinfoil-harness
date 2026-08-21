package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/tools"
)

const (
	maxTurns      = 8
	maxSSEEvent   = 1 << 20
	maxToolOutput = 30000
)

// run drives one request to completion; all of its state arrives in arguments.
func (h *harness) run(ctx context.Context, out *sse, req *request, m *model, upstream *http.Client) error {
	set, err := tools.Open(ctx, func(token string, detail json.RawMessage) {
		out.event(event{ID: token, Status: "progress", Progress: detail})
	}, h.sources...)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	defer set.Close()

	messages := req.messages
	for turn := 1; ; turn++ {
		body, err := req.upstream(m.name, messages, set.Defs)
		if err != nil {
			return err
		}
		calls, err := h.turn(ctx, out, upstream, body, req.apiKey)
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}
		if turn == maxTurns {
			return fmt.Errorf("model called tools for %d turns without answering", maxTurns)
		}
		messages = append(messages, requested(calls))
		messages = append(messages, execute(ctx, out, set, calls)...)
	}
}

// turn streams one model turn, keeping tool-call deltas rather than forwarding them.
func (h *harness) turn(ctx context.Context, out *sse, upstream *http.Client, body []byte, apiKey string) ([]toolCall, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.gateway+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	// Metered against the caller: the harness has no key of its own.
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := upstream.Do(req)
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

// execute runs a turn's tool calls concurrently, announcing each before dispatch.
func execute(ctx context.Context, out *sse, set *tools.Set, calls []toolCall) []json.RawMessage {
	type outcome struct {
		tools.Result
		err error
	}
	ids := make([]string, len(calls))
	results := make([]outcome, len(calls))

	var wg sync.WaitGroup
	for i, call := range calls {
		ids[i] = "ws_" + token()
		out.event(event{ID: ids[i], Status: "in_progress", Tool: call.name, Args: valid(call.args)})
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := set.Call(ctx, call.name, ids[i], json.RawMessage(call.args))
			results[i] = outcome{result, err}
		}()
	}
	wg.Wait()

	messages := make([]json.RawMessage, len(calls))
	for i, call := range calls {
		done := event{ID: ids[i], Status: "completed", Tool: call.name, Meta: results[i].Meta}
		content := results[i].Output
		if err := results[i].err; err != nil {
			// The model reads the failure and can retry rather than the turn dying.
			failure, _ := json.Marshal(map[string]string{"error": err.Error()})
			content, done.Status, done.Error = string(failure), "failed", clip(err.Error())
		}
		out.event(done)
		messages[i] = answered(call.id, clip(content))
	}
	return messages
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

// event is harness progress, carried in a chunk field an OpenAI client ignores.
type event struct {
	ID       string          `json:"item_id,omitempty"`
	Status   string          `json:"status"`
	Tool     string          `json:"tool,omitempty"`
	Args     json.RawMessage `json:"arguments,omitempty"`
	Meta     json.RawMessage `json:"meta,omitempty"`
	Error    string          `json:"error,omitempty"`
	Progress json.RawMessage `json:"progress,omitempty"`
}

type sse struct {
	w       http.ResponseWriter
	flush   http.Flusher
	id      string
	model   string
	created int64

	// mu serializes writes against the MCP session's goroutine; shut drops late ones.
	mu   sync.Mutex
	open bool
	shut bool
}

func newSSE(w http.ResponseWriter, model string) (*sse, error) {
	flush, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("cannot stream to this connection")
	}
	return &sse{w: w, flush: flush, id: "chatcmpl-" + token(), model: model, created: time.Now().Unix()}, nil
}

// pass keeps every field the enclave sent and replaces only the id.
func (s *sse) pass(chunk map[string]json.RawMessage) {
	chunk["id"] = quote(s.id)
	if encoded, err := json.Marshal(chunk); err == nil {
		s.write(string(encoded))
	}
}

func (s *sse) event(e event) {
	chunk, err := json.Marshal(map[string]any{
		"id":      s.id,
		"object":  "chat.completion.chunk",
		"created": s.created,
		"model":   s.model,
		"choices": []any{},
		"tinfoil": e,
	})
	if err != nil {
		return
	}
	s.write(string(chunk))
}

// fail reports an error raised after the stream opened, as a chunk.
func (s *sse) fail(err error) {
	chunk, _ := json.Marshal(map[string]any{
		"error": map[string]string{"message": err.Error(), "type": "server_error"},
	})
	s.write(string(chunk))
}

func (s *sse) done() { s.write("[DONE]") }

func (s *sse) opened() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.open
}

func (s *sse) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shut = true
}

func (s *sse) write(payload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shut {
		return
	}
	if !s.open {
		s.open = true
		s.w.Header().Set("Content-Type", "text/event-stream")
		s.w.Header().Set("Cache-Control", "no-store")
		s.w.Header().Set("X-Accel-Buffering", "no")
	}
	fmt.Fprintf(s.w, "data: %s\n\n", payload)
	s.flush.Flush()
}

func token() string {
	var raw [16]byte
	rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}
