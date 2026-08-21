package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
)

// This file is the gateway's half of the connection, and the only place that
// knows the shape the enclave answers in: how a turn is asked for, and how
// its stream is split into the caller's answer and the loop's tool calls.

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
