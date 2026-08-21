package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
)

// This file is the agent, and the only one that decides anything: how many
// turns a question is worth, how much of a tool's output the model reads, and
// what happens when a tool fails. No wire format appears here.

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
