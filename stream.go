package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

func resumeFrom(header string) int {
	n, err := strconv.Atoi(header)
	if err != nil || n < 0 {
		return 0
	}
	return n + 1
}

type event struct {
	Type       string          `json:"type"`
	Timestamp  string          `json:"timestamp,omitempty"`
	ThreadID   string          `json:"threadId,omitempty"`
	RunID      string          `json:"runId,omitempty"`
	MessageID  string          `json:"messageId,omitempty"`
	ParentID   string          `json:"parentMessageId,omitempty"`
	ToolCallID string          `json:"toolCallId,omitempty"`
	ToolName   string          `json:"toolCallName,omitempty"`
	Role       string          `json:"role,omitempty"`
	Delta      any             `json:"delta,omitempty"`
	Snapshot   any             `json:"snapshot,omitempty"`
	Messages   any             `json:"messages,omitempty"`
	HasOlder   *bool           `json:"hasOlder,omitempty"`
	Queued     bool            `json:"queued,omitempty"`
	Code       string          `json:"code,omitempty"`
	Kind       string          `json:"kind,omitempty"`
	ResetsAt   string          `json:"resetsAt,omitempty"`
	RetryAfter int             `json:"retryAfter,omitempty"`
	Content    json.RawMessage `json:"content,omitempty"`
	Activity   string          `json:"activityType,omitempty"`
	Patch      json.RawMessage `json:"patch,omitempty"`
	Message    string          `json:"message,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

type usage struct {
	Prompt     int64 `json:"prompt_tokens"`
	Completion int64 `json:"completion_tokens"`
	Total      int64 `json:"total_tokens"`
}

type stream struct {
	run            *run
	concreteModel  string
	modelAccepted  bool
	replyID        string
	turnReasoning  string
	reasoningField string // which of reasoning_content and reasoning the last turn streamed
	threadID       string
	runID          string

	// mu serializes writes against the tool goroutines.
	mu      sync.Mutex
	spend   usage
	observe func(event)
	sources []citationSource // pages the run's tools showed the model
}

func (s *stream) cite(sources []citationSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources = append(s.sources, sources...)
}

func (s *stream) emit(e event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.Timestamp == "" {
		e.Timestamp = timestamp()
	}
	s.frame(e)
}

func (s *stream) text(message, delta string) {
	s.emit(event{Type: "TEXT_MESSAGE_CHUNK", MessageID: message, Role: "assistant", Delta: delta})
}

func (s *stream) reasoning(message, delta string) {
	s.emit(event{Type: "REASONING_MESSAGE_CHUNK", MessageID: message + "-reasoning", Delta: delta})
}

func (s *stream) callStart(id, name, parent string) {
	s.emit(event{Type: "TOOL_CALL_START", ToolCallID: id, ToolName: name, ParentID: parent})
}

func (s *stream) callArgs(id, delta string) {
	s.emit(event{Type: "TOOL_CALL_ARGS", ToolCallID: id, Delta: delta})
}

func (s *stream) callEnd(id string) {
	s.emit(event{Type: "TOOL_CALL_END", ToolCallID: id})
}

// activity opens one call's output under an id derived from the call itself.
func (s *stream) activity(call toolCall) {
	content, err := json.Marshal(map[string]any{
		"toolCallId": call.id, "tool": call.name, "progress": 0, "output": []string{}})
	if err != nil {
		return
	}
	s.emit(event{Type: "ACTIVITY_SNAPSHOT", MessageID: "act_" + call.id, Activity: activityTool, Content: content})
}

// output is a list because RFC 6902 cannot grow a string.
func (s *stream) progress(call, note string, done float64) {
	ops := []map[string]any{{"op": "replace", "path": "/progress", "value": done}}
	if note != "" {
		ops = append(ops, map[string]any{"op": "add", "path": "/output/-", "value": note})
	}
	patch, err := json.Marshal(ops)
	if err != nil {
		return
	}
	s.emit(event{Type: "ACTIVITY_DELTA", MessageID: "act_" + call, Activity: activityTool, Patch: patch})
}

const activityTool = "TOOL"

func (s *stream) meter(raw json.RawMessage) {
	var turn usage
	if json.Unmarshal(raw, &turn) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spend.Prompt += turn.Prompt
	s.spend.Completion += turn.Completion
	s.spend.Total += turn.Total
}

func (s *stream) frame(e event) {
	encoded, err := json.Marshal(e)
	if err != nil {
		return
	}
	// Nothing can read a closed log, so stop the run rather than bill turns into it.
	if s.run.log.grow(encoded) != nil {
		s.run.stop()
	} else if s.observe != nil {
		s.observe(e)
	}
}

func follow(w http.ResponseWriter, r *http.Request, rn *run, from int) {
	flush := http.NewResponseController(w)
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	opened := false
	open := func() {
		if opened {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		opened = true
	}
	for {
		frames, wake, closed := rn.log.read(from)
		for i, frame := range frames {
			open()
			if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", from+i, frame); err != nil {
				return
			}
		}
		from += len(frames)
		if len(frames) > 0 {
			if err := flush.Flush(); err != nil {
				return
			}
		}
		if closed != nil {
			open()
			if closed != errDone {
				encoded, _ := json.Marshal(event{Type: "RUN_ERROR", Message: closed.Error(), Code: asAPIError(closed).Code})
				fmt.Fprintf(w, "id: %d\ndata: %s\n\n", from, encoded)
				flush.Flush()
			}
			return
		}
		select {
		case <-wake:
		case <-heartbeat.C:
			open()
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			if err := flush.Flush(); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}
