package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

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

	// mu serializes writes against the tool goroutines.
	mu       sync.Mutex
	spend    usage
	observe  func(e runEvent, at string)
	thoughts *thoughtSummaries // nil unless a summarizer summarizes the run's reasoning
	sources  []citationSource  // pages the run's tools showed the model
}

func (s *stream) cite(sources []citationSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources = append(s.sources, sources...)
}

func (s *stream) emit(e runEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frame(e, timestamp())
}

func (s *stream) text(message, delta string) {
	s.emit(textChunk{MessageID: message, Role: "assistant", Delta: delta})
}

func (s *stream) reasoning(message, delta string) {
	s.emit(reasoningChunk{MessageID: message + reasoningSuffix, Delta: delta})
}

func (s *stream) callStart(id, name, parent string) {
	s.emit(toolCallStart{ToolCallID: id, ToolCallName: name, ParentMessageID: parent})
}

func (s *stream) callArgs(id, delta string) {
	s.emit(toolCallArgs{ToolCallID: id, Delta: delta})
}

func (s *stream) callEnd(id string) {
	s.emit(toolCallEnd{ToolCallID: id})
}

// activity opens one call's output under an id derived from the call itself.
func (s *stream) activity(call toolCall) {
	s.emit(activitySnapshot{MessageID: activityPrefix + call.id, ActivityType: activityTool, Content: toolActivity{ToolCallID: call.id, Tool: call.name, Output: []string{}}, Display: searchDisplay(call.name, call.args, nil)})
}

// output is a list because RFC 6902 cannot grow a string.
func (s *stream) progress(call, note string, done float64) {
	patch := []activityPatch{{"replace", "/progress", done}}
	if note != "" {
		patch = append(patch, activityPatch{"add", "/output/-", note})
	}
	s.emit(activityDelta{MessageID: activityPrefix + call, ActivityType: activityTool, Patch: patch})
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

func (s *stream) frame(e runEvent, at string) {
	encoded, err := encodeEvent(e, at)
	if err != nil {
		return
	}
	// Nothing can read a closed log, so stop the run rather than bill turns into it.
	if s.run.log.grow(encoded) != nil {
		s.run.stop()
	} else if s.observe != nil {
		s.observe(e, at)
		if s.thoughts != nil {
			s.thoughts.follow(e)
		}
	}
}

// follow streams a run's log from the frame after the cursor a client
// applied last, or from its first frame, and tails it until the run ends. A
// cursor the run never sent is not the client's for this run, so the client
// is sent every frame.
func follow(w http.ResponseWriter, r *http.Request, rn *run, after int) {
	from := 0
	switch sent, _, closed := rn.log.read(0); {
	case after >= 0 && after < len(sent):
		from = after + 1
	// A log that failed ends its streams with the error, under the cursor
	// after its last frame. A client that applied that error is sent it
	// again, which ends this stream too.
	case after == len(sent) && closed != nil && closed != errDone:
		from = after
	}
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
				e := *asAPIError(closed)
				e.Message = closed.Error()
				encoded, _ := encodeEvent(runError{apiError: &e}, "")
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
