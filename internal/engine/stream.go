package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

type Stream struct {
	Run            *Log
	concreteModel  string
	modelAccepted  bool
	replyID        string
	turnReasoning  string
	reasoningField string // which of reasoning_content and reasoning the last turn streamed

	// mu serializes writes against the tool goroutines.
	mu       sync.Mutex
	spend    api.Usage
	Observe  func(e domain.Event, at string)
	thoughts *thoughtSummaries // nil unless a summarizer summarizes the run's reasoning
	sources  []citationSource  // pages the run's tools showed the model
}

func (s *Stream) cite(sources []citationSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources = append(s.sources, sources...)
}

func (s *Stream) emit(e domain.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frame(e, api.Timestamp())
}

func (s *Stream) text(message, delta string) {
	s.emit(domain.TextChunk{MessageID: message, Role: "assistant", Delta: delta})
}

func (s *Stream) reasoning(message, delta string) {
	s.emit(domain.ReasoningChunk{MessageID: message + domain.ReasoningSuffix, Delta: delta})
}

func (s *Stream) callStart(id, name, parent string) {
	s.emit(domain.ToolCallStart{ToolCallID: id, ToolCallName: name, ParentMessageID: parent})
}

func (s *Stream) callArgs(id, delta string) {
	s.emit(domain.ToolCallArgs{ToolCallID: id, Delta: delta})
}

func (s *Stream) callEnd(id string) {
	s.emit(domain.ToolCallEnd{ToolCallID: id})
}

// activity opens one call's output under an id derived from the call itself.
func (s *Stream) activity(call ToolCall) {
	s.emit(domain.ActivitySnapshot{MessageID: domain.ActivityPrefix + call.ID, ActivityType: activityTool, Content: domain.ToolActivity{ToolCallID: call.ID, Tool: call.Name, Output: []string{}}, Display: domain.SearchDisplay(call.Name, call.Args, nil)})
}

// output is a list because RFC 6902 cannot grow a string.
func (s *Stream) progress(call, note string, done float64) {
	patch := []domain.ActivityPatch{{Op: "replace", Path: "/progress", Value: done}}
	if note != "" {
		patch = append(patch, domain.ActivityPatch{Op: "add", Path: "/output/-", Value: note})
	}
	s.emit(domain.ActivityDelta{MessageID: domain.ActivityPrefix + call, ActivityType: activityTool, Patch: patch})
}

const activityTool = "TOOL"

// Accepted marks the model as having accepted a request of the run.
func (s *Stream) Accepted() {
	s.mu.Lock()
	s.modelAccepted = true
	s.mu.Unlock()
}

func (s *Stream) Meter(raw json.RawMessage) {
	var turn api.Usage
	if json.Unmarshal(raw, &turn) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spend.Prompt += turn.Prompt
	s.spend.Completion += turn.Completion
	s.spend.Total += turn.Total
}

func (s *Stream) frame(e domain.Event, at string) {
	encoded, err := domain.EncodeEvent(e, at)
	if err != nil {
		return
	}
	// Nothing can read a closed log, so stop the run rather than bill turns into it.
	if s.Run.Frames.grow(encoded) != nil {
		s.Run.stop()
	} else if s.Observe != nil {
		s.Observe(e, at)
		if s.thoughts != nil {
			s.thoughts.follow(e)
		}
	}
}

// Follow streams a run's log from the frame from, and tails it until the
// run ends.
func Follow(w http.ResponseWriter, r *http.Request, rn *Log, from int) {
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
		frames, wake, closed := rn.Frames.Read(from)
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
			if closed != ErrDone {
				e := *api.AsError(closed)
				e.Message = closed.Error()
				encoded, _ := domain.EncodeEvent(domain.RunError{Error: &e}, "")
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
