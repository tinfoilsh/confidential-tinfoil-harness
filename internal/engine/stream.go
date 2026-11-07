package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
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
	truncated      bool   // the model filled the room its window left the reply

	// mu serializes writes against the tool goroutines.
	mu       sync.Mutex
	spend    api.Usage
	Observe  func(e domain.Event, at string)
	thoughts *thoughtSummaries // nil unless a summarizer summarizes the run's reasoning
	sources  []citationSource  // pages the run's tools showed the model

	// folding is set while the reply streams, when a log past its budget is
	// folded into a checkpoint; not before the reply starts or once it ends.
	folding bool
	// activities is each tool's activity as its events have left it, which
	// a checkpoint sends in their place.
	activities []*domain.ActivitySnapshot
	// pending is the deltas held back to leave as one frame; lastDelta is
	// what the log's last frame extends, when it is a delta, and lastAt
	// when it was framed.
	pending   *pendingDelta
	lastDelta string
	lastAt    time.Time
}

// A delta that continues the frame before it within deltaWindow is held
// back until the window ends, and the deltas to its target that follow join
// it, so a fast model's reply grows by a frame a window rather than a frame
// a token, which a slow client falls behind on. A delta after anything else
// leaves at once, and anything else sends the held deltas before it.
const deltaWindow = 50 * time.Millisecond

// pendingDelta is deltas to one target held back until their window ends.
type pendingDelta struct {
	first domain.TextDelta
	key   string
	text  strings.Builder
	// at is when the last of them was emitted, which their frame keeps, as
	// a client times reasoning by when it last grew.
	at    string
	timer *time.Timer
}

// track keeps each tool's activity as its events leave it: the snapshot a
// call opens with, then the progress and lines it reports.
func (s *Stream) track(e domain.Event) {
	switch e := e.(type) {
	case domain.ActivitySnapshot:
		e.Content.Output = slices.Clone(e.Content.Output)
		if i := slices.IndexFunc(s.activities, func(a *domain.ActivitySnapshot) bool { return a.MessageID == e.MessageID }); i >= 0 {
			s.activities[i] = &e
		} else {
			s.activities = append(s.activities, &e)
		}
	case domain.ActivityDelta:
		i := slices.IndexFunc(s.activities, func(a *domain.ActivitySnapshot) bool { return a.MessageID == e.MessageID })
		if i < 0 {
			return
		}
		a := s.activities[i]
		for _, op := range e.Patch {
			switch value := op.Value.(type) {
			case float64:
				if op.Path == "/progress" {
					a.Content.Progress = value
				}
			case string:
				if op.Path == "/output/-" {
					a.Content.Output = append(a.Content.Output, value)
				}
			}
		}
		if e.Display != nil {
			a.Display = e.Display
		}
	}
}

// activityFrame is the frame of a tool's activity as its events have left
// it, or nil for one the stream does not follow.
func (s *Stream) activityFrame(id string) []byte {
	i := slices.IndexFunc(s.activities, func(a *domain.ActivitySnapshot) bool { return a.MessageID == id })
	if i < 0 {
		return nil
	}
	encoded, err := domain.EncodeEvent(*s.activities[i], api.Timestamp())
	if err != nil {
		return nil
	}
	return encoded
}

// merged is one frame for a run of frames of domain.TextDelta events to the
// same target: the first's members, with each frame's delta in turn.
func merged(deltas [][]byte) []byte {
	var first map[string]json.RawMessage
	json.Unmarshal(deltas[0], &first)
	var text strings.Builder
	for _, frame := range deltas {
		var d struct {
			Delta string `json:"delta"`
		}
		json.Unmarshal(frame, &d)
		text.WriteString(d.Delta)
	}
	first["delta"], _ = json.Marshal(text.String())
	encoded, _ := json.Marshal(first)
	return encoded
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

// frame frames e, unless it is a delta held back to join others.
func (s *Stream) frame(e domain.Event, at string) {
	kind, key := kindOf(e)
	if kind != frameDelta {
		s.framePending()
		s.write(e, kind, key, at)
		return
	}
	d := e.(domain.TextDelta)
	if p := s.pending; p != nil && p.key == key && p.text.Len()+len(d.Text()) <= mergedFrameBytes {
		p.text.WriteString(d.Text())
		p.at = at
		return
	}
	s.framePending()
	if wait := deltaWindow - time.Since(s.lastAt); key == s.lastDelta && wait > 0 {
		p := &pendingDelta{first: d, key: key, at: at}
		p.text.WriteString(d.Text())
		p.timer = time.AfterFunc(wait, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.pending == p {
				s.framePending()
			}
		})
		s.pending = p
		return
	}
	s.write(e, kind, key, at)
}

// framePending frames the deltas held back as one, if there are any.
func (s *Stream) framePending() {
	p := s.pending
	if p == nil {
		return
	}
	s.pending = nil
	p.timer.Stop()
	s.write(p.first.WithText(p.text.String()), frameDelta, p.key, p.at)
}

func (s *Stream) write(e domain.Event, kind frameKind, key, at string) {
	encoded, err := domain.EncodeEvent(e, at)
	if err != nil {
		return
	}
	full := s.Run.Frames.grow(encoded, kind, key)
	s.lastDelta = ""
	if kind == frameDelta {
		s.lastDelta, s.lastAt = key, time.Now()
	}
	s.track(e)
	if s.Observe != nil {
		s.Observe(e, at)
		if s.thoughts != nil {
			s.thoughts.follow(e)
		}
	}
	// The activities are tracked up to this frame, under the lock that
	// orders frames, so they stand for every frame folded.
	if full && s.folding {
		s.Run.Frames.fold(merged, s.activityFrame)
	}
}

// kindOf is what a checkpoint does with an event, and what the event extends
// or reports on. A checkpoint merges text deltas and keeps each tool's
// activity as it stands; every other event, the run's snapshots and the
// calls and their results among them, it sends as it was.
func kindOf(e domain.Event) (frameKind, string) {
	switch e := e.(type) {
	case domain.TextDelta:
		return frameDelta, fmt.Sprintf("%T:%s", e, e.Target())
	case domain.ActivitySnapshot:
		return frameActivity, e.MessageID
	case domain.ActivityDelta:
		return frameActivity, e.MessageID
	}
	return frameKept, ""
}

const (
	// maxSnapshotBytes bounds a MESSAGES_SNAPSHOT, so a run starts by sending
	// no more of a long chat than this; a client loads older messages as it
	// shows them.
	maxSnapshotBytes = 8 << 20
	// snapshotEnvelope is room for what a snapshot's event holds besides
	// its messages.
	snapshotEnvelope = 4 << 10
)

// fittedSnapshot is a snapshot of messages that fits in maxSnapshotBytes:
// the oldest that do not fit are left out, as older messages, and the latest
// is always sent. A latest message too large alone, as one carrying large
// documents, is sent with its attachments but not their text, which clients
// read on demand.
func fittedSnapshot(messages []domain.MessageView, hasOlder bool) domain.MessagesSnapshot {
	sizes, total := make([]int, len(messages)), 0
	for i := range messages {
		encoded, _ := json.Marshal(messages[i])
		sizes[i] = len(encoded) + 1
		total += sizes[i]
	}
	start := 0
	for start < len(messages)-1 && total > maxSnapshotBytes-snapshotEnvelope {
		total -= sizes[start]
		start++
	}
	fitted := messages[start:]
	if len(fitted) == 1 && total > maxSnapshotBytes-snapshotEnvelope {
		latest := fitted[0]
		latest.Attachments = slices.Clone(latest.Attachments)
		for i := range latest.Attachments {
			latest.Attachments[i].TextContent = ""
		}
		fitted = []domain.MessageView{latest}
	}
	return domain.MessagesSnapshot{Messages: fitted, HasOlder: hasOlder || start > 0}
}

// Follow streams a run's log from the frame from, and tails it until the
// run ends. Only a checkpoint's last event names a cursor, the one before the
// first frame after it, so a follower that misses part of a checkpoint is
// sent all of it again.
// It reports whether it sent the run to its end.
func Follow(w http.ResponseWriter, r *http.Request, rn *Log, from int) bool {
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
	send := func(checkpoint, frames [][]byte, first int) error {
		for i, frame := range checkpoint {
			open()
			var err error
			if i == len(checkpoint)-1 {
				_, err = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", first-1, frame)
			} else {
				_, err = fmt.Fprintf(w, "data: %s\n\n", frame)
			}
			if err != nil {
				return err
			}
		}
		for i, frame := range frames {
			open()
			if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", first+i, frame); err != nil {
				return err
			}
		}
		return nil
	}
	for {
		checkpoint, frames, first, wake, closed, done := rn.Frames.Read(from)
		err := send(checkpoint, frames, first)
		done()
		if err != nil {
			return false
		}
		from = first + len(frames)
		if len(checkpoint)+len(frames) > 0 {
			if err := flush.Flush(); err != nil {
				return false
			}
		}
		if closed != nil {
			open()
			return true
		}
		select {
		case <-wake:
		case <-heartbeat.C:
			open()
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return false
			}
			if err := flush.Flush(); err != nil {
				return false
			}
		case <-r.Context().Done():
			return false
		}
	}
}
