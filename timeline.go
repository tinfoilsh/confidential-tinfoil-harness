package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// apply runs under stream.mu, so tool goroutines and model deltas use one order.
type messageAssembler struct {
	ordered []*Message
	byID    map[string]*Message
	calls   map[string]blockRef
	sealed  map[string]int // leading blocks of a continued reply that new text starts after
}

// blockRef is a tool call's block, which stays at its index as a reply grows.
type blockRef struct {
	message *Message
	index   int
}

func (r blockRef) block() *Block { return &r.message.Timeline[r.index] }

func newMessageAssembler() *messageAssembler {
	return &messageAssembler{byID: map[string]*Message{}, calls: map[string]blockRef{}, sealed: map[string]int{}}
}

// seed starts from a reply that a continuation extends. As on the web
// client, only a trailing content block takes the new text.
func (a *messageAssembler) seed(reply Message) {
	m := reply.clone()
	if m.Timeline == nil {
		m.Timeline = []Block{}
	}
	sealed := len(m.Timeline)
	if sealed > 0 && m.Timeline[sealed-1].Type == "content" {
		sealed--
	}
	a.byID[m.ID], a.sealed[m.ID] = &m, sealed
	a.ordered = append(a.ordered, &m)
}
func (a *messageAssembler) message(id string) *Message {
	if id == "" {
		id = "msg_orphan"
	}
	if m := a.byID[id]; m != nil {
		return m
	}
	m := &Message{ID: id, Role: "assistant", CreatedAt: api.Timestamp(), Timeline: []Block{}}
	a.byID[id] = m
	a.ordered = append(a.ordered, m)
	return m
}

// stamp dates a reply by its first event; a continued reply keeps its date.
func (a *messageAssembler) stamp(id string, m *Message, at string) {
	if _, seeded := a.sealed[id]; at != "" && !seeded && len(m.Timeline) == 0 {
		m.CreatedAt = at
	}
}

// streamed adds text or reasoning a reply streamed to its timeline.
func (a *messageAssembler) streamed(id, kind, delta, emitted string) {
	m := a.message(id)
	a.stamp(id, m, emitted)
	at := float64(millis(emitted))
	blocks := m.Timeline
	if kind == "thinking" {
		// Reasoning that arrives after the reply's text began is the tail
		// of the reasoning before that text, as on the web client.
		if n := len(blocks); n-2 >= a.sealed[id] && blocks[n-1].Type == "content" && blocks[n-2].Type == "thinking" {
			blocks[n-2].Content += delta
			return
		}
	}
	if len(blocks) <= a.sealed[id] || blocks[len(blocks)-1].Type != kind {
		block := Block{ID: fmt.Sprintf("%s_%s_%d", id, kind, len(blocks)), Type: kind}
		if kind == "thinking" {
			block.StartedAt = at
		}
		m.Timeline = append(blocks, block)
	}
	last := &m.Timeline[len(m.Timeline)-1]
	last.Content += delta
	if kind == "content" {
		m.Content += delta
	} else if last.StartedAt > 0 && at >= last.StartedAt {
		last.EndedAt, last.Duration = at, (at-last.StartedAt)/1000
	}
}
func (a *messageAssembler) apply(e runEvent, at string) {
	switch e := e.(type) {
	case textChunk:
		a.streamed(e.MessageID, "content", e.Delta, at)
	case reasoningChunk:
		a.streamed(strings.TrimSuffix(e.MessageID, reasoningSuffix), "thinking", e.Delta, at)
	case toolCallStart:
		parent := e.ParentMessageID
		if parent == "" && len(a.ordered) > 0 {
			parent = a.ordered[len(a.ordered)-1].ID
		}
		if parent == "" {
			parent = "msg_" + e.ToolCallID
		}
		m := a.message(parent)
		a.stamp(parent, m, at)
		a.calls[e.ToolCallID] = blockRef{m, len(m.Timeline)}
		m.Timeline = append(m.Timeline, Block{ID: e.ToolCallID, Type: "tool_call", ToolCallID: e.ToolCallID, Name: e.ToolCallName, Progress: []json.RawMessage{}, StartedAt: float64(millis(at))})
	case toolCallArgs:
		if call, ok := a.calls[e.ToolCallID]; ok {
			call.block().Arguments += e.Delta
		}
	case toolCallEnd:
		if call, ok := a.calls[e.ToolCallID]; ok {
			call.block().Complete = true
		}
	case toolCallResult:
		if call, ok := a.calls[e.ToolCallID]; ok {
			b := call.block()
			b.Result = e.Content
			if at := float64(millis(at)); at >= b.StartedAt {
				b.EndedAt = at
			}
			if len(e.Metadata) > 0 {
				b.Metadata = e.Metadata
			}
			if e.Display != nil {
				b.Display = e.Display
			}
		}
	case activitySnapshot:
		if call, ok := a.calls[strings.TrimPrefix(e.MessageID, activityPrefix)]; ok && e.Display != nil {
			call.block().Display = e.Display
		}
	case activityDelta:
		if call, ok := a.calls[strings.TrimPrefix(e.MessageID, activityPrefix)]; ok {
			for _, op := range e.Patch {
				if op.Path == "/output/-" {
					b := call.block()
					b.Progress = append(b.Progress, api.Raw(op.Value))
				}
			}
		}
	}
}

// replaceReplyText rewrites a reply's text as the web client does: its
// content blocks collapse into one holding the text, where the first was.
func replaceReplyText(message *Message, text string) {
	timeline := message.Timeline
	first := slices.IndexFunc(timeline, func(block Block) bool { return block.Type == "content" })
	edited := Block{Type: "content", ID: "edited-content", Content: text}
	if first < 0 {
		first = len(timeline)
	} else {
		edited.ID = timeline[first].ID
	}
	blocks := []Block{}
	for i := range len(timeline) + 1 {
		if i == first && text != "" {
			blocks = append(blocks, edited)
		}
		if i < len(timeline) && timeline[i].Type != "content" {
			blocks = append(blocks, timeline[i])
		}
	}
	message.Content = text
	message.Timeline = nil
	if len(blocks) > 0 {
		message.Timeline = blocks
	}
}

// lastBlock is the block a reply streams into now, if any.
func (a *messageAssembler) lastBlock(id string) *Block {
	m := a.byID[id]
	if m == nil || len(m.Timeline) == 0 {
		return nil
	}
	return &m.Timeline[len(m.Timeline)-1]
}

func (a *messageAssembler) messages() []Message {
	out := []Message{}
	for _, m := range a.ordered {
		out = append(out, m.clone())
	}
	return out
}
