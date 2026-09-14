package main

import (
	"fmt"
	"strings"
)

// apply runs under stream.mu, so tool goroutines and model deltas use one order.
type messageAssembler struct {
	ordered []object
	byID    map[string]object
	calls   map[string]object
}

func newMessageAssembler() *messageAssembler {
	return &messageAssembler{byID: map[string]object{}, calls: map[string]object{}}
}
func (a *messageAssembler) message(id string) object {
	if id == "" {
		id = "msg_orphan"
	}
	if m := a.byID[id]; m != nil {
		return m
	}
	m := object{"id": id, "role": "assistant", "content": "", "createdAt": timestamp(), "timestamp": timestamp(), "timeline": []any{}}
	a.byID[id] = m
	a.ordered = append(a.ordered, m)
	return m
}
func (a *messageAssembler) apply(e event) {
	switch e.Type {
	case "TEXT_MESSAGE_CHUNK", "REASONING_MESSAGE_CHUNK":
		id, kind := e.MessageID, "content"
		if e.Type == "REASONING_MESSAGE_CHUNK" {
			id = strings.TrimSuffix(id, "-reasoning")
			kind = "thinking"
		}
		m := a.message(id)
		if e.Timestamp != "" && len(arr(m["timeline"])) == 0 {
			m["createdAt"], m["timestamp"] = e.Timestamp, e.Timestamp
		}
		blocks := arr(m["timeline"])
		var b object
		if len(blocks) > 0 && obj(blocks[len(blocks)-1])["type"] == kind {
			b = obj(blocks[len(blocks)-1])
		} else {
			b = object{"id": fmt.Sprintf("%s_%s_%d", id, kind, len(blocks)), "type": kind, "content": ""}
			blocks = append(blocks, b)
			m["timeline"] = blocks
		}
		b["content"] = str(b["content"]) + str(e.Delta)
		if kind == "content" {
			m["content"] = str(m["content"]) + str(e.Delta)
		} else {
			m["thoughts"] = str(m["thoughts"]) + str(e.Delta)
		}
	case "TOOL_CALL_START":
		parent := e.ParentID
		if parent == "" && len(a.ordered) > 0 {
			parent = str(a.ordered[len(a.ordered)-1]["id"])
		}
		if parent == "" {
			parent = "msg_" + e.ToolCallID
		}
		m := a.message(parent)
		if e.Timestamp != "" && len(arr(m["timeline"])) == 0 {
			m["createdAt"], m["timestamp"] = e.Timestamp, e.Timestamp
		}
		b := object{"id": e.ToolCallID, "type": "tool_call", "toolCallId": e.ToolCallID, "name": e.ToolName, "arguments": "", "progress": []any{}, "resolvedAt": nil, "resolution": nil}
		a.calls[e.ToolCallID] = b
		m["timeline"] = append(arr(m["timeline"]), b)
	case "TOOL_CALL_ARGS":
		if b := a.calls[e.ToolCallID]; b != nil {
			b["arguments"] = str(b["arguments"]) + str(e.Delta)
		}
	case "TOOL_CALL_END":
		if b := a.calls[e.ToolCallID]; b != nil {
			b["complete"] = true
		}
	case "TOOL_CALL_RESULT":
		if b := a.calls[e.ToolCallID]; b != nil {
			b["result"] = decodeValue(e.Content)
			if len(e.Metadata) > 0 {
				b["metadata"] = decodeValue(e.Metadata)
			}
		}
	case "ACTIVITY_DELTA":
		if b := a.calls[strings.TrimPrefix(e.MessageID, "act_")]; b != nil {
			for _, value := range arr(decodeValue(e.Patch)) {
				op := obj(value)
				if op["path"] == "/output/-" {
					b["progress"] = append(arr(b["progress"]), op["value"])
				}
			}
		}
	}
}
func (a *messageAssembler) messages() []object {
	out := []object{}
	for _, m := range a.ordered {
		out = append(out, clone(m))
	}
	return out
}
