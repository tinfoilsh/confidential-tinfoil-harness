package main

import (
	"bytes"
	"encoding/json"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// runEvent is one event of a run, each type as protocol/openapi.yaml names it.
type runEvent interface{ eventType() string }

type runStarted struct {
	ThreadID  string `json:"threadId"`
	RunID     string `json:"runId"`
	Instance  string `json:"instance"`
	MessageID string `json:"messageId,omitempty"` // the reply a continuation streams into
}

type messagesSnapshot struct {
	Messages []messageView `json:"messages"`
	HasOlder bool          `json:"hasOlder"`
}

type stateSnapshotEvent struct {
	Snapshot stateSnapshot `json:"snapshot"`
}

// stateDelta changes a run's state. One naming a run is about a turn queued
// behind the running reply.
type stateDelta struct {
	ThreadID string       `json:"threadId,omitempty"`
	RunID    string       `json:"runId,omitempty"`
	Delta    []statePatch `json:"delta"`
}

type textChunk struct {
	MessageID string `json:"messageId"`
	Role      string `json:"role"`
	Delta     string `json:"delta"`
}

// reasoningChunk streams a reply's reasoning, under the reply's id with
// reasoningSuffix after it.
type reasoningChunk struct {
	MessageID string `json:"messageId"`
	Delta     string `json:"delta"`
}

const reasoningSuffix = "-reasoning"

type reasoningSummary struct {
	MessageID string `json:"messageId"`
	BlockID   string `json:"blockId"`
	Content   string `json:"content"`
}

type toolCallStart struct {
	ToolCallID      string `json:"toolCallId"`
	ToolCallName    string `json:"toolCallName"`
	ParentMessageID string `json:"parentMessageId,omitempty"`
}

type toolCallArgs struct {
	ToolCallID string `json:"toolCallId"`
	Delta      string `json:"delta"`
}

type toolCallEnd struct {
	ToolCallID string `json:"toolCallId"`
}

type toolCallResult struct {
	MessageID  string          `json:"messageId"`
	ToolCallID string          `json:"toolCallId"`
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	Display    *ToolDisplay    `json:"display,omitempty"`
}

// activitySnapshot opens a tool's output while it runs, under the call's id
// with activityPrefix before it.
type activitySnapshot struct {
	MessageID    string       `json:"messageId"`
	ActivityType string       `json:"activityType"`
	Content      toolActivity `json:"content"`
	Display      *ToolDisplay `json:"display,omitempty"`
}

type activityDelta struct {
	MessageID    string          `json:"messageId"`
	ActivityType string          `json:"activityType"`
	Patch        []activityPatch `json:"patch"`
	Display      *ToolDisplay    `json:"display,omitempty"`
}

const activityPrefix = "act_"

// toolActivity is a tool's output as it runs: its progress, and the lines
// it printed.
type toolActivity struct {
	ToolCallID string   `json:"toolCallId"`
	Tool       string   `json:"tool"`
	Progress   float64  `json:"progress"`
	Output     []string `json:"output"`
}

type activityPatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

type runFinished struct {
	ThreadID string      `json:"threadId"`
	RunID    string      `json:"runId"`
	Metadata runMetadata `json:"metadata"`
}

// runError ends a failed run with the members a refusal has. One a stream
// ends with after its run was lost names neither the chat nor the run.
type runError struct {
	ThreadID string `json:"threadId,omitempty"`
	RunID    string `json:"runId,omitempty"`
	*api.Error
	Metadata *runMetadata `json:"metadata,omitempty"`
}

func failedRun(threadID, runID string, e *api.Error, metadata *runMetadata) runError {
	return runError{ThreadID: threadID, RunID: runID, Error: e, Metadata: metadata}
}

func (runStarted) eventType() string         { return "RUN_STARTED" }
func (messagesSnapshot) eventType() string   { return "MESSAGES_SNAPSHOT" }
func (stateSnapshotEvent) eventType() string { return "STATE_SNAPSHOT" }
func (stateDelta) eventType() string         { return "STATE_DELTA" }
func (textChunk) eventType() string          { return "TEXT_MESSAGE_CHUNK" }
func (reasoningChunk) eventType() string     { return "REASONING_MESSAGE_CHUNK" }
func (reasoningSummary) eventType() string   { return "REASONING_SUMMARY" }
func (toolCallStart) eventType() string      { return "TOOL_CALL_START" }
func (toolCallArgs) eventType() string       { return "TOOL_CALL_ARGS" }
func (toolCallEnd) eventType() string        { return "TOOL_CALL_END" }
func (toolCallResult) eventType() string     { return "TOOL_CALL_RESULT" }
func (activitySnapshot) eventType() string   { return "ACTIVITY_SNAPSHOT" }
func (activityDelta) eventType() string      { return "ACTIVITY_DELTA" }
func (runFinished) eventType() string        { return "RUN_FINISHED" }
func (runError) eventType() string           { return "RUN_ERROR" }

// encodeEvent is an event's frame: its type, the time it was emitted unless
// none is given, then its own members.
func encodeEvent(e runEvent, at string) ([]byte, error) {
	members, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	header, err := json.Marshal(struct {
		Type      string `json:"type"`
		Timestamp string `json:"timestamp,omitempty"`
	}{e.eventType(), at})
	if err != nil {
		return nil, err
	}
	if bytes.Equal(members, []byte("{}")) {
		return header, nil
	}
	return append(append(header[:len(header)-1], ','), members[1:]...), nil
}
