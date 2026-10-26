package domain

import (
	"bytes"
	"encoding/json"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Event is one event of a run, each type as protocol/openapi.yaml names it.
type Event interface{ eventType() string }

type RunStarted struct {
	ThreadID  string `json:"threadId"`
	RunID     string `json:"runId"`
	Instance  string `json:"instance"`
	MessageID string `json:"messageId,omitempty"` // the reply a continuation streams into
}

type MessagesSnapshot struct {
	Messages []MessageView `json:"messages"`
	HasOlder bool          `json:"hasOlder"`
}

type StateSnapshotEvent struct {
	Snapshot StateSnapshot `json:"snapshot"`
}

// StateDelta changes a run's state. One naming a run is about a turn queued
// behind the running reply.
type StateDelta struct {
	ThreadID string       `json:"threadId,omitempty"`
	RunID    string       `json:"runId,omitempty"`
	Delta    []StatePatch `json:"delta"`
}

type TextChunk struct {
	MessageID string `json:"messageId"`
	Role      string `json:"role"`
	Delta     string `json:"delta"`
}

// ReasoningChunk streams a reply's reasoning, under the reply's id with
// ReasoningSuffix after it.
type ReasoningChunk struct {
	MessageID string `json:"messageId"`
	Delta     string `json:"delta"`
}

const ReasoningSuffix = "-reasoning"

type ReasoningSummary struct {
	MessageID string `json:"messageId"`
	BlockID   string `json:"blockId"`
	Content   string `json:"content"`
}

type ToolCallStart struct {
	ToolCallID      string `json:"toolCallId"`
	ToolCallName    string `json:"toolCallName"`
	ParentMessageID string `json:"parentMessageId,omitempty"`
}

type ToolCallArgs struct {
	ToolCallID string `json:"toolCallId"`
	Delta      string `json:"delta"`
}

type ToolCallEnd struct {
	ToolCallID string `json:"toolCallId"`
}

type ToolCallResult struct {
	MessageID  string          `json:"messageId"`
	ToolCallID string          `json:"toolCallId"`
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	Display    *ToolDisplay    `json:"display,omitempty"`
}

// ActivitySnapshot opens a tool's output while it runs, under the call's id
// with activityPrefix before it.
type ActivitySnapshot struct {
	MessageID    string       `json:"messageId"`
	ActivityType string       `json:"activityType"`
	Content      ToolActivity `json:"content"`
	Display      *ToolDisplay `json:"display,omitempty"`
}

type ActivityDelta struct {
	MessageID    string          `json:"messageId"`
	ActivityType string          `json:"activityType"`
	Patch        []ActivityPatch `json:"patch"`
	Display      *ToolDisplay    `json:"display,omitempty"`
}

const ActivityPrefix = "act_"

// ToolActivity is a tool's output as it runs: its progress, and the lines
// it printed.
type ToolActivity struct {
	ToolCallID string   `json:"toolCallId"`
	Tool       string   `json:"tool"`
	Progress   float64  `json:"progress"`
	Output     []string `json:"output"`
}

type ActivityPatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

type RunFinished struct {
	ThreadID string      `json:"threadId"`
	RunID    string      `json:"runId"`
	Metadata RunMetadata `json:"metadata"`
}

// RunError ends a failed run with the members a refusal has. One a stream
// ends with after its run was lost names neither the chat nor the run.
type RunError struct {
	ThreadID string `json:"threadId,omitempty"`
	RunID    string `json:"runId,omitempty"`
	*api.Error
	Metadata *RunMetadata `json:"metadata,omitempty"`
}

func FailedRun(threadID, runID string, e *api.Error, metadata *RunMetadata) RunError {
	return RunError{ThreadID: threadID, RunID: runID, Error: e, Metadata: metadata}
}

func (RunStarted) eventType() string         { return "RUN_STARTED" }
func (MessagesSnapshot) eventType() string   { return "MESSAGES_SNAPSHOT" }
func (StateSnapshotEvent) eventType() string { return "STATE_SNAPSHOT" }
func (StateDelta) eventType() string         { return "STATE_DELTA" }
func (TextChunk) eventType() string          { return "TEXT_MESSAGE_CHUNK" }
func (ReasoningChunk) eventType() string     { return "REASONING_MESSAGE_CHUNK" }
func (ReasoningSummary) eventType() string   { return "REASONING_SUMMARY" }
func (ToolCallStart) eventType() string      { return "TOOL_CALL_START" }
func (ToolCallArgs) eventType() string       { return "TOOL_CALL_ARGS" }
func (ToolCallEnd) eventType() string        { return "TOOL_CALL_END" }
func (ToolCallResult) eventType() string     { return "TOOL_CALL_RESULT" }
func (ActivitySnapshot) eventType() string   { return "ACTIVITY_SNAPSHOT" }
func (ActivityDelta) eventType() string      { return "ACTIVITY_DELTA" }
func (RunFinished) eventType() string        { return "RUN_FINISHED" }
func (RunError) eventType() string           { return "RUN_ERROR" }

// EncodeEvent is an event's frame: its type, the time it was emitted unless
// none is given, then its own members.
func EncodeEvent(e Event, at string) ([]byte, error) {
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
