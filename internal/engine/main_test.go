package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func TestARunPastItsLogBudgetGoesOnFromACheckpoint(t *testing.T) {
	rn := NewLog("00")
	rn.Frames.budget = 4 << 10
	// The request that started the run is draining it.
	rn.Frames.Attach()
	stopped := false
	rn.stop = func() { stopped = true }
	question := "Search, then write at length"
	shown := domain.ThreadWindow{Messages: []domain.MessageView{{ID: "question", Role: "user", Content: &question}}}
	assembler := domain.NewMessageAssembler()
	out := &Stream{Run: rn}
	out.emit(domain.RunStarted{ThreadID: "thread", RunID: rn.ID})
	out.emit(fittedSnapshot(shown.Messages, false))
	out.emit(domain.StateSnapshotEvent{})
	out.Observe, out.checkpoint = assembler.Apply, checkpointOf(shown, assembler)
	out.callStart("call", "search", "reply")
	out.activity(ToolCall{ID: "call", Name: "search"})
	out.progress("call", "Read 3 pages", 1)
	out.emit(domain.ToolCallResult{MessageID: "result", ToolCallID: "call", Role: "tool", Content: json.RawMessage(`"pages"`)})
	var text strings.Builder
	for i := 0; text.Len() <= 3*rn.Frames.budget; i++ {
		chunk := fmt.Sprintf("[%d]%s", i, strings.Repeat("x", 100))
		text.WriteString(chunk)
		out.text("reply", chunk)
		if rn.Frames.bytes > rn.Frames.budget {
			t.Fatalf("the log holds %d bytes of frames past its checkpoint", rn.Frames.bytes)
		}
	}
	// As a finishing run does, the run stops checkpointing before its end.
	out.Observe, out.checkpoint = nil, nil
	out.emit(domain.RunFinished{ThreadID: "thread", RunID: rn.ID})
	if stopped {
		t.Fatal("a run past its log budget was stopped")
	}
	base := rn.Frames.base
	if base == 0 {
		t.Fatal("the log never folded its frames into a checkpoint")
	}
	rn.Frames.close(ErrDone)

	// A client following from the start, or from any cursor before the
	// checkpoint, is sent it, then the frames after it, and ends with the
	// whole reply once.
	for _, after := range []string{"", "3", strconv.Itoa(base - 2)} {
		from, err := ResumeFrom(rn.Frames, after)
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		Follow(w, httptest.NewRequest("POST", "/v1/threads/follow", nil), rn, from)
		frames := followed(t, w)
		types, reply := []string{}, ""
		for i, f := range frames {
			types = append(types, f.Type)
			// The checkpoint's last frame has the cursor before the first
			// frame after it, and its others none.
			want := base + i - 5
			if i < 4 && f.ID != nil || i >= 4 && (f.ID == nil || *f.ID != want) {
				t.Fatalf("after %q, frame %d (%s) has cursor %v, not %d", after, i, f.Type, f.ID, want)
			}
			if f.Type == "MESSAGES_SNAPSHOT" {
				if len(f.Messages) != 2 || f.Messages[0].ID != "question" || f.Messages[1].ID != "reply" || f.Messages[1].Timeline[0].ToolCallID != "call" || string(f.Messages[1].Timeline[0].Result) != `"pages"` {
					t.Fatalf("after %q, the checkpoint's snapshot is %+v", after, f.Messages)
				}
				reply = f.Messages[1].Timeline[1].Content
			}
			reply += f.Delta
		}
		if !slices.Equal(types[:5], []string{"RUN_STARTED", "MESSAGES_SNAPSHOT", "STATE_SNAPSHOT", "ACTIVITY_SNAPSHOT", "ACTIVITY_DELTA"}) || types[5] != "TEXT_MESSAGE_CHUNK" {
			t.Fatalf("after %q, the checkpoint and the frame after it are %v", after, types[:6])
		}
		if types[len(types)-1] != "RUN_FINISHED" || reply != text.String() {
			t.Fatalf("after %q, a client ended with %d characters of %d, and %v", after, len(reply), text.Len(), types[len(types)-1])
		}
	}
	// A client that applied the checkpoint resumes after it.
	if from, err := ResumeFrom(rn.Frames, strconv.Itoa(base-1)); err != nil || from != base {
		t.Fatalf("after the checkpoint, a client resumes at %d (%v), not %d", from, err, base)
	}

	// The log is wiped once its last client leaves.
	checkpoint, _, _, _, _ := rn.Frames.Read(0)
	rn.Frames.Detach()
	if again, frames, _, _, _ := rn.Frames.Read(0); again != nil || frames != nil || bytes.ContainsFunc(checkpoint[0], func(b rune) bool { return b != 0 }) {
		t.Fatalf("the ended log kept %d checkpoint frames and %d frames; first %.40q", len(again), len(frames), checkpoint[0])
	}
}

// A snapshot too large for a client's event leaves out its oldest messages.
func TestASnapshotFitsInAnEventAClientReads(t *testing.T) {
	big := strings.Repeat("x", maxEventBytes/3)
	messages := []domain.MessageView{{ID: "a", Role: "user", Content: &big}, {ID: "b", Role: "user", Content: &big}, {ID: "c", Role: "user", Content: &big}}
	snapshot := fittedSnapshot(messages, false)
	encoded, err := domain.EncodeEvent(snapshot, api.Timestamp())
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxEventBytes || len(snapshot.Messages) != 2 || snapshot.Messages[0].ID != "b" || !snapshot.HasOlder {
		t.Fatalf("a snapshot of %d bytes holds %d messages, has older %v", len(encoded), len(snapshot.Messages), snapshot.HasOlder)
	}
}

func TestAPanickingToolCallDoesNotTakeTheRun(t *testing.T) {
	// The search session names the call, so invoke gets past the widget
	// branch and panics on the nil MCP session behind it.
	set := &Toolset{search: &session{remote: map[string]string{"boom": "boom"}}}
	rn := NewLog("00")
	out := &Stream{Run: rn}
	message := Invoke(context.Background(), out, set, ToolCall{ID: "call_1", Name: "boom"})
	if !strings.Contains(string(message), "tool boom failed") {
		t.Fatalf("a panicking tool call answered %q", message)
	}
	if rn.Frames.closed != nil {
		t.Fatalf("a panicking tool call closed the log: %v", rn.Frames.closed)
	}
}

type followedFrame struct {
	ID       *int
	Type     string `json:"type"`
	Delta    string `json:"delta"`
	Messages []struct {
		ID       string `json:"id"`
		Timeline []struct {
			ToolCallID string          `json:"toolCallId"`
			Result     json.RawMessage `json:"result"`
			Content    string          `json:"content"`
		} `json:"timeline"`
	} `json:"messages"`
}

// followed is the frames a streamed response holds, with their cursors.
func followed(t *testing.T, w *httptest.ResponseRecorder) []followedFrame {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	frames := []followedFrame{}
	for _, event := range strings.Split(strings.TrimSuffix(w.Body.String(), "\n\n"), "\n\n") {
		var frame followedFrame
		for _, line := range strings.Split(event, "\n") {
			if id, ok := strings.CutPrefix(line, "id: "); ok {
				cursor, err := strconv.Atoi(id)
				if err != nil {
					t.Fatal(err)
				}
				frame.ID = &cursor
			} else if data, ok := strings.CutPrefix(line, "data: "); ok {
				if err := json.Unmarshal([]byte(data), &frame); err != nil {
					t.Fatal(err)
				}
			}
		}
		frames = append(frames, frame)
	}
	return frames
}
