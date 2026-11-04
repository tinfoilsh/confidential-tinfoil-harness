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
	rn.Frames.budget = 512 << 10
	// The request that started the run is draining it.
	rn.Frames.Attach()
	stopped := false
	rn.stop = func() { stopped = true }
	// The chat before the question fills half of a snapshot, and the reply
	// outgrows a whole one.
	earlier, question := strings.Repeat("e", maxEventBytes/2), "Search, then write at length"
	shown := []domain.MessageView{{ID: "earlier", Role: "user", Content: &earlier}, {ID: "question", Role: "user", Content: &question}}
	out := &Stream{Run: rn}
	out.emit(domain.RunStarted{ThreadID: "thread", RunID: rn.ID})
	out.emit(fittedSnapshot(shown, false))
	out.emit(domain.StateSnapshotEvent{})
	out.folding = true
	out.callStart("call", "search", "reply")
	out.activity(ToolCall{ID: "call", Name: "search"})
	// A tool's progress reports are folded into its activity, not kept.
	for i := range 100 {
		out.progress("call", "", float64(i)/100)
	}
	out.progress("call", "Read 3 pages", 1)
	out.emit(domain.ToolCallResult{MessageID: "result", ToolCallID: "call", Role: "tool", Content: json.RawMessage(`"pages"`)})
	var text strings.Builder
	for i := 0; text.Len() <= maxEventBytes; i++ {
		chunk := fmt.Sprintf("[%d]%s", i, strings.Repeat("x", 64<<10))
		text.WriteString(chunk)
		out.text("reply", chunk)
		if rn.Frames.bytes > rn.Frames.budget {
			t.Fatalf("the log holds %d bytes of frames past its checkpoint", rn.Frames.bytes)
		}
	}
	// As a finishing run does, the run stops checkpointing before its end.
	out.folding = false
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
	// chat the run showed and the whole reply once, in frames no larger
	// than a snapshot.
	for _, after := range []string{"", "3", strconv.Itoa(base - 2)} {
		from, err := ResumeFrom(rn.Frames, after)
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		Follow(w, httptest.NewRequest("POST", "/v1/threads/follow", nil), rn, from)
		frames := followed(t, w)
		// The checkpoint's last frame has the cursor before the first frame
		// after it, and its others none.
		last := slices.IndexFunc(frames, func(f followedFrame) bool { return f.ID != nil })
		types, reply, merged, chunks := []string{}, "", 0, 0
		for i, f := range frames {
			types = append(types, f.Type)
			if want := base - 1 + i - last; i < last && f.ID != nil || i >= last && (f.ID == nil || *f.ID != want) {
				t.Fatalf("after %q, frame %d (%s) has cursor %v, not %d", after, i, f.Type, f.ID, want)
			}
			if f.Size > maxEventBytes {
				t.Fatalf("after %q, frame %d (%s) holds %d bytes", after, i, f.Type, f.Size)
			}
			if a := f.activity(); f.Type == "ACTIVITY_SNAPSHOT" && (a.Progress != 1 || !slices.Equal(a.Output, []string{"Read 3 pages"})) {
				t.Fatalf("after %q, the checkpoint's activity is %+v", after, a)
			}
			if f.Type == "MESSAGES_SNAPSHOT" && (len(f.Messages) != 2 || f.Messages[0].ID != "earlier" || f.Messages[1].ID != "question") {
				t.Fatalf("after %q, the checkpoint's snapshot is %+v", after, f.Messages)
			}
			if f.Type == "TEXT_MESSAGE_CHUNK" && i <= last {
				merged, chunks = merged+1, chunks+strings.Count(f.Delta, "[")
			}
			reply += f.Delta
		}
		if !slices.Equal(types[:7], []string{"RUN_STARTED", "MESSAGES_SNAPSHOT", "STATE_SNAPSHOT", "TOOL_CALL_START", "ACTIVITY_SNAPSHOT", "TOOL_CALL_RESULT", "TEXT_MESSAGE_CHUNK"}) {
			t.Fatalf("after %q, the checkpoint starts %v", after, types[:7])
		}
		// The text the checkpoint holds is merged from the run's chunks.
		if merged == 0 || merged*10 > chunks {
			t.Fatalf("after %q, the checkpoint holds %d chunks in %d frames", after, chunks, merged)
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
	// A latest message carrying documents too large alone is sent with them,
	// but not their text, and the chat's own message keeps it.
	document := strings.Repeat("x", maxEventBytes)
	question := "Summarize this"
	messages = []domain.MessageView{{ID: "q", Role: "user", Content: &question, Attachments: []domain.Attachment{{ID: "doc", Kind: "document", FileName: "report.md", TextContent: document}}}}
	snapshot = fittedSnapshot(messages, false)
	if encoded, err = domain.EncodeEvent(snapshot, api.Timestamp()); err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxEventBytes || len(snapshot.Messages) != 1 || snapshot.Messages[0].Attachments[0].ID != "doc" || snapshot.Messages[0].Attachments[0].TextContent != "" || messages[0].Attachments[0].TextContent != document {
		t.Fatalf("a snapshot of %d bytes sent %+v of the document", len(encoded), snapshot.Messages[0].Attachments[0].FileName)
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
	Size     int             // the bytes of its data
	Type     string          `json:"type"`
	Delta    string          `json:"delta"`
	Content  json.RawMessage `json:"content"`
	Messages []struct {
		ID string `json:"id"`
	} `json:"messages"`
}

// activity is the tool activity an ACTIVITY_SNAPSHOT frame holds.
func (f followedFrame) activity() (a struct {
	Progress float64  `json:"progress"`
	Output   []string `json:"output"`
}) {
	json.Unmarshal(f.Content, &a)
	return a
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
				frame.Size = len(data)
			}
		}
		frames = append(frames, frame)
	}
	return frames
}
