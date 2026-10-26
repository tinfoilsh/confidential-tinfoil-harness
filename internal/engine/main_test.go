package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

func TestOutgrowingTheLogStopsTheRun(t *testing.T) {
	rn := NewLog("00")
	// The request that started the run is draining it.
	rn.Frames.Attach()
	stopped := false
	rn.stop = func() { stopped = true }
	out := &Stream{Run: rn}
	for i := 0; !stopped && i <= maxRunLog; i += 1 << 16 {
		out.text("msg", strings.Repeat("x", 1<<16))
	}
	if !stopped {
		t.Fatal("a run that outgrew its log was left producing into it")
	}
	if rn.Frames.closed != ErrTooLong {
		t.Fatalf("closed = %v, want errTooLong", rn.Frames.closed)
	}
	// A client following the run is told the same error a request is.
	w := httptest.NewRecorder()
	Follow(w, httptest.NewRequest("POST", "/v1/threads/follow", nil), rn, 0)
	last := lastEventIn(t, w)
	want := api.AsError(ErrTooLong)
	if last.Type != "RUN_ERROR" || last.Code != "RUN_LOG_FULL" || last.Code != want.Code || last.Message != want.Message {
		t.Fatalf("the run ended with %+v, not %+v", last, want)
	}
	// A client that applied that error and follows again is sent it alone,
	// under the same cursor.
	frames, _, _ := rn.Frames.Read(0)
	if len(frames) == 0 {
		t.Fatal("the log kept none of the frames a client is draining")
	}
	from, err := ResumeFrom(rn.Frames, strconv.Itoa(len(frames)))
	if err != nil {
		t.Fatalf("following after the error: %v", err)
	}
	again := httptest.NewRecorder()
	Follow(again, httptest.NewRequest("POST", "/v1/threads/follow", nil), rn, from)
	if body := again.Body.String(); !strings.HasPrefix(body, "id: "+strconv.Itoa(len(frames))+"\ndata: {\"type\":\"RUN_ERROR\"") || strings.Count(body, "data: ") != 1 {
		t.Fatalf("following after the error sent %.200s", body)
	}
	// The run is still live, so a follower arriving once its client has left
	// is sent the frames; they are wiped only once the run ends.
	rn.Frames.Detach()
	kept, _, _ := rn.Frames.Read(0)
	if len(kept) == 0 {
		t.Fatal("the frames were wiped while the run was live")
	}
	rn.Frames.close(ErrDone)
	if frames, _, closed := rn.Frames.Read(0); frames != nil || closed != ErrTooLong || bytes.ContainsFunc(kept[0], func(b rune) bool { return b != 0 }) {
		t.Fatalf("the ended run kept %d frames, closed with %v; first frame %.40q", len(frames), closed, kept[0])
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

// lastEventIn is the last event a streamed response holds.
func lastEventIn(t *testing.T, w *httptest.ResponseRecorder) (last struct{ Type, Code, Message string }) {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			if err := json.Unmarshal([]byte(data), &last); err != nil {
				t.Fatal(err)
			}
		}
	}
	return last
}
