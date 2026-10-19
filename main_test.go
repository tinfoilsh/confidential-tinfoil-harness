package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

// caller is a request context carrying one API key.
func caller(apiKey string) context.Context {
	return context.WithValue(context.Background(), apiKeyKey{}, apiKey)
}

func mustRun(t *testing.T, id string) *run {
	t.Helper()
	rn, err := newContentRun(id, contentKey{})
	if err != nil {
		t.Fatal(err)
	}
	return rn
}

func TestFollowReplaysFromCursorAndEndsOnClose(t *testing.T) {
	rn := mustRun(t, "00")
	out := &stream{run: rn}
	out.emit(event{Type: "RUN_STARTED"})
	out.emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "one"})
	out.emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "two"})
	out.emit(event{Type: "RUN_FINISHED"})
	rn.log.close(errDone)

	whole := httptest.NewRecorder()
	follow(whole, httptest.NewRequest("POST", "/v1/threads/follow", nil), rn, 0)
	if got := strings.Count(whole.Body.String(), "data: "); got != 4 {
		t.Fatalf("whole log: got %d frames, want 4", got)
	}
	if !strings.HasPrefix(whole.Body.String(), "id: 0\ndata: {\"type\":\"RUN_STARTED\"") {
		t.Fatalf("whole log did not open with RUN_STARTED: %q", whole.Body.String())
	}

	rest := httptest.NewRecorder()
	follow(rest, httptest.NewRequest("POST", "/v1/threads/follow", nil), rn, 2)
	body := rest.Body.String()
	if strings.Contains(body, "one") || !strings.Contains(body, "two") {
		t.Fatalf("resume from 2 replayed the wrong frames: %q", body)
	}
	if !strings.Contains(body, `"id": 3`) && !strings.Contains(body, "id: 3") {
		t.Fatalf("resume lost its ids: %q", body)
	}
}

func TestOutgrowingTheLogStopsTheRun(t *testing.T) {
	rn := mustRun(t, "00")
	stopped := false
	rn.stop = func() { stopped = true }
	out := &stream{run: rn}
	for i := 0; !stopped && i <= maxRunLog; i += 1 << 16 {
		out.text("msg", strings.Repeat("x", 1<<16))
	}
	if !stopped {
		t.Fatal("a run that outgrew its log was left producing into it")
	}
	if rn.log.closed != errTooLong {
		t.Fatalf("closed = %v, want errTooLong", rn.log.closed)
	}
}

func TestLogWithoutATerminalFrameReplaysAsAbandoned(t *testing.T) {
	rn := mustRun(t, "00")
	(&stream{run: rn}).emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "half"})
	var wire []byte
	for _, frame := range rn.log.frames {
		wire = append(wire, byte(len(frame)>>24), byte(len(frame)>>16), byte(len(frame)>>8), byte(len(frame)))
		wire = append(wire, frame...)
	}
	back := mustRun(t, "00")
	if err := back.rehydrate(wire); err != nil {
		t.Fatal(err)
	}
	if back.log.closed != errAbandoned {
		t.Fatalf("closed = %v, want errAbandoned", back.log.closed)
	}
	rec := httptest.NewRecorder()
	follow(rec, httptest.NewRequest("POST", "/v1/threads/follow", nil), back, 0)
	if !strings.Contains(rec.Body.String(), "RUN_ERROR") {
		t.Fatalf("abandoned run did not report itself: %q", rec.Body.String())
	}
}

func TestAPanickingToolCallDoesNotTakeTheRun(t *testing.T) {
	// byName holds the call's name, so invoke gets past the widget branch and
	// panics on the nil session behind it.
	set := &toolset{byName: map[string]*session{"boom": nil}}
	set.byName["boom"] = &session{}
	rn := mustRun(t, "00")
	out := &stream{run: rn}
	message := invoke(context.Background(), out, set, toolCall{id: "call_1", name: "boom"})
	if !strings.Contains(string(message), "tool boom failed") {
		t.Fatalf("a panicking tool call answered %q", message)
	}
	if rn.log.closed != nil {
		t.Fatalf("a panicking tool call closed the log: %v", rn.log.closed)
	}
}
