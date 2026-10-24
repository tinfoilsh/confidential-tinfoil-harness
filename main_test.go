package main

import (
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// caller is a request context carrying one API key.
func caller(apiKey string) context.Context {
	return context.WithValue(context.Background(), apiKeyKey{}, apiKey)
}

func TestOutgrowingTheLogStopsTheRun(t *testing.T) {
	rn := newRun("00")
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
	// A client following the run is told the same error a request is.
	w := httptest.NewRecorder()
	follow(w, httptest.NewRequest("POST", "/v1/threads/follow", nil), rn, 0)
	events := eventsIn(t, w)
	want := asAPIError(errTooLong)
	if last := events[len(events)-1]; last.Type != "RUN_ERROR" || last.Code != "RUN_LOG_FULL" || last.Code != want.Code || last.Message != want.Message {
		t.Fatalf("the run ended with %+v, not %+v", last, want)
	}
	// A client that applied that error and follows again is sent it alone,
	// under the same cursor.
	frames, _, _ := rn.log.read(0)
	from, err := resumeFrom(rn.log, strconv.Itoa(len(frames)))
	if err != nil {
		t.Fatalf("following after the error: %v", err)
	}
	again := httptest.NewRecorder()
	follow(again, httptest.NewRequest("POST", "/v1/threads/follow", nil), rn, from)
	if body := again.Body.String(); !strings.HasPrefix(body, "id: "+strconv.Itoa(len(frames))+"\ndata: {\"type\":\"RUN_ERROR\"") || strings.Count(body, "data: ") != 1 {
		t.Fatalf("following after the error sent %.200s", body)
	}
}

func TestAPanickingToolCallDoesNotTakeTheRun(t *testing.T) {
	// The search session names the call, so invoke gets past the widget
	// branch and panics on the nil MCP session behind it.
	set := &toolset{search: &session{remote: map[string]string{"boom": "boom"}}}
	rn := newRun("00")
	out := &stream{run: rn}
	message := invoke(context.Background(), out, set, toolCall{id: "call_1", name: "boom"})
	if !strings.Contains(string(message), "tool boom failed") {
		t.Fatalf("a panicking tool call answered %q", message)
	}
	if rn.log.closed != nil {
		t.Fatalf("a panicking tool call closed the log: %v", rn.log.closed)
	}
}
