package main

import (
	"context"
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
}

func TestAPanickingToolCallDoesNotTakeTheRun(t *testing.T) {
	// byName holds the call's name, so invoke gets past the widget branch and
	// panics on the nil session behind it.
	set := &toolset{byName: map[string]*session{"boom": nil}}
	set.byName["boom"] = &session{}
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
