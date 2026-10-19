package main

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

const (
	// A run holds its whole log in memory, so this is its share of the enclave.
	maxRunLog  = 8 << 20
	runTimeout = 30 * time.Minute
)

var (
	errDone    = errors.New("run finished")
	errTooLong = errors.New("run outgrew the log this harness can hold for it")
)

// rescued logs a panic and turns it into an error worth surfacing. Without one
// of these on every goroutine the harness spawns, a panic in any single run
// takes the process, and with it every other run this enclave holds in memory.
// The panic value may contain plaintext, so only the stack is logged.
//
// It takes the recovered value rather than recovering itself, because recover
// works only in the function a defer names, which is one frame above this.
func rescued(what string, p any) error {
	if p == nil {
		return nil
	}
	slog.Error("recovered a panic", "in", what, "stack", string(debug.Stack()))
	return fmt.Errorf("%s failed", what)
}

type runlog struct {
	mu     sync.Mutex
	frames [][]byte
	bytes  int
	closed error
	wake   chan struct{}
}

func newRunlog() *runlog { return &runlog{wake: make(chan struct{})} }

// grow appends one frame and reports the error the log has closed with.
func (l *runlog) grow(frame []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed == nil {
		if l.bytes+len(frame) > maxRunLog {
			l.closed = errTooLong
		} else {
			l.frames, l.bytes = append(l.frames, frame), l.bytes+len(frame)
		}
		l.signal()
	}
	return l.closed
}

func (l *runlog) signal() {
	close(l.wake)
	l.wake = make(chan struct{})
}

func (l *runlog) close(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed != nil {
		return
	}
	l.closed = err
	l.signal()
}

func (l *runlog) read(from int) ([][]byte, <-chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if from < len(l.frames) {
		return l.frames[from:], l.wake, l.closed
	}
	return nil, l.wake, l.closed
}

type run struct {
	id   string
	log  *runlog
	stop func() // cancels the run itself
}

func newRun(id string) *run {
	return &run{id: id, log: newRunlog(), stop: func() {}}
}
