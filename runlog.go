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
	errDone = errors.New("run finished")
	// errTooLong ends a run that outgrew the log this harness can hold for it.
	errTooLong = apiErr(413, "RUN_LOG_FULL", "The run log is full")
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
	// ended marks the run as ended and no longer registered, so nothing
	// more attaches; a log that is ended and that its last reader has left
	// is wiped. A log a run outgrew is closed before the run ends.
	ended   bool
	readers int
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

// attach counts a stream about to drain the log. It is called while the run
// is registered, which a run is until just before its log is ended, so the
// log still holds every frame.
func (l *runlog) attach() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.readers++
}

// detach ends a stream's draining of the log.
func (l *runlog) detach() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.readers--
	l.wipe()
}

// wipe zeroes and drops the frames of an ended log that nothing drains, so
// the run's plaintext leaves memory as its last stream ends rather than when
// the collector reuses it. l.mu is held.
func (l *runlog) wipe() {
	if !l.ended || l.readers > 0 {
		return
	}
	for _, frame := range l.frames {
		clear(frame)
	}
	l.frames = nil
}

func (l *runlog) signal() {
	close(l.wake)
	l.wake = make(chan struct{})
}

// close ends the log with err, once its run has left the registry. A log
// already closed, because the run outgrew it, keeps that error.
func (l *runlog) close(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed == nil {
		l.closed = err
		l.signal()
	}
	l.ended = true
	l.wipe()
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
