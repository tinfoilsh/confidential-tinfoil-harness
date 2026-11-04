package engine

import (
	"errors"
	"sync"
	"time"
)

const (
	// A run holds its recent frames in memory for clients that follow it
	// again. Past this budget they are folded into a checkpoint, so a run's
	// share of the enclave is the reply it is writing and this budget.
	runLogBudget = 32 << 20
	RunTimeout   = 30 * time.Minute
)

var ErrDone = errors.New("run finished")

// frameKind is what a checkpoint does with a frame.
type frameKind uint8

const (
	// frameFolded is reply content, which the checkpoint's snapshot holds.
	frameFolded frameKind = iota
	// frameKept is sent again as it was to a follower behind a checkpoint.
	frameKept
	// frameSnapshot is the chat's messages, which a checkpoint replaces
	// with its snapshots of the chat and the tools' activity so far.
	frameSnapshot
)

type entry struct {
	data []byte
	kind frameKind
}

type FrameLog struct {
	mu sync.Mutex
	// base is the cursor of frames[0]. The frames before it are folded into
	// checkpoint: the frames kept as they were sent, with the snapshot in
	// place of the chat's messages.
	base       int
	checkpoint []entry
	frames     []entry
	bytes      int // the size of frames
	budget     int // the size of frames past which they are folded
	closed     error
	wake       chan struct{}
	// ended marks the run as ended and no longer registered, so nothing
	// more attaches; a log that is ended and that its last reader has left
	// is wiped.
	ended    bool
	draining int
}

// Readers is how many streams are draining the log.
func (l *FrameLog) Readers() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining
}

func newRunlog() *FrameLog { return &FrameLog{wake: make(chan struct{}), budget: runLogBudget} }

// grow appends one frame and reports whether the frames since the last
// checkpoint outgrew the budget.
func (l *FrameLog) grow(frame []byte, kind frameKind) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed == nil {
		l.frames, l.bytes = append(l.frames, entry{frame, kind}), l.bytes+len(frame)
		l.signal()
	}
	return l.bytes > l.budget
}

// fold replaces every frame the log holds with a checkpoint: the frames kept
// as they were, with snapshots in the place of the chat's first, and wipes
// the frames it drops.
func (l *FrameLog) fold(snapshots [][]byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed != nil {
		return
	}
	checkpoint, placed := []entry{}, false
	place := func() {
		for _, snapshot := range snapshots {
			checkpoint = append(checkpoint, entry{snapshot, frameSnapshot})
		}
		placed = true
	}
	for _, e := range append(l.checkpoint, l.frames...) {
		switch {
		case e.kind == frameKept:
			checkpoint = append(checkpoint, e)
			continue
		case e.kind == frameSnapshot && !placed:
			place()
		}
		clear(e.data)
	}
	if !placed {
		place()
	}
	l.checkpoint, l.base, l.frames, l.bytes = checkpoint, l.base+len(l.frames), nil, 0
}

// Attach counts a stream about to drain the log. It is called while the run
// is registered, which a run is until just before its log is ended, so the
// log still holds every frame or their checkpoint.
func (l *FrameLog) Attach() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.draining++
}

// Detach ends a stream's draining of the log.
func (l *FrameLog) Detach() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.draining--
	l.wipe()
}

// wipe zeroes and drops the frames of an ended log that nothing drains, so
// the run's plaintext leaves memory as its last stream ends rather than when
// the collector reuses it. l.mu is held.
func (l *FrameLog) wipe() {
	if !l.ended || l.draining > 0 {
		return
	}
	for _, e := range append(l.checkpoint, l.frames...) {
		clear(e.data)
	}
	l.checkpoint, l.frames = nil, nil
}

func (l *FrameLog) signal() {
	close(l.wake)
	l.wake = make(chan struct{})
}

// close ends the log with err, once its run has left the registry.
func (l *FrameLog) close(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed == nil {
		l.closed = err
		l.signal()
	}
	l.ended = true
	l.wipe()
}

// Read is what a follower at cursor from is sent next: the frames from it,
// and the cursor of the first of them. A follower behind the frames the log
// holds is sent their checkpoint first.
func (l *FrameLog) Read(from int) (checkpoint, frames [][]byte, first int, wake <-chan struct{}, closed error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if from < l.base {
		for _, e := range l.checkpoint {
			checkpoint = append(checkpoint, e.data)
		}
		from = l.base
	}
	for _, e := range l.frames[min(from-l.base, len(l.frames)):] {
		frames = append(frames, e.data)
	}
	return checkpoint, frames, from, l.wake, l.closed
}

// sent is the cursor after the last frame the log appended.
func (l *FrameLog) sent() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.base + len(l.frames)
}

type Log struct {
	ID     string
	Frames *FrameLog
	stop   func() // cancels the run itself
}

func NewLog(id string) *Log {
	return &Log{ID: id, Frames: newRunlog(), stop: func() {}}
}
