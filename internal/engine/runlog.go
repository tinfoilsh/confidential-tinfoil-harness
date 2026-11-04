package engine

import (
	"errors"
	"sync"
	"time"
)

const (
	// A run holds its recent frames in memory for clients that follow it
	// again. Past this budget they are folded into a checkpoint, so a run's
	// share of the enclave is the chat it showed, the reply it is writing
	// and this budget.
	runLogBudget = 32 << 20
	// mergedFrameBytes bounds a frame a checkpoint merges deltas into.
	mergedFrameBytes = 1 << 20
	RunTimeout       = 30 * time.Minute
)

var ErrDone = errors.New("run finished")

// frameKind is what a checkpoint does with a frame.
type frameKind uint8

const (
	// frameKept is sent again as it was to a follower behind a checkpoint.
	frameKept frameKind = iota
	// frameDelta extends a message, its reasoning or a call's arguments. A
	// checkpoint merges each run of deltas to the same one into one frame.
	frameDelta
	// frameActivity reports a tool's activity, which a checkpoint sends as
	// it stands in the place of the activity's first frame.
	frameActivity
)

type entry struct {
	data []byte
	kind frameKind
	key  string // what a delta extends, or whose activity a frame reports
}

type FrameLog struct {
	mu sync.Mutex
	// base is the cursor of frames[0]. The frames before it are folded into
	// checkpoint: the same events, with deltas merged and each tool's
	// activity as it stood.
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
func (l *FrameLog) grow(frame []byte, kind frameKind, key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed == nil {
		l.frames, l.bytes = append(l.frames, entry{frame, kind, key}), l.bytes+len(frame)
		l.signal()
	}
	return l.bytes > l.budget
}

// fold replaces every frame the log holds with a checkpoint of the same
// events: kept frames as they were, each run of deltas to the same target
// merged by merge into a frame of at most mergedFrameBytes followed by the
// run's last delta, and each tool's activity as activity reports it, in the
// place of its first frame. It wipes the frames it drops.
func (l *FrameLog) fold(merge func(deltas [][]byte) []byte, activity func(key string) []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed != nil {
		return
	}
	var checkpoint, run []entry
	size := 0
	// flush puts the run of deltas so far in the checkpoint: every one but
	// the last merged at the time of the first, then the last as it was, so
	// a client still sees when the run began and when it last grew, as it
	// times reasoning by them.
	flush := func() {
		if len(run) > 2 {
			merged := run[:len(run)-1]
			deltas := make([][]byte, len(merged))
			for i, e := range merged {
				deltas[i] = e.data
			}
			checkpoint = append(checkpoint, entry{merge(deltas), frameDelta, run[0].key})
			for _, e := range merged {
				clear(e.data)
			}
			run = run[len(run)-1:]
		}
		checkpoint = append(checkpoint, run...)
		run, size = nil, 0
	}
	placed := map[string]bool{}
	for _, e := range append(l.checkpoint, l.frames...) {
		if e.kind == frameDelta && len(run) > 0 && run[0].key == e.key && size+len(e.data) <= mergedFrameBytes {
			run, size = append(run, e), size+len(e.data)
			continue
		}
		flush()
		switch e.kind {
		case frameDelta:
			run, size = []entry{e}, len(e.data)
			continue
		case frameKept:
			checkpoint = append(checkpoint, e)
			continue
		case frameActivity:
			if !placed[e.key] {
				placed[e.key] = true
				if now := activity(e.key); now != nil {
					checkpoint = append(checkpoint, entry{now, frameActivity, e.key})
				}
			}
		}
		clear(e.data)
	}
	flush()
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
