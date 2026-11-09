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
	// RunTimeout bounds a reply, and researchTimeout one that researches.
	// What a run holds for as long as it lasts is sized from them: its
	// deadline and the marker that tells other clients a reply is
	// streaming from its own, and its storage grant from the longer.
	RunTimeout      = 30 * time.Minute
	researchTimeout = researchTime + researchReportTime
	longestRun      = max(RunTimeout, researchTimeout)
	// runGrant is how long a run may use its chat's rows: twice what the
	// longest run and its last writes take, so a turn that waited a while
	// in its chat's queue starts without asking again.
	runGrant = 2 * (longestRun + CommitTimeout)
)

// runBudget is how long a run may last.
func runBudget(research bool) time.Duration {
	if research {
		return researchTimeout
	}
	return RunTimeout
}

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
	// held keeps an ended log from being wiped while the client of its
	// device chat may still collect it.
	held bool
	// A stream sends the frames it read after the lock is released, so the
	// frames a fold drops are kept in retired until every stream that read
	// before the fold has sent them. folds counts the folds, and lent the
	// streams holding frames by the folds made before they read them.
	folds   int
	lent    map[int]int
	retired []retiredFrames
}

// retiredFrames are the frames a fold dropped, and the folds made with it.
type retiredFrames struct {
	folds  int
	frames [][]byte
}

// Readers is how many streams are draining the log.
func (l *FrameLog) Readers() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining
}

func newRunlog() *FrameLog {
	return &FrameLog{wake: make(chan struct{}), budget: runLogBudget, lent: map[int]int{}}
}

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
// place of its first frame. It wipes the frames it drops once no stream
// holds them.
func (l *FrameLog) fold(merge func(deltas [][]byte) []byte, activity func(key string) []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed != nil {
		return
	}
	var checkpoint, run []entry
	var dropped [][]byte
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
			dropped = append(dropped, deltas...)
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
		dropped = append(dropped, e.data)
	}
	flush()
	l.checkpoint, l.base, l.frames, l.bytes = checkpoint, l.base+len(l.frames), nil, 0
	l.folds++
	l.retired = append(l.retired, retiredFrames{l.folds, dropped})
	l.reclaim()
}

// reclaim wipes the retired frames no stream holds, those every stream
// holding frames read after their fold. l.mu is held.
func (l *FrameLog) reclaim() {
	oldest := l.folds
	for folds := range l.lent {
		oldest = min(oldest, folds)
	}
	kept := l.retired[:0]
	for _, r := range l.retired {
		if r.folds > oldest {
			kept = append(kept, r)
			continue
		}
		for _, frame := range r.frames {
			clear(frame)
		}
	}
	clear(l.retired[len(kept):])
	l.retired = kept
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
	if !l.ended || l.draining > 0 || l.held {
		return
	}
	for _, e := range append(l.checkpoint, l.frames...) {
		clear(e.data)
	}
	for _, r := range l.retired {
		for _, frame := range r.frames {
			clear(frame)
		}
	}
	l.checkpoint, l.frames, l.retired = nil, nil, nil
}

func (l *FrameLog) hold() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = true
}

// release ends a hold, wiping the log if its run ended and nothing drains it.
func (l *FrameLog) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = false
	l.wipe()
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
// holds is sent their checkpoint first. The frames stay intact until the
// follower calls done, as it does once it has sent them.
func (l *FrameLog) Read(from int) (checkpoint, frames [][]byte, first int, wake <-chan struct{}, closed error, done func()) {
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
	folds := l.folds
	l.lent[folds]++
	done = func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.lent[folds]--; l.lent[folds] == 0 {
			delete(l.lent, folds)
		}
		l.reclaim()
	}
	return checkpoint, frames, from, l.wake, l.closed, done
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
