// Package jobs runs one background job per owner at a time and keeps its
// last report for a while after it ends, so a repeated request sees the
// job it started rather than starting another.
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// State is where a job is in its life.
type State string

const (
	Running  State = "running"
	Complete State = "complete"
	Failed   State = "failed"
)

// Status is where a job stands: its state, its latest report, the error
// that failed it, and when it started and last changed.
type Status[R any] struct {
	State     State
	Report    R
	Err       error
	StartedAt time.Time
	UpdatedAt time.Time
}

// Runner starts jobs and finds the one an owner already has.
type Runner[R any] struct {
	budget, retention time.Duration

	mu   sync.Mutex
	jobs map[string]*Job[R]
}

// New is a runner whose jobs end at the budget and stay findable for the
// retention after they end.
func New[R any](budget, retention time.Duration) *Runner[R] {
	return &Runner[R]{budget: budget, retention: retention, jobs: map[string]*Job[R]{}}
}

// Start runs work for owner unless a job of theirs is running or ended within
// the retention, in which case that job is returned and started is false.
// work runs detached from ctx (its values kept, its cancellation ignored)
// with the budget as its deadline; it reports progress through report; a
// panic is logged with its stack and ends it as failed.
func (r *Runner[R]) Start(ctx context.Context, owner string, work func(ctx context.Context, report func(R)) (R, error)) (*Job[R], bool) {
	return r.start(ctx, owner, work, false)
}

// Restart is Start for an owner whose last job gives way once it has
// ended: a running job is returned as Start returns it, an ended one is
// forgotten and the new one started.
func (r *Runner[R]) Restart(ctx context.Context, owner string, work func(ctx context.Context, report func(R)) (R, error)) (*Job[R], bool) {
	return r.start(ctx, owner, work, true)
}

func (r *Runner[R]) start(ctx context.Context, owner string, work func(ctx context.Context, report func(R)) (R, error), replaceEnded bool) (*Job[R], bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if job, ok := r.jobs[owner]; ok {
		if !replaceEnded || job.Status().State == Running {
			return job, false
		}
		delete(r.jobs, owner)
	}
	now := time.Now()
	job := &Job[R]{id: newID(), status: Status[R]{State: Running, StartedAt: now, UpdatedAt: now}, done: make(chan struct{})}
	// The job's context is made here, so a cancel can end it before its
	// goroutine has run.
	ctx, job.cancel = context.WithTimeout(context.WithoutCancel(ctx), r.budget)
	r.jobs[owner] = job
	go r.run(ctx, owner, job, work)
	return job, true
}

// Get is the job owner has, running or ended within the retention, or nil.
func (r *Runner[R]) Get(owner string) *Job[R] {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.jobs[owner]
}

// idBytes is the length of a job's id before hex.
const idBytes = 16

// newID names a job so a client can tell the one it started from the
// owner's next.
func newID() string {
	var b [idBytes]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Cancel ends owner's job, running or ended, and forgets it, so the next
// Start begins at once. A running job's work sees its context end; the
// job returned, if any, is done once that work has returned.
func (r *Runner[R]) Cancel(owner string) *Job[R] {
	r.mu.Lock()
	job := r.jobs[owner]
	delete(r.jobs, owner)
	r.mu.Unlock()
	if job != nil {
		job.cancel()
	}
	return job
}

func (r *Runner[R]) run(ctx context.Context, owner string, job *Job[R], work func(context.Context, func(R)) (R, error)) {
	defer job.cancel()
	final, err := guarded(ctx, job.report, work)
	// The job ends and, without a retention, leaves the runner in one
	// step, so whoever sees it done finds the owner free.
	r.mu.Lock()
	job.finish(final, err)
	if r.retention <= 0 && r.jobs[owner] == job {
		delete(r.jobs, owner)
	}
	r.mu.Unlock()
	if r.retention > 0 {
		time.AfterFunc(r.retention, func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.jobs[owner] == job {
				delete(r.jobs, owner)
			}
		})
	}
}

// errPanicked ends a job whose work panicked. The panic's value may hold
// what the job was working on, so only the stack is logged.
var errPanicked = errors.New("jobs: the job panicked")

// guarded runs work, turning a panic into its error, with the stack in
// the log. A panic returns no final report, which leaves the last one the
// work made.
func guarded[R any](ctx context.Context, report func(R), work func(context.Context, func(R)) (R, error)) (final *R, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("recovered a panic", "in", "a background job", "stack", string(debug.Stack()))
			final, err = nil, errPanicked
		}
	}()
	result, err := work(ctx, report)
	return &result, err
}

// Job is one owner's background work.
type Job[R any] struct {
	id     string
	mu     sync.Mutex
	status Status[R]
	done   chan struct{}
	cancel context.CancelFunc
}

// ID names the job, unlike any other's.
func (j *Job[R]) ID() string { return j.id }

// Status is where the job stands now.
func (j *Job[R]) Status() Status[R] {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.status
}

// Done is closed once the job has ended.
func (j *Job[R]) Done() <-chan struct{} { return j.done }

func (j *Job[R]) report(progress R) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status.Report, j.status.UpdatedAt = progress, time.Now()
}

func (j *Job[R]) finish(final *R, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status.UpdatedAt = time.Now()
	if err != nil {
		j.status.State, j.status.Err = Failed, err
	} else {
		j.status.State = Complete
	}
	if final != nil {
		j.status.Report = *final
	}
	close(j.done)
}
