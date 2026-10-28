package jobs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type ownerKey struct{}

// ample is a budget no test's work reaches.
const ample = time.Minute

func TestASecondStartWhileRunningReturnsTheSameJobDetachedFromItsCaller(t *testing.T) {
	r := New[int](ample, time.Hour)
	release := make(chan struct{})
	seen := make(chan string, 1)
	work := func(ctx context.Context, _ func(int)) (int, error) {
		<-release
		owner, _ := ctx.Value(ownerKey{}).(string)
		if ctx.Err() != nil {
			owner = "canceled"
		}
		seen <- owner
		return 1, nil
	}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ownerKey{}, "alice"))
	first, started := r.Start(ctx, "alice", work)
	if !started {
		t.Fatal("the first start for an owner did not start a job")
	}
	if again, started := r.Start(context.Background(), "alice", work); started || again != first {
		t.Fatalf("a second start while running: started=%v same job=%v", started, again == first)
	}
	if other, started := r.Start(context.Background(), "bob", func(context.Context, func(int)) (int, error) { return 0, nil }); !started || other == first {
		t.Fatalf("another owner's start: started=%v same job=%v", started, other == first)
	}
	if status := first.Status(); status.State != Running {
		t.Fatalf("state while running = %s", status.State)
	}
	// The caller going away does not end its job.
	cancel()
	close(release)
	<-first.Done()
	if owner := <-seen; owner != "alice" {
		t.Fatalf("the work ran under %q, want the caller's values without its cancellation", owner)
	}
	if status := first.Status(); status.State != Complete || status.Report != 1 || status.Err != nil {
		t.Fatalf("status after the work returned = %+v", status)
	}
}

func TestAnEndedJobIsReturnedWithinTheRetentionAndReplacedAfterIt(t *testing.T) {
	work := func(context.Context, func(int)) (int, error) { return 7, nil }
	kept := New[int](ample, time.Hour)
	first, _ := kept.Start(context.Background(), "alice", work)
	<-first.Done()
	again, started := kept.Start(context.Background(), "alice", work)
	if started || again != first || again.Status().Report != 7 {
		t.Fatalf("a start within the retention: started=%v same job=%v report=%d", started, again == first, again.Status().Report)
	}

	forgetting := New[int](ample, 0)
	first, _ = forgetting.Start(context.Background(), "alice", work)
	<-first.Done()
	if next, started := forgetting.Start(context.Background(), "alice", work); !started || next == first {
		t.Fatalf("a start after the retention: started=%v same job=%v", started, next == first)
	}
}

func TestProgressShowsBeforeTheJobEnds(t *testing.T) {
	r := New[int](ample, 0)
	reported, release := make(chan struct{}), make(chan struct{})
	job, _ := r.Start(context.Background(), "alice", func(_ context.Context, report func(int)) (int, error) {
		report(1)
		close(reported)
		<-release
		return 2, nil
	})
	<-reported
	if status := job.Status(); status.State != Running || status.Report != 1 {
		t.Fatalf("status after the first report = %+v, want running at 1", status)
	}
	close(release)
	<-job.Done()
	if status := job.Status(); status.State != Complete || status.Report != 2 {
		t.Fatalf("status at the end = %+v, want complete at 2", status)
	}
}

func TestAPanicEndsTheJobFailedAndTheRunnerStartsTheNext(t *testing.T) {
	r := New[int](ample, 0)
	job, _ := r.Start(context.Background(), "alice", func(_ context.Context, report func(int)) (int, error) {
		report(3)
		panic("a nil map")
	})
	<-job.Done()
	status := job.Status()
	// The panic's value stays out of the error: it may hold what the job
	// was working on.
	if status.State != Failed || !errors.Is(status.Err, errPanicked) || strings.Contains(status.Err.Error(), "a nil map") || status.Report != 3 {
		t.Fatalf("status after a panic = %+v, want failed without the panic's value and with the last report", status)
	}
	next, started := r.Start(context.Background(), "alice", func(context.Context, func(int)) (int, error) { return 1, nil })
	if !started {
		t.Fatal("the owner's next job did not start after a panic")
	}
	<-next.Done()
	if status := next.Status(); status.State != Complete || status.Report != 1 {
		t.Fatalf("the next job's status = %+v", status)
	}
}

func TestTheBudgetCancelsTheWork(t *testing.T) {
	r := New[int](time.Millisecond, 0)
	job, _ := r.Start(context.Background(), "alice", func(ctx context.Context, _ func(int)) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	<-job.Done()
	if status := job.Status(); status.State != Failed || !errors.Is(status.Err, context.DeadlineExceeded) {
		t.Fatalf("status after the budget = %+v, want failed at the deadline", status)
	}
}
