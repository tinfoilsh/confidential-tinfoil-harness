package main

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

var errShutdown = api.Err(503, "UPSTREAM_REFUSED", "The service is shutting down")

// stopRuns refuses new turns, cancels every running reply, and ends every
// queued turn.
func (eng *engine) stopRuns() {
	eng.live.mu.Lock()
	defer eng.live.mu.Unlock()
	eng.live.closing = true
	for _, s := range eng.live.threads {
		if s.active != nil {
			s.active.cancelled = true
			s.active.log.stop()
		}
		for _, r := range s.queue {
			eng.endQueued(r, errShutdown)
		}
		s.queue = nil
	}
}

// waitRuns waits until every started run has made its last write.
func (eng *engine) waitRuns(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		eng.live.working.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
