package main

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

var errShutdown = api.Err(503, "UPSTREAM_REFUSED", "The service is shutting down")

// stopRuns refuses new turns, cancels every running reply, and ends every
// queued turn.
func (h *harness) stopRuns() {
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	h.live.closing = true
	for _, s := range h.live.threads {
		if s.active != nil {
			s.active.cancelled = true
			s.active.log.stop()
		}
		for _, r := range s.queue {
			h.endQueued(r, errShutdown)
		}
		s.queue = nil
	}
}

// waitRuns waits until every started run has made its last write.
func (h *harness) waitRuns(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		h.live.working.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
