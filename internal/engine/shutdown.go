package engine

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

var errShutdown = api.Err(503, "UPSTREAM_REFUSED", "The service is shutting down")

// Stop refuses new turns, cancels every running reply, and ends every
// queued turn.
func (eng *Engine) Stop() {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	eng.Live.closing = true
	for _, s := range eng.Live.threads {
		if s.Active != nil {
			s.Active.cancelled = true
			s.Active.Log.stop()
		}
		for _, r := range s.Queue {
			eng.endQueued(r, errShutdown)
		}
		s.Queue = nil
	}
}

// Wait waits until every started run has made its last write.
func (eng *Engine) Wait(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		eng.Live.working.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
