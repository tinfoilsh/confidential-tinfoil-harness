package engine

import (
	"context"
	"slices"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

func (eng *Engine) CancelThread(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *RunRequest) (any, error) {
	key, err := auth.RequestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := eng.authorizedRun(p, key, in.ThreadID, in.RunID)
	if err != nil {
		return nil, err
	}
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	if r.Thread.Active != r || r.ending {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "This run is no longer active")
	}
	r.cancelled = true
	r.Log.stop()
	return struct{}{}, nil
}

// Move the selected queued turn first, then finish the active run normally.
func (eng *Engine) SendQueued(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *RunRequest) (any, error) {
	key, err := auth.RequestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := eng.authorizedRun(p, key, in.ThreadID, in.QueueID)
	if err != nil {
		return nil, err
	}
	eng.Live.mu.Lock()
	s := r.Thread
	index := slices.Index(s.Queue, r)
	if index < 0 {
		eng.Live.mu.Unlock()
		return nil, api.Err(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.Queue = append([]*Run{r}, slices.Delete(s.Queue, index, index+1)...)
	queueChanged(s)
	// A reply already stopped and finishing still hands the chat on.
	s.Active.yielded = true
	if !s.Active.ending {
		s.Active.cancelled = true
		s.Active.Log.stop()
	}
	eng.Live.mu.Unlock()
	return struct{}{}, nil
}

func (eng *Engine) RemoveQueued(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *RunRequest) (any, error) {
	key, err := auth.RequestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := eng.authorizedRun(p, key, in.ThreadID, in.QueueID)
	if err != nil {
		return nil, err
	}
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	s := r.Thread
	index := slices.Index(s.Queue, r)
	if index < 0 {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.Queue = slices.Delete(s.Queue, index, index+1)
	r.cancelled = true
	eng.endQueued(r, nil)
	queueChanged(s)
	eng.dropIdle(s)
	return struct{}{}, nil
}

// StopThread stops a chat being deleted: its running reply is cancelled and
// each queued turn ends.
func (eng *Engine) StopThread(p *auth.Principal, id string) {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	s := eng.Live.threads[liveKey(p, id)]
	if s == nil {
		return
	}
	s.deleted = true
	if s.Active != nil {
		s.Active.cancelled = true
		s.Active.Log.stop()
	}
	for _, r := range s.Queue {
		eng.endQueued(r, nil)
	}
	s.Queue = nil
	eng.dropIdle(s)
}
