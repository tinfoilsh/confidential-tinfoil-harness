package main

import (
	"context"
	"slices"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

func (h *harness) cancelThread(ctx context.Context, p *principal, _ contentKey, in *runRequest) (any, error) {
	key, err := requestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := h.authorizedRun(p, key, in.ThreadID, in.RunID)
	if err != nil {
		return nil, err
	}
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	if r.thread.active != r || r.ending {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "This run is no longer active")
	}
	r.cancelled = true
	r.log.stop()
	return struct{}{}, nil
}

// Move the selected queued turn first, then finish the active run normally.
func (h *harness) sendQueued(ctx context.Context, p *principal, _ contentKey, in *runRequest) (any, error) {
	key, err := requestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := h.authorizedRun(p, key, in.ThreadID, in.QueueID)
	if err != nil {
		return nil, err
	}
	h.live.mu.Lock()
	s := r.thread
	index := slices.Index(s.queue, r)
	if index < 0 {
		h.live.mu.Unlock()
		return nil, api.Err(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.queue = append([]*chatRun{r}, slices.Delete(s.queue, index, index+1)...)
	queueChanged(s)
	// A reply already stopped and finishing still hands the chat on.
	s.active.yielded = true
	if !s.active.ending {
		s.active.cancelled = true
		s.active.log.stop()
	}
	h.live.mu.Unlock()
	return struct{}{}, nil
}

func (h *harness) removeQueued(ctx context.Context, p *principal, _ contentKey, in *runRequest) (any, error) {
	key, err := requestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := h.authorizedRun(p, key, in.ThreadID, in.QueueID)
	if err != nil {
		return nil, err
	}
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	s := r.thread
	index := slices.Index(s.queue, r)
	if index < 0 {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.queue = slices.Delete(s.queue, index, index+1)
	r.cancelled = true
	h.endQueued(r, nil)
	queueChanged(s)
	h.dropIdle(s)
	return struct{}{}, nil
}

// stopThread stops a chat being deleted: its running reply is cancelled and
// each queued turn ends.
func (h *harness) stopThread(p *principal, id string) {
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	s := h.live.threads[liveKey(p, id)]
	if s == nil {
		return
	}
	s.deleted = true
	if s.active != nil {
		s.active.cancelled = true
		s.active.log.stop()
	}
	for _, r := range s.queue {
		h.endQueued(r, nil)
	}
	s.queue = nil
	h.dropIdle(s)
}
