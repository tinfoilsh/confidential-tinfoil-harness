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
	if eng.removeQueued(r) {
		return struct{}{}, nil
	}
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
	if !eng.removeQueued(r) {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	return struct{}{}, nil
}

// removeQueued ends a turn still waiting in its chat's queue, and reports
// whether it was waiting; live.mu is held.
func (eng *Engine) removeQueued(r *Run) bool {
	s := r.Thread
	index := slices.Index(s.Queue, r)
	if index < 0 {
		return false
	}
	s.Queue = slices.Delete(s.Queue, index, index+1)
	r.cancelled = true
	eng.endQueued(r, nil)
	queueChanged(s)
	eng.dropIdle(s)
	return true
}

// StopThread stops a chat being deleted: its running reply is cancelled and
// each queued turn ends.
func (eng *Engine) StopThread(p *auth.Principal, id string) {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	if s := eng.Live.threads[liveKey(p, id)]; s != nil {
		eng.stop(s)
	}
}

// StopDeviceThread stops a device chat its client deleted, as StopThread
// stops a chat, and drops the replies kept of it. A request that names a
// device chat carries no content key, so it stops nothing else.
func (eng *Engine) StopDeviceThread(p *auth.Principal, id string) {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	if s := eng.Live.threads[liveKey(p, id)]; s != nil && isDevice(s) {
		eng.stop(s)
	}
	eng.Live.dropHeld(func(h *heldRun) bool { return h.owner == p.Scope() && h.threadID == id })
}

// StopDeviceThreads stops every device chat of the caller, as
// StopDeviceThread stops one, when the caller deletes them all. It returns
// how many chats it held anything of.
func (eng *Engine) StopDeviceThreads(p *auth.Principal) int {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	chats := map[string]bool{}
	for _, s := range eng.Live.threads {
		if s.owner == p.Scope() && isDevice(s) {
			chats[s.ID] = true
			eng.stop(s)
		}
	}
	eng.Live.dropHeld(func(h *heldRun) bool {
		if h.owner != p.Scope() {
			return false
		}
		chats[h.threadID] = true
		return true
	})
	return len(chats)
}

// isDevice reports whether a chat is kept by its client's device, which
// sends it with each turn and needs no content key.
func isDevice(s *threadSession) bool { return s.local && s.keyless }

// stop ends what a chat being deleted runs; live.mu is held.
func (eng *Engine) stop(s *threadSession) {
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
