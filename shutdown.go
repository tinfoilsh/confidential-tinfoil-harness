package main

import "context"

func (h *harness) stopRuns() []*chatRun {
	h.chatMu.Lock()
	h.closing = true
	states := []*threadSession{}
	for _, state := range h.threads {
		states = append(states, state)
	}
	h.chatMu.Unlock()
	active := []*chatRun{}
	for _, state := range states {
		state.mu.Lock()
		if state.active != nil {
			state.active.cancelled = true
			state.active.log.stop()
			active = append(active, state.active)
		}
		for _, run := range state.queue {
			run.log.log.close(errAbandoned)
			clear(run.key[:])
			run.input = nil
			close(run.finished)
		}
		state.queue = nil
		state.mu.Unlock()
	}
	h.mu.Lock()
	for _, run := range h.runs {
		run.stop()
	}
	h.mu.Unlock()
	return active
}
func waitRuns(ctx context.Context, runs []*chatRun) {
	for _, run := range runs {
		select {
		case <-run.finished:
		case <-ctx.Done():
			return
		}
		if run.spilled != nil {
			select {
			case <-run.spilled:
			case <-ctx.Done():
				return
			}
		}
	}
}
