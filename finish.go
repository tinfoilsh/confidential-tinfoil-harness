package main

import (
	"cmp"
	"context"
	"slices"
)

// finishRun commits what the run produced and frames its end, then hands the
// chat to the next queued turn. Its slower work, the safeguards submission,
// follows the terminal frame and holds no lock.
func (h *harness) finishRun(r *chatRun, runErr error) {
	defer h.live.working.Done()
	defer close(r.finished)
	if r.titleDone != nil {
		<-r.titleDone
	}
	s := r.thread
	h.live.mu.Lock()
	r.ending = true
	p, cancelled, deleted := r.principal, r.cancelled, s.deleted
	h.live.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, apiKeyKey{}, string(r.credential.Key))
	ctx = context.WithValue(ctx, usageContextKey{}, usageForRun(r))
	ctx = context.WithValue(ctx, usageSinkKey{}, r.out)
	r.out.mu.Lock()
	messages := r.assembler.messages()
	spend, sources, reply := r.out.spend, r.out.sources, r.out.replyID
	r.out.observe, r.out.sources = nil, nil
	r.out.mu.Unlock()
	continued := r.turn.Kind == "continue"
	committed := []committedReply{}
	for i := range messages {
		m := &messages[i]
		m.IsInterrupted, m.IsError = new(cancelled), new(runErr != nil && !cancelled)
		m.Model = cmp.Or(r.out.concreteModel, r.row.Model)
		m.ModelDisplayName = h.catalog.get().displayName(m.Model)
		m.Usage = raw(messageUsage{PromptTokens: spend.Prompt, CompletionTokens: spend.Completion})
		// A continued reply keeps the citations it already had.
		for _, annotation := range citationAnnotations(m.Content, sources) {
			if !slices.ContainsFunc(m.Annotations, func(kept Annotation) bool { return citedFrom(kept) == citedFrom(annotation) }) {
				m.Annotations = append(m.Annotations, annotation)
			}
		}
		// What the run commits to each reply beyond its streamed content, so a
		// client that stores the thread itself stores the same messages.
		committed = append(committed, committedReply{ID: m.ID, Model: m.Model, ModelDisplayName: m.ModelDisplayName, Usage: m.Usage, IsError: m.IsError, IsInterrupted: m.IsInterrupted, Annotations: m.Annotations})
	}
	commit := func(t *Thread) error {
		if r.retryTarget != nil && r.replacement != "" {
			if err := replaceWidget(t, r.retryTarget.parentID, r.input.ToolCallID, r.retryTarget.block.Arguments, r.replacement); err != nil {
				return err
			}
		} else {
			for _, m := range messages {
				index := t.messageIndex(m.ID)
				if index < 0 {
					t.Messages = append(t.Messages, m)
				} else if continued && m.ID == reply {
					t.Messages[index] = m
				}
			}
		}
		if t.ActiveRun != nil && t.ActiveRun.RunID == r.log.id {
			t.ActiveRun = nil
		}
		return nil
	}
	if !deleted {
		if s.local {
			if err := commit(&r.row.Thread); err != nil {
				runErr = err
			}
		} else {
			row, err := h.mutateThread(ctx, p, r.key, s.id, commit)
			if err != nil {
				runErr = err
			} else {
				r.row = row
			}
		}
	}
	limit := h.auth.currentLimit(p)
	if r.out.modelAccepted {
		limit = h.auth.debit(p, spend)
	}
	if runErr != nil && asAPIError(runErr).Code == "QUOTA_EXHAUSTED" {
		limit = h.auth.exhausted(p)
	}
	summary := summarize(&r.row.Thread)
	metadata := runMetadata{Messages: committed}
	// A device chat's client stores the thread the run leaves as it is.
	if s.local && r.input.Kind != "ask" {
		metadata.Thread = new(deviceThread(&r.row.Thread))
	}
	h.live.mu.Lock()
	queue := queueItems(s)
	if s.local {
		s.row = r.row
	}
	next := h.advance(r, !cancelled || r.yielded)
	deleted = s.deleted
	h.live.mu.Unlock()
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: stateSnapshot{summary, queue, limit}})
	metadata.RateLimit = &limit
	if runErr != nil && !cancelled {
		e := asAPIError(runErr)
		r.out.emit(event{Type: "RUN_ERROR", ThreadID: s.id, RunID: r.log.id, Message: e.Message, Code: e.Code, Kind: e.Kind, ResetsAt: e.ResetsAt, RetryAfter: e.RetryAfter, Metadata: raw(metadata)})
	} else {
		metadata.Usage, metadata.Cancelled = &spend, &cancelled
		r.out.emit(event{Type: "RUN_FINISHED", ThreadID: s.id, RunID: r.log.id, Metadata: raw(metadata)})
	}
	r.log.log.close(errDone)
	if next != nil {
		go h.runQueue(next)
	}
	h.screen(r, messages, runErr)
	r.release()
}

// citedFrom is where in its reply an annotation's link starts.
func citedFrom(a Annotation) int {
	if a.URLCitation == nil {
		return 0
	}
	return a.URLCitation.StartIndex
}
