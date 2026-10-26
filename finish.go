package main

import (
	"cmp"
	"context"
	"slices"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
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
	p, cancelled, deleted, held := r.principal, r.cancelled, s.deleted, r.credential.Limit
	h.live.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	ctx = auth.WithCallerKey(ctx, string(r.credential.Key))
	ctx = auth.WithUsage(ctx, usageForRun(r))
	ctx = withUsageSink(ctx, r.out)
	r.out.mu.Lock()
	messages := r.assembler.Messages()
	spend, sources, reply := r.out.spend, r.out.sources, r.out.replyID
	r.out.observe, r.out.sources = nil, nil
	if r.out.thoughts != nil {
		r.out.thoughts.close()
		r.out.thoughts = nil
	}
	r.out.mu.Unlock()
	continued := r.turn.Kind == "continue"
	committed := []domain.CommittedReply{}
	for i := range messages {
		m := &messages[i]
		m.IsInterrupted, m.IsError = new(cancelled), new(runErr != nil && !cancelled)
		m.Model = cmp.Or(r.out.concreteModel, r.row.Model)
		m.ModelDisplayName = h.models.catalog.get().DisplayName(m.Model)
		m.Usage = api.Raw(domain.MessageUsage{PromptTokens: spend.Prompt, CompletionTokens: spend.Completion})
		// A continued reply keeps the citations it already had.
		for _, annotation := range citationAnnotations(m.Content, sources) {
			if !slices.ContainsFunc(m.Annotations, func(kept domain.Annotation) bool { return citedFrom(kept) == citedFrom(annotation) }) {
				m.Annotations = append(m.Annotations, annotation)
			}
		}
		// What the run commits to each reply beyond its streamed content, so a
		// client that stores the thread itself stores the same messages. A
		// widget retry commits only its call, so its reply keeps its own.
		if r.retryTarget == nil {
			committed = append(committed, domain.CommittedReply{ID: m.ID, Model: m.Model, ModelDisplayName: m.ModelDisplayName, Usage: m.Usage, IsError: m.IsError, IsInterrupted: m.IsInterrupted, Annotations: m.Annotations})
		}
	}
	commit := func(t *domain.Thread) error {
		if r.retryTarget != nil && r.replacement.Arguments != "" {
			if err := domain.ReplaceWidget(t, r.retryTarget.parentID, r.input.ToolCallID, r.retryTarget.block.Arguments, r.replacement); err != nil {
				return err
			}
		} else {
			for _, m := range messages {
				index := t.MessageIndex(m.ID)
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
			row, err := h.store.MutateThread(ctx, p, r.key, s.id, commit)
			if err != nil {
				runErr = err
			} else {
				r.row = row
			}
		}
	}
	clear(r.key[:])
	limit := held
	if r.out.modelAccepted {
		limit = held.Debited(spend)
	}
	if runErr != nil && api.AsError(runErr).Code == "QUOTA_EXHAUSTED" {
		limit = auth.Exhausted(p, held)
	}
	summary := domain.Summarize(&r.row.Thread)
	metadata := domain.RunMetadata{Messages: committed}
	// A device chat's client stores the thread the run leaves as it is.
	if s.local && r.input.Kind != "ask" {
		metadata.Thread = new(domain.DeviceThread(&r.row.Thread))
	}
	h.live.mu.Lock()
	if s.local {
		s.row = r.row
	}
	next := h.advance(r, !cancelled || r.yielded)
	queue := queueItems(s)
	deleted = s.deleted
	h.live.mu.Unlock()
	r.out.emit(domain.StateSnapshotEvent{Snapshot: domain.StateSnapshot{Thread: summary, Queue: queue, RateLimit: limit}})
	metadata.RateLimit = &limit
	if runErr != nil && !cancelled {
		r.out.emit(domain.FailedRun(s.id, r.log.id, api.AsError(runErr), &metadata))
	} else {
		metadata.Usage, metadata.Cancelled = &spend, &cancelled
		r.out.emit(domain.RunFinished{ThreadID: s.id, RunID: r.log.id, Metadata: metadata})
	}
	r.log.log.close(errDone)
	if next != nil {
		go h.runQueue(next)
	}
	h.screen(r, messages, runErr)
	r.release()
}

// citedFrom is where in its reply an annotation's link starts.
func citedFrom(a domain.Annotation) int {
	if a.URLCitation == nil {
		return 0
	}
	return a.URLCitation.StartIndex
}
