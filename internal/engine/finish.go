package engine

import (
	"cmp"
	"context"
	"slices"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
)

// finishRun commits what the run produced and frames its end, then hands the
// chat to the next queued turn. Its slower work, the safeguards submission,
// follows the terminal frame and holds no lock.
func (eng *Engine) finishRun(r *Run, runErr error) {
	defer eng.Live.working.Done()
	defer close(r.Finished)
	if r.titleDone != nil {
		<-r.titleDone
	}
	s := r.Thread
	eng.Live.mu.Lock()
	r.ending = true
	p, cancelled, deleted, held := r.Principal, r.cancelled, s.deleted, r.credential.Limit
	eng.Live.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), CommitTimeout)
	defer cancel()
	ctx = auth.WithCaller(ctx, r.access)
	ctx = auth.WithUsage(ctx, usageForRun(r))
	ctx = inference.WithUsageSink(ctx, r.out)
	r.out.mu.Lock()
	r.out.framePending()
	messages := r.assembler.Messages()
	spend, sources, reply, truncated := r.out.spend, r.out.sources, r.out.replyID, r.out.truncated
	r.out.Observe, r.out.folding, r.out.activities, r.out.sources = nil, false, nil, nil
	if r.out.thoughts != nil {
		r.out.thoughts.close()
		r.out.thoughts = nil
	}
	r.out.mu.Unlock()
	continued := r.turn.Kind == "continue"
	committed := []domain.CommittedReply{}
	for i := range messages {
		m := &messages[i]
		// A reply cut off at the end of the window is stopped where it ended,
		// for a continue turn to go on from.
		m.IsInterrupted, m.IsError = new(cancelled || truncated), new(runErr != nil && !cancelled)
		m.Model = cmp.Or(r.out.concreteModel, r.row.Model)
		m.ModelDisplayName = eng.Models.Catalog.Get().DisplayName(m.Model)
		m.Usage = api.Raw(domain.MessageUsage{PromptTokens: spend.Prompt, CompletionTokens: spend.Completion})
		// A research that ended before its report is read back with its
		// steps, which hold what it found.
		if r.researched && m.ID == reply && m.Content != "" {
			m.Research = &domain.Research{Version: researchVersion}
		}
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
			committed = append(committed, domain.CommittedReply{ID: m.ID, Model: m.Model, ModelDisplayName: m.ModelDisplayName, Usage: m.Usage, IsError: m.IsError, IsInterrupted: m.IsInterrupted, Annotations: m.Annotations, Research: m.Research})
		}
	}
	compacted := r.compacted
	// unreplaced is why a widget retry's call could not be replaced, which
	// fails the run without undoing the rest of what it commits.
	var unreplaced error
	commit := func(t *domain.Thread) error {
		unreplaced = nil
		// The run's compaction is kept before the first question it kept,
		// even when the reply then failed, unless that question is gone.
		if compacted != nil && t.MessageIndex(compacted.marker.ID) < 0 {
			if at := t.MessageIndex(compacted.beforeID); at >= 0 {
				t.Messages = slices.Insert(t.Messages, at, compacted.marker)
				t.ContextUsage = compacted.usage
			}
		}
		if r.retryTarget != nil && r.replacement.Arguments != "" {
			unreplaced = domain.ReplaceWidget(t, r.retryTarget.parentID, r.input.ToolCallID, r.retryTarget.block.Arguments, r.replacement)
		} else {
			for _, m := range messages {
				index := t.MessageIndex(m.ID)
				if index < 0 {
					t.Messages = append(t.Messages, m)
				} else if continued && m.ID == reply {
					// Answers given while the reply was continued are kept.
					m.KeepAnswers(&t.Messages[index])
					t.Messages[index] = m
				}
			}
		}
		if t.ActiveRun != nil && t.ActiveRun.RunID == r.Log.ID {
			t.ActiveRun = nil
		}
		// A retry compacts again only after a compaction failed.
		if r.turn.Kind == "compact" {
			if runErr != nil && !cancelled {
				t.LastRequestKind = "compact"
			} else if t.LastRequestKind == "compact" {
				t.LastRequestKind = ""
			}
		}
		return nil
	}
	if !deleted {
		var err error
		if s.local {
			err = commit(&r.row.Thread)
		} else {
			var row *domain.ThreadRow
			if row, err = eng.Store.MutateThread(ctx, p, r.key, s.ID, commit); err == nil {
				r.row = row
			}
		}
		if err != nil {
			runErr = err
		} else if unreplaced != nil {
			runErr = unreplaced
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
	eng.Live.mu.Lock()
	if s.local {
		s.row = r.row
	}
	next := eng.advance(r, !cancelled || r.yielded)
	// A chat deleted as its reply ran is not kept for its client.
	if metadata.Thread != nil && !s.deleted {
		eng.Live.hold(r)
	}
	queue := queueItems(s)
	// The turn starting next is listed until its run starts, which first
	// reads and saves its chat, so its client does not lose sight of it.
	if next != nil {
		queue = append([]domain.QueueItem{next.queueItem()}, queue...)
	}
	deleted = s.deleted
	eng.Live.mu.Unlock()
	r.out.emit(domain.StateSnapshotEvent{Snapshot: domain.StateSnapshot{Thread: summary, Queue: queue, RateLimit: limit}})
	metadata.RateLimit = &limit
	eng.Metrics.FinishRun(r.admittedAt, cancelled, runErr != nil)
	if runErr != nil && !cancelled {
		r.out.emit(domain.FailedRun(s.ID, r.Log.ID, api.AsError(runErr), &metadata))
	} else {
		metadata.Usage, metadata.Cancelled, metadata.Truncated = &spend, &cancelled, truncated && !cancelled
		r.out.emit(domain.RunFinished{ThreadID: s.ID, RunID: r.Log.ID, Metadata: metadata})
	}
	r.Log.Frames.close(ErrDone)
	if next != nil {
		go eng.runQueue(next)
	}
	eng.screen(r, messages, runErr)
	r.release()
}

// citedFrom is where in its reply an annotation's link starts.
func citedFrom(a domain.Annotation) int {
	if a.URLCitation == nil {
		return 0
	}
	return a.URLCitation.StartIndex
}
