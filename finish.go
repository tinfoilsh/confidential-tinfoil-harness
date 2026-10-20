package main

import (
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
	continued := r.input["kind"] == "continue"
	start := func(annotation any) int { return integer(obj(obj(annotation)["url_citation"])["start_index"]) }
	for _, m := range messages {
		m["isInterrupted"], m["isError"] = cancelled, runErr != nil && !cancelled
		m["model"] = firstString(r.out.concreteModel, r.row.Data["model"])
		if definition := h.catalog.get().definition(str(m["model"])); definition != nil {
			m["modelDisplayName"] = definition["name"]
		}
		m["usage"] = object{"promptTokens": spend.Prompt, "completionTokens": spend.Completion}
		// A continued reply keeps the citations it already had.
		annotations := arr(m["annotations"])
		for _, annotation := range citationAnnotations(str(m["content"]), sources) {
			if !slices.ContainsFunc(annotations, func(kept any) bool { return start(kept) == start(annotation) }) {
				annotations = append(annotations, annotation)
			}
		}
		if len(annotations) > 0 {
			m["annotations"] = annotations
		}
	}
	// What the run commits to each reply beyond its streamed content, so a
	// client that stores the thread itself stores the same messages.
	committed := []object{}
	for _, m := range messages {
		committed = append(committed, selectFields(m, "id model modelDisplayName usage isError isInterrupted annotations"))
	}
	commit := func(data object) error {
		if r.retryTarget != nil && r.replacement != "" {
			if err := replaceWidget(data, str(r.retryTarget["parentId"]), str(r.input["toolCallId"]), str(r.retryTarget["arguments"]), r.replacement); err != nil {
				return err
			}
			if obj(data["activeRun"])["runId"] == r.log.id {
				data["activeRun"] = nil
			}
			return nil
		}
		all := arr(data["messages"])
		for _, m := range messages {
			index := slices.IndexFunc(all, func(v any) bool { return obj(v)["id"] == m["id"] })
			if index < 0 {
				all = append(all, m)
			} else if continued && m["id"] == reply {
				all[index] = m
			}
		}
		data["messages"] = all
		if obj(data["activeRun"])["runId"] == r.log.id {
			data["activeRun"] = nil
		}
		return nil
	}
	if !deleted {
		if s.local {
			if err := commit(r.row.Data); err != nil {
				runErr = err
			}
		} else {
			row, err := h.mutate(ctx, p, r.key, "chat", s.id, false, commit)
			if err != nil {
				runErr = err
			} else {
				r.row = row
			}
		}
	}
	limit := h.auth.currentLimit(p)
	if r.out.modelAccepted {
		limit = h.auth.consumed(p)
	}
	if p.Anonymous && runErr != nil && asAPIError(runErr).Code == "QUOTA_EXHAUSTED" {
		limit = exhaustedLimit("free_daily")
	}
	summary := threadSummary(normalizeThread(r.row))
	metadata := object{"messages": committed}
	// A device chat's client stores the thread the run leaves as it is.
	if s.local && r.input["kind"] != "ask" {
		metadata["thread"] = deviceThread(r.row)
	}
	h.live.mu.Lock()
	queue := queueItems(s)
	if s.local {
		s.row = r.row
	}
	next := h.advance(r, !cancelled || r.yielded)
	deleted = s.deleted
	h.live.mu.Unlock()
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: object{"thread": summary, "queue": queue, "rateLimit": limit}})
	if runErr != nil && !cancelled {
		e := asAPIError(runErr)
		r.out.emit(event{Type: "RUN_ERROR", ThreadID: s.id, RunID: r.log.id, Message: e.Message, Code: e.Code, Kind: e.Kind, ResetsAt: e.ResetsAt, RetryAfter: e.RetryAfter, Metadata: raw(metadata)})
	} else {
		metadata["usage"], metadata["rateLimit"], metadata["cancelled"] = spend, limit, cancelled
		r.out.emit(event{Type: "RUN_FINISHED", ThreadID: s.id, RunID: r.log.id, Metadata: raw(metadata)})
	}
	r.log.log.close(errDone)
	if next != nil {
		go h.runQueue(next)
	}
	h.screen(r, messages, runErr)
	r.release()
}
