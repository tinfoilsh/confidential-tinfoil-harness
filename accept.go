package main

import "context"

// errAlreadyApplied answers a retried request whose turn its chat holds, once
// the run it started is no longer live.
var errAlreadyApplied = apiErr(409, "ALREADY_APPLIED", "This request was already applied; reload the chat")

// maxQueuedTurns bounds the turns one chat holds behind its running reply.
const maxQueuedTurns = 32

func (h *harness) acceptTurn(ctx context.Context, p *principal, key contentKey, in *turnRequest) (*chatRun, error) {
	nonce, digest := in.ClientRequestID, in.digest()
	var r *chatRun
	for r == nil {
		h.live.mu.Lock()
		if h.live.closing {
			h.live.mu.Unlock()
			return nil, errShutdown
		}
		if h.live.requests == nil {
			h.live.threads, h.live.runs, h.live.requests = map[string]*threadSession{}, map[string]*chatRun{}, map[string]*chatRun{}
		}
		previous := h.live.requests[liveKey(p, nonce)]
		if previous == nil {
			r = &chatRun{nonce: nonce, digest: digest, accepted: make(chan struct{}), finished: make(chan struct{})}
			h.live.requests[liveKey(p, nonce)] = r
		}
		h.live.mu.Unlock()
		if previous == nil {
			break
		}
		// A retried request follows the run it started.
		if previous.digest != digest {
			return nil, apiErr(409, "REVISION_CONFLICT", "The request nonce was already used")
		}
		// A run already accepted is followed even by a caller that has left.
		select {
		case <-previous.accepted:
		default:
			select {
			case <-previous.accepted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if previous.refused == nil {
			if !previous.thread.keyless && previous.thread.keyHash != key.fingerprint() {
				return nil, apiErr(409, "REVISION_CONFLICT", "The request nonce was already used")
			}
			return previous, nil
		}
	}
	if err := h.admit(ctx, p, key, in, r); err != nil {
		h.live.mu.Lock()
		if h.live.requests[liveKey(p, nonce)] == r {
			delete(h.live.requests, liveKey(p, nonce))
		}
		h.live.mu.Unlock()
		r.refused = err
		close(r.accepted)
		return nil, err
	}
	if !r.queued {
		if err := h.launch(ctx, r); err != nil {
			if next := h.endUnstarted(r, err); next != nil {
				go h.runQueue(next)
			}
			r.refused = err
			close(r.accepted)
			return nil, err
		}
	}
	close(r.accepted)
	return r, nil
}

func quotaExhausted(limit rateLimit) error {
	e := apiErr(429, "QUOTA_EXHAUSTED", "The daily quota is exhausted")
	e.Kind = str(limit.Kind)
	e.ResetsAt = limit.ResetsAt
	return e
}

// newThread is a chat's first form, filed under a project and preset.
func newThread(id string, projectID, presetID *string) Thread {
	return Thread{ID: id, Title: "New Chat", TitleState: "placeholder", CreatedAt: timestamp(), UpdatedAt: timestamp(), Model: "auto", ProjectID: projectID, PresetID: presetID, WebSearchEnabled: true, Messages: []Message{}}
}

// askThread is an ask's thread: it starts from the conversation it asks
// about and hides it from its client.
func askThread(id string, source *Thread) *threadRow {
	t := newThread(id, source.ProjectID, source.PresetID)
	t.Messages, t.HiddenCount = source.Messages, len(source.Messages)
	return &threadRow{Thread: t}
}

// admit gives a turn its chat: it starts now if the chat has no running
// reply, or waits in the chat's queue.
func (h *harness) admit(ctx context.Context, p *principal, key contentKey, in *turnRequest, r *chatRun) error {
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return err
	}
	if credential.Limit.exhausted() {
		return quotaExhausted(credential.Limit)
	}
	id, local := in.ThreadID, in.Local
	keyless := p.Anonymous || local
	var source *Thread
	if in.Kind == "ask" {
		if local {
			source = new(readDeviceThread(id, in.Thread))
		} else {
			row, err := h.loadThread(ctx, p, key, id)
			if err != nil {
				return err
			}
			source = &row.Thread
		}
		id = ""
	}
	// A stored chat's project is read as its first turn starts.
	if project := valueOr(in.ProjectID, ""); source == nil && project != "" {
		if err := requireID("projectId", project); err != nil {
			return err
		}
	}
	if id == "" {
		id = rowID()
	}
	// An idle local thread starts from the client's copy, which may have been
	// edited since the last turn. A queued turn continues the active run's row.
	// An ask is a new thread that hides the copy it starts from.
	var seed *threadRow
	if in.Thread != nil && local && source == nil {
		seed = &threadRow{Thread: readDeviceThread(id, in.Thread)}
		if seed.LastRequestID == r.nonce {
			return errAlreadyApplied
		}
	}
	if local {
		r.profile = localProfile(in.Profile)
	}
	clean := *in
	// The transcript seeds the row; holding it again per run would double a
	// local thread's memory while it waits in the queue.
	clean.Key, clean.Thread, clean.Profile = "", nil, nil
	stopped, stop := context.WithCancel(context.Background())
	r.log = newRun(token())
	r.log.stop = stop
	// The user message a turn adds is named by the client's request.
	r.stopped, r.input, r.received, r.principal, r.key, r.messageID, r.credential, r.assembler = stopped, &clean, timestamp(), p, key, r.nonce, credential, newMessageAssembler()
	r.out = &stream{run: r.log}

	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	if h.live.closing {
		return errShutdown
	}
	s := h.live.threads[liveKey(p, id)]
	// An ask's run holds its row, as a device chat's does.
	held := local || source != nil
	if s != nil && (s.deleted || s.local != held || !s.keyless && s.keyHash != key.fingerprint()) {
		return apiErr(404, "THREAD_NOT_FOUND", "The thread is unavailable")
	}
	if s != nil && s.active != nil && len(s.queue) >= maxQueuedTurns {
		return apiErr(429, "QUOTA_EXHAUSTED", "The turn queue is full")
	}
	if (s == nil || s.active == nil) && local && source == nil && seed == nil {
		return apiErr(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	if s == nil {
		s = &threadSession{id: id, owner: p.scope(), keyHash: key.fingerprint(), local: held, keyless: keyless}
		h.live.threads[liveKey(p, id)] = s
	}
	r.thread = s
	h.live.runs[liveKey(p, r.log.id)] = r
	if s.active != nil {
		r.queued = true
		s.queue = append(s.queue, r)
		r.out.emit(event{Type: "STATE_DELTA", ThreadID: id, RunID: r.log.id, Delta: []statePatch{{"add", "/queue/-", r.queueItem()}}})
		queueChanged(s)
		return nil
	}
	switch {
	case seed != nil:
		s.row = seed
	case source != nil:
		s.row = askThread(id, source)
	}
	h.makeActive(s, r)
	return nil
}
