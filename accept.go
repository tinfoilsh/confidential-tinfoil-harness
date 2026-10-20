package main

import "context"

// errAlreadyApplied answers a retried request whose turn its chat holds, once
// the run it started is no longer live.
var errAlreadyApplied = apiErr(409, "ALREADY_APPLIED", "This request was already applied; reload the chat")

// maxQueuedTurns bounds the turns one chat holds behind its running reply.
const maxQueuedTurns = 32

func (h *harness) acceptTurn(ctx context.Context, p *principal, key contentKey, in object) (*chatRun, error) {
	nonce, digest := str(in["clientRequestId"]), requestDigest(in)
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

// newThreadRow is a chat's first row. An ask starts from the conversation it
// asks about and hides it from its client.
func newThreadRow(id string, in, source object) *storedRow {
	data := object{"id": id, "title": "New Chat", "titleState": "placeholder", "createdAt": timestamp(), "updatedAt": timestamp(), "messages": []any{}, "model": "auto", "activeRun": nil}
	data["projectId"], data["presetId"] = in["projectId"], in["presetId"]
	if source != nil {
		data["messages"] = source["messages"]
		data["harnessHiddenCount"] = len(arr(source["messages"]))
		data["projectId"], data["presetId"] = source["projectId"], source["presetId"]
	}
	row := &storedRow{ID: id, Data: data}
	normalizeThread(row)
	return row
}

// admit gives a turn its chat: it starts now if the chat has no running
// reply, or waits in the chat's queue.
func (h *harness) admit(ctx context.Context, p *principal, key contentKey, in object, r *chatRun) error {
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return err
	}
	if credential.Limit.exhausted() {
		return quotaExhausted(credential.Limit)
	}
	id, local := str(in["threadId"]), boolean(in["local"], false)
	keyless := p.Anonymous || local
	var source object
	if in["kind"] == "ask" {
		if local {
			source = localThread(id, obj(in["thread"])).Data
		} else {
			row, err := h.storeRows.load(ctx, p, key, "chat", id)
			if err != nil {
				return err
			}
			source = clone(normalizeThread(row))
		}
		id = ""
	}
	// A stored chat's project is read as its first turn starts.
	if source == nil && str(in["projectId"]) != "" {
		if _, err := requiredID(in, "projectId"); err != nil {
			return err
		}
	}
	if id == "" {
		id = rowID()
	}
	// An idle local thread starts from the client's copy, which may have been
	// edited since the last turn. A queued turn continues the active run's row.
	// An ask is a new thread that hides the copy it starts from.
	var seed *storedRow
	if thread, ok := in["thread"]; ok && local && source == nil {
		seed = localThread(id, obj(thread))
		if seed.Data["lastRequestId"] == r.nonce {
			return errAlreadyApplied
		}
	}
	clean := clone(in)
	delete(clean, "key")
	// The transcript seeds the row; holding it again per run would double a
	// local thread's memory while it waits in the queue.
	delete(clean, "thread")
	delete(clean, "profile")
	clean["createdAt"] = timestamp()
	stopped, stop := context.WithCancel(context.Background())
	r.log = newRun(token())
	r.log.stop = stop
	// The user message a turn adds is named by the client's request.
	r.stopped, r.input, r.principal, r.key, r.messageID, r.credential, r.assembler = stopped, clean, p, key, r.nonce, credential, newMessageAssembler()
	if local {
		r.profile = localProfile(obj(in["profile"]))
	}
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
		r.out.emit(event{Type: "STATE_DELTA", ThreadID: id, RunID: r.log.id, Delta: []object{{"op": "add", "path": "/queue/-", "value": queueItem(r)}}})
		queueChanged(s)
		return nil
	}
	switch {
	case seed != nil:
		s.row = seed
	case source != nil:
		s.row = newThreadRow(id, in, source)
	}
	h.makeActive(s, r)
	return nil
}
