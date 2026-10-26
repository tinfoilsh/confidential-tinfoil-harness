package main

import (
	"context"
	"errors"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// errAlreadyApplied answers a retried request whose turn its chat holds, once
// the run it started is no longer live.
var errAlreadyApplied = api.Err(409, "ALREADY_APPLIED", "This request was already applied; reload the chat")

// maxQueuedTurns bounds the turns one chat holds behind its running reply.
const maxQueuedTurns = 32

// errQueueFull refuses a turn past a full queue. It is not the caller's
// quota, which stays as it was.
var errQueueFull = api.Err(429, "QUEUE_FULL", "The turn queue is full")

func (h *harness) acceptTurn(ctx context.Context, p *auth.Principal, credential auth.Credential, key auth.ContentKey, in *turnRequest) (*chatRun, error) {
	defer clear(key[:])
	in.ClearKey()
	nonce, digest := in.ClientRequestID, in.digest()
	var r *chatRun
	for r == nil {
		h.live.mu.Lock()
		if h.live.closing {
			h.live.mu.Unlock()
			return nil, errShutdown
		}
		if h.live.requests == nil {
			h.live.runs, h.live.requests = map[string]*chatRun{}, map[string]*chatRun{}
			if h.live.threads == nil {
				h.live.threads = map[string]*threadSession{}
			}
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
			return nil, api.Err(409, "REVISION_CONFLICT", "The request nonce was already used")
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
			if !previous.thread.keyless && previous.thread.keyHash != key.Fingerprint() {
				return nil, api.Err(409, "REVISION_CONFLICT", "The request nonce was already used")
			}
			// A run that ended meanwhile is checked against the chat as a
			// new request is.
			h.live.mu.Lock()
			live := h.live.requests[liveKey(p, nonce)] == previous
			if live {
				previous.log.log.attach()
			}
			h.live.mu.Unlock()
			if live {
				return previous, nil
			}
		}
	}
	if err := h.admit(ctx, p, credential, key, in, r); err != nil {
		clear(r.key[:])
		h.live.mu.Lock()
		if h.live.requests[liveKey(p, nonce)] == r {
			delete(h.live.requests, liveKey(p, nonce))
		}
		h.live.mu.Unlock()
		r.refused = err
		close(r.accepted)
		return nil, err
	}
	if r.queued {
		return r, nil
	}
	if err := h.launch(ctx, r); err != nil {
		err = h.withQuota(p, r.credential.Limit, err)
		if next := h.endUnstarted(r, err); next != nil {
			go h.runQueue(next)
		}
		r.log.log.detach()
		r.refused = err
		close(r.accepted)
		return nil, err
	}
	close(r.accepted)
	return r, nil
}

func quotaExhausted(limit api.RateLimit) error {
	e := api.Err(429, "QUOTA_EXHAUSTED", "The daily quota is exhausted")
	e.Kind, _ = limit.Kind.(string)
	e.ResetsAt, e.RateLimit = limit.ResetsAt, &limit
	return e
}

// withQuota gives a refusal for an exhausted quota the caller's quota as the
// refusal leaves it. A signed-out caller's is the one its request held.
func (h *harness) withQuota(p *auth.Principal, held api.RateLimit, err error) error {
	var e *api.Error
	if p == nil || !errors.As(err, &e) || e.Code != "QUOTA_EXHAUSTED" || e.RateLimit != nil {
		return err
	}
	refused, limit := *e, auth.Exhausted(p, held)
	refused.RateLimit = &limit
	return &refused
}

// newThread is a chat's first form, filed under a project and preset.
func newThread(id string, projectID, presetID *string) Thread {
	return Thread{ID: id, Title: "New Chat", TitleState: "placeholder", CreatedAt: api.Timestamp(), UpdatedAt: api.Timestamp(), Model: "auto", ProjectID: projectID, PresetID: presetID, WebSearchEnabled: true, Messages: []Message{}}
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
func (h *harness) admit(ctx context.Context, p *auth.Principal, credential auth.Credential, key auth.ContentKey, in *turnRequest, r *chatRun) error {
	defer clear(key[:])
	if credential.Limit.Exhausted() {
		return quotaExhausted(credential.Limit)
	}
	id, local := in.ThreadID, in.Local
	keyless := p.Anonymous || local
	if !keyless {
		granted, err := h.storeRows.authorizeRun(ctx, p, key, in.ThreadID, in.ProjectID)
		if err != nil {
			return err
		}
		copy := *p
		copy.SyncToken, copy.SyncExpires = granted.Token, granted.ExpiresAt
		p = &copy
	}
	var source *Thread
	if in.Kind == "ask" {
		if local {
			t, err := readDeviceThread(id, in.Thread)
			if err != nil {
				return err
			}
			source = &t
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
		if err := api.RequireID("projectId", project); err != nil {
			return err
		}
	}
	if id == "" {
		id = rowID()
	}
	// An idle local thread starts from the client's copy, which may have been
	// edited since the last turn. A queued turn continues the active run's row.
	// An ask is a new thread that hides the copy it starts from. A device
	// chat's first turn sends no thread and starts the chat under its
	// client's id.
	var seed *threadRow
	switch {
	case !local || source != nil:
	case in.Thread != nil:
		t, err := readDeviceThread(id, in.Thread)
		if err != nil {
			return err
		}
		seed = &threadRow{Thread: t}
		if seed.LastRequestID == r.nonce {
			return errAlreadyApplied
		}
	case rowIDPattern.MatchString(id):
		seed = &threadRow{Thread: newThread(id, nil, nil)}
	}
	if local {
		r.profile = localProfile(in.Profile)
	}
	clean := *in
	// The transcript seeds the row; holding it again per run would double a
	// local thread's memory while it waits in the queue.
	clean.Key, clean.Thread, clean.Profile = nil, nil, nil
	stopped, stop := context.WithCancel(context.Background())
	r.log = newRun(token())
	r.log.stop = stop
	// The user message a turn adds is named by the client's request.
	r.stopped, r.input, r.received, r.principal, r.key, r.messageID, r.credential, r.assembler = stopped, &clean, api.Timestamp(), p, key, r.nonce, credential, newMessageAssembler()
	if keyless {
		clear(r.key[:])
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
	if s != nil && (s.deleted || s.local != held || !s.keyless && s.keyHash != key.Fingerprint()) {
		return api.Err(404, "THREAD_NOT_FOUND", "The thread is unavailable")
	}
	if s != nil && s.active != nil && len(s.queue) >= maxQueuedTurns {
		return errQueueFull
	}
	if (s == nil || s.active == nil) && local && source == nil && seed == nil {
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	if s == nil {
		s = &threadSession{id: id, owner: p.Scope(), keyHash: key.Fingerprint(), local: held, keyless: keyless}
		h.live.threads[liveKey(p, id)] = s
	}
	r.thread = s
	h.live.runs[liveKey(p, r.log.id)] = r
	// The accepting request drains the run until it answers.
	r.log.log.attach()
	if s.active != nil {
		r.queued = true
		s.queue = append(s.queue, r)
		r.out.emit(stateDelta{ThreadID: id, RunID: r.log.id, Delta: []statePatch{{"add", "/queue/-", r.queueItem()}}})
		queueChanged(s)
		// A queued turn is accepted as it is registered, so a stop that
		// ends it cannot come between the two, and a retried request finds
		// it either queued or ended, never missing.
		close(r.accepted)
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
