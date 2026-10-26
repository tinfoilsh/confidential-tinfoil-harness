package main

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// errAlreadyApplied answers a retried request whose turn its chat holds, once
// the run it started is no longer live.
var errAlreadyApplied = api.Err(409, "ALREADY_APPLIED", "This request was already applied; reload the chat")

// maxQueuedTurns bounds the turns one chat holds behind its running reply.
const maxQueuedTurns = 32

// errQueueFull refuses a turn past a full queue. It is not the caller's
// quota, which stays as it was.
var errQueueFull = api.Err(429, "QUEUE_FULL", "The turn queue is full")

func (eng *engine) acceptTurn(ctx context.Context, p *auth.Principal, credential auth.Credential, key auth.ContentKey, in *turnRequest) (*chatRun, error) {
	defer clear(key[:])
	in.ClearKey()
	nonce, digest := in.ClientRequestID, in.digest()
	var r *chatRun
	for r == nil {
		eng.live.mu.Lock()
		if eng.live.closing {
			eng.live.mu.Unlock()
			return nil, errShutdown
		}
		if eng.live.requests == nil {
			eng.live.runs, eng.live.requests = map[string]*chatRun{}, map[string]*chatRun{}
			if eng.live.threads == nil {
				eng.live.threads = map[string]*threadSession{}
			}
		}
		previous := eng.live.requests[liveKey(p, nonce)]
		if previous == nil {
			r = &chatRun{nonce: nonce, digest: digest, accepted: make(chan struct{}), finished: make(chan struct{})}
			eng.live.requests[liveKey(p, nonce)] = r
		}
		eng.live.mu.Unlock()
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
			eng.live.mu.Lock()
			live := eng.live.requests[liveKey(p, nonce)] == previous
			if live {
				previous.log.log.attach()
			}
			eng.live.mu.Unlock()
			if live {
				return previous, nil
			}
		}
	}
	if err := eng.admit(ctx, p, credential, key, in, r); err != nil {
		clear(r.key[:])
		eng.live.mu.Lock()
		if eng.live.requests[liveKey(p, nonce)] == r {
			delete(eng.live.requests, liveKey(p, nonce))
		}
		eng.live.mu.Unlock()
		r.refused = err
		close(r.accepted)
		return nil, err
	}
	if r.queued {
		return r, nil
	}
	if err := eng.launch(ctx, r); err != nil {
		err = auth.WithQuota(p, r.credential.Limit, err)
		if next := eng.endUnstarted(r, err); next != nil {
			go eng.runQueue(next)
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

// newThread is a chat's first form, filed under a project and preset.
func newThread(id string, projectID, presetID *string) domain.Thread {
	return domain.Thread{ID: id, Title: "New Chat", TitleState: "placeholder", CreatedAt: api.Timestamp(), UpdatedAt: api.Timestamp(), Model: "auto", ProjectID: projectID, PresetID: presetID, WebSearchEnabled: true, Messages: []domain.Message{}}
}

// askThread is an ask's thread: it starts from the conversation it asks
// about and hides it from its client.
func askThread(id string, source *domain.Thread) *domain.ThreadRow {
	t := newThread(id, source.ProjectID, source.PresetID)
	t.Messages, t.HiddenCount = source.Messages, len(source.Messages)
	return &domain.ThreadRow{Thread: t}
}

// admit gives a turn its chat: it starts now if the chat has no running
// reply, or waits in the chat's queue.
func (eng *engine) admit(ctx context.Context, p *auth.Principal, credential auth.Credential, key auth.ContentKey, in *turnRequest, r *chatRun) error {
	defer clear(key[:])
	if credential.Limit.Exhausted() {
		return quotaExhausted(credential.Limit)
	}
	id, local := in.ThreadID, in.Local
	keyless := p.Anonymous || local
	if !keyless {
		granted, err := eng.store.Rows.AuthorizeRun(ctx, p, key, in.ThreadID, in.ProjectID)
		if err != nil {
			return err
		}
		copy := *p
		copy.SyncToken, copy.SyncExpires = granted.Token, granted.ExpiresAt
		p = &copy
	}
	var source *domain.Thread
	if in.Kind == "ask" {
		if local {
			t, err := domain.ReadDeviceThread(id, in.Thread)
			if err != nil {
				return err
			}
			source = &t
		} else {
			row, err := eng.store.LoadThread(ctx, p, key, id)
			if err != nil {
				return err
			}
			source = &row.Thread
		}
		id = ""
	}
	// A stored chat's project is read as its first turn starts.
	if project := domain.ValueOr(in.ProjectID, ""); source == nil && project != "" {
		if err := api.RequireID("projectId", project); err != nil {
			return err
		}
	}
	if id == "" {
		id = domain.RowID()
	}
	// An idle local thread starts from the client's copy, which may have been
	// edited since the last turn. A queued turn continues the active run's row.
	// An ask is a new thread that hides the copy it starts from. A device
	// chat's first turn sends no thread and starts the chat under its
	// client's id.
	var seed *domain.ThreadRow
	switch {
	case !local || source != nil:
	case in.Thread != nil:
		t, err := domain.ReadDeviceThread(id, in.Thread)
		if err != nil {
			return err
		}
		seed = &domain.ThreadRow{Thread: t}
		if seed.LastRequestID == r.nonce {
			return errAlreadyApplied
		}
	case domain.RowIDPattern.MatchString(id):
		seed = &domain.ThreadRow{Thread: newThread(id, nil, nil)}
	}
	if local {
		r.profile = domain.LocalProfile(in.Profile)
	}
	clean := *in
	// The transcript seeds the row; holding it again per run would double a
	// local thread's memory while it waits in the queue.
	clean.Key, clean.Thread, clean.Profile = nil, nil, nil
	stopped, stop := context.WithCancel(context.Background())
	r.log = newRun(api.Token())
	r.log.stop = stop
	// The user message a turn adds is named by the client's request.
	r.stopped, r.input, r.received, r.principal, r.key, r.messageID, r.credential, r.assembler = stopped, &clean, api.Timestamp(), p, key, r.nonce, credential, domain.NewMessageAssembler()
	if keyless {
		clear(r.key[:])
	}
	r.out = &stream{run: r.log}

	eng.live.mu.Lock()
	defer eng.live.mu.Unlock()
	if eng.live.closing {
		return errShutdown
	}
	s := eng.live.threads[liveKey(p, id)]
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
		eng.live.threads[liveKey(p, id)] = s
	}
	r.thread = s
	eng.live.runs[liveKey(p, r.log.id)] = r
	// The accepting request drains the run until it answers.
	r.log.log.attach()
	if s.active != nil {
		r.queued = true
		s.queue = append(s.queue, r)
		r.out.emit(domain.StateDelta{ThreadID: id, RunID: r.log.id, Delta: []domain.StatePatch{{Op: "add", Path: "/queue/-", Value: r.queueItem()}}})
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
	eng.makeActive(s, r)
	return nil
}
