package main

import (
	"context"
	"sync"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// liveRuns holds what this instance is executing: each chat's running reply
// and the turns queued behind it, and nothing once they end. A run is found
// by its chat, by its own id, and by the client request that started it. mu
// guards these maps and every field of the sessions and runs they hold that
// changes after the run is published, and is never held across I/O.
type liveRuns struct {
	mu       sync.Mutex
	closing  bool
	threads  map[string]*threadSession
	runs     map[string]*chatRun
	requests map[string]*chatRun
	// working counts the runs that have started until their last write.
	working sync.WaitGroup
}

// counts is how many chats, runs and client requests the registry holds.
func (l *liveRuns) counts() (threads, runs, requests int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.threads), len(l.runs), len(l.requests)
}

// locked runs f under the registry's lock.
func (l *liveRuns) locked(f func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f()
}

func liveKey(p *auth.Principal, id string) string { return p.Scope() + "/" + id }

var errRunNotFound = api.Err(404, "RUN_NOT_FOUND", "The run is not live on this instance")

// forget unregisters a run that is ending.
func (l *liveRuns) forget(r *chatRun) {
	if key := r.thread.owner + "/" + r.log.id; l.runs[key] == r {
		delete(l.runs, key)
	}
	if key := r.thread.owner + "/" + r.nonce; l.requests[key] == r {
		delete(l.requests, key)
	}
}

type threadSession struct {
	id, owner, keyHash string
	deleted            bool
	// A local thread is stored by its client, which sends its transcript
	// with each turn; an ask is stored by nobody. The run holds either's
	// row. A keyless thread, local or anonymous, never uses the content key.
	local, keyless bool
	// row is a local thread's transcript, which a queued turn continues from.
	row    *domain.ThreadRow
	active *chatRun
	queue  []*chatRun
}

type chatRun struct {
	thread                   *threadSession
	log                      *run
	out                      *stream
	input                    *turnRequest
	turn                     *turnRequest // the turn as carried out, once started
	received                 string       // when the turn's request arrived
	principal                *auth.Principal
	key                      auth.ContentKey
	credential               auth.Credential
	nonce, digest, messageID string
	queued, cancelled        bool
	running                  bool            // its RUN_STARTED is framed
	ending                   bool            // finishing what it produced; too late to cancel
	stopped                  context.Context // done once the run is cancelled
	accepted                 chan struct{}   // closed once the turn is queued, started, or refused
	refused                  error
	finished                 chan struct{}
	titleDone                chan struct{}
	row                      *domain.ThreadRow // the thread as the run started and committed it
	assembler                *domain.MessageAssembler
	retryTarget              *retryTarget
	replacement              domain.ReplacedWidget
	profile                  *domain.Profile // a local thread's settings, sent by its client
	screening                *screening

	// yielded marks an active run stopped so a queued turn starts now.
	yielded bool
}

// holdsKey reports whether the run still holds its caller's content key.
func (r *chatRun) holdsKey() bool { return r.key != (auth.ContentKey{}) }

// release drops what a run holds of its caller once nothing reads it.
func (r *chatRun) release() {
	clear(r.key[:])
	r.input, r.turn, r.retryTarget, r.assembler, r.screening, r.profile = nil, nil, nil, nil, nil, nil
	r.replacement = domain.ReplacedWidget{}
	r.principal = &auth.Principal{ID: r.principal.ID, AnonymousID: r.principal.AnonymousID, Anonymous: r.principal.Anonymous}
	r.credential.Key = ""
}

func (h *harness) authorizedRun(p *auth.Principal, key auth.ContentKey, id, runID string) (*chatRun, error) {
	if err := api.RequireID("threadId", id); err != nil {
		return nil, err
	}
	if err := api.RequireID("runId", runID); err != nil {
		return nil, err
	}
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	r := h.live.runs[liveKey(p, runID)]
	if r == nil || r.thread.id != id {
		return nil, errRunNotFound
	}
	if !r.thread.keyless && r.thread.keyHash != key.Fingerprint() {
		return nil, api.Err(409, "KEY_MISMATCH", "The run uses another key")
	}
	return r, nil
}

// makeActive gives a chat's running slot to a turn about to start; live.mu is held.
func (h *harness) makeActive(s *threadSession, r *chatRun) {
	s.active = r
}

// queueChanged tells a chat's running reply its queue; live.mu is held. A
// reply that has not started is told by its first state snapshot.
func queueChanged(s *threadSession) {
	if s.active != nil && s.active.running {
		s.active.out.emit(domain.StateDelta{Delta: []domain.StatePatch{{Op: "replace", Path: "/queue", Value: queueItems(s)}}})
	}
}

func (r *chatRun) queueItem() domain.QueueItem {
	attachments := []domain.QueuedAttachment{}
	for _, a := range r.input.Attachments {
		attachments = append(attachments, domain.QueuedAttachment{ID: a.ID, Kind: a.Kind, FileName: a.FileName, MimeType: a.MimeType, Size: a.Size, Thumbnail: a.Thumbnail})
	}
	return domain.QueueItem{QueueID: r.log.id, Kind: r.input.Kind, Content: r.input.Content, Quote: r.input.Quote, Attachments: attachments, CreatedAt: r.received}
}

func queueItems(s *threadSession) []domain.QueueItem {
	out := []domain.QueueItem{}
	for _, r := range s.queue {
		out = append(out, r.queueItem())
	}
	return out
}

// advance retires a run from its chat and returns the queued turn to start
// next, if the chat goes on; live.mu is held. A chat whose reply was stopped
// ends the turns queued behind it, so the harness keeps nothing of a chat
// once its reply stops. A chat with nothing running or queued leaves the
// registry.
func (h *harness) advance(r *chatRun, goOn bool) *chatRun {
	s := r.thread
	h.live.forget(r)
	if s.active == r {
		s.active = nil
	}
	var next *chatRun
	if goOn && !s.deleted && !h.live.closing && len(s.queue) > 0 {
		next, s.queue = s.queue[0], s.queue[1:]
		h.makeActive(s, next)
	} else {
		for _, queued := range s.queue {
			h.endQueued(queued, nil)
		}
		s.queue = nil
	}
	h.dropIdle(s)
	return next
}

// dropIdle removes a chat with nothing running or queued from the registry;
// live.mu is held.
func (h *harness) dropIdle(s *threadSession) {
	if s.active != nil || len(s.queue) > 0 {
		return
	}
	if h.live.threads[s.owner+"/"+s.id] == s {
		delete(h.live.threads, s.owner+"/"+s.id)
	}
}

// endQueued ends a turn that never started: removed when why is nil, and
// failed with why otherwise. live.mu is held.
func (h *harness) endQueued(r *chatRun, why error) {
	h.live.forget(r)
	if why == nil {
		r.out.emit(domain.RunFinished{ThreadID: r.thread.id, RunID: r.log.id, Metadata: domain.RunMetadata{Cancelled: new(true)}})
	} else {
		r.out.emit(domain.FailedRun(r.thread.id, r.log.id, api.AsError(why), nil))
	}
	r.log.log.close(errDone)
	r.release()
	close(r.finished)
}

// instanceID names this process, which alone holds its live runs. Clients
// send it back with requests about a run, so that a deployment of several
// can route them here.
var instanceID = api.Token()

// instance is the id of this process.
func instance() string { return instanceID }
