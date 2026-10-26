package engine

import (
	"context"
	"sync"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

type Engine struct {
	Live        liveRuns
	Safeguards  *Submitter
	Attachments *attach.Service
	Auth        *auth.Authenticator
	Models      *inference.Models
	Store       *store.Store
}

// liveRuns holds what this instance is executing: each chat's running reply
// and the turns queued behind it, and nothing once they end. A run is found
// by its chat, by its own id, and by the client request that started it. mu
// guards these maps and every field of the sessions and runs they hold that
// changes after the run is published, and is never held across I/O.
type liveRuns struct {
	mu       sync.Mutex
	closing  bool
	threads  map[string]*threadSession
	runs     map[string]*Run
	requests map[string]*Run
	// working counts the runs that have started until their last write.
	working sync.WaitGroup
}

// Counts is how many chats, runs and client requests the registry holds.
func (l *liveRuns) Counts() (threads, runs, requests int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.threads), len(l.runs), len(l.requests)
}

// Locked runs f under the registry's lock.
func (l *liveRuns) Locked(f func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f()
}

func liveKey(p *auth.Principal, id string) string { return p.Scope() + "/" + id }

var errRunNotFound = api.Err(404, "RUN_NOT_FOUND", "The run is not live on this instance")

// forget unregisters a run that is ending.
func (l *liveRuns) forget(r *Run) {
	if key := r.Thread.owner + "/" + r.Log.ID; l.runs[key] == r {
		delete(l.runs, key)
	}
	if key := r.Thread.owner + "/" + r.nonce; l.requests[key] == r {
		delete(l.requests, key)
	}
}

type threadSession struct {
	ID, owner, keyHash string
	deleted            bool
	// A local thread is stored by its client, which sends its transcript
	// with each turn; an ask is stored by nobody. The run holds either's
	// row. A keyless thread, local or anonymous, never uses the content key.
	local, keyless bool
	// row is a local thread's transcript, which a queued turn continues from.
	row    *domain.ThreadRow
	Active *Run
	Queue  []*Run
}

type Run struct {
	Thread                   *threadSession
	Log                      *Log
	out                      *Stream
	input                    *TurnRequest
	turn                     *TurnRequest // the turn as carried out, once started
	received                 string       // when the turn's request arrived
	Principal                *auth.Principal
	key                      auth.ContentKey
	credential               auth.Credential
	nonce, digest, messageID string
	Queued, cancelled        bool
	running                  bool            // its RUN_STARTED is framed
	ending                   bool            // finishing what it produced; too late to cancel
	stopped                  context.Context // done once the run is cancelled
	accepted                 chan struct{}   // closed once the turn is queued, started, or refused
	refused                  error
	Finished                 chan struct{}
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

// HoldsKey reports whether the run still holds its caller's content key.
func (r *Run) HoldsKey() bool { return r.key != (auth.ContentKey{}) }

// release drops what a run holds of its caller once nothing reads it.
func (r *Run) release() {
	clear(r.key[:])
	r.input, r.turn, r.retryTarget, r.assembler, r.screening, r.profile = nil, nil, nil, nil, nil, nil
	r.replacement = domain.ReplacedWidget{}
	r.Principal = &auth.Principal{ID: r.Principal.ID, AnonymousID: r.Principal.AnonymousID, Anonymous: r.Principal.Anonymous}
	r.credential.Key = ""
}

func (eng *Engine) authorizedRun(p *auth.Principal, key auth.ContentKey, id, runID string) (*Run, error) {
	if err := api.RequireID("threadId", id); err != nil {
		return nil, err
	}
	if err := api.RequireID("runId", runID); err != nil {
		return nil, err
	}
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	r := eng.Live.runs[liveKey(p, runID)]
	if r == nil || r.Thread.ID != id {
		return nil, errRunNotFound
	}
	if !r.Thread.keyless && r.Thread.keyHash != key.Fingerprint() {
		return nil, api.Err(409, "KEY_MISMATCH", "The run uses another key")
	}
	return r, nil
}

// makeActive gives a chat's running slot to a turn about to start; live.mu is held.
func (eng *Engine) makeActive(s *threadSession, r *Run) {
	s.Active = r
}

// queueChanged tells a chat's running reply its queue; live.mu is held. A
// reply that has not started is told by its first state snapshot.
func queueChanged(s *threadSession) {
	if s.Active != nil && s.Active.running {
		s.Active.out.emit(domain.StateDelta{Delta: []domain.StatePatch{{Op: "replace", Path: "/queue", Value: queueItems(s)}}})
	}
}

func (r *Run) queueItem() domain.QueueItem {
	attachments := []domain.QueuedAttachment{}
	for _, a := range r.input.Attachments {
		attachments = append(attachments, domain.QueuedAttachment{ID: a.ID, Kind: a.Kind, FileName: a.FileName, MimeType: a.MimeType, Size: a.Size, Thumbnail: a.Thumbnail})
	}
	return domain.QueueItem{QueueID: r.Log.ID, Kind: r.input.Kind, Content: r.input.Content, Quote: r.input.Quote, Attachments: attachments, CreatedAt: r.received}
}

func queueItems(s *threadSession) []domain.QueueItem {
	out := []domain.QueueItem{}
	for _, r := range s.Queue {
		out = append(out, r.queueItem())
	}
	return out
}

// advance retires a run from its chat and returns the queued turn to start
// next, if the chat goes on; live.mu is held. A chat whose reply was stopped
// ends the turns queued behind it, so the harness keeps nothing of a chat
// once its reply stops. A chat with nothing running or queued leaves the
// registry.
func (eng *Engine) advance(r *Run, goOn bool) *Run {
	s := r.Thread
	eng.Live.forget(r)
	if s.Active == r {
		s.Active = nil
	}
	var next *Run
	if goOn && !s.deleted && !eng.Live.closing && len(s.Queue) > 0 {
		next, s.Queue = s.Queue[0], s.Queue[1:]
		eng.makeActive(s, next)
	} else {
		for _, queued := range s.Queue {
			eng.endQueued(queued, nil)
		}
		s.Queue = nil
	}
	eng.dropIdle(s)
	return next
}

// dropIdle removes a chat with nothing running or queued from the registry;
// live.mu is held.
func (eng *Engine) dropIdle(s *threadSession) {
	if s.Active != nil || len(s.Queue) > 0 {
		return
	}
	if eng.Live.threads[s.owner+"/"+s.ID] == s {
		delete(eng.Live.threads, s.owner+"/"+s.ID)
	}
}

// endQueued ends a turn that never started: removed when why is nil, and
// failed with why otherwise. live.mu is held.
func (eng *Engine) endQueued(r *Run, why error) {
	eng.Live.forget(r)
	if why == nil {
		r.out.emit(domain.RunFinished{ThreadID: r.Thread.ID, RunID: r.Log.ID, Metadata: domain.RunMetadata{Cancelled: new(true)}})
	} else {
		r.out.emit(domain.FailedRun(r.Thread.ID, r.Log.ID, api.AsError(why), nil))
	}
	r.Log.Frames.close(ErrDone)
	r.release()
	close(r.Finished)
}

// instanceID names this process, which alone holds its live runs. Clients
// send it back with requests about a run, so that a deployment of several
// can route them here.
var instanceID = api.Token()

// InstanceID is the id of this process.
func InstanceID() string { return instanceID }
