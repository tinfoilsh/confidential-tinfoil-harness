package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
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

func liveKey(p *principal, id string) string { return p.scope() + "/" + id }

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
	ephemeral, deleted bool
	// A local thread is stored by the client, which sends its transcript with
	// each turn. Like an anonymous thread, it never uses the content key.
	local, keyless bool
	// row is a temporary or local thread's transcript, which a queued turn
	// continues from.
	row    *storedRow
	active *chatRun
	queue  []*chatRun
	// waiting ends the queue a stopped run leaves if its client never sends it.
	waiting *time.Timer
}
type chatRun struct {
	thread                   *threadSession
	log                      *run
	out                      *stream
	input                    object
	principal                *principal
	key                      contentKey
	credential               inferenceCredential
	nonce, digest, messageID string
	queued, cancelled        bool
	running                  bool            // its RUN_STARTED is framed
	ending                   bool            // finishing what it produced; too late to cancel
	stopped                  context.Context // done once the run is cancelled
	accepted                 chan struct{}   // closed once the turn is queued, started, or refused
	refused                  error
	finished                 chan struct{}
	titleDone                chan struct{}
	row                      *storedRow // the thread as the run started and committed it
	assembler                *messageAssembler
	retryTarget              object
	replacement              string
	profile                  object // a local thread's settings, sent by its client
	screening                *screening

	// yielded marks an active run stopped so a queued turn starts now.
	yielded bool
}

// release drops what a run holds of its caller once nothing reads it.
func (r *chatRun) release() {
	clear(r.key[:])
	r.input, r.retryTarget, r.assembler, r.screening, r.profile = nil, nil, nil, nil, nil
	r.replacement = ""
	r.principal = &principal{ID: r.principal.ID, AnonymousID: r.principal.AnonymousID, Anonymous: r.principal.Anonymous}
	r.credential.Key = ""
}

func (h *harness) handleTurn(w http.ResponseWriter, r *http.Request) {
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	in, err := decodeJSONWithin(w, r, maxTurnBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if _, err := uuid.Parse(str(in["clientRequestId"])); err != nil {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "clientRequestId must be a UUID"))
		return
	}
	kind := firstString(in["kind"], "send")
	in["kind"] = kind
	if !slices.Contains([]string{"send", "edit", "regenerate", "continue", "resolve", "retryToolCall", "ask"}, kind) {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "Unknown turn kind"))
		return
	}
	local := boolean(in["local"], false)
	if kind == "ask" || local {
		in["ephemeral"] = true
	}
	ephemeral := boolean(in["ephemeral"], false)
	if p.Anonymous && !ephemeral {
		respond(w, nil, apiErr(401, "UNAUTHENTICATED", "Anonymous turns must be ephemeral"))
		return
	}
	var key contentKey
	if !p.Anonymous && !local || str(in["key"]) != "" {
		key, err = decodeContentKey(str(in["key"]))
		if err != nil {
			respond(w, nil, err)
			return
		}
	}
	defer clear(key[:])
	if kind == "ask" && p.Anonymous && !local {
		respond(w, nil, apiErr(401, "UNAUTHENTICATED", "Ask requires access to a stored thread"))
		return
	}
	if _, err := requiredID(in, "threadId"); err != nil {
		respond(w, nil, err)
		return
	}
	if err := validateTurn(in); err != nil {
		respond(w, nil, err)
		return
	}
	accepted, err := h.acceptTurn(r.Context(), p, key, in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	follow(w, r, accepted.log)
}
func validateTurn(in object) error {
	if err := validateFields(in, "kind clientRequestId content quote messageId toolCallId", "ephemeral local", "threadId projectId presetId"); err != nil {
		return err
	}
	local := boolean(in["local"], false)
	if local && in["projectId"] != nil {
		return apiErr(400, "BAD_REQUEST", "Projects require cloud storage")
	}
	for _, field := range []string{"thread", "profile"} {
		value, ok := in[field]
		if !ok {
			continue
		}
		if !local {
			return apiErr(400, "BAD_REQUEST", field+" is only accepted for local threads")
		}
		if !validObject(value) {
			return apiErr(400, "BAD_REQUEST", field+" must be an object")
		}
	}
	if err := validateLocalThread(obj(in["thread"])); err != nil {
		return err
	}
	if err := validateProfile(localProfile(obj(in["profile"]))); err != nil {
		return err
	}
	if value, ok := in["options"]; ok {
		if !validObject(value) {
			return apiErr(400, "BAD_REQUEST", "options must be an object")
		}
		if err := validateFields(obj(value), "model autoIntelligence reasoningEffort timezone", "thinking webSearch codeExecution sandbox piiCheck genUI", ""); err != nil {
			return err
		}
	}
	if in["kind"] == "resolve" {
		if !validObject(in["resolution"]) {
			return apiErr(400, "BAD_REQUEST", "resolution must be an object")
		}
		if err := validateFields(obj(in["resolution"]), "text", "", ""); err != nil {
			return err
		}
	}

	for _, field := range []string{"content", "quote"} {
		if v, ok := in[field]; ok {
			if _, ok := v.(string); !ok {
				return apiErr(400, "BAD_REQUEST", field+" must be a string")
			}
		}
	}
	if v, ok := in["attachments"]; ok {
		if _, ok := v.([]any); !ok {
			return apiErr(400, "BAD_REQUEST", "attachments must be an array")
		}
		for _, item := range arr(v) {
			if err := validateSentAttachment(item); err != nil {
				return err
			}
		}
	}
	if in["kind"] == "send" && strings.TrimSpace(str(in["content"])) == "" && len(arr(in["attachments"])) == 0 {
		return apiErr(400, "BAD_REQUEST", "A message or attachment is required")
	}
	return nil
}
func requestDigest(in object) string {
	copy := clone(in)
	delete(copy, "key")
	digest := sha256.Sum256(raw(copy))
	return hex.EncodeToString(digest[:])
}

// errAlreadyApplied answers a retried request whose turn its chat holds, once
// the run it started is no longer live.
var errAlreadyApplied = apiErr(409, "ALREADY_APPLIED", "This request was already applied; reload the chat")

// maxQueuedTurns bounds the turns one chat holds behind its running reply.
const maxQueuedTurns = 32

// commitTimeout bounds a finished run's writes and a queued turn's start.
const commitTimeout = time.Minute

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
	ephemeral, id := boolean(in["ephemeral"], false), str(in["threadId"])
	local := boolean(in["local"], false)
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
	if source == nil && str(in["projectId"]) != "" {
		project, err := requiredID(in, "projectId")
		if err != nil {
			return err
		}
		if p.Anonymous {
			return apiErr(401, "UNAUTHENTICATED", "Sign in to use a project")
		}
		// A stored chat's project is read as its first turn starts.
		if ephemeral {
			if _, err := h.storeRows.load(ctx, p, key, "project", project); err != nil {
				return err
			}
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
	r.out = &stream{run: r.log, threadID: id, runID: r.log.id}

	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	if h.live.closing {
		return errShutdown
	}
	s := h.live.threads[liveKey(p, id)]
	if s != nil && (s.deleted || s.ephemeral != ephemeral || s.local != local || !s.keyless && s.keyHash != key.fingerprint()) {
		return apiErr(404, "THREAD_NOT_FOUND", "The thread is unavailable")
	}
	if s != nil && s.active != nil && len(s.queue) >= maxQueuedTurns {
		return apiErr(429, "QUOTA_EXHAUSTED", "The turn queue is full")
	}
	if (s == nil || s.active == nil) && local && source == nil && seed == nil {
		return apiErr(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	if s == nil {
		s = &threadSession{id: id, owner: p.scope(), keyHash: key.fingerprint(), ephemeral: ephemeral, local: local, keyless: keyless}
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
	case ephemeral && s.row == nil:
		s.row = newThreadRow(id, in, source)
	}
	h.makeActive(s, r)
	return nil
}

// makeActive gives a chat's running slot to a turn about to start; live.mu is held.
func (h *harness) makeActive(s *threadSession, r *chatRun) {
	s.active = r
	if s.waiting != nil {
		s.waiting.Stop()
		s.waiting = nil
	}
}

// queueChanged tells a chat's running reply its queue; live.mu is held. A
// reply that has not started is told by its first state snapshot.
func queueChanged(s *threadSession) {
	if s.active != nil && s.active.running {
		s.active.out.emit(event{Type: "STATE_DELTA", Delta: []object{{"op": "replace", "path": "/queue", "value": queueItems(s)}}})
	}
}

func queueItem(r *chatRun) object {
	attachments := []any{}
	for _, a := range arr(r.input["attachments"]) {
		attachments = append(attachments, selectFields(obj(a), "id kind fileName mimeType size thumbnail"))
	}
	return object{"queueId": r.log.id, "kind": r.input["kind"], "content": r.input["content"], "quote": r.input["quote"], "attachments": attachments, "createdAt": r.input["createdAt"]}
}
func queueItems(s *threadSession) []object {
	out := []object{}
	for _, r := range s.queue {
		out = append(out, queueItem(r))
	}
	return out
}

// launch starts the chat's active turn. It commits the turn and the active
// run's marker before any model I/O, and holds no lock while it reads or
// writes; a chat being deleted or the service stopping refuses it at the end.
func (h *harness) launch(ctx context.Context, r *chatRun) error {
	s := r.thread
	h.live.mu.Lock()
	p := r.principal
	var row *storedRow
	if s.ephemeral {
		row = &storedRow{ID: s.row.ID, Data: clone(s.row.Data)}
	}
	h.live.mu.Unlock()
	ctx = context.WithValue(ctx, apiKeyKey{}, string(r.credential.Key))
	ctx = context.WithValue(ctx, usageContextKey{}, usageForRun(r))
	ctx = context.WithValue(ctx, usageSinkKey{}, r.out)
	profile, project, docs := object{}, object{}, []object{}
	if !s.ephemeral {
		var err error
		profile, err = h.profile(ctx, p, r.key)
		if err != nil {
			return err
		}
		// A new chat's first turn creates its row under the client's id.
		row, err = h.storeRows.load(ctx, p, r.key, "chat", s.id)
		if err != nil && (asAPIError(err).HTTP != 404 || !rowIDPattern.MatchString(s.id)) {
			return err
		}
		if err != nil {
			row = newThreadRow(s.id, r.input, nil)
		}
		normalizeThread(row)
		if row.Data["lastRequestId"] == r.nonce {
			return errAlreadyApplied
		}
		if id := str(row.Data["projectId"]); id != "" {
			stored, err := h.storeRows.load(ctx, p, r.key, "project", id)
			if err != nil {
				return err
			}
			project = stored.Data
			docs, err = h.projectDocuments(ctx, p, r.key, id)
			if err != nil {
				return err
			}
		}
	}
	if s.local {
		profile = clone(r.profile)
	}
	// A new chat starts with the default prompt preset, as the web client
	// stamps a blank chat. A first turn naming a preset, or null, overrides it.
	if _, named := r.input["presetId"]; !named && !s.ephemeral && row.ETag == "" && len(arr(row.Data["messages"])) == 0 {
		if preset := str(profile["defaultPromptPresetId"]); preset != "" {
			row.Data["presetId"] = preset
		}
	}
	working := clone(row.Data)
	attachments, err := turnAttachments(r.input, working)
	if err != nil {
		return err
	}
	if err := applyTurn(working, r.input, r.messageID, attachments); err != nil {
		return err
	}
	options := obj(r.input["options"])
	if row.ETag == "" && len(arr(row.Data["messages"])) == 0 {
		working["model"] = firstString(profile["selectedModel"], "auto")
	}
	working["model"] = normalizeModel(firstString(options["model"], working["model"], profile["selectedModel"], "auto"))
	if _, ok := options["webSearch"]; ok {
		working["webSearchEnabled"] = options["webSearch"]
	}
	// Sync stores a chat only if every attachment it names is stored under the
	// chat. Offloaded documents are read back before the budget counts them.
	// An ask's stored source has attachments in sync; a local source carries them.
	temporary := s.ephemeral && (r.input["kind"] != "ask" || s.local)
	if !s.ephemeral {
		if err := h.placeAttachments(ctx, p, s.id, working); err != nil {
			return err
		}
	}
	if !temporary {
		if err := h.hydrateDocuments(ctx, p, working); err != nil {
			return err
		}
	}
	preview, _, err := h.buildRequest(working, profile, project, docs, r.input, r.log.id)
	if err != nil {
		return err
	}
	if err := preflightBudget(preview); err != nil {
		return err
	}
	if err := h.hydrateContext(ctx, p, working, temporary); err != nil {
		return err
	}
	titleText := titleSource(working)
	request, model, err := h.buildRequest(working, profile, project, docs, r.input, r.log.id)
	if err != nil {
		return err
	}
	r.screening = h.screening(request)
	if r.input["kind"] == "retryToolCall" {
		for _, message := range arr(working["messages"]) {
			for _, b := range arr(obj(message)["timeline"]) {
				block := obj(b)
				if firstString(block["toolCallId"], block["id"]) == str(r.input["toolCallId"]) {
					r.retryTarget = clone(block)
					r.retryTarget["parentId"] = obj(message)["id"]
				}
			}
		}
		if r.retryTarget == nil {
			return apiErr(404, "THREAD_NOT_FOUND", "The widget call was not found")
		}
		if !slices.ContainsFunc(h.catalog.get().selectedWidgets(arr(r.input["widgets"]), true), func(w widget) bool { return w.Name == str(r.retryTarget["name"]) }) {
			return apiErr(503, "TOOL_UNAVAILABLE", "This widget is not enabled for the client")
		}
	}
	request.cacheScope = cacheScope(p.scope())
	request.refreshAuth = func(ctx context.Context) (inferenceCredential, error) {
		h.live.mu.Lock()
		p, rejected := *r.principal, r.credential.Key
		h.live.mu.Unlock()
		h.auth.reject(&p, rejected)
		credential, err := h.auth.inference(ctx, &p)
		if err == nil {
			h.live.mu.Lock()
			r.credential = credential
			h.live.mu.Unlock()
		}
		return credential, err
	}
	if err := preflightBudget(request); err != nil {
		return err
	}
	working["contextUsage"] = requestContextUsage(request)
	if !s.ephemeral {
		initial := row.Data
		stored, err := h.mutate(ctx, p, r.key, "chat", s.id, row.ETag == "", nil, func(data object) error {
			// Another attempt of this commit may have stored it already.
			if data["lastRequestId"] == r.nonce && obj(data["activeRun"])["runId"] != r.log.id {
				return errAlreadyApplied
			}
			if _, exists := data["messages"]; !exists {
				for field, value := range initial {
					data[field] = value
				}
			}
			keepDerivedAttachments(s.id, data)
			// The prompt's copy keeps attachment content; the stored row must not.
			stored := []any{}
			for _, a := range attachments {
				stored = append(stored, clone(obj(a)))
			}
			if err := applyTurn(data, r.input, r.messageID, stored); err != nil {
				return err
			}
			data["model"], data["webSearchEnabled"] = working["model"], working["webSearchEnabled"]
			data["contextUsage"] = working["contextUsage"]
			copyAttachmentDerivatives(working, data)
			stripInlineAttachments(data)
			data["activeRun"], data["lastRequestId"] = activeRun(r.log.id), r.nonce
			delete(data, "harnessRetry")
			delete(data, "harnessRequests")
			return nil
		})
		if err != nil {
			return err
		}
		row = stored
	} else {
		row.Data = working
		row.Data["activeRun"], row.Data["lastRequestId"] = activeRun(r.log.id), r.nonce
	}
	// A continuation streams into the reply it extends, which RUN_STARTED names.
	if r.input["kind"] == "continue" {
		messages := arr(row.Data["messages"])
		reply := obj(messages[len(messages)-1])
		r.assembler.seed(reply)
		r.out.replyID = str(reply["id"])
	}
	window := publicThread(row, "", 50)
	hasOlder := boolean(window["hasOlder"], false)
	summary := threadSummary(normalizeThread(row))
	h.live.mu.Lock()
	if s.deleted {
		h.live.mu.Unlock()
		return apiErr(404, "THREAD_NOT_FOUND", "The thread was deleted")
	}
	if h.live.closing {
		h.live.mu.Unlock()
		return errShutdown
	}
	r.row, r.running = row, true
	// Framed under the lock, so no queue change reaches the run before them.
	r.out.emit(event{Type: "RUN_STARTED", ThreadID: s.id, RunID: r.log.id, Queued: r.queued, MessageID: r.out.replyID})
	r.out.emit(event{Type: "MESSAGES_SNAPSHOT", Messages: window["messages"], HasOlder: &hasOlder})
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: object{"thread": summary, "queue": queueItems(s), "rateLimit": r.credential.Limit}})
	r.out.mu.Lock()
	r.out.observe = r.assembler.apply
	r.out.mu.Unlock()
	titlePrincipal := *r.principal
	h.live.working.Add(1)
	h.live.mu.Unlock()
	runCtx, cancel := context.WithTimeout(r.stopped, runTimeout)
	runCtx = context.WithValue(runCtx, apiKeyKey{}, string(r.credential.Key))
	runCtx = context.WithValue(runCtx, usageContextKey{}, usageForRun(r))
	runCtx = context.WithValue(runCtx, usageSinkKey{}, r.out)
	r.titleDone = make(chan struct{})
	titleState := row.Data["titleState"]
	go func() {
		defer close(r.titleDone)
		defer func() { rescued("title", recover()) }()
		if !s.ephemeral || s.local {
			titleCtx, stop := context.WithTimeout(runCtx, 30*time.Second)
			defer stop()
			h.generateTitle(titleCtx, r, &titlePrincipal, titleState, titleText)
		}
	}()
	go func() {
		defer cancel()
		var runErr error
		defer func() {
			if err := rescued("chat run", recover()); err != nil {
				runErr = err
			}
			h.finishRun(r, runErr)
		}()
		if r.retryTarget != nil {
			runErr = h.retryWidget(runCtx, r, request, model)
		} else {
			runErr = h.loop(runCtx, r.out, request, model)
		}
	}()
	return nil
}

// activeRun tells clients a reply may be streaming, until the run's deadline.
// It never locks the chat: a run whose process died leaves it behind.
func activeRun(runID string) object {
	return object{"runId": runID, "expiresAt": nowUTC().Add(runTimeout).Format(time.RFC3339Nano)}
}

func usageForRun(r *chatRun) usagereporting.Context {
	return usagereporting.Context{ContextID: r.log.id, RootRequestID: r.log.id, ParentService: "harness", APIKeyHash: usagereporting.HashAPIKey(string(r.credential.Key)), Depth: 1}
}

func applyTurn(thread, in object, messageID string, attachments []any) error {
	messages := arr(thread["messages"])
	for _, m := range messages {
		if obj(m)["id"] == messageID {
			return nil
		}
	}
	kind := str(in["kind"])
	content, quote := str(in["content"]), in["quote"]
	if kind == "edit" || kind == "regenerate" || kind == "continue" {
		index := -1
		for i, m := range messages {
			if obj(m)["id"] == in["messageId"] {
				index = i
				break
			}
		}
		// A continuation names its reply or extends the last message.
		if kind == "continue" && str(in["messageId"]) == "" {
			index = len(messages) - 1
		}
		if index < 0 {
			return apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		role := "user"
		if kind != "edit" {
			role = "assistant"
		}
		if obj(messages[index])["role"] != role {
			return apiErr(400, "BAD_REQUEST", "The turn kind does not match the message role")
		}
		if kind == "edit" {
			if _, present := in["attachments"]; !present {
				attachments = arr(obj(messages[index])["attachments"])
			}
			if _, present := in["quote"]; !present {
				quote = obj(messages[index])["quote"]
			}
		}
		// As on the web client, the continued reply becomes the last message.
		if kind == "continue" {
			index++
		}
		messages = messages[:index]
	}
	if kind == "resolve" {
		found := false
		for _, m := range messages {
			for _, b := range arr(obj(m)["timeline"]) {
				block := obj(b)
				if firstString(block["toolCallId"], block["id"]) == str(in["toolCallId"]) {
					block["resolution"], block["resolvedAt"] = in["resolution"], timestamp()
					found = true
				}
			}
		}
		if !found {
			return apiErr(404, "THREAD_NOT_FOUND", "The tool call was not found")
		}
		content = str(obj(in["resolution"])["text"])
	}
	if kind == "ask" {
		content = str(in["quote"])
		quote = nil
	}
	if kind != "regenerate" && kind != "retryToolCall" && kind != "continue" {
		messages = append(messages, object{"id": messageID, "role": "user", "content": content, "quote": quote, "attachments": attachments, "createdAt": timestamp(), "timestamp": timestamp()})
	}
	thread["messages"] = messages
	return nil
}

// finishRun commits what the run produced and frames its end, then hands the
// chat to the next queued turn. Its slower work, memory extraction and the
// safeguards submission, follows the terminal frame and holds no lock.
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
		committed = append(committed, selectFields(m, "id model modelDisplayName usage isError isInterrupted"))
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
		if s.ephemeral {
			if err := commit(r.row.Data); err != nil {
				runErr = err
			}
		} else {
			row, err := h.mutate(ctx, p, r.key, "chat", s.id, false, nil, commit)
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
	h.live.mu.Lock()
	queue := queueItems(s)
	if s.ephemeral {
		s.row = r.row
	}
	next := h.advance(r, !cancelled || r.yielded)
	deleted = s.deleted
	h.live.mu.Unlock()
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: object{"thread": summary, "queue": queue, "rateLimit": limit}})
	metadata := object{"messages": committed}
	// A device chat's client stores the thread the run leaves as it is.
	if s.local && r.input["kind"] != "ask" {
		metadata["thread"] = deviceThread(r.row)
	}
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
	if runErr == nil && !s.ephemeral && !deleted {
		h.extractMemory(ctx, p, r.key, r.row)
	}
	h.screen(r, messages, runErr)
	r.release()
}

// advance retires a run from its chat and returns the queued turn to start
// next, if the chat goes on; live.mu is held. A chat with nothing running or
// queued leaves the registry. A queue a stopped run leaves waits for its
// client to send or remove a turn, as long as a run may last.
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
	}
	switch {
	case s.active == nil && len(s.queue) == 0:
		if h.live.threads[s.owner+"/"+s.id] == s {
			delete(h.live.threads, s.owner+"/"+s.id)
		}
	case s.active == nil && s.waiting == nil:
		s.waiting = time.AfterFunc(runTimeout, func() { h.endWaitingQueue(s) })
	}
	return next
}

// runQueue starts queued turns until one runs or the chat's queue empties.
// A turn that cannot start ends with its own RUN_ERROR.
func (h *harness) runQueue(next *chatRun) {
	for next != nil {
		ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
		err := h.launch(ctx, next)
		cancel()
		if err == nil {
			return
		}
		next = h.endUnstarted(next, err)
	}
}

// endUnstarted ends a turn that failed to start and returns the queued turn
// to start after it.
func (h *harness) endUnstarted(r *chatRun, err error) *chatRun {
	h.live.mu.Lock()
	next := h.advance(r, true)
	h.live.mu.Unlock()
	e := asAPIError(err)
	r.out.emit(event{Type: "RUN_ERROR", ThreadID: r.thread.id, RunID: r.log.id, Message: e.Message, Code: e.Code, Kind: e.Kind, ResetsAt: e.ResetsAt, RetryAfter: e.RetryAfter})
	r.log.log.close(errDone)
	r.release()
	close(r.finished)
	return next
}

// endQueued ends a turn that never started with the frame that says why;
// live.mu is held.
func (h *harness) endQueued(r *chatRun, end event) {
	h.live.forget(r)
	end.ThreadID, end.RunID = r.thread.id, r.log.id
	r.out.emit(end)
	r.log.log.close(errDone)
	r.release()
	close(r.finished)
}

var removedTurn = event{Type: "RUN_FINISHED", Metadata: raw(object{"cancelled": true})}

// endWaitingQueue ends the queue of a stopped run that nobody came back to.
func (h *harness) endWaitingQueue(s *threadSession) {
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	if s.active != nil || h.live.threads[s.owner+"/"+s.id] != s {
		return
	}
	for _, r := range s.queue {
		h.endQueued(r, removedTurn)
	}
	s.queue, s.waiting = nil, nil
	delete(h.live.threads, s.owner+"/"+s.id)
}

func (h *harness) authorizedRun(p *principal, key contentKey, in object) (*chatRun, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, err
	}
	runID, err := requiredID(in, "runId")
	if err != nil {
		return nil, err
	}
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	r := h.live.runs[liveKey(p, runID)]
	if r == nil || r.thread.id != id {
		return nil, apiErr(404, "RUN_NOT_FOUND", "The run is not live on this instance")
	}
	if !r.thread.keyless && r.thread.keyHash != key.fingerprint() {
		return nil, apiErr(409, "KEY_MISMATCH", "The run uses another key")
	}
	return r, nil
}

// A missing key is checked against the run: keyless threads accept it, and
// every other thread compares the key's fingerprint.
func requestKey(in object) (contentKey, error) {
	if str(in["key"]) == "" {
		return contentKey{}, nil
	}
	return decodeContentKey(str(in["key"]))
}
func (h *harness) cancelThread(ctx context.Context, p *principal, _ contentKey, in object) (any, error) {
	key, err := requestKey(in)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := h.authorizedRun(p, key, in)
	if err != nil {
		return nil, err
	}
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	if r.thread.active != r || r.ending {
		return nil, apiErr(409, "RUN_IN_FLIGHT", "This run is no longer active")
	}
	r.cancelled = true
	r.log.stop()
	return object{}, nil
}

// Move the selected queued turn first, then finish the active run normally.
func (h *harness) sendQueued(ctx context.Context, p *principal, _ contentKey, in object) (any, error) {
	key, err := requestKey(in)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	copy := clone(in)
	copy["runId"] = in["queueId"]
	r, err := h.authorizedRun(p, key, copy)
	if err != nil {
		return nil, err
	}
	h.live.mu.Lock()
	s := r.thread
	index := slices.Index(s.queue, r)
	if index < 0 {
		h.live.mu.Unlock()
		return nil, apiErr(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.queue = append([]*chatRun{r}, slices.Delete(s.queue, index, index+1)...)
	if s.active != nil {
		queueChanged(s)
		if !s.active.ending {
			s.active.cancelled, s.active.yielded = true, true
			s.active.log.stop()
		}
		h.live.mu.Unlock()
		return object{}, nil
	}
	// After a stop the queue waits, so there is no run to yield.
	s.queue = s.queue[1:]
	h.makeActive(s, r)
	h.live.mu.Unlock()
	h.runQueue(r)
	return object{}, nil
}

func (h *harness) removeQueued(ctx context.Context, p *principal, _ contentKey, in object) (any, error) {
	key, err := requestKey(in)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	copy := clone(in)
	copy["runId"] = in["queueId"]
	r, err := h.authorizedRun(p, key, copy)
	if err != nil {
		return nil, err
	}
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	s := r.thread
	index := slices.Index(s.queue, r)
	if index < 0 {
		return nil, apiErr(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.queue = slices.Delete(s.queue, index, index+1)
	r.cancelled = true
	h.endQueued(r, removedTurn)
	queueChanged(s)
	if s.active == nil && len(s.queue) == 0 {
		if s.waiting != nil {
			s.waiting.Stop()
		}
		delete(h.live.threads, s.owner+"/"+s.id)
	}
	return object{}, nil
}

// stopThread stops a chat being deleted: its running reply is cancelled and
// each queued turn ends.
func (h *harness) stopThread(p *principal, id string) {
	h.live.mu.Lock()
	defer h.live.mu.Unlock()
	s := h.live.threads[liveKey(p, id)]
	if s == nil {
		return
	}
	s.deleted = true
	if s.active != nil {
		s.active.cancelled = true
		s.active.log.stop()
	}
	for _, r := range s.queue {
		h.endQueued(r, removedTurn)
	}
	s.queue = nil
	if s.active == nil {
		if s.waiting != nil {
			s.waiting.Stop()
		}
		delete(h.live.threads, liveKey(p, id))
	}
}
func (h *harness) handleFollow(w http.ResponseWriter, r *http.Request) {
	in, err := decodeJSON(w, r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	key, err := requestKey(in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	live, err := h.authorizedRun(p, key, in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if !p.Anonymous {
		h.live.mu.Lock()
		if live.thread.active == live {
			live.principal = p
		}
		h.live.mu.Unlock()
	}
	follow(w, r, live.log)
}
