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

type threadSession struct {
	mu                 sync.Mutex
	id, owner, keyHash string
	ephemeral, deleted bool
	// A local thread is stored by the client, which sends its transcript with
	// each turn. Like an anonymous thread, it never uses the content key.
	local, keyless bool
	row            *storedRow
	active         *chatRun
	queue          []*chatRun
	runs           map[string]*chatRun
	expires        time.Time
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
	finished                 chan struct{}
	titleDone                chan struct{}
	assembler                *messageAssembler
	retryTarget              object
	replacement              string
	profile                  object // a local thread's settings, sent by its client
	screening                *screening

	// yielded marks an active run stopped so a queued turn starts now.
	yielded bool
}

func (h *harness) threadState(p *principal, id string) *threadSession {
	h.chatMu.Lock()
	defer h.chatMu.Unlock()
	if h.threads == nil {
		h.threads = map[string]*threadSession{}
		h.requests = map[string]*chatRun{}
	}
	return h.threads[p.scope()+"/"+id]
}
func (h *harness) handleTurn(w http.ResponseWriter, r *http.Request) {
	in, err := decodeJSONWithin(w, r, maxTurnBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	p, err := h.auth.authenticate(r, true)
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
	if id := str(in["threadId"]); id != "" {
		if _, err := requiredID(object{"id": id}, "id"); err != nil {
			respond(w, nil, err)
			return
		}
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
	follow(w, r, accepted.log, resumeFrom(r.Header.Get("Last-Event-ID")))
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
			return apiErr(400, "BAD_REQUEST", "attachments must be an array of ids")
		}
		for _, id := range arr(v) {
			if _, err := requiredID(object{"id": id}, "id"); err != nil {
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

func (h *harness) acceptTurn(ctx context.Context, p *principal, key contentKey, in object) (*chatRun, error) {
	nonce, digest := str(in["clientRequestId"]), requestDigest(in)
	requestID := p.scope() + "/" + nonce
	h.chatMu.Lock()
	if h.closing {
		h.chatMu.Unlock()
		return nil, apiErr(503, "UPSTREAM_REFUSED", "The service is shutting down")
	}
	if h.starting == nil {
		h.starting = map[string]chan struct{}{}
	}
	if pending := h.starting[requestID]; pending != nil {
		h.chatMu.Unlock()
		select {
		case <-pending:
			return h.acceptTurn(ctx, p, key, in)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if len(h.starting)+len(h.requests) >= 128 && h.requests[requestID] == nil {
		h.chatMu.Unlock()
		return nil, apiErr(503, "UPSTREAM_REFUSED", "The run registry is at capacity")
	}
	h.starting[requestID] = make(chan struct{})
	defer func() {
		h.chatMu.Lock()
		close(h.starting[requestID])
		delete(h.starting, requestID)
		h.chatMu.Unlock()
	}()
	if h.requests == nil {
		h.requests = map[string]*chatRun{}
		h.threads = map[string]*threadSession{}
	}
	if previous := h.requests[requestID]; previous != nil {
		h.chatMu.Unlock()
		if previous.digest != digest || !previous.thread.keyless && previous.thread.keyHash != key.fingerprint() {
			return nil, apiErr(409, "REVISION_CONFLICT", "The request nonce was already used")
		}
		return previous, nil
	}
	h.chatMu.Unlock()
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return nil, err
	}
	if p.Anonymous && credential.Limit.exhausted() {
		e := apiErr(429, "QUOTA_EXHAUSTED", "The daily quota is exhausted")
		e.Kind = str(credential.Limit.Kind)
		e.ResetsAt = credential.Limit.ResetsAt
		return nil, e
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
				return nil, err
			}
			source = clone(normalizeThread(row))
		}
		id = ""
	}
	if id == "" && str(in["projectId"]) != "" {
		project, err := requiredID(in, "projectId")
		if err != nil {
			return nil, err
		}
		if p.Anonymous {
			return nil, apiErr(401, "UNAUTHENTICATED", "Sign in to use a project")
		}
		if _, err := h.storeRows.load(ctx, p, key, "project", project); err != nil {
			return nil, err
		}
	}
	if id == "" {
		if !ephemeral {
			var err error
			id, err = h.reserveThreadID(ctx, p, key, nonce, digest)
			if err != nil {
				return nil, err
			}
		} else {
			id = rowID()
		}
	}
	h.chatMu.Lock()
	state := h.threads[p.scope()+"/"+id]
	if state == nil {
		if len(h.threads) >= 128 {
			h.chatMu.Unlock()
			return nil, apiErr(503, "UPSTREAM_REFUSED", "The thread registry is at capacity")
		}
		state = &threadSession{id: id, owner: p.scope(), keyHash: key.fingerprint(), ephemeral: ephemeral, local: local, keyless: keyless, runs: map[string]*chatRun{}, expires: nowUTC().Add(runTimeout)}
		h.threads[p.scope()+"/"+id] = state
	}
	h.chatMu.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleted || state.ephemeral != ephemeral || state.local != local || !state.keyless && state.keyHash != key.fingerprint() {
		return nil, apiErr(404, "THREAD_NOT_FOUND", "The thread is unavailable")
	}
	h.chatMu.Lock()
	if previous := h.requests[requestID]; previous != nil {
		h.chatMu.Unlock()
		if previous.digest != digest {
			return nil, apiErr(409, "REVISION_CONFLICT", "The request nonce was already used")
		}
		return previous, nil
	}
	h.chatMu.Unlock()
	// An idle local thread starts from the client's copy, which may have been
	// edited since the last turn. A queued turn continues the active run's row.
	// An ask is a new thread that hides the copy it starts from, below.
	if state.local && state.active == nil && source == nil {
		if thread, ok := in["thread"]; ok {
			state.row = localThread(id, obj(thread))
		} else if str(in["threadId"]) != "" && source == nil {
			return nil, apiErr(400, "BAD_REQUEST", "A local thread requires its transcript")
		}
	}
	if state.row == nil {
		if !ephemeral {
			row, err := h.storeRows.load(ctx, p, key, "chat", id)
			if err == nil {
				normalizeThread(row)
				state.row = row
			} else if asAPIError(err).HTTP != 404 {
				return nil, err
			}
		}
	}
	if state.row == nil {
		if str(in["threadId"]) != "" && source == nil && !ephemeral {
			row, err := h.storeRows.load(ctx, p, key, "chat", id)
			if err != nil {
				return nil, err
			}
			normalizeThread(row)
			state.row = row
		} else {
			data := object{"id": id, "title": "New Chat", "titleState": "placeholder", "createdAt": timestamp(), "updatedAt": timestamp(), "messages": []any{}, "model": "auto", "activeRun": nil}
			data["projectId"], data["presetId"] = in["projectId"], in["presetId"]
			if source != nil {
				data["messages"] = source["messages"]
				data["harnessHiddenCount"] = len(arr(source["messages"]))
				data["projectId"], data["presetId"] = source["projectId"], source["presetId"]
			}
			state.row = &storedRow{ID: id, Data: data}
			normalizeThread(state.row)
		}
	}
	if state.active != nil && len(state.queue) >= 32 {
		return nil, apiErr(429, "QUOTA_EXHAUSTED", "The turn queue is full")
	}
	if saved := obj(obj(state.row.Data["harnessRequests"])[nonce]); len(saved) > 0 {
		if str(saved["digest"]) != digest {
			return nil, apiErr(409, "REVISION_CONFLICT", "The request nonce was already used")
		}
		return nil, apiErr(409, "REVISION_CONFLICT", "This request was already accepted")
	}
	if credential.Limit.exhausted() {
		e := apiErr(429, "QUOTA_EXHAUSTED", "The daily quota is exhausted")
		e.Kind = str(credential.Limit.Kind)
		e.ResetsAt = credential.Limit.ResetsAt
		return nil, e
	}
	runID := token()
	log := newRun(runID)
	clean := clone(in)
	delete(clean, "key")
	// The transcript has already seeded the row; holding it again per run would
	// double a local thread's memory while it waits in the queue.
	delete(clean, "thread")
	delete(clean, "profile")
	clean["createdAt"] = timestamp()
	accepted := &chatRun{thread: state, log: log, input: clean, principal: p, key: key, nonce: nonce, digest: digest, messageID: "msg_" + token(), credential: credential, finished: make(chan struct{}), assembler: newMessageAssembler()}
	if state.local {
		accepted.profile = localProfile(obj(in["profile"]))
	}
	accepted.queued = state.active != nil
	accepted.out = &stream{run: log, threadID: id, runID: runID}
	if accepted.queued {
		state.queue = append(state.queue, accepted)
		accepted.out.emit(event{Type: "STATE_DELTA", ThreadID: id, RunID: runID, Delta: []object{{"op": "add", "path": "/queue/-", "value": queueItem(accepted)}}})
		state.active.out.emit(event{Type: "STATE_DELTA", Delta: []object{{"op": "replace", "path": "/queue", "value": queueItems(state)}}})
	} else {
		if err := h.beginChatRun(ctx, accepted); err != nil {
			clear(accepted.key[:])
			accepted.input = nil
			if !state.ephemeral || state.local {
				state.row = nil
			}
			return nil, err
		}
	}
	state.runs[runID] = accepted
	h.chatMu.Lock()
	h.requests[requestID] = accepted
	h.chatMu.Unlock()
	return accepted, nil
}
func queueItem(r *chatRun) object {
	return object{"queueId": r.log.id, "kind": r.input["kind"], "content": r.input["content"], "quote": r.input["quote"], "attachments": arr(r.input["attachments"]), "createdAt": r.input["createdAt"]}
}
func queueItems(s *threadSession) []object {
	out := []object{}
	for _, r := range s.queue {
		out = append(out, queueItem(r))
	}
	return out
}

// Called with the thread lock held. The active marker commits before model I/O.
func (h *harness) beginChatRun(ctx context.Context, r *chatRun) error {
	s := r.thread
	ctx = context.WithValue(ctx, apiKeyKey{}, string(r.credential.Key))
	ctx = context.WithValue(ctx, usageContextKey{}, usageForRun(r))
	ctx = context.WithValue(ctx, usageSinkKey{}, r.out)
	profile, project, docs := object{}, object{}, []object{}
	if !s.ephemeral {
		var err error
		profile, err = h.profile(ctx, r.principal, r.key)
		if err != nil {
			return err
		}
		if s.row.ETag != "" {
			row, err := h.storeRows.load(ctx, r.principal, r.key, "chat", s.id)
			if err != nil {
				return err
			}
			s.row = row
			normalizeThread(s.row)
		}
		if id := str(s.row.Data["projectId"]); id != "" {
			row, err := h.storeRows.load(ctx, r.principal, r.key, "project", id)
			if err != nil {
				return err
			}
			project = row.Data
			docs, err = h.projectDocuments(ctx, r.principal, r.key, id)
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
	if _, named := r.input["presetId"]; !named && !s.ephemeral && s.row.ETag == "" && len(arr(s.row.Data["messages"])) == 0 {
		if preset := str(profile["defaultPromptPresetId"]); preset != "" {
			s.row.Data["presetId"] = preset
		}
	}
	working := clone(s.row.Data)
	attachments, err := h.turnAttachments(ctx, r.principal, r.key, r.input, working, s.ephemeral, s.keyless)
	if err != nil {
		return err
	}
	if err := applyTurn(working, r.input, r.messageID, attachments); err != nil {
		return err
	}
	options := obj(r.input["options"])
	if s.row.ETag == "" && len(arr(s.row.Data["messages"])) == 0 {
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
		if err := h.placeAttachments(ctx, r.principal, s.id, working); err != nil {
			return err
		}
	}
	if !temporary {
		if err := h.hydrateDocuments(ctx, r.principal, working); err != nil {
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
	if err := h.hydrateContext(ctx, r.principal, r.key, working, temporary, s.local); err != nil {
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
	request.cacheScope = cacheScope(r.principal.scope())
	request.refreshAuth = func(ctx context.Context) (inferenceCredential, error) {
		s.mu.Lock()
		p, rejected := *r.principal, r.credential.Key
		s.mu.Unlock()
		h.auth.reject(&p, rejected)
		credential, err := h.auth.inference(ctx, &p)
		if err == nil {
			s.mu.Lock()
			r.credential = credential
			s.mu.Unlock()
		}
		return credential, err
	}
	if err := preflightBudget(request); err != nil {
		return err
	}
	working["contextUsage"] = requestContextUsage(request)
	if !s.ephemeral {
		row, err := h.mutate(ctx, r.principal, r.key, "chat", s.id, s.row.ETag == "", nil, func(data object) error {
			if active := str(obj(data["activeRun"])["runId"]); active != "" && active != r.log.id {
				return apiErr(409, "RUN_IN_FLIGHT", "Another harness owns the active run")
			}
			if _, exists := data["messages"]; !exists {
				for field, value := range s.row.Data {
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
			data["activeRun"] = object{"runId": r.log.id, "lastEventId": 0, "expiresAt": nowUTC().Add(runTimeout).Format(time.RFC3339Nano)}
			delete(data, "harnessRetry")
			if r.retryTarget != nil {
				data["harnessRetry"] = object{"runId": r.log.id, "messageId": r.retryTarget["parentId"], "toolCallId": r.input["toolCallId"], "arguments": r.retryTarget["arguments"]}
			}
			requests := obj(data["harnessRequests"])
			requests[r.nonce] = object{"runId": r.log.id, "digest": r.digest}
			data["harnessRequests"] = requests
			return nil
		})
		if err != nil {
			return err
		}
		s.row = row
	} else {
		s.row.Data = working
		s.row.Data["activeRun"] = object{"runId": r.log.id, "lastEventId": 0, "expiresAt": nowUTC().Add(runTimeout).Format(time.RFC3339Nano)}
	}
	// A continuation streams into the reply it extends, which RUN_STARTED names.
	if r.input["kind"] == "continue" {
		messages := arr(s.row.Data["messages"])
		reply := obj(messages[len(messages)-1])
		r.assembler.seed(reply)
		r.out.replyID = str(reply["id"])
	}
	s.active, s.expires = r, nowUTC().Add(runTimeout)
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runTimeout)
	runCtx = context.WithValue(runCtx, apiKeyKey{}, string(r.credential.Key))
	runCtx = context.WithValue(runCtx, usageContextKey{}, usageForRun(r))
	r.log.stop = cancel
	r.out.emit(event{Type: "RUN_STARTED", ThreadID: s.id, RunID: r.log.id, Queued: r.queued, MessageID: r.out.replyID})
	window := publicThread(s.row, "", 50)
	hasOlder := boolean(window["hasOlder"], false)
	r.out.emit(event{Type: "MESSAGES_SNAPSHOT", Messages: window["messages"], HasOlder: &hasOlder})
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: object{"thread": threadSummary(normalizeThread(s.row)), "queue": queueItems(s), "rateLimit": r.credential.Limit}})
	r.out.observe = r.assembler.apply
	r.titleDone = make(chan struct{})
	titlePrincipal := *r.principal
	titleRun := &chatRun{thread: s, out: r.out, principal: &titlePrincipal, key: r.key}
	titleRow := &storedRow{ID: s.row.ID, ETag: s.row.ETag, Data: object{"titleState": s.row.Data["titleState"]}}
	go func() {
		defer close(r.titleDone)
		defer clear(titleRun.key[:])
		defer func() { rescued("title", recover()) }()
		if !s.ephemeral || s.local {
			titleCtx, stop := context.WithTimeout(runCtx, 30*time.Second)
			defer stop()
			h.generateTitle(titleCtx, titleRun, titleRow, titleText)
		}
	}()
	go func() {
		defer cancel()
		var runErr error
		defer func() {
			if err := rescued("chat run", recover()); err != nil {
				runErr = err
			}
			h.finishChatRun(r, runErr)
		}()
		if r.retryTarget != nil {
			runErr = h.retryWidget(runCtx, r, request, model)
		} else {
			runErr = h.loop(runCtx, r.out, request, model)
		}
	}()
	return nil
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

func (h *harness) finishChatRun(r *chatRun, runErr error) {
	if r.titleDone != nil {
		<-r.titleDone
	}
	s := r.thread
	s.mu.Lock()
	defer s.mu.Unlock()
	defer close(r.finished)
	defer func() {
		clear(r.key[:])
		r.out.mu.Lock()
		r.out.observe, r.out.sources = nil, nil
		r.out.mu.Unlock()
		r.input, r.retryTarget, r.assembler, r.screening = nil, nil, nil, nil
		r.replacement = ""
		r.principal = &principal{ID: r.principal.ID, AnonymousID: r.principal.AnonymousID, Anonymous: r.principal.Anonymous}
		r.credential.Key = ""
		r.profile = nil
		// The client keeps a local thread, so only a queued turn needs its row.
		if (!s.ephemeral || s.local) && s.active == nil && len(s.queue) == 0 {
			s.row = nil
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx = context.WithValue(ctx, apiKeyKey{}, string(r.credential.Key))
	ctx = context.WithValue(ctx, usageContextKey{}, usageForRun(r))
	ctx = context.WithValue(ctx, usageSinkKey{}, r.out)
	r.out.mu.Lock()
	messages := r.assembler.messages()
	spend, sources, reply := r.out.spend, r.out.sources, r.out.replyID
	r.out.mu.Unlock()
	continued := r.input["kind"] == "continue"
	start := func(annotation any) int { return integer(obj(obj(annotation)["url_citation"])["start_index"]) }
	for _, m := range messages {
		m["isInterrupted"], m["isError"] = r.cancelled, runErr != nil && !r.cancelled
		m["model"] = firstString(r.out.concreteModel, s.row.Data["model"])
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
				delete(data, "harnessRetry")
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
			delete(data, "harnessRetry")
		}
		return nil
	}
	if !s.deleted {
		if s.ephemeral {
			if err := commit(s.row.Data); err != nil {
				runErr = err
			}
		} else {
			row, err := h.mutate(ctx, r.principal, r.key, "chat", s.id, false, nil, commit)
			if err != nil {
				runErr = err
			} else {
				s.row = row
			}
		}
	}
	h.screen(r, messages, runErr)
	limit := h.auth.currentLimit(r.principal)
	if r.out.modelAccepted {
		limit = h.auth.consumed(r.principal)
	}
	if r.principal.Anonymous && runErr != nil && asAPIError(runErr).Code == "QUOTA_EXHAUSTED" {
		limit = exhaustedLimit("free_daily")
	}
	if runErr == nil && !s.ephemeral {
		h.extractMemory(ctx, r, s.row)
	}
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: object{"thread": threadSummary(normalizeThread(s.row)), "queue": queueItems(s), "rateLimit": limit}})
	if runErr != nil && !r.cancelled {
		e := asAPIError(runErr)
		r.out.emit(event{Type: "RUN_ERROR", ThreadID: s.id, RunID: r.log.id, Message: e.Message, Code: e.Code, Kind: e.Kind, ResetsAt: e.ResetsAt, RetryAfter: e.RetryAfter, Metadata: raw(object{"messages": committed})})
	} else {
		r.out.emit(event{Type: "RUN_FINISHED", ThreadID: s.id, RunID: r.log.id, Metadata: raw(object{"usage": spend, "rateLimit": limit, "cancelled": r.cancelled, "messages": committed})})
	}
	r.log.log.close(errDone)
	s.active = nil
	s.expires = nowUTC().Add(runTimeout)
	if (!r.cancelled || r.yielded) && !s.deleted && len(s.queue) > 0 {
		h.startQueued(ctx, s)
	}
}

// startQueued begins the thread's first queued turn; s.mu must be held.
func (h *harness) startQueued(ctx context.Context, s *threadSession) {
	next := s.queue[0]
	s.queue = s.queue[1:]
	if err := h.beginChatRun(ctx, next); err != nil {
		e := asAPIError(err)
		next.out.emit(event{Type: "RUN_ERROR", ThreadID: s.id, RunID: next.log.id, Message: e.Message, Code: e.Code})
		next.log.log.close(errDone)
		close(next.finished)
		clear(next.key[:])
	}
}

func (h *harness) authorizedRun(p *principal, key contentKey, in object) (*threadSession, *chatRun, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, nil, err
	}
	runID, err := requiredID(in, "runId")
	if err != nil {
		return nil, nil, err
	}
	s := h.threadState(p, id)
	if s == nil {
		return nil, nil, apiErr(404, "THREAD_NOT_FOUND", "The run is not in memory")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.keyless && s.keyHash != key.fingerprint() {
		return nil, nil, apiErr(409, "KEY_MISMATCH", "The run uses another key")
	}
	r := s.runs[runID]
	if r == nil {
		return nil, nil, apiErr(409, "RUN_IN_FLIGHT", "The run does not belong to this thread")
	}
	return s, r, nil
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
	s, r, err := h.authorizedRun(p, key, in)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != r {
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
	s, r, err := h.authorizedRun(p, key, copy)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index := slices.Index(s.queue, r)
	if index < 0 {
		return nil, apiErr(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.queue = append([]*chatRun{r}, slices.Delete(s.queue, index, index+1)...)
	if s.active != nil {
		s.active.out.emit(event{Type: "STATE_DELTA", Delta: []object{{"op": "replace", "path": "/queue", "value": queueItems(s)}}})
		s.active.cancelled, s.active.yielded = true, true
		s.active.log.stop()
	} else {
		// After a stop the queue waits, so there is no run to yield.
		h.startQueued(ctx, s)
	}
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
	s, r, err := h.authorizedRun(p, key, copy)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index := slices.Index(s.queue, r)
	if index < 0 {
		return nil, apiErr(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.queue = slices.Delete(s.queue, index, index+1)
	r.cancelled = true
	r.out.emit(event{Type: "RUN_FINISHED", ThreadID: s.id, RunID: r.log.id, Metadata: raw(object{"cancelled": true})})
	r.log.log.close(errDone)
	clear(r.key[:])
	r.input = nil
	r.credential.Key = ""
	r.principal = &principal{ID: p.ID, AnonymousID: p.AnonymousID, Anonymous: p.Anonymous}
	close(r.finished)
	if s.active != nil {
		s.active.out.emit(event{Type: "STATE_DELTA", Delta: []object{{"op": "replace", "path": "/queue", "value": queueItems(s)}}})
	}
	return object{}, nil
}
func (h *harness) stopThread(p *principal, id string) {
	if s := h.threadState(p, id); s != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.deleted = true
		if s.active != nil {
			s.active.cancelled = true
			s.active.log.stop()
		}
		for _, r := range s.queue {
			r.log.log.close(errDone)
			clear(r.key[:])
			close(r.finished)
		}
		s.queue = nil
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
	_, live, err := h.authorizedRun(p, key, in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if !p.Anonymous {
		live.thread.mu.Lock()
		if live.thread.active == live {
			live.principal = p
		}
		live.thread.mu.Unlock()
	}
	followChat(w, r, live.log, resumeFrom(r.Header.Get("Last-Event-ID")))
}

func (h *harness) expireThreads() {
	h.chatMu.Lock()
	states := make([]*threadSession, 0, len(h.threads))
	for _, s := range h.threads {
		states = append(states, s)
	}
	h.chatMu.Unlock()
	for _, s := range states {
		s.mu.Lock()
		if s.active == nil && nowUTC().After(s.expires) {
			h.chatMu.Lock()
			delete(h.threads, s.owner+"/"+s.id)
			for _, r := range s.runs {
				delete(h.requests, s.owner+"/"+r.nonce)
				clear(r.key[:])
				r.principal = nil
				r.assembler = nil
				r.input = nil
			}
			h.chatMu.Unlock()
			for _, r := range s.queue {
				r.log.log.close(errDone)
			}
			s.queue = nil
			s.row = nil
		}
		s.mu.Unlock()
	}
}
