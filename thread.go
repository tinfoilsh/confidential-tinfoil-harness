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
	row                *storedRow
	active             *chatRun
	queue              []*chatRun
	runs               map[string]*chatRun
	expires            time.Time
	key                contentKey // retained only for an ephemeral thread's lifetime
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
	spilled                  chan struct{}
	assembler                *messageAssembler
	retryTarget              object
	replacement              string
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
	if _, err := uuid.Parse(str(in["clientRequestId"])); err != nil {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "clientRequestId must be a UUID"))
		return
	}
	kind := firstString(in["kind"], "send")
	in["kind"] = kind
	if !slices.Contains([]string{"send", "edit", "regenerate", "resolve", "retryToolCall", "ask"}, kind) {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "Unknown turn kind"))
		return
	}
	if kind == "ask" {
		in["ephemeral"] = true
	}
	ephemeral := boolean(in["ephemeral"], false)
	if p.Anonymous && !ephemeral {
		respond(w, nil, apiErr(401, "UNAUTHENTICATED", "Anonymous turns must be ephemeral"))
		return
	}
	var key contentKey
	if !p.Anonymous || str(in["key"]) != "" {
		key, err = decodeContentKey(str(in["key"]))
		if err != nil {
			respond(w, nil, err)
			return
		}
	}
	defer clear(key[:])
	if kind == "ask" && p.Anonymous {
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
	if err := validateFields(in, "kind clientRequestId content quote messageId toolCallId", "ephemeral", "threadId projectId presetId"); err != nil {
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
		if previous.digest != digest || !p.Anonymous && previous.thread.keyHash != key.fingerprint() {
			return nil, apiErr(409, "REVISION_CONFLICT", "The request nonce was already used")
		}
		return previous, nil
	}
	h.chatMu.Unlock()
	ephemeral, id := boolean(in["ephemeral"], false), str(in["threadId"])
	var source object
	if in["kind"] == "ask" {
		row, err := h.storeRows.load(ctx, p, key, "chat", id)
		if err != nil {
			return nil, err
		}
		source = clone(normalizeThread(row))
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
		if p.Anonymous {
			key = randomKey()
		}
		state = &threadSession{id: id, owner: p.scope(), keyHash: key.fingerprint(), ephemeral: ephemeral, runs: map[string]*chatRun{}, expires: nowUTC().Add(runTimeout)}
		if ephemeral {
			state.key = key
		}
		h.threads[p.scope()+"/"+id] = state
	}
	h.chatMu.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleted || state.ephemeral != ephemeral || !p.Anonymous && state.keyHash != key.fingerprint() {
		return nil, apiErr(404, "THREAD_NOT_FOUND", "The thread is unavailable")
	}
	if p.Anonymous {
		key = state.key
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
			data := object{"id": id, "title": "New Chat", "titleState": "placeholder", "createdAt": timestamp(), "updatedAt": timestamp(), "messages": []any{}, "model": "auto", "activeRun": nil, "codeExecutionAccessToken": hex.EncodeToString(randomBytes(32))}
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
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return nil, err
	}
	if saved := obj(obj(state.row.Data["harnessRequests"])[nonce]); len(saved) > 0 {
		if str(saved["digest"]) != digest {
			return nil, apiErr(409, "REVISION_CONFLICT", "The request nonce was already used")
		}
		runID := str(saved["runId"])
		log, err := newContentRun(runID, key)
		if err == nil {
			err = log.rehydrate(h.fetch(context.WithValue(ctx, apiKeyKey{}, string(credential.Key)), runID))
		}
		if err != nil {
			return nil, apiErr(409, "REVISION_CONFLICT", "This request was already accepted; its log is unavailable")
		}
		if err := h.settleRecovered(ctx, p, key, state.row, log); err != nil {
			return nil, err
		}
		recovered := &chatRun{thread: state, log: log, nonce: nonce, digest: digest, principal: p, finished: make(chan struct{})}
		close(recovered.finished)
		state.runs[runID] = recovered
		h.chatMu.Lock()
		h.requests[requestID] = recovered
		h.chatMu.Unlock()
		return recovered, nil
	}
	if credential.Limit.Kind != nil && credential.Limit.Remaining <= 0 {
		e := apiErr(429, "QUOTA_EXHAUSTED", "The daily quota is exhausted")
		e.Kind = str(credential.Limit.Kind)
		e.ResetsAt = credential.Limit.ResetsAt
		return nil, e
	}
	runID := token() // recovery store accepts 32 hex characters
	log, err := newContentRun(runID, key)
	if err != nil {
		return nil, err
	}
	clean := clone(in)
	delete(clean, "key")
	clean["createdAt"] = timestamp()
	accepted := &chatRun{thread: state, log: log, input: clean, principal: p, key: key, nonce: nonce, digest: digest, messageID: "msg_" + token(), credential: credential, finished: make(chan struct{}), assembler: newMessageAssembler()}
	accepted.queued = state.active != nil
	accepted.out = &stream{run: log, threadID: id, runID: runID, managed: true}
	if accepted.queued {
		state.queue = append(state.queue, accepted)
		accepted.out.emit(event{Type: "STATE_DELTA", ThreadID: id, RunID: runID, Delta: []object{{"op": "add", "path": "/queue/-", "value": queueItem(accepted)}}})
		state.active.out.emit(event{Type: "STATE_DELTA", Delta: []object{{"op": "replace", "path": "/queue", "value": queueItems(state)}}})
	} else {
		if err := h.beginChatRun(ctx, accepted); err != nil {
			clear(accepted.key[:])
			accepted.input = nil
			if !state.ephemeral {
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
	working := clone(s.row.Data)
	attachments, err := h.turnAttachments(ctx, r.principal, r.key, r.input, working, s.ephemeral)
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
	if str(working["codeExecutionAccessToken"]) == "" {
		working["codeExecutionAccessToken"] = hex.EncodeToString(randomBytes(32))
	}
	preview, _, err := h.buildRequest(working, profile, project, docs, r.input, r.key, r.log.id)
	if err != nil {
		return err
	}
	if err := preflightBudget(preview); err != nil {
		return err
	}
	if !s.ephemeral {
		if err := h.storeLegacyAttachments(ctx, r.principal, working); err != nil {
			return err
		}
	}
	if err := h.hydrateContext(ctx, r.principal, r.key, working, s.ephemeral && r.input["kind"] != "ask"); err != nil {
		return err
	}
	request, model, err := h.buildRequest(working, profile, project, docs, r.input, r.key, r.log.id)
	if err != nil {
		return err
	}
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
			if err := applyTurn(data, r.input, r.messageID, attachments); err != nil {
				return err
			}
			data["model"], data["webSearchEnabled"], data["codeExecutionAccessToken"] = working["model"], working["webSearchEnabled"], working["codeExecutionAccessToken"]
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
	s.active, s.expires = r, nowUTC().Add(runTimeout)
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runTimeout)
	runCtx = context.WithValue(runCtx, apiKeyKey{}, string(r.credential.Key))
	runCtx = context.WithValue(runCtx, usageContextKey{}, usageForRun(r))
	r.log.stop = cancel
	r.out.emit(event{Type: "RUN_STARTED", ThreadID: s.id, RunID: r.log.id, Queued: r.queued})
	window := publicThread(s.row, "", 50)
	hasOlder := boolean(window["hasOlder"], false)
	r.out.emit(event{Type: "MESSAGES_SNAPSHOT", Messages: window["messages"], HasOlder: &hasOlder})
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: object{"thread": threadSummary(normalizeThread(s.row)), "queue": queueItems(s), "rateLimit": r.credential.Limit}})
	r.out.observe = r.assembler.apply
	if !s.ephemeral {
		spillCtx, stopSpill := context.WithTimeout(context.WithoutCancel(runCtx), runTimeout+time.Minute)
		r.log.abandon = func() { cancel(); stopSpill() }
		r.spilled = make(chan struct{})
		go func() {
			defer stopSpill()
			defer close(r.spilled)
			defer func() { rescued("chat run spill", recover()) }()
			h.spill(spillCtx, r.log)
		}()
	}
	r.titleDone = make(chan struct{})
	titlePrincipal := *r.principal
	titleRun := &chatRun{thread: s, out: r.out, principal: &titlePrincipal, key: r.key}
	titleRow := &storedRow{ID: s.row.ID, ETag: s.row.ETag, Data: clone(s.row.Data)}
	go func() {
		defer close(r.titleDone)
		defer clear(titleRun.key[:])
		defer func() { rescued("title", recover()) }()
		if !s.ephemeral {
			titleCtx, stop := context.WithTimeout(runCtx, 30*time.Second)
			defer stop()
			h.generateTitle(titleCtx, titleRun, titleRow)
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
	if kind == "edit" || kind == "regenerate" {
		index := -1
		for i, m := range messages {
			if obj(m)["id"] == in["messageId"] {
				index = i
				break
			}
		}
		if index < 0 {
			return apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		role := "user"
		if kind == "regenerate" {
			role = "assistant"
		}
		if obj(messages[index])["role"] != role {
			return apiErr(400, "BAD_REQUEST", "The turn kind does not match the message role")
		}
		if kind == "edit" {
			if _, present := in["attachments"]; !present {
				attachments = arr(obj(messages[index])["attachments"])
			}
		}
		messages = messages[:index]
	}
	content := str(in["content"])
	quote := in["quote"]
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
	if kind != "regenerate" && kind != "retryToolCall" {
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
		r.out.observe = nil
		r.out.mu.Unlock()
		r.input, r.retryTarget, r.assembler = nil, nil, nil
		r.replacement = ""
		r.principal = &principal{ID: r.principal.ID, IP: r.principal.IP, Anonymous: r.principal.Anonymous}
		r.credential.Key = ""
		if !s.ephemeral && s.active == nil {
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
	spend := r.out.spend
	r.out.mu.Unlock()
	for _, m := range messages {
		m["isInterrupted"], m["isError"] = r.cancelled, runErr != nil && !r.cancelled
		m["model"] = firstString(r.out.concreteModel, s.row.Data["model"])
		if definition := h.catalog.get().definition(str(m["model"])); definition != nil {
			m["modelDisplayName"] = definition["name"]
		}
		m["usage"] = object{"promptTokens": spend.Prompt, "completionTokens": spend.Completion}
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
			if !slices.ContainsFunc(all, func(v any) bool { return obj(v)["id"] == m["id"] }) {
				all = append(all, m)
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
	limit := h.auth.currentLimit(r.principal)
	if r.out.modelAccepted {
		limit = h.auth.consumed(r.principal)
	}
	if runErr == nil && !s.ephemeral {
		h.extractMemory(ctx, r, s.row)
	}
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: object{"thread": threadSummary(normalizeThread(s.row)), "queue": queueItems(s), "rateLimit": limit}})
	if runErr != nil && !r.cancelled {
		e := asAPIError(runErr)
		r.out.emit(event{Type: "RUN_ERROR", ThreadID: s.id, RunID: r.log.id, Message: e.Message, Code: e.Code, Kind: e.Kind, ResetsAt: e.ResetsAt, RetryAfter: e.RetryAfter})
	} else {
		r.out.emit(event{Type: "RUN_FINISHED", ThreadID: s.id, RunID: r.log.id, Metadata: raw(object{"usage": spend, "rateLimit": limit, "cancelled": r.cancelled})})
	}
	r.log.log.close(errDone)
	s.active = nil
	s.expires = nowUTC().Add(runTimeout)
	if !r.cancelled && !s.deleted && len(s.queue) > 0 {
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
	if !p.Anonymous && s.keyHash != key.fingerprint() {
		return nil, nil, apiErr(409, "KEY_MISMATCH", "The run uses another key")
	}
	r := s.runs[runID]
	if r == nil {
		return nil, nil, apiErr(409, "RUN_IN_FLIGHT", "The run does not belong to this thread")
	}
	return s, r, nil
}
func requestKey(p *principal, in object) (contentKey, error) {
	if p.Anonymous && str(in["key"]) == "" {
		return contentKey{}, nil
	}
	return decodeContentKey(str(in["key"]))
}
func (h *harness) cancelThread(ctx context.Context, p *principal, _ contentKey, in object) (any, error) {
	key, err := requestKey(p, in)
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
	key, err := requestKey(p, in)
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
		s.active.cancelled = true
		s.active.log.stop()
	}
	return object{}, nil
}

func (h *harness) removeQueued(ctx context.Context, p *principal, _ contentKey, in object) (any, error) {
	key, err := requestKey(p, in)
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
	r.principal = &principal{ID: p.ID, IP: p.IP, Anonymous: p.Anonymous}
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
	key, err := requestKey(p, in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	_, live, err := h.authorizedRun(p, key, in)
	if err == nil {
		if !p.Anonymous {
			live.thread.mu.Lock()
			if live.thread.active == live {
				live.principal = p
			} else if !live.thread.ephemeral {
				row, loadErr := h.storeRows.load(r.Context(), p, key, "chat", live.thread.id)
				if loadErr != nil {
					err = loadErr
				} else {
					err = h.settleRecovered(r.Context(), p, key, row, live.log)
				}
			}
			live.thread.mu.Unlock()
			if err != nil {
				respond(w, nil, err)
				return
			}
		}
		followChat(w, r, live.log, resumeFrom(r.Header.Get("Last-Event-ID")))
		return
	}
	if asAPIError(err).HTTP != 404 || p.Anonymous {
		respond(w, nil, err)
		return
	}
	id, err := requiredID(in, "threadId")
	if err != nil {
		respond(w, nil, err)
		return
	}
	runID, err := requiredID(in, "runId")
	if err != nil {
		respond(w, nil, err)
		return
	}
	row, err := h.storeRows.load(r.Context(), p, key, "chat", id)
	if err != nil {
		respond(w, nil, err)
		return
	}
	known := obj(row.Data["activeRun"])["runId"] == runID
	for _, value := range obj(row.Data["harnessRequests"]) {
		if obj(value)["runId"] == runID {
			known = true
		}
	}
	if !known {
		respond(w, nil, apiErr(409, "RUN_IN_FLIGHT", "The run does not belong to this thread"))
		return
	}
	cred, err := h.auth.inference(r.Context(), p)
	if err != nil {
		respond(w, nil, err)
		return
	}
	ctx := context.WithValue(r.Context(), apiKeyKey{}, string(cred.Key))
	log, err := newContentRun(runID, key)
	if err == nil {
		err = log.rehydrate(h.fetch(ctx, runID))
	}
	if err != nil {
		respond(w, nil, apiErr(404, "THREAD_NOT_FOUND", "The run log could not be opened"))
		return
	}
	if err := h.settleRecovered(r.Context(), p, key, row, log); err != nil {
		respond(w, nil, err)
		return
	}
	followChat(w, r, log, resumeFrom(r.Header.Get("Last-Event-ID")))
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
			clear(s.key[:])
		}
		s.mu.Unlock()
	}
}
