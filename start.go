package main

import (
	"context"
	"slices"
	"time"

	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

// commitTimeout bounds a finished run's writes and a queued turn's start.
const commitTimeout = time.Minute

// turnPlan is what a turn starts from: its thread with the turn applied, in
// the form it is stored, and the request its run makes.
type turnPlan struct {
	row         *storedRow
	request     *request
	model       *model
	titleText   string
	retryTarget object
}

// prepareTurn applies a turn to its chat as read now. A stored chat's
// attachments are placed under it; the prompt reads their content.
func (h *harness) prepareTurn(ctx context.Context, r *chatRun, p *principal) (*turnPlan, error) {
	s := r.thread
	var row *storedRow
	profile, project, docs := object{}, object{}, []object{}
	if s.local {
		h.live.mu.Lock()
		row = &storedRow{ID: s.row.ID, Data: clone(s.row.Data)}
		h.live.mu.Unlock()
		profile = clone(r.profile)
	} else {
		var err error
		profile, err = h.profile(ctx, p, r.key)
		if err != nil {
			return nil, err
		}
		// A new chat's first turn creates its row under the client's id.
		row, err = h.storeRows.load(ctx, p, r.key, "chat", s.id)
		if err != nil && (asAPIError(err).HTTP != 404 || !rowIDPattern.MatchString(s.id)) {
			return nil, err
		}
		if err != nil {
			row = newThreadRow(s.id, r.input, nil)
		}
		normalizeThread(row)
		if row.Data["lastRequestId"] == r.nonce {
			return nil, errAlreadyApplied
		}
		if id := str(row.Data["projectId"]); id != "" {
			stored, err := h.storeRows.load(ctx, p, r.key, "project", id)
			if err != nil {
				return nil, err
			}
			project = stored.Data
			docs, err = h.projectDocuments(ctx, p, r.key, id)
			if err != nil {
				return nil, err
			}
		}
	}
	data := row.Data
	created := row.ETag == "" && len(arr(data["messages"])) == 0
	// A new chat starts with the default prompt preset, as the web client
	// stamps a blank chat. A first turn naming a preset, or null, overrides it.
	if _, named := r.input["presetId"]; !named && !s.local && created {
		if preset := str(profile["defaultPromptPresetId"]); preset != "" {
			data["presetId"] = preset
		}
	}
	attachments, err := turnAttachments(r.input, data)
	if err != nil {
		return nil, err
	}
	if err := applyTurn(data, r.input, r.messageID, attachments); err != nil {
		return nil, err
	}
	options := obj(r.input["options"])
	if created {
		data["model"] = firstString(profile["selectedModel"], "auto")
	}
	data["model"] = firstString(options["model"], data["model"], "auto")
	if _, ok := options["webSearch"]; ok {
		data["webSearchEnabled"] = options["webSearch"]
	}
	// Sync stores a chat only if every attachment it names is stored under the
	// chat. An ask's stored source has attachments in sync; a keyless thread
	// carries its own. Image descriptions are stored, image bytes are not.
	if !s.local {
		if err := h.placeAttachments(ctx, p, s.id, data); err != nil {
			return nil, err
		}
	}
	if err := h.hydrateContext(ctx, p, data, s.keyless); err != nil {
		return nil, err
	}
	// Offloaded documents are read back for the prompt alone.
	working := clone(data)
	if !s.keyless {
		if err := h.hydrateDocuments(ctx, p, working); err != nil {
			return nil, err
		}
	}
	request, model, err := h.buildRequest(working, profile, project, docs, r.input, r.log.id)
	if err != nil {
		return nil, err
	}
	if err := preflightBudget(request); err != nil {
		return nil, err
	}
	plan := &turnPlan{row: row, request: request, model: model, titleText: titleSource(working)}
	if r.input["kind"] == "retryToolCall" {
		for _, message := range arr(data["messages"]) {
			for _, b := range arr(obj(message)["timeline"]) {
				block := obj(b)
				if firstString(block["toolCallId"], block["id"]) == str(r.input["toolCallId"]) {
					plan.retryTarget = clone(block)
					plan.retryTarget["parentId"] = obj(message)["id"]
				}
			}
		}
		if plan.retryTarget == nil {
			return nil, apiErr(404, "THREAD_NOT_FOUND", "The widget call was not found")
		}
		if !slices.ContainsFunc(h.catalog.get().selectedWidgets(arr(r.input["widgets"]), true), func(w widget) bool { return w.Name == str(plan.retryTarget["name"]) }) {
			return nil, apiErr(503, "TOOL_UNAVAILABLE", "This widget is not enabled for the client")
		}
	}
	data["contextUsage"] = requestContextUsage(request)
	data["activeRun"], data["lastRequestId"] = activeRun(r.log.id), r.nonce
	if !s.local {
		stripInlineAttachments(data)
	}
	return plan, nil
}

// launch starts the chat's active turn. It commits the turn and the active
// run's marker before any model I/O, and holds no lock while it reads or
// writes; a chat being deleted or the service stopping refuses it at the end.
func (h *harness) launch(ctx context.Context, r *chatRun) error {
	s := r.thread
	h.live.mu.Lock()
	p := r.principal
	h.live.mu.Unlock()
	ctx = context.WithValue(ctx, apiKeyKey{}, string(r.credential.Key))
	ctx = context.WithValue(ctx, usageContextKey{}, usageForRun(r))
	ctx = context.WithValue(ctx, usageSinkKey{}, r.out)
	var plan *turnPlan
	// A chat another writer changed since it was read takes the turn again.
	for attempt := 1; ; attempt++ {
		var err error
		if plan, err = h.prepareTurn(ctx, r, p); err != nil {
			return err
		}
		if s.local {
			break
		}
		err = h.store(ctx, p, r.key, "chat", plan.row)
		if err == nil {
			break
		}
		if asAPIError(err).Code != "REVISION_CONFLICT" || attempt == maxCommitAttempts {
			return err
		}
	}
	row, request, model, titleText := plan.row, plan.request, plan.model, plan.titleText
	r.screening, r.retryTarget = h.screening(request), plan.retryTarget
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
	// A continuation streams into the reply it extends, which RUN_STARTED names.
	if r.input["kind"] == "continue" {
		messages := arr(row.Data["messages"])
		reply := obj(messages[len(messages)-1])
		r.assembler.seed(reply)
		r.out.replyID = str(reply["id"])
	}
	window := publicThread(row, "", messageWindow)
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
	r.out.emit(event{Type: "RUN_STARTED", ThreadID: s.id, RunID: r.log.id, MessageID: r.out.replyID})
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
		// An ask's thread is discarded with its run.
		if r.input["kind"] != "ask" {
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
