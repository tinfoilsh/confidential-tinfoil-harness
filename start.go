package main

import (
	"context"
	"slices"
	"time"

	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

// commitTimeout bounds a finished run's writes and a queued turn's start.
const commitTimeout = time.Minute

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
