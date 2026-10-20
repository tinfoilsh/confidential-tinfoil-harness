package main

import (
	"cmp"
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
	row         *threadRow
	turn        *turnRequest // the turn carried out: a retry is the turn it repeats
	request     *request
	model       *model
	titleText   string
	retryTarget *retryTarget
}

// retryTarget is the widget call a retry regenerates, and the reply it is in.
type retryTarget struct {
	block    Block
	parentID string
}

// prepareTurn applies a turn to its chat as read now. A stored chat's
// attachments are placed under it; the prompt reads their content.
func (h *harness) prepareTurn(ctx context.Context, r *chatRun, p *principal) (*turnPlan, error) {
	s := r.thread
	var row *threadRow
	var profile Profile
	var project *Project
	var docs []ProjectDocument
	if s.local {
		h.live.mu.Lock()
		row = &threadRow{Thread: s.row.clone()}
		h.live.mu.Unlock()
		if r.profile != nil {
			profile = *r.profile
		}
	} else {
		var err error
		profile, err = h.profile(ctx, p, r.key)
		if err != nil {
			return nil, err
		}
		// A new chat's first turn creates its row under the client's id.
		row, err = h.loadThread(ctx, p, r.key, s.id)
		if err != nil && (asAPIError(err).HTTP != 404 || !rowIDPattern.MatchString(s.id)) {
			return nil, err
		}
		if err != nil {
			row = &threadRow{Thread: newThread(s.id, r.input.ProjectID, r.input.PresetID.value)}
		}
		if row.LastRequestID == r.nonce {
			return nil, errAlreadyApplied
		}
		if id := valueOr(row.ProjectID, ""); id != "" {
			stored, err := h.loadProject(ctx, p, r.key, id)
			if err != nil {
				return nil, err
			}
			project = &stored
			docs, err = h.projectDocuments(ctx, p, r.key, id)
			if err != nil {
				return nil, err
			}
		}
	}
	t := &row.Thread
	turn := r.input
	if turn.Kind == "retry" {
		var err error
		if turn, err = retried(t, r.input); err != nil {
			return nil, err
		}
	}
	// A new chat starts on the user's model and default prompt preset, whose
	// settings it takes. A first turn naming a preset, or null, overrides
	// the default, and the turn's own settings override the preset's.
	if row.etag == "" && len(t.Messages) == 0 {
		preset := r.input.PresetID.value
		if id := valueOr(profile.DefaultPromptPresetID, ""); !r.input.PresetID.set && id != "" {
			preset = &id
		}
		t.Model = cmp.Or(valueOr(profile.SelectedModel, ""), autoModelName)
		h.catalog.get().applyPreset(t, &profile, preset)
	}
	attachments, err := turnAttachments(turn, t)
	if err != nil {
		return nil, err
	}
	if err := applyTurn(t, turn, r.messageID, attachments); err != nil {
		return nil, err
	}
	options := r.input.Options
	t.Model = cmp.Or(options.Model, t.Model, autoModelName)
	t.WebSearchEnabled = valueOr(options.WebSearch, t.WebSearchEnabled)
	// Sync stores a chat only if every attachment it names is stored under the
	// chat. An ask's stored source has attachments in sync; a keyless thread
	// carries its own. Image descriptions are stored, image bytes are not.
	if !s.local {
		if err := h.placeAttachments(ctx, p, s.id, t); err != nil {
			return nil, err
		}
	}
	if err := h.hydrateContext(ctx, p, t, s.keyless); err != nil {
		return nil, err
	}
	// Offloaded documents are read back for the prompt alone.
	working := t.clone()
	if !s.keyless {
		if err := h.hydrateDocuments(ctx, p, &working); err != nil {
			return nil, err
		}
	}
	request, model, err := h.buildRequest(&working, &profile, project, docs, turn, r.log.id)
	if err != nil {
		return nil, err
	}
	if err := preflightBudget(request); err != nil {
		return nil, err
	}
	plan := &turnPlan{row: row, turn: turn, request: request, model: model, titleText: titleSource(&working)}
	if r.input.Kind == "retryToolCall" {
		for _, message := range t.Messages {
			for _, block := range message.Timeline {
				if block.callID() == r.input.ToolCallID {
					plan.retryTarget = &retryTarget{block: block, parentID: message.ID}
				}
			}
		}
		if plan.retryTarget == nil {
			return nil, apiErr(404, "THREAD_NOT_FOUND", "The widget call was not found")
		}
		if !slices.ContainsFunc(h.catalog.get().selectedWidgets(r.input.Widgets, true), func(w widget) bool { return w.Name == plan.retryTarget.block.Name }) {
			return nil, apiErr(503, "TOOL_UNAVAILABLE", "This widget is not enabled for the client")
		}
	}
	t.ContextUsage = requestContextUsage(request)
	t.ActiveRun, t.LastRequestID, t.LastRequestKind = activeRun(r.log.id), r.nonce, turn.Kind
	if !s.local {
		stripInlineAttachments(t)
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
		err = h.saveThread(ctx, p, r.key, plan.row)
		if err == nil {
			break
		}
		if asAPIError(err).Code != "REVISION_CONFLICT" || attempt == maxCommitAttempts {
			return err
		}
	}
	row, request, model, titleText := plan.row, plan.request, plan.model, plan.titleText
	r.screening, r.retryTarget, r.turn = h.screening(request), plan.retryTarget, plan.turn
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
	if r.turn.Kind == "continue" {
		reply := row.Messages[len(row.Messages)-1]
		r.assembler.seed(reply)
		r.out.replyID = reply.ID
	}
	shown := window(row, "", messageWindow)
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
	r.out.emit(event{Type: "MESSAGES_SNAPSHOT", Messages: shown.Messages, HasOlder: &shown.HasOlder})
	r.out.emit(event{Type: "STATE_SNAPSHOT", Snapshot: stateSnapshot{shown.threadSummary, queueItems(s), r.credential.Limit}})
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
	titleState := row.TitleState
	go func() {
		defer close(r.titleDone)
		defer func() { rescued("title", recover()) }()
		// An ask's thread is discarded with its run.
		if r.input.Kind != "ask" {
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

// activeRun marks a chat's reply as streaming until the run's deadline.
func activeRun(runID string) *ActiveRun {
	return &ActiveRun{RunID: runID, ExpiresAt: nowUTC().Add(runTimeout).Format(time.RFC3339Nano)}
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
	e := asAPIError(h.withQuota(r.principal, err))
	limit := valueOr(e.RateLimit, h.auth.currentLimit(r.principal))
	r.out.emit(event{Type: "RUN_ERROR", ThreadID: r.thread.id, RunID: r.log.id, Message: e.Message, Code: e.Code, Kind: e.Kind, ResetsAt: e.ResetsAt, RetryAfter: e.RetryAfter, Metadata: raw(runMetadata{RateLimit: &limit})})
	r.log.log.close(errDone)
	r.release()
	close(r.finished)
	return next
}
