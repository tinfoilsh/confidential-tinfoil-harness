package main

import (
	"cmp"
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

// commitTimeout bounds a finished run's writes and a queued turn's start.
const commitTimeout = time.Minute

// turnPlan is what a turn starts from: its thread with the turn applied, in
// the form it is stored, and the request its run makes.
type turnPlan struct {
	row         *domain.ThreadRow
	turn        *turnRequest // the turn carried out: a retry is the turn it repeats
	request     *request
	model       *inference.Model
	titleText   string
	retryTarget *retryTarget
}

// retryTarget is the widget call a retry regenerates, and the reply it is in.
type retryTarget struct {
	block    domain.Block
	parentID string
}

// errThreadDeleted refuses a turn on a chat deleted while the turn started.
var errThreadDeleted = api.Err(404, "THREAD_NOT_FOUND", "The thread was deleted")

// prepareTurn applies a turn to its chat as read now. A stored chat's
// attachments are placed under it; the prompt reads their content. With
// create, a new chat's missing row is made.
func (h *harness) prepareTurn(ctx context.Context, r *chatRun, p *auth.Principal, create bool) (*turnPlan, error) {
	s := r.thread
	var row *domain.ThreadRow
	var profile domain.Profile
	var project *domain.Project
	var docs []domain.ProjectDocument
	// The run holds a device chat's row and an ask's. A device chat's
	// settings are the ones its client sends; an ask about a stored chat
	// reads the user's and the chat's project, as the chat does.
	if s.local {
		h.live.mu.Lock()
		row = &domain.ThreadRow{Thread: s.row.Clone()}
		h.live.mu.Unlock()
	}
	if r.input.Local {
		if r.profile != nil {
			profile = *r.profile
		}
	} else {
		var err error
		profile, err = h.store.Profile(ctx, p, r.key)
		if err != nil {
			return nil, err
		}
		if !s.local {
			// A new chat's first turn creates its row under the client's id.
			row, err = h.store.LoadThread(ctx, p, r.key, s.id)
			if err != nil && (api.AsError(err).HTTP != 404 || !domain.RowIDPattern.MatchString(s.id) || !create) {
				return nil, err
			}
			if err != nil {
				row = &domain.ThreadRow{Thread: newThread(s.id, r.input.ProjectID, r.input.PresetID.Value)}
			}
			if row.LastRequestID == r.nonce {
				return nil, errAlreadyApplied
			}
		}
		if id := domain.ValueOr(row.ProjectID, ""); id != "" {
			stored, err := h.store.LoadProject(ctx, p, r.key, id)
			if err != nil {
				return nil, err
			}
			project = &stored
			docs, err = h.store.ProjectDocuments(ctx, p, r.key, id, store.RefuseUnreadable)
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
	// A new chat starts on the user's model, web search setting and default
	// prompt preset, whose settings it takes. A first turn naming a preset,
	// or null, overrides the default, and the turn's own settings override
	// the preset's.
	if row.ETag == "" && len(t.Messages) == 0 {
		preset := r.input.PresetID.Value
		if id := domain.ValueOr(profile.DefaultPromptPresetID, ""); !r.input.PresetID.Set && id != "" {
			preset = &id
		}
		c := h.models.Catalog.Get()
		t.Model = c.SelectedModel(&profile)
		t.WebSearchEnabled = domain.ValueOr(profile.WebSearchEnabled, t.WebSearchEnabled)
		c.ApplyPreset(t, &profile, preset)
	}
	attachments, err := attach.OfTurn(turn.Attachments, t)
	if err != nil {
		return nil, err
	}
	if err := applyTurn(t, turn, r.messageID, attachments); err != nil {
		return nil, err
	}
	options := r.input.Options
	t.Model = cmp.Or(options.Model, t.Model, domain.AutoModelName)
	t.WebSearchEnabled = domain.ValueOr(options.WebSearch, t.WebSearchEnabled)
	// Sync stores a chat only if every attachment it names is stored under the
	// chat. An ask's stored source has attachments in sync; a keyless thread
	// carries its own. Image descriptions are stored, image bytes are not.
	if !s.local {
		if err := h.attachments.PlaceAttachments(ctx, p, s.id, t); err != nil {
			return nil, err
		}
	}
	if err := h.attachments.HydrateContext(ctx, p, t, s.keyless); err != nil {
		return nil, err
	}
	// Offloaded documents are read back for the prompt alone.
	working := t.Clone()
	if !s.keyless {
		if err := h.attachments.HydrateDocuments(ctx, p, &working); err != nil {
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
				if block.CallID() == r.input.ToolCallID {
					plan.retryTarget = &retryTarget{block: block, parentID: message.ID}
				}
			}
		}
		if plan.retryTarget == nil {
			return nil, api.Err(404, "THREAD_NOT_FOUND", "The widget call was not found")
		}
		if !slices.ContainsFunc(h.models.Catalog.Get().SelectedWidgets(r.input.Widgets, true), func(w domain.Widget) bool { return w.Name == plan.retryTarget.block.Name }) {
			return nil, api.Err(503, "TOOL_UNAVAILABLE", "This widget is not enabled for the client").WithAction(api.ActionStop)
		}
	}
	t.ContextUsage = requestContextUsage(request)
	t.ActiveRun, t.LastRequestID, t.LastRequestKind = activeRun(r.log.id), r.nonce, turn.Kind
	if !s.local {
		attach.StripInlineAttachments(t)
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
	if !s.keyless && !p.SyncExpires.After(api.Now().Add(runTimeout+commitTimeout)) {
		fresh := *p
		fresh.SyncToken = ""
		grant, err := h.store.Rows.AuthorizeRun(ctx, &fresh, r.key, r.input.ThreadID, r.input.ProjectID)
		if err != nil {
			return err
		}
		fresh.SyncToken, fresh.SyncExpires = grant.Token, grant.ExpiresAt
		p = &fresh
		h.live.mu.Lock()
		r.principal = p
		h.live.mu.Unlock()
	}
	ctx = auth.WithCallerKey(ctx, string(r.credential.Key))
	ctx = auth.WithUsage(ctx, usageForRun(r))
	ctx = inference.WithUsageSink(ctx, r.out)
	if err := h.refuseFlagged(ctx, r, p); err != nil {
		return err
	}
	var plan *turnPlan
	// A chat another writer changed since it was read takes the turn again.
	// One that writer removed was deleted, so only a first attempt creates it.
	for attempt := 1; ; attempt++ {
		var err error
		if plan, err = h.prepareTurn(ctx, r, p, attempt == 1); err != nil {
			return err
		}
		if s.local {
			break
		}
		h.live.mu.Lock()
		deleted := s.deleted
		h.live.mu.Unlock()
		if deleted {
			return errThreadDeleted
		}
		err = h.store.SaveThread(ctx, p, r.key, plan.row)
		if err == nil {
			break
		}
		if api.AsError(err).Code != "REVISION_CONFLICT" || attempt == store.MaxCommitAttempts {
			return err
		}
	}
	row, request, model, titleText := plan.row, plan.request, plan.model, plan.titleText
	r.screening, r.retryTarget, r.turn = h.screening(request), plan.retryTarget, plan.turn
	request.cacheScope = cacheScope(p, s.id, r.input.Options.CacheSecret)
	request.refreshAuth = func(ctx context.Context) (auth.Credential, error) {
		h.live.mu.Lock()
		p, rejected := *r.principal, r.credential.Key
		h.live.mu.Unlock()
		credential, err := h.auth.Renewed(&p, rejected)
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
		r.assembler.Seed(reply)
		r.out.replyID = reply.ID
	}
	shown := domain.Window(row, "", domain.MessageWindow)
	h.live.mu.Lock()
	if s.deleted {
		h.live.mu.Unlock()
		return errThreadDeleted
	}
	if h.live.closing {
		h.live.mu.Unlock()
		return errShutdown
	}
	r.row, r.running = row, true
	// Framed under the lock, so no queue change reaches the run before them.
	r.out.emit(domain.RunStarted{ThreadID: s.id, RunID: r.log.id, Instance: instance(), MessageID: r.out.replyID})
	r.out.emit(domain.MessagesSnapshot{Messages: shown.Messages, HasOlder: shown.HasOlder})
	r.out.emit(domain.StateSnapshotEvent{Snapshot: domain.StateSnapshot{Thread: shown.ThreadSummary, Queue: queueItems(s), RateLimit: r.credential.Limit}})
	r.out.mu.Lock()
	r.out.observe = r.assembler.Apply
	r.out.mu.Unlock()
	titlePrincipal := *r.principal
	h.live.working.Add(1)
	h.live.mu.Unlock()
	runCtx, cancel := context.WithTimeout(r.stopped, runTimeout)
	runCtx = auth.WithCallerKey(runCtx, string(r.credential.Key))
	runCtx = auth.WithUsage(runCtx, usageForRun(r))
	runCtx = inference.WithUsageSink(runCtx, r.out)
	r.out.mu.Lock()
	r.out.thoughts = h.thoughtSummaries(runCtx, r.out, r.assembler)
	r.out.mu.Unlock()
	r.titleDone = make(chan struct{})
	titleState := row.TitleState
	go func() {
		defer close(r.titleDone)
		defer func() { api.Rescued("title", recover()) }()
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
			if err := api.Rescued("chat run", recover()); err != nil {
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

// refuseFlagged refuses a turn on a chat the safeguards flagged, which a
// stored chat then records. An ask is a turn on the chat it asks about.
func (h *harness) refuseFlagged(ctx context.Context, r *chatRun, p *auth.Principal) error {
	chat := r.thread.id
	if r.input.Kind == "ask" {
		chat = r.input.ThreadID
	}
	if !h.auth.FlaggedChats(ctx, p)[chat] {
		return nil
	}
	if !r.thread.local && r.input.Kind != "ask" {
		_, err := h.store.MutateThread(ctx, p, r.key, chat, func(t *domain.Thread) error {
			t.Flagged = true
			return nil
		})
		if err != nil && api.AsError(err).HTTP != http.StatusNotFound {
			return err
		}
	}
	return auth.ErrChatFlagged
}

// activeRun marks a chat's reply as streaming until the run's deadline.
func activeRun(runID string) *domain.ActiveRun {
	return &domain.ActiveRun{RunID: runID, ExpiresAt: api.Now().Add(runTimeout).Format(time.RFC3339Nano), Instance: instance()}
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
	e := api.AsError(auth.WithQuota(r.principal, r.credential.Limit, err))
	limit := domain.ValueOr(e.RateLimit, r.credential.Limit)
	r.out.emit(domain.FailedRun(r.thread.id, r.log.id, e, &domain.RunMetadata{RateLimit: &limit}))
	r.log.log.close(errDone)
	r.release()
	close(r.finished)
	return next
}
