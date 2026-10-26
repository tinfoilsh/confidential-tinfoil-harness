package main

import (
	"cmp"
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func (h *harness) apiRoutes() []routeSpec {
	routes := []routeSpec{
		{"/v1/session", true, false, h.session},
		{"/v1/changes", false, false, h.changes},
		{"/v1/threads/list", false, true, decoded(h.listThreads)}, {"/v1/threads/get", false, true, decoded(h.getThread)},
		{"/v1/threads/delete", false, true, decoded(h.deleteThread)},
		{"/v1/threads/search", false, true, decoded(h.searchThreads)}, {"/v1/threads/cancel", true, false, decoded(h.cancelThread)},
		{"/v1/threads/queue/send", true, false, decoded(h.sendQueued)}, {"/v1/threads/queue/remove", true, false, decoded(h.removeQueued)}, {"/v1/threads/share", false, false, decoded(h.shareThread)},
		{"/v1/threads/share/revoke", false, false, decoded(h.revokeShare)}, {"/v1/threads/share/status", false, false, decoded(h.shareStatus)},
		{"/v1/threads/transcript", false, true, decoded(h.threadTranscript)},
		{"/v1/profile/get", false, true, h.getProfile}, {"/v1/profile/update", false, true, decoded(h.updateProfile)}, {"/v1/profile/upgrade", true, false, decoded(h.upgradeDeviceProfile)},
		{"/v1/profile/merge", false, true, decoded(h.mergeDeviceProfile)},
		{"/v1/projects/list", false, true, decoded(h.listProjects)}, {"/v1/projects/get", false, true, decoded(h.getProject)},
		{"/v1/projects/create", false, true, decoded(h.createProject)}, {"/v1/projects/update", false, true, decoded(h.updateProject)},
		{"/v1/projects/delete", false, true, decoded(h.deleteProject)}, {"/v1/projects/documents/delete", false, true, decoded(h.deleteDocument)},
		{"/v1/shares/open", true, false, decoded(h.openShare)},
		{"/v1/import/status", false, false, decoded(h.importStatus)},
		{"/v1/threads/delete-all", false, true, decoded(h.deleteAllThreads)},
		{"/v1/projects/delete-all", false, true, h.deleteAllProjects},
		{"/v1/metadata/favicon", true, false, decoded(h.favicon)}, {"/v1/metadata/link", true, false, decoded(h.linkPreview)},
	}
	for _, pair := range [][2]string{{"current", "/v1/key/current"}, {"register", "/v1/key/register"}, {"add-bundle", "/v1/key/add-bundle"}, {"remove-bundle", "/v1/key/remove-bundle"}, {"migrate-all", "/v1/blobs/migrate-all"}} {
		routes = append(routes, routeSpec{"/v1/keys/" + pair[0], false, false, func(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in json.RawMessage) (any, error) {
			return h.keyCall(ctx, p, pair[1], in)
		}})
	}
	return routes
}

func (h *harness) changes(ctx context.Context, p *auth.Principal, _ auth.ContentKey, _ json.RawMessage) (any, error) {
	return h.storageChanges(ctx, p)
}

// threadOperations change a stored chat or a device chat in one way each.
func (h *harness) threadOperations() map[string]jsonEndpoint {
	return map[string]jsonEndpoint{"/v1/threads/update": decoded(h.updateThread), "/v1/threads/fork": decoded(h.forkThread),
		"/v1/threads/messages/delete": decoded(h.deleteMessage), "/v1/threads/messages/update": decoded(h.updateMessage)}
}

func (h *harness) session(ctx context.Context, p *auth.Principal, _ auth.ContentKey, _ json.RawMessage) (any, error) {
	c := h.catalog.get()
	credential, err := h.auth.Inference(ctx, p)
	// A signed-in client opens the app before or without chat access, as at
	// its hourly cap, which it learned of when it asked for that access.
	if err != nil && p.Anonymous {
		return nil, err
	}
	presets := c.Presets
	if presets == nil {
		presets = []domain.Preset{}
	}
	widgets := c.EnabledWidgets
	if widgets == nil {
		widgets = []string{}
	}
	return sessionView{
		Protocol:        protocolView{Version: protocolVersion, Minimum: minimumProtocolVersion},
		User:            sessionUser{ID: p.ID, Anonymous: p.Anonymous},
		RateLimit:       credential.Limit,
		Models:          c.DisplayModels(),
		Auto:            autoView{Default: domain.DefaultIntelligence, Levels: domain.AutoLevels()},
		DefaultModel:    domain.AutoModelName,
		Widgets:         widgets,
		ProfileDefaults: domain.DefaultProfile(),
		Composer:        composer(),
		Presets:         presets,
		Features:        map[string]bool{"webSearch": webSearch.client != nil, "transcription": h.serviceReady("router"), "speech": h.serviceReady("router")},
	}, nil
}

func (h *harness) listThreads(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *listRequest) (any, error) {
	rows, next, err := h.threadPage(ctx, p, key, in.Cursor, in.ProjectID, in.Limit)
	if err != nil {
		return nil, err
	}
	if err := h.applyPins(ctx, p, key, rows); err != nil {
		return nil, err
	}
	flags := h.auth.FlaggedChats(ctx, p)
	list := []domain.ThreadSummary{}
	for _, row := range rows {
		markFlagged(flags, &row.Thread)
		list = append(list, domain.Summarize(&row.Thread))
	}
	var pinned []domain.ThreadSummary
	if in.Cursor == "" {
		pinned, err = h.pinnedThreads(ctx, p, key, flags)
		if err != nil {
			return nil, err
		}
	}
	return struct {
		Threads    []domain.ThreadSummary `json:"threads"`
		NextCursor string                 `json:"nextCursor"`
		Pinned     []domain.ThreadSummary `json:"pinned"`
	}{list, next, pinned}, nil
}
func (h *harness) getThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *windowRequest) (any, error) {
	row, err := h.loadThread(ctx, p, key, in.ID)
	if err != nil {
		return nil, err
	}
	if err := h.applyPins(ctx, p, key, []*domain.ThreadRow{row}); err != nil {
		return nil, err
	}
	markFlagged(h.auth.FlaggedChats(ctx, p), &row.Thread)
	return domain.Window(row, in.Before, in.Limit), nil
}
func (h *harness) updateThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *updateThreadRequest) (any, error) {
	if in.Model != nil && !h.catalog.get().Offers(*in.Model) {
		return nil, api.Err(404, "MODEL_UNAVAILABLE", "Unknown model")
	}
	if project := domain.ValueOr(in.ProjectID.Value, ""); project != "" {
		if _, err := h.loadProject(ctx, p, key, project); err != nil {
			return nil, err
		}
	}
	var profile domain.Profile
	if in.PresetID.Value != nil {
		var err error
		if profile, err = h.sourceProfile(ctx, p, key, in.threadSource); err != nil {
			return nil, err
		}
	}
	row, err := h.changeThread(ctx, p, key, in.threadSource, in.ID, func(t *domain.Thread) error {
		in.apply(t, h.catalog.get(), &profile)
		t.Pinned = domain.ValueOr(in.Pinned, t.Pinned)
		if in.ProjectID.Set {
			t.ProjectID = in.ProjectID.Value
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	markFlagged(h.auth.FlaggedChats(ctx, p), &row.Thread)
	if in.Local {
		return deviceReply(row), nil
	}
	if in.Pinned != nil {
		if err := h.setPin(ctx, p, key, in.ID, *in.Pinned); err != nil {
			return nil, err
		}
	}
	return domain.Summarize(&row.Thread), nil
}

func (h *harness) pinnedThreads(ctx context.Context, p *auth.Principal, key auth.ContentKey, flags auth.ChatFlags) ([]domain.ThreadSummary, error) {
	ids, err := h.pinnedIDs(ctx, p, key)
	if err != nil {
		return nil, err
	}
	threads := []domain.ThreadSummary{}
	rows, err := h.threadBatch(ctx, p, key, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*domain.ThreadRow, len(rows))
	for _, row := range rows {
		byID[row.ID] = row
	}
	for _, id := range ids {
		row := byID[id]
		if row == nil {
			continue
		}
		row.Pinned = true
		markFlagged(flags, &row.Thread)
		threads = append(threads, domain.Summarize(&row.Thread))
	}
	return threads, nil
}

// changeThread applies change to a chat: a stored one as read now, saved
// once changed, or the device chat its client sent.
func (h *harness) changeThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, source threadSource, id string, change func(*domain.Thread) error) (*domain.ThreadRow, error) {
	if !source.Local {
		return h.mutateThread(ctx, p, key, id, change)
	}
	t, err := domain.ReadDeviceThread(id, source.Thread)
	if err != nil {
		return nil, err
	}
	row := &domain.ThreadRow{Thread: t}
	if err := change(&row.Thread); err != nil {
		return nil, err
	}
	row.UpdatedAt = api.Timestamp()
	return row, nil
}

// sourceProfile is the settings a chat's prompt reads: the stored profile,
// or the one a device chat's client sends.
func (h *harness) sourceProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey, source threadSource) (domain.Profile, error) {
	if !source.Local {
		return h.profile(ctx, p, key)
	}
	return *domain.LocalProfile(source.Profile), nil
}

// deviceReply answers an operation on a device chat with the chat its client
// stores, which leaves out the images the client holds.
func deviceReply(row *domain.ThreadRow) any {
	return struct {
		Thread domain.Thread `json:"thread"`
	}{domain.DeviceThread(&row.Thread)}
}

func (h *harness) deleteThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *deleteThreadRequest) (any, error) {
	var err error
	if in.Revision != "" {
		err = h.finishTransfer(ctx, p, key, in)
	} else {
		h.stopThread(p, in.ID)
		err = h.removeRow(ctx, p, key, "chat", in.ID)
	}
	if err != nil {
		return nil, err
	}
	if err := h.setPin(ctx, p, key, in.ID, false); err != nil {
		slog.Warn("could not remove a deleted chat from favorites", "error", err)
	}
	return struct{}{}, nil
}
func forkTitle(title string) string {
	title = cmp.Or(title, "Untitled")
	if strings.HasSuffix(title, " (fork)") {
		return title
	}
	return title + " (fork)"
}

// forkedCount is how many leading messages of a chat a fork through the
// given one copies.
func forkedCount(t *domain.Thread, messageID string) (int, error) {
	count := 1 + t.MessageIndex(messageID)
	if count == 0 {
		return 0, api.Err(404, "THREAD_NOT_FOUND", "The message was not found")
	}
	return count, nil
}

// startFork leaves out of a fork what belongs to the chat it was copied
// from. Its title is the one it was given.
func startFork(t *domain.Thread) {
	t.ActiveRun, t.ContextUsage, t.Pinned, t.Flagged, t.TitleState = nil, nil, false, false, "manual"
	t.LastRequestID, t.LastRequestKind = "", ""
}

// forkThread starts a new chat from a chat's messages through the given one,
// under an id the harness makes. Sync copies a stored chat, with its own
// copies of the attachments; a device chat's fork keeps their ids.
func (h *harness) forkThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *messageRequest) (any, error) {
	if in.Local {
		source, err := domain.ReadDeviceThread(in.ThreadID, in.Thread)
		if err != nil {
			return nil, err
		}
		count, err := forkedCount(&source, in.MessageID)
		if err != nil {
			return nil, err
		}
		fork := source.Clone()
		fork.ID, fork.Title, fork.Messages, fork.CreatedAt, fork.UpdatedAt = domain.RowID(), forkTitle(source.Title), fork.Messages[:count], api.Timestamp(), api.Timestamp()
		startFork(&fork)
		return deviceReply(&domain.ThreadRow{Thread: fork}), nil
	}
	row, err := h.loadThread(ctx, p, key, in.ThreadID)
	if err != nil {
		return nil, err
	}
	count, err := forkedCount(&row.Thread, in.MessageID)
	if err != nil {
		return nil, err
	}
	target := domain.RowID()
	if err := h.forkStored(ctx, p, key, in.ThreadID, target, forkTitle(row.Title), count); err != nil {
		return nil, err
	}
	// Sync leaves out the web client's bookkeeping, not the harness's.
	forked, err := h.mutateThread(ctx, p, key, target, func(t *domain.Thread) error {
		startFork(t)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return domain.Summarize(&forked.Thread), nil
}

// Message actions change a chat's transcript.
func (h *harness) deleteMessage(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *messageRequest) (any, error) {
	return h.changeMessages(ctx, p, key, in, func(t *domain.Thread, index int) error {
		// As on the web client, a chat keeps its last message.
		if len(t.Messages) == 1 {
			return api.Err(400, "BAD_REQUEST", "A chat keeps its last message")
		}
		t.Messages = slices.Delete(t.Messages, index, index+1)
		return nil
	})
}
func (h *harness) updateMessage(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *messageRequest) (any, error) {
	if in.Content == nil {
		return nil, api.Err(400, "BAD_REQUEST", "content must be a string")
	}
	return h.changeMessages(ctx, p, key, in, func(t *domain.Thread, index int) error {
		if t.Messages[index].Role != "assistant" {
			return api.Err(400, "BAD_REQUEST", "Only a reply is edited in place")
		}
		domain.ReplaceReplyText(&t.Messages[index], *in.Content)
		return nil
	})
}
func (h *harness) changeMessages(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *messageRequest, change func(*domain.Thread, int) error) (any, error) {
	row, err := h.changeThread(ctx, p, key, in.threadSource, in.ThreadID, func(t *domain.Thread) error {
		index := t.MessageIndex(in.MessageID)
		if index < 0 {
			return api.Err(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		return change(t, index)
	})
	if err != nil {
		return nil, err
	}
	markFlagged(h.auth.FlaggedChats(ctx, p), &row.Thread)
	if in.Local {
		return deviceReply(row), nil
	}
	return domain.Window(row, "", domain.MessageWindow), nil
}
func (h *harness) deleteAllThreads(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *deleteAllRequest) (any, error) {
	// Every id is listed before any is deleted, so a shrinking list cannot
	// skip a page, and rows this key cannot open are deleted too.
	ids, err := h.chatIDs(ctx, p, domain.ValueOr(in.ProjectID, ""))
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := h.deleteThread(ctx, p, key, &deleteThreadRequest{idRequest: idRequest{ID: id}}); err != nil {
			return nil, err
		}
	}
	return deletedCount{len(ids)}, nil
}

// deletedCount answers a bulk deletion.
type deletedCount struct {
	Deleted int `json:"deleted"`
}

// profile is a user's settings as a prompt reads them.
func (h *harness) profile(ctx context.Context, p *auth.Principal, key auth.ContentKey) (domain.Profile, error) {
	stored, err := h.loadProfile(ctx, p, key)
	if err != nil || stored == nil {
		return domain.Profile{}, err
	}
	return stored.Profile, nil
}

// viewProfile is a stored profile as its client reads it.
func (h *harness) viewProfile(stored domain.StoredProfile) domain.ProfileView {
	return domain.ProfileView{Profile: h.catalog.get().EffectiveProfile(stored.Profile), UpdatedAt: stored.UpdatedAt}
}
func (h *harness) getProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey, _ json.RawMessage) (any, error) {
	stored, err := h.loadProfile(ctx, p, key)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		stored = &domain.StoredProfile{}
	}
	return h.viewProfile(*stored), nil
}

// upgradeDeviceProfile reads the settings a client keeps on its device as
// the profile a stored one reads as. It stores nothing.
func (h *harness) upgradeDeviceProfile(_ context.Context, _ *auth.Principal, _ auth.ContentKey, in *profileUpgradeRequest) (any, error) {
	return struct {
		Profile domain.Profile `json:"profile"`
	}{h.catalog.get().EffectiveProfile(*domain.LocalProfile(in.Profile))}, nil
}
func (h *harness) updateProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *profileUpdateRequest) (any, error) {
	patch, clearsDefault, err := in.patch()
	if err != nil {
		return nil, err
	}
	stored, err := h.mutateProfile(ctx, p, key, func(profile *domain.StoredProfile) error {
		profile.Apply(patch)
		if clearsDefault {
			profile.DefaultPromptPresetID = nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return h.viewProfile(stored), nil
}

func (h *harness) mergeDeviceProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *profileMergeRequest) (any, error) {
	local := domain.LocalProfile(in.Profile)
	merged, err := h.mutateProfile(ctx, p, key, func(stored *domain.StoredProfile) error {
		stored.Profile = domain.MergeDeviceProfile(stored.Profile, *local, in.ChangedFields)
		return stored.Profile.Validate()
	})
	if err != nil {
		return nil, err
	}
	return h.viewProfile(merged), nil
}
func (h *harness) listProjects(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *listRequest) (any, error) {
	projects, next, err := h.projectPage(ctx, p, key, in.Cursor, 50)
	if err != nil {
		return nil, err
	}
	return struct {
		Projects   []domain.Project `json:"projects"`
		NextCursor string           `json:"nextCursor"`
	}{projects, next}, nil
}
func (h *harness) getProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *idRequest) (any, error) {
	project, err := h.loadProject(ctx, p, key, in.ID)
	if err != nil {
		return nil, err
	}
	docs, err := h.projectDocuments(ctx, p, key, in.ID, refuseUnreadable)
	if err != nil {
		return nil, err
	}
	return struct {
		domain.Project
		Documents []domain.ProjectDocument `json:"documents"`
	}{project, docs}, nil
}
func (h *harness) createProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *projectRequest) (any, error) {
	if strings.TrimSpace(domain.ValueOr(in.Name, "")) == "" {
		return nil, api.Err(400, "BAD_REQUEST", "A project name is required")
	}
	return h.mutateProject(ctx, p, key, domain.RowID(), true, func(project *domain.Project) {
		project.Name, project.Description, project.SystemInstructions, project.Color = *in.Name, domain.ValueOr(in.Description, ""), domain.ValueOr(in.SystemInstructions, ""), in.Color.Value
	})
}
func (h *harness) updateProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *projectRequest) (any, error) {
	if err := api.RequireID("id", in.ID); err != nil {
		return nil, err
	}
	return h.mutateProject(ctx, p, key, in.ID, false, func(project *domain.Project) {
		project.Name = domain.ValueOr(in.Name, project.Name)
		project.Description = domain.ValueOr(in.Description, project.Description)
		project.SystemInstructions = domain.ValueOr(in.SystemInstructions, project.SystemInstructions)
		if in.Color.Set {
			project.Color = in.Color.Value
		}
	})
}
func (h *harness) deleteProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *idRequest) (any, error) {
	return struct{}{}, h.removeRow(ctx, p, key, "project", in.ID)
}

func (h *harness) deleteAllProjects(ctx context.Context, p *auth.Principal, key auth.ContentKey, _ json.RawMessage) (any, error) {
	deleted, err := h.removeAllProjects(ctx, p, key)
	if err != nil {
		return nil, err
	}
	return deletedCount{deleted}, nil
}

func (h *harness) deleteDocument(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *projectDocumentRequest) (any, error) {
	return struct{}{}, h.removeRow(ctx, p, key, "project_document", in.ProjectID+"/"+in.DocumentID)
}
func (h *harness) searchThreads(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *searchRequest) (any, error) {
	ids, indexing, err := h.searchChats(ctx, p, key, in.Query, in.Limit)
	if err != nil {
		return nil, err
	}
	if indexing {
		if indexing, err = h.reindexChats(ctx, p, key); err != nil {
			return nil, err
		}
		if !indexing {
			if ids, indexing, err = h.searchChats(ctx, p, key, in.Query, in.Limit); err != nil {
				return nil, err
			}
		}
	}
	found, unreadable, err := h.searchResults(ctx, p, key, ids)
	if err != nil {
		return nil, err
	}
	flags := h.auth.FlaggedChats(ctx, p)
	results := []domain.ThreadSummary{}
	for _, row := range found {
		markFlagged(flags, &row.Thread)
		results = append(results, domain.Summarize(&row.Thread))
	}
	return struct {
		Results    []domain.ThreadSummary `json:"results"`
		Unreadable int                    `json:"unreadable"`
		Indexing   bool                   `json:"indexing"`
	}{results, unreadable, indexing}, nil
}

// markFlagged marks the chats among threads that the safeguards flagged.
func markFlagged(flags auth.ChatFlags, threads ...*domain.Thread) {
	for _, t := range threads {
		t.Flagged = t.Flagged || flags[t.ID]
	}
}
