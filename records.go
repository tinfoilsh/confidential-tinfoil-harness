package main

import (
	"cmp"
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
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
		routes = append(routes, routeSpec{"/v1/keys/" + pair[0], false, false, func(ctx context.Context, p *principal, _ contentKey, in json.RawMessage) (any, error) {
			return h.keyCall(ctx, p, pair[1], in)
		}})
	}
	return routes
}

func (h *harness) changes(ctx context.Context, p *principal, _ contentKey, _ json.RawMessage) (any, error) {
	return h.storageChanges(ctx, p)
}

// threadOperations change a stored chat or a device chat in one way each.
func (h *harness) threadOperations() map[string]jsonEndpoint {
	return map[string]jsonEndpoint{"/v1/threads/update": decoded(h.updateThread), "/v1/threads/fork": decoded(h.forkThread),
		"/v1/threads/messages/delete": decoded(h.deleteMessage), "/v1/threads/messages/update": decoded(h.updateMessage)}
}

func (h *harness) session(ctx context.Context, p *principal, _ contentKey, _ json.RawMessage) (any, error) {
	c := h.catalog.get()
	credential, err := h.auth.inference(ctx, p)
	// A signed-in client opens the app before or without chat access, as at
	// its hourly cap, which it learned of when it asked for that access.
	if err != nil && p.Anonymous {
		return nil, err
	}
	presets := c.Presets
	if presets == nil {
		presets = []preset{}
	}
	widgets := c.EnabledWidgets
	if widgets == nil {
		widgets = []string{}
	}
	return sessionView{
		Protocol:        protocolView{Version: protocolVersion, Minimum: minimumProtocolVersion},
		User:            sessionUser{ID: p.ID, Anonymous: p.Anonymous},
		RateLimit:       credential.Limit,
		Models:          c.displayModels(),
		Auto:            autoView{Default: defaultIntelligence, Levels: autoLevels()},
		DefaultModel:    autoModelName,
		Widgets:         widgets,
		ProfileDefaults: defaultProfile(),
		Composer:        composer(),
		Presets:         presets,
		Features:        map[string]bool{"webSearch": webSearch.client != nil, "transcription": h.serviceReady("router"), "speech": h.serviceReady("router")},
	}, nil
}

func (h *harness) listThreads(ctx context.Context, p *principal, key contentKey, in *listRequest) (any, error) {
	rows, next, err := h.threadPage(ctx, p, key, in.Cursor, in.ProjectID, in.Limit)
	if err != nil {
		return nil, err
	}
	if err := h.applyPins(ctx, p, key, rows); err != nil {
		return nil, err
	}
	flags := h.auth.flaggedChats(ctx, p)
	list := []threadSummary{}
	for _, row := range rows {
		flags.mark(&row.Thread)
		list = append(list, summarize(&row.Thread))
	}
	var pinned []threadSummary
	if in.Cursor == "" {
		pinned, err = h.pinnedThreads(ctx, p, key, flags)
		if err != nil {
			return nil, err
		}
	}
	return struct {
		Threads    []threadSummary `json:"threads"`
		NextCursor string          `json:"nextCursor"`
		Pinned     []threadSummary `json:"pinned"`
	}{list, next, pinned}, nil
}
func (h *harness) getThread(ctx context.Context, p *principal, key contentKey, in *windowRequest) (any, error) {
	row, err := h.loadThread(ctx, p, key, in.ID)
	if err != nil {
		return nil, err
	}
	if err := h.applyPins(ctx, p, key, []*threadRow{row}); err != nil {
		return nil, err
	}
	h.auth.flaggedChats(ctx, p).mark(&row.Thread)
	return window(row, in.Before, in.Limit), nil
}
func (h *harness) updateThread(ctx context.Context, p *principal, key contentKey, in *updateThreadRequest) (any, error) {
	if in.Model != nil && !h.catalog.get().offers(*in.Model) {
		return nil, apiErr(404, "MODEL_UNAVAILABLE", "Unknown model")
	}
	if project := valueOr(in.ProjectID.value, ""); project != "" {
		if _, err := h.loadProject(ctx, p, key, project); err != nil {
			return nil, err
		}
	}
	var profile Profile
	if in.PresetID.value != nil {
		var err error
		if profile, err = h.sourceProfile(ctx, p, key, in.threadSource); err != nil {
			return nil, err
		}
	}
	row, err := h.changeThread(ctx, p, key, in.threadSource, in.ID, func(t *Thread) error {
		in.apply(t, h.catalog.get(), &profile)
		t.Pinned = valueOr(in.Pinned, t.Pinned)
		if in.ProjectID.set {
			t.ProjectID = in.ProjectID.value
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	h.auth.flaggedChats(ctx, p).mark(&row.Thread)
	if in.Local {
		return deviceReply(row), nil
	}
	if in.Pinned != nil {
		if err := h.setPin(ctx, p, key, in.ID, *in.Pinned); err != nil {
			return nil, err
		}
	}
	return summarize(&row.Thread), nil
}

func (h *harness) pinnedThreads(ctx context.Context, p *principal, key contentKey, flags chatFlags) ([]threadSummary, error) {
	ids, err := h.pinnedIDs(ctx, p, key)
	if err != nil {
		return nil, err
	}
	threads := []threadSummary{}
	rows, err := h.threadBatch(ctx, p, key, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*threadRow, len(rows))
	for _, row := range rows {
		byID[row.ID] = row
	}
	for _, id := range ids {
		row := byID[id]
		if row == nil {
			continue
		}
		row.Pinned = true
		flags.mark(&row.Thread)
		threads = append(threads, summarize(&row.Thread))
	}
	return threads, nil
}

// changeThread applies change to a chat: a stored one as read now, saved
// once changed, or the device chat its client sent.
func (h *harness) changeThread(ctx context.Context, p *principal, key contentKey, source threadSource, id string, change func(*Thread) error) (*threadRow, error) {
	if !source.Local {
		return h.mutateThread(ctx, p, key, id, change)
	}
	t, err := readDeviceThread(id, source.Thread)
	if err != nil {
		return nil, err
	}
	row := &threadRow{Thread: t}
	if err := change(&row.Thread); err != nil {
		return nil, err
	}
	row.UpdatedAt = timestamp()
	return row, nil
}

// sourceProfile is the settings a chat's prompt reads: the stored profile,
// or the one a device chat's client sends.
func (h *harness) sourceProfile(ctx context.Context, p *principal, key contentKey, source threadSource) (Profile, error) {
	if !source.Local {
		return h.profile(ctx, p, key)
	}
	return *localProfile(source.Profile), nil
}

// deviceReply answers an operation on a device chat with the chat its client
// stores, which leaves out the images the client holds.
func deviceReply(row *threadRow) any {
	return struct {
		Thread Thread `json:"thread"`
	}{deviceThread(&row.Thread)}
}

func (h *harness) deleteThread(ctx context.Context, p *principal, key contentKey, in *deleteThreadRequest) (any, error) {
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
func forkedCount(t *Thread, messageID string) (int, error) {
	count := 1 + t.messageIndex(messageID)
	if count == 0 {
		return 0, apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
	}
	return count, nil
}

// startFork leaves out of a fork what belongs to the chat it was copied
// from. Its title is the one it was given.
func startFork(t *Thread) {
	t.ActiveRun, t.ContextUsage, t.Pinned, t.Flagged, t.TitleState = nil, nil, false, false, "manual"
	t.LastRequestID, t.LastRequestKind = "", ""
}

// forkThread starts a new chat from a chat's messages through the given one,
// under an id the harness makes. Sync copies a stored chat, with its own
// copies of the attachments; a device chat's fork keeps their ids.
func (h *harness) forkThread(ctx context.Context, p *principal, key contentKey, in *messageRequest) (any, error) {
	if in.Local {
		source, err := readDeviceThread(in.ThreadID, in.Thread)
		if err != nil {
			return nil, err
		}
		count, err := forkedCount(&source, in.MessageID)
		if err != nil {
			return nil, err
		}
		fork := source.clone()
		fork.ID, fork.Title, fork.Messages, fork.CreatedAt, fork.UpdatedAt = rowID(), forkTitle(source.Title), fork.Messages[:count], timestamp(), timestamp()
		startFork(&fork)
		return deviceReply(&threadRow{Thread: fork}), nil
	}
	row, err := h.loadThread(ctx, p, key, in.ThreadID)
	if err != nil {
		return nil, err
	}
	count, err := forkedCount(&row.Thread, in.MessageID)
	if err != nil {
		return nil, err
	}
	target := rowID()
	if err := h.forkStored(ctx, p, key, in.ThreadID, target, forkTitle(row.Title), count); err != nil {
		return nil, err
	}
	// Sync leaves out the web client's bookkeeping, not the harness's.
	forked, err := h.mutateThread(ctx, p, key, target, func(t *Thread) error {
		startFork(t)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return summarize(&forked.Thread), nil
}

// Message actions change a chat's transcript.
func (h *harness) deleteMessage(ctx context.Context, p *principal, key contentKey, in *messageRequest) (any, error) {
	return h.changeMessages(ctx, p, key, in, func(t *Thread, index int) error {
		// As on the web client, a chat keeps its last message.
		if len(t.Messages) == 1 {
			return apiErr(400, "BAD_REQUEST", "A chat keeps its last message")
		}
		t.Messages = slices.Delete(t.Messages, index, index+1)
		return nil
	})
}
func (h *harness) updateMessage(ctx context.Context, p *principal, key contentKey, in *messageRequest) (any, error) {
	if in.Content == nil {
		return nil, apiErr(400, "BAD_REQUEST", "content must be a string")
	}
	return h.changeMessages(ctx, p, key, in, func(t *Thread, index int) error {
		if t.Messages[index].Role != "assistant" {
			return apiErr(400, "BAD_REQUEST", "Only a reply is edited in place")
		}
		replaceReplyText(&t.Messages[index], *in.Content)
		return nil
	})
}
func (h *harness) changeMessages(ctx context.Context, p *principal, key contentKey, in *messageRequest, change func(*Thread, int) error) (any, error) {
	row, err := h.changeThread(ctx, p, key, in.threadSource, in.ThreadID, func(t *Thread) error {
		index := t.messageIndex(in.MessageID)
		if index < 0 {
			return apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		return change(t, index)
	})
	if err != nil {
		return nil, err
	}
	h.auth.flaggedChats(ctx, p).mark(&row.Thread)
	if in.Local {
		return deviceReply(row), nil
	}
	return window(row, "", messageWindow), nil
}
func (h *harness) deleteAllThreads(ctx context.Context, p *principal, key contentKey, in *deleteAllRequest) (any, error) {
	// Every id is listed before any is deleted, so a shrinking list cannot
	// skip a page, and rows this key cannot open are deleted too.
	ids, err := h.chatIDs(ctx, p, valueOr(in.ProjectID, ""))
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
func (h *harness) profile(ctx context.Context, p *principal, key contentKey) (Profile, error) {
	stored, err := h.loadProfile(ctx, p, key)
	if err != nil || stored == nil {
		return Profile{}, err
	}
	return stored.Profile, nil
}

// viewProfile is a stored profile as its client reads it.
func (h *harness) viewProfile(stored storedProfile) profileView {
	return profileView{Profile: h.catalog.get().effectiveProfile(stored.Profile), UpdatedAt: stored.UpdatedAt}
}
func (h *harness) getProfile(ctx context.Context, p *principal, key contentKey, _ json.RawMessage) (any, error) {
	stored, err := h.loadProfile(ctx, p, key)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		stored = &storedProfile{}
	}
	return h.viewProfile(*stored), nil
}

// upgradeDeviceProfile reads the settings a client keeps on its device as
// the profile a stored one reads as. It stores nothing.
func (h *harness) upgradeDeviceProfile(_ context.Context, _ *principal, _ contentKey, in *profileUpgradeRequest) (any, error) {
	return struct {
		Profile Profile `json:"profile"`
	}{h.catalog.get().effectiveProfile(*localProfile(in.Profile))}, nil
}
func (h *harness) updateProfile(ctx context.Context, p *principal, key contentKey, in *profileUpdateRequest) (any, error) {
	patch, clearsDefault, err := in.patch()
	if err != nil {
		return nil, err
	}
	stored, err := h.mutateProfile(ctx, p, key, func(profile *storedProfile) error {
		profile.apply(patch)
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

func (h *harness) mergeDeviceProfile(ctx context.Context, p *principal, key contentKey, in *profileMergeRequest) (any, error) {
	local := localProfile(in.Profile)
	merged, err := h.mutateProfile(ctx, p, key, func(stored *storedProfile) error {
		stored.Profile = mergeDeviceProfile(stored.Profile, *local, in.ChangedFields)
		return stored.Profile.validate()
	})
	if err != nil {
		return nil, err
	}
	return h.viewProfile(merged), nil
}
func (h *harness) listProjects(ctx context.Context, p *principal, key contentKey, in *listRequest) (any, error) {
	projects, next, err := h.projectPage(ctx, p, key, in.Cursor, 50)
	if err != nil {
		return nil, err
	}
	return struct {
		Projects   []Project `json:"projects"`
		NextCursor string    `json:"nextCursor"`
	}{projects, next}, nil
}
func (h *harness) getProject(ctx context.Context, p *principal, key contentKey, in *idRequest) (any, error) {
	project, err := h.loadProject(ctx, p, key, in.ID)
	if err != nil {
		return nil, err
	}
	docs, err := h.projectDocuments(ctx, p, key, in.ID, refuseUnreadable)
	if err != nil {
		return nil, err
	}
	return struct {
		Project
		Documents []ProjectDocument `json:"documents"`
	}{project, docs}, nil
}
func (h *harness) createProject(ctx context.Context, p *principal, key contentKey, in *projectRequest) (any, error) {
	if strings.TrimSpace(valueOr(in.Name, "")) == "" {
		return nil, apiErr(400, "BAD_REQUEST", "A project name is required")
	}
	return h.mutateProject(ctx, p, key, rowID(), true, func(project *Project) {
		project.Name, project.Description, project.SystemInstructions, project.Color = *in.Name, valueOr(in.Description, ""), valueOr(in.SystemInstructions, ""), in.Color.value
	})
}
func (h *harness) updateProject(ctx context.Context, p *principal, key contentKey, in *projectRequest) (any, error) {
	if err := requireID("id", in.ID); err != nil {
		return nil, err
	}
	return h.mutateProject(ctx, p, key, in.ID, false, func(project *Project) {
		project.Name = valueOr(in.Name, project.Name)
		project.Description = valueOr(in.Description, project.Description)
		project.SystemInstructions = valueOr(in.SystemInstructions, project.SystemInstructions)
		if in.Color.set {
			project.Color = in.Color.value
		}
	})
}
func (h *harness) deleteProject(ctx context.Context, p *principal, key contentKey, in *idRequest) (any, error) {
	return struct{}{}, h.removeRow(ctx, p, key, "project", in.ID)
}

func (h *harness) deleteAllProjects(ctx context.Context, p *principal, key contentKey, _ json.RawMessage) (any, error) {
	deleted, err := h.removeAllProjects(ctx, p, key)
	if err != nil {
		return nil, err
	}
	return deletedCount{deleted}, nil
}

func (h *harness) deleteDocument(ctx context.Context, p *principal, key contentKey, in *projectDocumentRequest) (any, error) {
	return struct{}{}, h.removeRow(ctx, p, key, "project_document", in.ProjectID+"/"+in.DocumentID)
}
func (h *harness) searchThreads(ctx context.Context, p *principal, key contentKey, in *searchRequest) (any, error) {
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
	flags := h.auth.flaggedChats(ctx, p)
	results := []threadSummary{}
	for _, id := range ids {
		row, err := h.loadThread(ctx, p, key, id)
		if err != nil {
			// A deleted or unreadable chat leaves the other results intact.
			if code := asAPIError(err).HTTP; code == 404 || code == 409 {
				continue
			}
			return nil, err
		}
		flags.mark(&row.Thread)
		results = append(results, summarize(&row.Thread))
	}
	return struct {
		Results  []threadSummary `json:"results"`
		Indexing bool            `json:"indexing"`
	}{results, indexing}, nil
}
