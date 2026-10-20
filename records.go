package main

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"strings"
)

func (h *harness) apiRoutes() []routeSpec {
	routes := []routeSpec{
		{"/v1/session", true, false, h.session},
		{"/v1/threads/list", false, true, h.listThreads}, {"/v1/threads/get", false, true, h.getThread},
		{"/v1/threads/update", false, true, h.updateThread}, {"/v1/threads/delete", false, true, h.deleteThread}, {"/v1/threads/fork", false, true, h.forkThread},
		{"/v1/threads/messages/delete", false, true, h.deleteMessage}, {"/v1/threads/messages/update", false, true, h.updateMessage},
		{"/v1/threads/search", false, true, h.searchThreads}, {"/v1/threads/cancel", true, false, h.cancelThread},
		{"/v1/threads/queue/send", true, false, h.sendQueued}, {"/v1/threads/queue/remove", true, false, h.removeQueued}, {"/v1/threads/share", false, false, h.shareThread},
		{"/v1/threads/share/revoke", false, false, h.revokeShare}, {"/v1/threads/share/status", false, false, h.shareStatus},
		{"/v1/threads/transcript", false, true, h.threadTranscript},
		{"/v1/profile/get", false, true, h.getProfile}, {"/v1/profile/update", false, true, h.updateProfile},
		{"/v1/projects/list", false, true, h.listProjects}, {"/v1/projects/get", false, true, h.getProject},
		{"/v1/projects/create", false, true, h.createProject}, {"/v1/projects/update", false, true, h.updateProject},
		{"/v1/projects/delete", false, true, h.deleteProject}, {"/v1/projects/documents/delete", false, true, h.deleteDocument},
		{"/v1/shares/open", true, false, h.openShare},
		{"/v1/import/status", false, false, h.importStatus},
		{"/v1/threads/delete-all", false, true, h.deleteAllThreads},
		{"/v1/projects/delete-all", false, true, h.deleteAllProjects},
		{"/v1/metadata/favicon", true, false, h.favicon},
	}
	for _, pair := range [][2]string{{"current", "/v1/key/current"}, {"register", "/v1/key/register"}, {"add-bundle", "/v1/key/add-bundle"}, {"remove-bundle", "/v1/key/remove-bundle"}, {"migrate-all", "/v1/blobs/migrate-all"}} {
		routes = append(routes, routeSpec{"/v1/keys/" + pair[0], false, false, func(ctx context.Context, p *principal, _ contentKey, in object) (any, error) {
			return h.keyCall(ctx, p, pair[1], raw(in))
		}})
	}
	return routes
}

func (h *harness) session(ctx context.Context, p *principal, _ contentKey, _ object) (any, error) {
	c := h.catalog.get()
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		// A subscriber at the hourly cap still opens the app and sees when it resets.
		if e := asAPIError(err); e.Code != "QUOTA_EXHAUSTED" || e.Kind != "hourly" {
			return nil, err
		}
		credential.Limit = h.auth.currentLimit(p)
	}
	features := object{"webSearch": webSearch.client != nil, "transcription": h.serviceReady("router"), "speech": h.serviceReady("router")}
	presets := []object{}
	for _, p := range c.Presets {
		d := clone(p)
		presets = append(presets, d)
	}
	auto := object{"multimodal": true, "default": "high", "levels": intelligenceLevels}
	return object{"user": object{"id": p.ID, "anonymous": p.Anonymous}, "rateLimit": credential.Limit, "models": c.displayModels(), "auto": auto, "defaultModel": "auto", "widgets": c.EnabledWidgets, "presets": presets, "features": features}, nil
}

func (h *harness) listThreads(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	rows, next, err := h.threadPage(ctx, p, key, str(in["cursor"]), str(in["projectId"]), integer(in["limit"]))
	if err != nil {
		return nil, err
	}
	if err := h.readLegacyPins(ctx, p, key, rows); err != nil {
		return nil, err
	}
	list := []threadSummary{}
	for _, row := range rows {
		list = append(list, summarize(&row.Thread))
	}
	return struct {
		Threads    []threadSummary `json:"threads"`
		NextCursor string          `json:"nextCursor"`
	}{list, next}, nil
}
func (h *harness) getThread(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	row, err := h.loadThread(ctx, p, key, id)
	if err != nil {
		return nil, err
	}
	if err := h.readLegacyPins(ctx, p, key, []*threadRow{row}); err != nil {
		return nil, err
	}
	return window(row, str(in["before"]), integer(in["limit"])), nil
}
func (h *harness) updateThread(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "title model", "webSearchEnabled pinned", "projectId presetId"); err != nil {
		return nil, err
	}
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	if model, ok := in["model"]; ok && model != "auto" && h.catalog.get().definition(str(model)) == nil {
		return nil, apiErr(404, "MODEL_UNAVAILABLE", "Unknown model")
	}
	if project := str(in["projectId"]); project != "" {
		if _, err := h.loadProject(ctx, p, key, project); err != nil {
			return nil, err
		}
	}
	row, err := h.mutateThread(ctx, p, key, id, func(t *Thread) error {
		if title, ok := in["title"]; ok {
			t.Title, t.TitleState = str(title), "manual"
		}
		if model, ok := in["model"]; ok {
			t.Model = str(model)
		}
		if enabled, ok := in["webSearchEnabled"]; ok {
			t.WebSearchEnabled = boolean(enabled, false)
		}
		if project, ok := in["projectId"]; ok {
			t.ProjectID = optionalString(project)
		}
		if preset, ok := in["presetId"]; ok {
			t.PresetID = optionalString(preset)
		}
		if pinned, ok := in["pinned"]; ok {
			t.Pinned = boolean(pinned, false)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if pinned, ok := in["pinned"].(bool); ok && !pinned {
		if err := h.unpinLegacy(ctx, p, key, id); err != nil {
			return nil, err
		}
	}
	return summarize(&row.Thread), nil
}
func (h *harness) deleteThread(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	h.stopThread(p, id)
	return struct{}{}, h.removeRow(ctx, p, key, "chat", id)
}
func forkTitle(title string) string {
	title = cmp.Or(title, "Untitled")
	if strings.HasSuffix(title, " (fork)") {
		return title
	}
	return title + " (fork)"
}

// forkThread starts a new stored chat from a chat's messages through the
// given one. Sync makes the copy, with its own copies of the attachments.
func (h *harness) forkThread(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, err
	}
	messageID, err := requiredID(in, "messageId")
	if err != nil {
		return nil, err
	}
	row, err := h.loadThread(ctx, p, key, id)
	if err != nil {
		return nil, err
	}
	count := 1 + row.messageIndex(messageID)
	if count == 0 {
		return nil, apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
	}
	target := rowID()
	if err := h.forkStored(ctx, p, key, id, target, forkTitle(row.Title), count); err != nil {
		return nil, err
	}
	// Sync leaves out the web client's bookkeeping, not the harness's.
	forked, err := h.mutateThread(ctx, p, key, target, func(t *Thread) error {
		t.ActiveRun, t.ContextUsage, t.Pinned = nil, nil, false
		return nil
	})
	if err != nil {
		return nil, err
	}
	return summarize(&forked.Thread), nil
}

// Message actions change a stored chat's transcript. A local thread's client
// changes its own copy, which its next turn sends.
func (h *harness) deleteMessage(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	return h.changeMessages(ctx, p, key, in, func(t *Thread, index int) error {
		// As on the web client, a chat keeps its last message.
		if len(t.Messages) == 1 {
			return apiErr(400, "BAD_REQUEST", "A chat keeps its last message")
		}
		t.Messages = slices.Delete(t.Messages, index, index+1)
		return nil
	})
}
func (h *harness) updateMessage(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	text, ok := in["content"].(string)
	if !ok {
		return nil, apiErr(400, "BAD_REQUEST", "content must be a string")
	}
	return h.changeMessages(ctx, p, key, in, func(t *Thread, index int) error {
		if t.Messages[index].Role != "assistant" {
			return apiErr(400, "BAD_REQUEST", "Only a reply is edited in place")
		}
		replaceReplyText(&t.Messages[index], text)
		return nil
	})
}
func (h *harness) changeMessages(ctx context.Context, p *principal, key contentKey, in object, change func(*Thread, int) error) (any, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, err
	}
	messageID, err := requiredID(in, "messageId")
	if err != nil {
		return nil, err
	}
	row, err := h.mutateThread(ctx, p, key, id, func(t *Thread) error {
		index := t.messageIndex(messageID)
		if index < 0 {
			return apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		return change(t, index)
	})
	if err != nil {
		return nil, err
	}
	return window(row, "", messageWindow), nil
}
func (h *harness) deleteAllThreads(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "", "", "projectId"); err != nil {
		return nil, err
	}
	// Every id is listed before any is deleted, so a shrinking list cannot
	// skip a page, and rows this key cannot open are deleted too.
	ids, err := h.chatIDs(ctx, p, str(in["projectId"]))
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := h.deleteThread(ctx, p, key, object{"id": id}); err != nil {
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
	return viewProfile(*stored).Profile, nil
}

// viewProfile is a stored profile as its client reads it.
func viewProfile(stored storedProfile) profileView {
	view := profileView{Profile: stored.Profile, UpdatedAt: stored.UpdatedAt}
	view.SelectedModel = new(normalizeModel(valueOr(view.SelectedModel, "")))
	return view
}
func (h *harness) getProfile(ctx context.Context, p *principal, key contentKey, _ object) (any, error) {
	stored, err := h.loadProfile(ctx, p, key)
	if err != nil || stored == nil {
		return profileView{}, err
	}
	return viewProfile(*stored), nil
}

var profileFields = strings.Fields("themeMode chatFont language nickname profession traits additionalContext isUsingPersonalization isUsingCustomPrompt customSystemPrompt customPromptPresets favoritePromptPresetIds defaultPromptPresetId selectedModel autoIntelligence reasoningEffort thinkingEnabled webSearchEnabled codeExecutionEnabled piiCheckEnabled genUIEnabled pixelateSidebarChatTitlesEnabled browserTabChatTitleEnabled enterToNewlineEnabled hasSeenOnboarding hasSeenWebSearchIntro dismissed")

func (h *harness) updateProfile(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if !validObject(in["patch"]) {
		return nil, apiErr(400, "BAD_REQUEST", "patch must be an object")
	}
	patch := obj(in["patch"])
	if err := validateProfile(patch); err != nil {
		return nil, err
	}
	for f := range patch {
		if !slices.Contains(profileFields, f) {
			return nil, apiErr(400, "BAD_REQUEST", "Unknown profile field: "+f)
		}
	}
	var changes Profile
	json.Unmarshal(raw(patch), &changes)
	clearsDefault := patch["defaultPromptPresetId"] == nil
	if _, ok := patch["defaultPromptPresetId"]; !ok {
		clearsDefault = false
	}
	stored, err := h.mutateProfile(ctx, p, key, func(profile *storedProfile) {
		profile.apply(changes)
		if clearsDefault {
			profile.DefaultPromptPresetID = nil
		}
	})
	if err != nil {
		return nil, err
	}
	return viewProfile(stored), nil
}
func (h *harness) listProjects(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	projects, next, err := h.projectPage(ctx, p, key, str(in["cursor"]), 50)
	if err != nil {
		return nil, err
	}
	return struct {
		Projects   []Project `json:"projects"`
		NextCursor string    `json:"nextCursor"`
	}{projects, next}, nil
}
func (h *harness) getProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	project, err := h.loadProject(ctx, p, key, id)
	if err != nil {
		return nil, err
	}
	docs, err := h.projectDocuments(ctx, p, key, id)
	if err != nil {
		return nil, err
	}
	return struct {
		Project
		Documents []ProjectDocument `json:"documents"`
	}{project, docs}, nil
}
func (h *harness) createProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "name description systemInstructions color", "", ""); err != nil {
		return nil, err
	}
	if strings.TrimSpace(str(in["name"])) == "" {
		return nil, apiErr(400, "BAD_REQUEST", "A project name is required")
	}
	return h.mutateProject(ctx, p, key, rowID(), true, func(project *Project) {
		project.Name, project.Description, project.SystemInstructions, project.Color = str(in["name"]), str(in["description"]), str(in["systemInstructions"]), optionalString(in["color"])
	})
}
func (h *harness) updateProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "name description systemInstructions color", "", ""); err != nil {
		return nil, err
	}
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	return h.mutateProject(ctx, p, key, id, false, func(project *Project) {
		for field, value := range map[string]*string{"name": &project.Name, "description": &project.Description, "systemInstructions": &project.SystemInstructions} {
			if v, ok := in[field]; ok {
				*value = str(v)
			}
		}
		if color, ok := in["color"]; ok {
			project.Color = optionalString(color)
		}
	})
}
func (h *harness) deleteProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	return struct{}{}, h.removeRow(ctx, p, key, "project", id)
}

func (h *harness) deleteAllProjects(ctx context.Context, p *principal, key contentKey, _ object) (any, error) {
	deleted, err := h.removeAllProjects(ctx, p, key)
	if err != nil {
		return nil, err
	}
	return deletedCount{deleted}, nil
}

func (h *harness) deleteDocument(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	project, err := requiredID(in, "projectId")
	if err != nil {
		return nil, err
	}
	doc, err := requiredID(in, "documentId")
	if err != nil {
		return nil, err
	}
	return struct{}{}, h.removeRow(ctx, p, key, "project_document", project+"/"+doc)
}
func (h *harness) searchThreads(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	ids, indexing, err := h.searchChats(ctx, p, key, str(in["query"]), integer(in["limit"]))
	if err != nil {
		return nil, err
	}
	if indexing {
		if indexing, err = h.reindexChats(ctx, p, key); err != nil {
			return nil, err
		}
	}
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
		results = append(results, summarize(&row.Thread))
	}
	return struct {
		Results  []threadSummary `json:"results"`
		Indexing bool            `json:"indexing"`
	}{results, indexing}, nil
}
