package main

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
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
			var out object
			err := h.syncAPI.call(ctx, p, pair[1], in, &out)
			return out, err
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

func jsonNumber(n int) string { return string(raw(n)) }
func threadSummary(t object) object {
	out := object{}
	for _, key := range []string{"id", "title", "titleState", "createdAt", "updatedAt", "projectId", "model", "webSearchEnabled", "pinned", "contextUsage"} {
		out[key] = t[key]
	}
	out["activeRun"] = nil
	run := obj(t["activeRun"])
	if expires, err := time.Parse(time.RFC3339Nano, str(run["expiresAt"])); err == nil && nowUTC().Before(expires) {
		out["activeRun"] = object{"runId": run["runId"], "expiresAt": run["expiresAt"]}
	}
	out["messageCount"] = max(0, len(arr(t["messages"]))-integer(t["harnessHiddenCount"]))
	return out
}
func publicMessage(m object) object {
	out := object{}
	for _, key := range []string{"id", "role", "createdAt", "quote", "model", "modelDisplayName", "isError", "isInterrupted", "usage", "annotations"} {
		if v, ok := m[key]; ok {
			out[key] = v
		}
	}
	if m["role"] == "assistant" {
		out["timeline"] = arr(m["timeline"])
	} else {
		out["content"] = str(m["content"])
	}
	if attachments := arr(m["attachments"]); len(attachments) > 0 {
		view := []any{}
		for _, item := range attachments {
			view = append(view, publicAttachment(obj(item)))
		}
		out["attachments"] = view
	}
	return out
}
func publicThread(row *storedRow, before string, limit int) object {
	t := normalizeThread(row)
	out := threadSummary(t)
	out["revision"], out["presetId"] = row.ETag, t["presetId"]
	all := arr(t["messages"])
	all = all[min(len(all), integer(t["harnessHiddenCount"])):]
	end := len(all)
	if before != "" {
		for i, m := range all {
			if obj(m)["id"] == before {
				end = i
				break
			}
		}
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	start := max(0, end-limit)
	messages := []any{}
	for _, message := range all[start:end] {
		messages = append(messages, publicMessage(obj(message)))
	}
	out["messages"], out["hasOlder"] = messages, start > 0
	return out
}
func (h *harness) listThreads(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	page, err := h.storeRows.list(ctx, p, key, "chat", str(in["cursor"]), str(in["projectId"]), integer(in["limit"]))
	if err != nil {
		return nil, err
	}
	if err := h.readLegacyPins(ctx, p, key, page.Rows); err != nil {
		return nil, err
	}
	list := []any{}
	for _, row := range page.Rows {
		list = append(list, threadSummary(normalizeThread(&row)))
	}
	return object{"threads": list, "nextCursor": page.Next}, nil
}
func (h *harness) getThread(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	row, err := h.storeRows.load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	rows := []storedRow{*row}
	if err := h.readLegacyPins(ctx, p, key, rows); err != nil {
		return nil, err
	}
	row = &rows[0]
	return publicThread(row, str(in["before"]), integer(in["limit"])), nil
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
		if _, err := h.storeRows.load(ctx, p, key, "project", project); err != nil {
			return nil, err
		}
	}
	row, err := h.mutate(ctx, p, key, "chat", id, false, func(data object) error {
		for _, f := range []string{"title", "model", "webSearchEnabled", "projectId", "presetId", "pinned"} {
			if v, ok := in[f]; ok {
				data[f] = v
			}
		}
		if _, ok := in["title"]; ok {
			data["titleState"] = "manual"
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
	return threadSummary(normalizeThread(row)), nil
}
func (h *harness) deleteThread(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	h.stopThread(p, id)
	return object{}, h.storeRows.remove(ctx, p, key, "chat", id)
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
	row, err := h.storeRows.load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	title := forkTitle(str(row.Data["title"]))
	t := normalizeThread(row)
	count := 1 + slices.IndexFunc(arr(t["messages"]), func(m any) bool { return obj(m)["id"] == messageID })
	if count == 0 {
		return nil, apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
	}
	target := rowID()
	if err := h.syncAPI.call(ctx, p, "/v1/sync/fork", object{"source_id": id, "target_id": target, "key": key.base64(), "message_count": count, "title": title, "created_at": timestamp(), "idempotency_key": uuid.NewString()}, nil); err != nil {
		return nil, err
	}
	// Sync leaves out the web client's bookkeeping, not the harness's.
	forked, err := h.mutate(ctx, p, key, "chat", target, false, func(data object) error {
		for _, field := range []string{"activeRun", "contextUsage", "pinned"} {
			delete(data, field)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return threadSummary(normalizeThread(forked)), nil
}

// Message actions change a stored chat's transcript. A local thread's client
// changes its own copy, which its next turn sends.
func (h *harness) deleteMessage(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	return h.changeMessages(ctx, p, key, in, func(messages []any, index int) ([]any, error) {
		// As on the web client, a chat keeps its last message.
		if len(messages) == 1 {
			return nil, apiErr(400, "BAD_REQUEST", "A chat keeps its last message")
		}
		return slices.Delete(messages, index, index+1), nil
	})
}
func (h *harness) updateMessage(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	text, ok := in["content"].(string)
	if !ok {
		return nil, apiErr(400, "BAD_REQUEST", "content must be a string")
	}
	return h.changeMessages(ctx, p, key, in, func(messages []any, index int) ([]any, error) {
		if obj(messages[index])["role"] != "assistant" {
			return nil, apiErr(400, "BAD_REQUEST", "Only a reply is edited in place")
		}
		replaceReplyText(obj(messages[index]), text)
		return messages, nil
	})
}
func (h *harness) changeMessages(ctx context.Context, p *principal, key contentKey, in object, change func([]any, int) ([]any, error)) (any, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, err
	}
	messageID, err := requiredID(in, "messageId")
	if err != nil {
		return nil, err
	}
	row, err := h.mutate(ctx, p, key, "chat", id, false, func(data object) error {
		messages := arr(data["messages"])
		index := slices.IndexFunc(messages, func(m any) bool { return obj(m)["id"] == messageID })
		if index < 0 {
			return apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		changed, err := change(messages, index)
		if err != nil {
			return err
		}
		data["messages"] = changed
		return nil
	})
	if err != nil {
		return nil, err
	}
	return publicThread(row, "", 50), nil
}
func (h *harness) deleteAllThreads(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "", "", "projectId"); err != nil {
		return nil, err
	}
	// Every id is listed before any is deleted, so a shrinking list cannot
	// skip a page, and rows this key cannot open are deleted too.
	ids, err := h.storeRows.ids(ctx, p, "chat", str(in["projectId"]))
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := h.deleteThread(ctx, p, key, object{"id": id}); err != nil {
			return nil, err
		}
	}
	return object{"deleted": len(ids)}, nil
}

func (h *harness) profile(ctx context.Context, p *principal, key contentKey) (object, error) {
	row, err := h.storeRows.load(ctx, p, key, "profile", "profile")
	if err != nil {
		if asAPIError(err).HTTP == 404 {
			return object{}, nil
		}
		return nil, err
	}
	return profileView(row.Data), nil
}

// profileView is a profile in the fields the harness reads, upgraded from
// what old clients stored.
func profileView(profile object) object {
	out := selectFields(profile, strings.Join(profileFields, " ")+" updatedAt")
	out["selectedModel"] = normalizeModel(str(out["selectedModel"]))
	return out
}
func (h *harness) getProfile(ctx context.Context, p *principal, key contentKey, _ object) (any, error) {
	return h.profile(ctx, p, key)
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
	row, err := h.mutate(ctx, p, key, "profile", "profile", true, func(data object) error {
		for field, value := range patch {
			data[field] = value
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return profileView(row.Data), nil
}
func projectView(row *storedRow) object {
	out := clone(row.Data)
	out["id"] = row.ID
	for _, field := range []string{"createdAt", "updatedAt"} {
		if value := isoTime(out[field]); value != "" {
			out[field] = value
		}
	}
	for _, f := range []string{"syncVersion", "clock", "writer", "clockVersion"} {
		delete(out, f)
	}
	return out
}
func (h *harness) listProjects(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	page, err := h.storeRows.list(ctx, p, key, "project", str(in["cursor"]), "", 50)
	if err != nil {
		return nil, err
	}
	list := []any{}
	for _, row := range page.Rows {
		list = append(list, projectView(&row))
	}
	return object{"projects": list, "nextCursor": page.Next}, nil
}
func (h *harness) projectDocuments(ctx context.Context, p *principal, key contentKey, id string) ([]object, error) {
	list, cursor, index := []object{}, "", map[string]int{}
	for {
		page, err := h.storeRows.list(ctx, p, key, "project_document", cursor, id, 100)
		if err != nil {
			return nil, err
		}
		for _, row := range page.Rows {
			doc := projectView(&row)
			doc["id"] = strings.TrimPrefix(row.ID, id+"/")
			doc["projectId"] = id
			// Unfiltered pages can repeat a row that changed while listing;
			// the later copy is the newer one.
			if i, ok := index[row.ID]; ok {
				list[i] = doc
				continue
			}
			index[row.ID] = len(list)
			list = append(list, doc)
		}
		if page.Next == "" {
			return list, nil
		}
		if page.Next == cursor {
			return nil, apiErr(502, "UPSTREAM_REFUSED", "The document cursor did not advance")
		}
		cursor = page.Next
	}
}
func (h *harness) getProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	row, err := h.storeRows.load(ctx, p, key, "project", id)
	if err != nil {
		return nil, err
	}
	docs, err := h.projectDocuments(ctx, p, key, id)
	if err != nil {
		return nil, err
	}
	out := projectView(row)
	out["documents"] = docs
	return out, nil
}
func (h *harness) createProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "name description systemInstructions color", "", ""); err != nil {
		return nil, err
	}
	if strings.TrimSpace(str(in["name"])) == "" {
		return nil, apiErr(400, "BAD_REQUEST", "A project name is required")
	}
	id := rowID()
	row, err := h.mutate(ctx, p, key, "project", id, true, func(data object) error {
		data["name"], data["description"], data["systemInstructions"], data["color"] = in["name"], str(in["description"]), str(in["systemInstructions"]), in["color"]
		return nil
	})
	if err != nil {
		return nil, err
	}
	return projectView(row), nil
}
func (h *harness) updateProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "name description systemInstructions color", "", ""); err != nil {
		return nil, err
	}
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	row, err := h.mutate(ctx, p, key, "project", id, false, func(data object) error {
		for _, f := range []string{"name", "description", "systemInstructions", "color"} {
			if v, ok := in[f]; ok {
				data[f] = v
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return projectView(row), nil
}
func (h *harness) deleteProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	return object{}, h.storeRows.remove(ctx, p, key, "project", id)
}

// Sync deletes every project and project document in one step.
func (h *harness) deleteAllProjects(ctx context.Context, p *principal, key contentKey, _ object) (any, error) {
	var result struct {
		Deleted int `json:"deleted"`
	}
	if err := h.syncAPI.call(ctx, p, "/v1/sync/delete-all-projects", object{"key": key.base64(), "idempotency_key": uuid.NewString()}, &result); err != nil {
		return nil, err
	}
	return object{"deleted": result.Deleted}, nil
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
	return object{}, h.storeRows.remove(ctx, p, key, "project_document", project+"/"+doc)
}
func (h *harness) searchThreads(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	var found object
	err := h.syncAPI.call(ctx, p, "/v1/search/query", object{"key": key.base64(), "query": str(in["query"]), "limit": integer(in["limit"])}, &found)
	if err != nil {
		return nil, err
	}
	indexing := boolean(found["needs_reindex"], false)
	if indexing {
		var job object
		if err := h.syncAPI.call(ctx, p, "/v1/search/reindex", object{"keys": []object{{"key": key.base64()}}}, &job); err != nil {
			return nil, err
		}
		indexing = job["status"] == "running"
	}
	results := []any{}
	for _, v := range arr(found["results"]) {
		match := obj(v)
		row, err := h.storeRows.load(ctx, p, key, "chat", str(match["id"]))
		if err != nil {
			// A deleted or unreadable chat leaves the other results intact.
			if code := asAPIError(err).HTTP; code == 404 || code == 409 {
				continue
			}
			return nil, err
		}
		results = append(results, threadSummary(normalizeThread(row)))
	}
	return object{"results": results, "indexing": indexing}, nil
}
