package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
)

func (h *harness) apiRoutes() []routeSpec {
	routes := []routeSpec{
		{"/v1/session", true, false, h.session}, {"/v1/verify", true, false, h.verification},
		{"/v1/threads/list", false, true, h.listThreads}, {"/v1/threads/get", false, true, h.getThread},
		{"/v1/threads/update", false, true, h.updateThread}, {"/v1/threads/delete", false, true, h.deleteThread},
		{"/v1/threads/search", false, true, h.searchThreads}, {"/v1/threads/cancel", true, false, h.cancelThread},
		{"/v1/threads/queue/send", true, false, h.sendQueued}, {"/v1/threads/queue/remove", true, false, h.removeQueued}, {"/v1/threads/share", false, true, h.shareThread},
		{"/v1/profile/get", false, true, h.getProfile}, {"/v1/profile/update", false, true, h.updateProfile},
		{"/v1/projects/list", false, true, h.listProjects}, {"/v1/projects/get", false, true, h.getProject},
		{"/v1/projects/create", false, true, h.createProject}, {"/v1/projects/update", false, true, h.updateProject},
		{"/v1/projects/delete", false, true, h.deleteProject}, {"/v1/projects/documents/delete", false, true, h.deleteDocument},
		{"/v1/projects/memory/get", false, true, h.getMemory}, {"/v1/projects/memory/update", false, true, h.updateMemory},
		{"/v1/attachments/delete", false, true, h.deleteAttachment}, {"/v1/shares/open", true, false, h.openShare},
		{"/v1/import/status", false, false, h.importStatus},
		{"/v1/threads/delete-all", false, true, h.deleteAllThreads},
		{"/v1/projects/delete-all", false, true, h.deleteAllProjects},
		{"/v1/metadata/favicon", true, false, h.favicon},
	}
	for _, pair := range [][2]string{{"current", "/v1/key/current"}, {"register", "/v1/key/register"}, {"add-bundle", "/v1/key/add-bundle"}, {"remove-bundle", "/v1/key/remove-bundle"}, {"migrate", "/v1/blobs/migrate"}, {"migrate-all", "/v1/blobs/migrate-all"}, {"migrate-status", "/v1/blobs/migrate-status"}} {
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
	if c == nil {
		return nil, apiErr(503, "MODEL_UNAVAILABLE", "The catalog is unavailable")
	}
	credential, err := h.auth.inference(ctx, p)
	if err != nil {
		return nil, err
	}
	keyInfo := object{"registered": false, "keyId": nil, "bundles": []any{}}
	if !p.Anonymous {
		var current object
		err := h.syncAPI.call(ctx, p, "/v1/key/current", object{}, &current)
		if err != nil && asAPIError(err).HTTP != 404 {
			return nil, err
		}
		keyInfo["registered"], keyInfo["keyId"] = current["key_id"] != nil, current["key_id"]
		bundles := []object{}
		for id, value := range obj(current["bundles"]) {
			bundles = append(bundles, object{"credentialId": id, "createdAt": obj(value)["created_at"]})
		}
		slices.SortFunc(bundles, func(a, b object) int { return strings.Compare(str(a["credentialId"]), str(b["credentialId"])) })
		keyInfo["bundles"] = bundles
		keyInfo["hasData"] = boolean(current["has_data"], false)
	}
	features := object{"webSearch": false, "codeExecution": false, "sandbox": h.serviceReady("sandbox"), "piiCheck": true, "genUI": len(c.EnabledWidgets) > 0, "transcription": h.serviceReady("audio"), "memory": h.memoryEnabled}
	for _, family := range families {
		features[family.name] = family.client != nil
	}
	presets := []object{}
	for _, p := range c.Presets {
		d := clone(p)
		presets = append(presets, d)
	}
	var auto any
	defaultModel := str(c.Models[0]["modelName"])
	if h.autoModel != nil {
		auto = object{"multimodal": false, "default": "high", "levels": intelligenceLevels}
		defaultModel = "auto"
	}
	return object{"user": object{"id": p.ID, "anonymous": p.Anonymous}, "rateLimit": credential.Limit, "key": keyInfo, "models": c.displayModels(), "auto": auto, "defaultModel": defaultModel, "widgets": c.EnabledWidgets, "presets": presets, "features": features, "upload": object{"accept": uploadAccept(), "maxBytes": maxUploadBytes, "maxTextBytes": maxTextBytes}}, nil
}

func normalizeThread(row *storedRow) object {
	t := row.Data
	t["id"] = row.ID
	for _, field := range []string{"createdAt", "updatedAt"} {
		if value := isoTime(t[field]); value != "" {
			t[field] = value
		}
	}
	if str(t["title"]) == "" {
		t["title"] = "New Chat"
	}
	if str(t["titleState"]) == "" {
		t["titleState"] = "generated"
		if t["title"] == "New Chat" {
			t["titleState"] = "placeholder"
		}
	}
	t["model"] = normalizeModel(str(t["model"]))
	if _, ok := t["activeRun"]; !ok {
		t["activeRun"] = nil
	}
	if _, ok := t["projectId"]; !ok {
		t["projectId"] = nil
	}
	if _, ok := t["presetId"]; !ok {
		t["presetId"] = nil
	}
	t["pinned"] = boolean(t["pinned"], false)
	t["webSearchEnabled"] = boolean(t["webSearchEnabled"], true)
	for i, item := range arr(t["messages"]) {
		m := obj(item)
		if str(m["id"]) == "" {
			m["id"] = row.ID + "-message-" + jsonNumber(i)
		}
		if str(m["createdAt"]) == "" {
			m["createdAt"] = firstString(isoTime(m["timestamp"]), isoTime(t["createdAt"]))
		}
		upgradeTimeline(m)
		upgradeAttachments(m)
	}
	return t
}
func jsonNumber(n int) string { return string(raw(n)) }
func threadSummary(t object) object {
	out := object{}
	for _, key := range []string{"id", "title", "titleState", "createdAt", "updatedAt", "projectId", "model", "webSearchEnabled", "pinned", "activeRun", "contextUsage"} {
		out[key] = t[key]
	}
	out["messageCount"] = max(0, len(arr(t["messages"]))-integer(t["harnessHiddenCount"]))
	return out
}
func publicMessage(m object) object {
	out := object{}
	for _, key := range []string{"id", "role", "createdAt", "quote", "model", "modelDisplayName", "isError", "isInterrupted", "usage"} {
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
	if err := h.syncPins(ctx, p, key, page.Rows); err != nil {
		return nil, err
	}
	list := []any{}
	for _, row := range page.Rows {
		t := normalizeThread(&row)
		if pin, ok := in["pinned"].(bool); ok && pin != boolean(t["pinned"], false) {
			continue
		}
		list = append(list, threadSummary(t))
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
	if err := h.syncPins(ctx, p, key, rows); err != nil {
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
	fields := []string{"title", "model", "webSearchEnabled", "projectId", "presetId", "pinned"}
	if model, ok := in["model"]; ok {
		name := normalizeModel(str(model))
		if name != "auto" && h.catalog.get().definition(name) == nil {
			return nil, apiErr(404, "MODEL_UNAVAILABLE", "Unknown model")
		}
		in["model"] = name
	}
	if project := str(in["projectId"]); project != "" {
		if _, err := h.storeRows.load(ctx, p, key, "project", project); err != nil {
			return nil, err
		}
	}
	row, err := h.mutate(ctx, p, key, "chat", id, false, fields, func(data object) error {
		for _, f := range fields {
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
	if value, ok := in["pinned"].(bool); ok {
		if err := h.writeLegacyPin(ctx, p, key, id, value); err != nil {
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
	return h.deleteRow(ctx, p, key, "chat", id)
}
func (h *harness) deleteAllThreads(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "", "", "projectId"); err != nil {
		return nil, err
	}
	ids, err := h.rowIDs(ctx, p, key, "chat", str(in["projectId"]))
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := h.deleteThread(ctx, p, key, object{"id": id}); err != nil && asAPIError(err).HTTP != 404 {
			return nil, err
		}
	}
	return object{"deleted": len(ids)}, nil
}

func (h *harness) deleteRow(ctx context.Context, p *principal, key contentKey, scope, id string) (any, error) {
	for i := 0; i < 3; i++ {
		row, err := h.storeRows.load(ctx, p, key, scope, id)
		if err != nil {
			return nil, err
		}
		err = h.storeRows.remove(ctx, p, key, scope, row)
		if err == nil || asAPIError(err).Code != "REVISION_CONFLICT" {
			return object{}, err
		}
	}
	return nil, apiErr(409, "REVISION_CONFLICT", "The row changed while deleting")
}
func (h *harness) profile(ctx context.Context, p *principal, key contentKey) (object, error) {
	row, err := h.storeRows.load(ctx, p, key, "profile", "profile")
	if err != nil {
		if asAPIError(err).HTTP == 404 {
			return object{}, nil
		}
		return nil, err
	}
	return row.Data, nil
}
func profileView(profile object) object {
	out := selectFields(profile, strings.Join(profileFields, " ")+" updatedAt")
	for _, f := range []string{"fieldClocks", "clockVersion", "version", "pinnedChatIds", "webSearchAvailable"} {
		delete(out, f)
	}
	out["selectedModel"] = normalizeModel(str(out["selectedModel"]))
	return out
}
func (h *harness) getProfile(ctx context.Context, p *principal, key contentKey, _ object) (any, error) {
	profile, err := h.profile(ctx, p, key)
	if err != nil {
		return nil, err
	}
	return profileView(profile), nil
}

var profileFields = strings.Fields("themeMode chatFont language nickname profession traits additionalContext isUsingPersonalization isUsingCustomPrompt customSystemPrompt customPromptPresets favoritePromptPresetIds selectedModel autoIntelligence reasoningEffort thinkingEnabled webSearchEnabled codeExecutionEnabled sandboxEnabled piiCheckEnabled genUIEnabled pixelateSidebarChatTitlesEnabled browserTabChatTitleEnabled enterToNewlineEnabled hasSeenOnboarding hasSeenWebSearchIntro dismissed")

func (h *harness) updateProfile(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if !validObject(in["patch"]) {
		return nil, apiErr(400, "BAD_REQUEST", "patch must be an object")
	}
	patch := obj(in["patch"])
	if err := validateProfile(patch); err != nil {
		return nil, err
	}
	fields := []string{}
	for f := range patch {
		if !slices.Contains(profileFields, f) {
			return nil, apiErr(400, "BAD_REQUEST", "Unknown profile field: "+f)
		}
		fields = append(fields, f)
	}
	row, err := h.mutate(ctx, p, key, "profile", "profile", true, fields, func(data object) error {
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
	for _, f := range []string{"syncVersion", "clock", "writer", "clockVersion", "harnessMemoryCursors"} {
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
	list, cursor := []object{}, ""
	for {
		page, err := h.storeRows.list(ctx, p, key, "project_document", cursor, id, 100)
		if err != nil {
			return nil, err
		}
		for _, row := range page.Rows {
			doc := projectView(&row)
			doc["id"] = strings.TrimPrefix(row.ID, id+"/")
			doc["projectId"] = id
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
	out["contextUsage"] = h.projectUsage(row.Data, docs)
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
	row, err := h.mutate(ctx, p, key, "project", id, true, nil, func(data object) error {
		data["name"], data["description"], data["systemInstructions"], data["color"], data["memory"] = in["name"], str(in["description"]), str(in["systemInstructions"]), in["color"], []any{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return projectView(row), nil
}
func (h *harness) updateProject(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	if err := validateFields(in, "name description systemInstructions color", "memoryEnabled", ""); err != nil {
		return nil, err
	}
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	row, err := h.mutate(ctx, p, key, "project", id, false, nil, func(data object) error {
		for _, f := range []string{"name", "description", "systemInstructions", "color", "memoryEnabled"} {
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
	return h.deleteRow(ctx, p, key, "project", id)
}
func (h *harness) deleteAllProjects(ctx context.Context, p *principal, key contentKey, _ object) (any, error) {
	ids, err := h.rowIDs(ctx, p, key, "project", "")
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := h.deleteProject(ctx, p, key, object{"id": id}); err != nil && asAPIError(err).HTTP != 404 {
			return nil, err
		}
	}
	return object{"deleted": len(ids)}, nil
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
	return h.deleteRow(ctx, p, key, "project_document", project+"/"+doc)
}
func (h *harness) getMemory(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "projectId")
	if err != nil {
		return nil, err
	}
	row, err := h.storeRows.load(ctx, p, key, "project", id)
	if err != nil {
		return nil, err
	}
	return object{"facts": arr(row.Data["memory"])}, nil
}
func (h *harness) updateMemory(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "projectId")
	if err != nil {
		return nil, err
	}
	if _, ok := in["facts"].([]any); !ok {
		return nil, apiErr(400, "BAD_REQUEST", "facts must be an array")
	}
	row, err := h.mutate(ctx, p, key, "project", id, false, nil, func(data object) error { data["memory"] = in["facts"]; return nil })
	if err != nil {
		return nil, err
	}
	return object{"facts": row.Data["memory"]}, nil
}
func (h *harness) projectUsage(project object, documents []object) object {
	instructions := tokenCount(str(project["systemInstructions"]))
	memory := 0
	if h.memoryEnabled && boolean(project["memoryEnabled"], true) {
		memory = tokenCount(string(raw(arr(project["memory"]))))
	}
	total, limit := instructions+memory, 0
	docs := []object{}
	for _, doc := range documents {
		tokens := tokenCount(str(doc["content"]))
		total += tokens
		docs = append(docs, object{"filename": doc["filename"], "tokens": tokens})
	}
	if catalog := h.catalog.get(); catalog != nil {
		for _, m := range catalog.Models {
			n := integer(m["budgetWindow"]) * 4 / 5
			if limit == 0 || n < limit {
				limit = n
			}
		}
	}
	return object{"systemInstructions": instructions, "documents": docs, "memory": memory, "totalUsed": total, "modelLimit": limit, "availableForChat": max(0, limit-total)}
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
			if asAPIError(err).HTTP == 404 {
				continue
			}
			return nil, err
		}
		summary := threadSummary(normalizeThread(row))
		summary["score"] = match["score"]
		results = append(results, summary)
	}
	return object{"results": results, "indexing": indexing}, nil
}

var _ = json.Valid
