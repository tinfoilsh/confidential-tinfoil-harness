package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tiktoken-go/tokenizer"
)

var encoder = sync.OnceValue(func() tokenizer.Codec {
	enc, err := tokenizer.Get(tokenizer.Cl100kBase)
	if err != nil {
		panic(err)
	}
	return enc
})
var tokenizerMu sync.Mutex

func tokenCount(s string) int {
	tokenizerMu.Lock()
	defer tokenizerMu.Unlock()
	count, err := encoder().Count(s)
	if err != nil {
		return len([]byte(s))
	}
	return count
}

func escapePrompt(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
func preferences(profile object) string {
	if !boolean(profile["isUsingPersonalization"], true) {
		return ""
	}
	var content strings.Builder
	for _, field := range []string{"nickname", "profession"} {
		if value := strings.TrimSpace(str(profile[field])); value != "" {
			fmt.Fprintf(&content, "\n  <%s>%s</%s>", field, escapePrompt(value), field)
		}
	}
	if traits := arr(profile["traits"]); len(traits) > 0 {
		content.WriteString("\n  <traits>")
		for _, value := range traits {
			fmt.Fprintf(&content, "\n    <trait>%s</trait>", escapePrompt(str(value)))
		}
		content.WriteString("\n  </traits>")
	}
	if value := strings.TrimSpace(str(profile["additionalContext"])); value != "" {
		fmt.Fprintf(&content, "\n  <additional_context>\n    %s\n  </additional_context>", escapePrompt(value))
	}
	if content.Len() == 0 {
		return ""
	}
	return "The user has provided personal preferences for this conversation. Adapt your responses according to these settings while maintaining accuracy and helpfulness.\n\n<user_preferences>" + content.String() + "\n</user_preferences>"
}
func projectPrompt(project object, documents []object) string {
	if len(project) == 0 {
		return ""
	}
	content := "## Project: " + escapePrompt(str(project["name"])) + "\n"
	if s := str(project["description"]); s != "" {
		content += "\n" + escapePrompt(s) + "\n"
	}
	if s := str(project["systemInstructions"]); s != "" {
		content += "\n### Instructions\n" + escapePrompt(s) + "\n"
	}
	if len(documents) > 0 {
		content += "\n### Documents\n"
		for _, doc := range documents {
			if text := str(doc["content"]); text != "" {
				content += "--- " + escapePrompt(str(doc["filename"])) + " ---\n" + escapePrompt(text) + "\n\n"
			}
		}
	}
	if boolean(project["harnessMemoryEnabled"], false) && len(arr(project["memory"])) > 0 {
		content += "\n### Memory\n"
		for _, item := range arr(project["memory"]) {
			content += "- " + escapePrompt(str(obj(item)["fact"])) + "\n"
		}
	}
	return "\n\n<project_context>\n" + content + "\n</project_context>"
}
func systemPrompt(c *chatCatalog, thread, profile, project object, documents []object, displayName, timezone string) string {
	base, rules := c.Prompt, c.Rules
	preset := ""
	for _, item := range c.Presets {
		if item["id"] == thread["presetId"] {
			preset = str(item["systemPrompt"])
		}
	}
	for _, item := range arr(profile["customPromptPresets"]) {
		if obj(item)["id"] == thread["presetId"] {
			preset = str(obj(item)["systemPrompt"])
		}
	}
	if strings.TrimSpace(preset) != "" {
		base = preset
	} else if boolean(profile["isUsingCustomPrompt"], false) {
		base = str(profile["customSystemPrompt"])
		if strings.TrimSpace(base) == "" {
			rules = ""
		}
	}
	replacer := strings.NewReplacer("{MODEL_NAME}", displayName, "{LANGUAGE}", firstString(profile["language"], "English"), "{TIMEZONE}", timezone, "{USER_PREFERENCES}", preferences(profile))
	base = replacer.Replace(base) + projectPrompt(project, documents)
	if rules != "" {
		base += "\n" + replacer.Replace(rules)
	}
	return base
}
func timeReminder(now time.Time, zone string) (string, error) {
	if zone == "" {
		zone = "UTC"
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return "", apiErr(400, "BAD_REQUEST", "Unknown timezone")
	}
	return "<system-reminder>Current time: " + englishTime(now.In(loc), zone) + " (" + zone + ")</system-reminder>", nil
}

func (h *harness) buildRequest(thread, profile, project object, documents []object, in object, key contentKey, runID string) (*request, *model, error) {
	c := h.catalog.get()
	if c == nil {
		return nil, nil, apiErr(503, "MODEL_UNAVAILABLE", "The model catalog is unavailable")
	}
	options := obj(in["options"])
	if boolean(options["sandbox"], boolean(profile["sandboxEnabled"], false)) {
		return nil, nil, apiErr(503, "TOOL_UNAVAILABLE", "Agent sandbox is not available")
	}
	name := normalizeModel(firstString(options["model"], thread["model"], profile["selectedModel"], "auto"))
	req := &request{threadID: str(thread["id"]), runID: runID, model: name, managed: true, families: map[string]mcp.Meta{}}
	def, display, policy, window := c.definition(name), "Auto", "none", 0
	var picked *model
	for _, m := range h.models {
		if m.name == name {
			picked = m
		}
	}
	if name == "auto" {
		picked = h.autoModel
		for _, candidate := range c.Models {
			limit := integer(candidate["budgetWindow"])
			if window == 0 || limit < window {
				window = limit
			}
			p := str(obj(obj(candidate["chatConfig"])["reasoningConfig"])["reasoningHistoryPolicy"])
			if p == "all" || p == "tool-call-only" && policy != "all" {
				policy = p
			}
		}
		level, err := intelligence(firstString(options["autoIntelligence"], thread["autoIntelligence"], profile["autoIntelligence"], "high"))
		if err != nil {
			return nil, nil, err
		}
		req.params = object{"auto_model_options": object{"intelligence": level}}
	} else {
		if def == nil {
			return nil, nil, apiErr(404, "MODEL_UNAVAILABLE", "The selected model is unavailable")
		}
		display, window = str(def["name"]), integer(def["budgetWindow"])
		reason := obj(obj(def["chatConfig"])["reasoningConfig"])
		policy = firstString(reason["reasoningHistoryPolicy"], "none")
		effort := firstString(options["reasoningEffort"], profile["reasoningEffort"], "medium")
		if !slices.Contains([]string{"low", "medium", "high"}, effort) {
			return nil, nil, apiErr(400, "BAD_REQUEST", "Unknown reasoning effort")
		}
		req.params = modelParams(def, boolean(options["thinking"], boolean(profile["thinkingEnabled"], boolean(reason["defaultEnabled"], true))), effort)
	}
	if picked == nil || picked.client == nil {
		return nil, nil, apiErr(404, "MODEL_UNAVAILABLE", "The selected model has no attested transport")
	}
	zone := firstString(options["timezone"], "UTC")
	reminder, err := timeReminder(nowUTC(), zone)
	if err != nil {
		return nil, nil, err
	}
	if h.memoryEnabled && boolean(project["memoryEnabled"], true) {
		project = clone(project)
		project["harnessMemoryEnabled"] = true
	}
	req.system = systemPrompt(c, thread, profile, project, documents, display, zone)
	req.window, req.policy = window, policy
	req.piiCheck = boolean(options["piiCheck"], boolean(profile["piiCheckEnabled"], false))
	for _, f := range families {
		on := false
		switch f.name {
		case "webSearch":
			on = boolean(options["webSearch"], boolean(thread["webSearchEnabled"], boolean(profile["webSearchEnabled"], true)))
		case "codeExecution":
			on = boolean(options["codeExecution"], boolean(profile["codeExecutionEnabled"], false))
		}
		if !on {
			continue
		}
		if f.client == nil {
			return nil, nil, apiErr(503, "TOOL_UNAVAILABLE", f.name+" is unavailable")
		}
		if f.name == "codeExecution" {
			req.families[f.name] = mcp.Meta{"tinfoil_code_exec": sandbox{AccessToken: str(thread["codeExecutionAccessToken"]), EncryptionKey: key.codeKey(), ContainerAuthToken: key.containerToken(str(thread["id"]))}}
		} else {
			req.families[f.name] = nil
		}
	}
	widgets := c.selectedWidgets(arr(in["widgets"]), boolean(options["genUI"], boolean(profile["genUIEnabled"], true)))
	if len(widgets) > 0 {
		req.widgetHint = c.WidgetHeader
		for _, w := range widgets {
			req.rendered = append(req.rendered, w.declaration())
			req.widgetHint += "\n- " + w.Name + ": " + w.Hint
		}
	}
	vision := boolean(def["multimodal"], false)
	if name == "auto" {
		vision = true
		for _, candidate := range c.Models {
			vision = vision && boolean(candidate["multimodal"], false)
		}
	}
	for _, item := range arr(thread["messages"]) {
		req.groups = append(req.groups, renderMessage(obj(item), vision, policy))
	}
	req.reminder = raw(object{"role": "user", "content": reminder})
	return req, picked, nil
}

func renderMessage(message object, vision bool, policy string) []json.RawMessage {
	if str(message["role"]) == "user" {
		return []json.RawMessage{raw(object{"role": "user", "content": userContent(message, vision)})}
	}
	content, reasoning := "", ""
	calls, results := []object{}, []json.RawMessage{}
	for _, item := range arr(message["timeline"]) {
		block := obj(item)
		switch block["type"] {
		case "content":
			content += str(block["content"])
		case "thinking":
			reasoning += str(block["content"])
		case "tool_call":
			id := firstString(block["toolCallId"], block["id"])
			calls = append(calls, object{"id": id, "type": "function", "function": object{"name": block["name"], "arguments": firstString(block["arguments"], "{}")}})
			result := "executed"
			if value, ok := block["result"]; ok && !strings.HasPrefix(str(block["name"]), "render_") {
				if text, ok := value.(string); ok {
					result = text
				} else {
					result = string(raw(value))
				}
			}
			results = append(results, answered(id, result))
		}
	}
	if content == "" {
		content = str(message["content"])
	}
	if reasoning == "" {
		reasoning = str(message["thoughts"])
	}
	assistant := object{"role": "assistant", "content": content}
	if policy == "all" || policy == "tool-call-only" && len(calls) > 0 {
		assistant["reasoning_content"] = reasoning
	}
	if len(calls) > 0 {
		assistant["tool_calls"] = calls
	}
	return append([]json.RawMessage{raw(assistant)}, results...)
}

func userContent(message object, vision bool) any {
	text := str(message["content"])
	if quote := str(message["quote"]); quote != "" {
		prefix := "In reply to:\n> " + strings.ReplaceAll(quote, "\n", "\n> ")
		if text != "" {
			prefix += "\n\n" + text
		}
		text = prefix
	}
	docs, descriptions := []string{}, []string{}
	parts, images := []object{}, []object{}
	for _, item := range arr(message["attachments"]) {
		a := obj(item)
		kind := firstString(a["type"], a["kind"])
		if kind == "image" {
			if vision && str(a["base64"]) != "" {
				images = append(images, object{"type": "image_url", "image_url": object{"url": "data:" + firstString(a["inferenceMimeType"], a["mimeType"]) + ";base64," + str(a["base64"])}})
			} else if str(a["description"]) != "" {
				descriptions = append(descriptions, "Image: "+str(a["fileName"])+"\nDescription:\n"+str(a["description"]))
			}
		} else if vision && len(arr(a["pages"])) > 0 {
			parts = append(parts, object{"type": "text", "text": "[Attached file: " + str(a["fileName"]) + "]"})
			for _, value := range arr(a["pages"]) {
				p := obj(value)
				label := fmt.Sprintf("Page %d", integer(p["page"]))
				if boolean(p["is_scanned"], false) {
					label += " (scanned)"
				}
				label += ":"
				if str(p["text"]) != "" {
					label += "\n" + str(p["text"])
				}
				parts = append(parts, object{"type": "text", "text": label})
				if str(p["image"]) != "" {
					parts = append(parts, object{"type": "image_url", "image_url": object{"url": "data:image/png;base64," + str(p["image"])}})
				}
			}
		} else if str(a["textContent"]) != "" {
			docs = append(docs, "Document title: "+str(a["fileName"])+"\nDocument contents:\n"+str(a["textContent"]))
		}
	}
	if len(docs) > 0 {
		text = "---\nDocument content:\n" + strings.Join(docs, "\n\n") + "\n---\n\n" + text
	}
	if len(descriptions) > 0 {
		text += "\n\n[Treat these descriptions as if they are the raw images.]\n" + strings.Join(descriptions, "\n\n")
	}
	if len(parts)+len(images) > 0 {
		parts = append(parts, object{"type": "text", "text": text})
		return append(parts, images...)
	}
	return text
}

func messageTokens(message json.RawMessage) int {
	var m object
	json.Unmarshal(message, &m)
	content := m["content"]
	delete(m, "content")
	count := 4 + tokenCount(string(raw(m)))
	if text, ok := content.(string); ok {
		return count + tokenCount(text)
	}
	for _, p := range arr(content) {
		if obj(p)["type"] == "image_url" {
			count += imageTokens
		} else {
			count += tokenCount(str(obj(p)["text"]))
		}
	}
	return count
}
func budgetConversation(req *request, set *toolset, definitions json.RawMessage) ([]json.RawMessage, error) {
	system := req.system
	for _, session := range set.sessions {
		system += "\n\n" + session.fam.prompt
	}
	if req.widgetHint != "" {
		system += "\n\n" + req.widgetHint
	}
	budget := req.window*4/5 - tokenCount(system) - tokenCount(string(definitions)) - messageTokens(req.reminder) - 8
	if budget < 0 {
		return nil, apiErr(413, "CONTEXT_TOO_LARGE", "The system and tools exceed the context window")
	}
	start := len(req.groups)
	for i := len(req.groups) - 1; i >= 0; i-- {
		cost := 0
		for _, message := range req.groups[i] {
			cost += messageTokens(message)
		}
		if cost > budget {
			if i == len(req.groups)-1 {
				return nil, apiErr(413, "CONTEXT_TOO_LARGE", "The latest message exceeds the context window")
			}
			break
		}
		budget -= cost
		start = i
	}
	// A history window starts with a user message; tool result groups stay intact.
	for start < len(req.groups) {
		if len(req.groups[start]) > 0 && strings.Contains(string(req.groups[start][0]), `"role":"user"`) {
			break
		}
		start++
	}
	conversation := []json.RawMessage{}
	if strings.TrimSpace(system) != "" {
		conversation = append(conversation, raw(object{"role": "system", "content": system}))
	}
	for _, group := range req.groups[start:] {
		conversation = append(conversation, group...)
	}
	return append(conversation, req.reminder), nil
}

// Trim only complete older user turns as tool output grows. The current user
// request and its entire tool exchange stay together, including the reminder.
func trimToolHistory(req *request, messages []json.RawMessage, defs json.RawMessage) ([]json.RawMessage, error) {
	cost := tokenCount(string(defs))
	for _, message := range messages {
		cost += messageTokens(message)
	}
	limit := req.window * 4 / 5
	if cost <= limit {
		return messages, nil
	}
	start := 0
	if len(messages) > 0 && obj(decodeValue(messages[0]))["role"] == "system" {
		start = 1
	}
	anchor := -1
	for i, message := range messages {
		if obj(decodeValue(message))["role"] == "user" && string(message) != string(req.reminder) {
			anchor = i
		}
	}
	end := start
	for end < anchor && cost > limit {
		cost -= messageTokens(messages[end])
		end++
		for end < anchor && obj(decodeValue(messages[end]))["role"] != "user" {
			cost -= messageTokens(messages[end])
			end++
		}
	}
	if cost > limit {
		return nil, apiErr(413, "CONTEXT_TOO_LARGE", "The current tool exchange exceeds the context window")
	}
	return append(append([]json.RawMessage{}, messages[:start]...), messages[end:]...), nil
}

func englishTime(now time.Time, zone string) string {
	name, offset := now.Zone()
	if zone != "UTC" && zone != "Etc/UTC" && !(strings.HasPrefix(zone, "America/") || zone == "Pacific/Honolulu") {
		name = "GMT"
		if offset != 0 {
			sign := "+"
			if offset < 0 {
				sign = "-"
				offset = -offset
			}
			name += sign + fmt.Sprint(offset/3600)
			if offset%3600 != 0 {
				name += fmt.Sprintf(":%02d", offset%3600/60)
			}
		}
	}
	return now.Format("Monday, January 2, 2006 at 03:04 PM ") + name
}

func preflightBudget(request *request) error {
	preflight := &toolset{}
	for _, family := range families {
		if _, on := request.families[family.name]; on {
			preflight.sessions = append(preflight.sessions, &session{fam: family})
		}
	}
	_, err := budgetConversation(request, preflight, raw(request.rendered))
	return err
}

// Report the full requested history so the UI can show when older turns are omitted.
func requestContextUsage(req *request) object {
	system := req.system + "\n\n" + req.widgetHint
	for _, family := range families {
		if _, on := req.families[family.name]; on {
			system += "\n\n" + family.prompt
		}
	}
	used := tokenCount(system) + tokenCount(string(raw(req.rendered))) + messageTokens(req.reminder) + 8
	for _, group := range req.groups {
		for _, message := range group {
			used += messageTokens(message)
		}
	}
	limit := req.window * 4 / 5
	return object{"usedTokens": used, "limitTokens": limit, "percentage": float64(used) * 100 / float64(max(1, limit))}
}
