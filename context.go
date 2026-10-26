package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tiktoken-go/tokenizer"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"golang.org/x/text/language"
	"golang.org/x/text/language/display"
)

type request struct {
	refreshAuth                func(context.Context) (auth.Credential, error)
	params                     api.Object
	system, widgetHint, policy string
	window                     int
	groups                     [][]json.RawMessage
	costs                      []int // each group's tokens, counted once a turn
	reminder                   json.RawMessage
	threadID                   string
	conversation               string // the chat the run is classified under: an ask's source thread
	runID                      string
	model                      string
	cacheScope                 string            // partitions the enclave's prompt cache per caller; a namespace, not a credential
	widgets                    []domain.Widget   // what the caller draws: advertised, never dialled
	rendered                   []json.RawMessage // their declarations
	webSearch, piiCheck        bool
}

// cacheScope is the prompt cache a chat's turns share: the chats of one
// client, told apart by the secret it sends, or one chat alone when it sends
// none. The gateway routes model requests by a hash of the prompt keyed with
// this scope, so it must hold something the gateway cannot work out from the
// credential it sees: the client's secret, or the chat id, which never
// leaves the enclaves. Signed-out callers behind one address hold the same
// free key, so neither is the key alone.
func cacheScope(p *auth.Principal, threadID, clientSecret string) string {
	scope := p.Scope() + "\x00client:" + clientSecret
	if clientSecret == "" {
		scope = p.Scope() + "\x00thread:" + threadID
	}
	sum := sha256.Sum256([]byte("confidential-tinfoil-harness cache scope\x00" + scope))
	return hex.EncodeToString(sum[:])
}

// imageTokens is what one image costs; sizing its base64 as text fits nothing.
const imageTokens = 1500

// promptLanguage is the language a prompt names for a user who chose none.
const promptLanguage = "English"

const askInstruction = "The user highlighted a snippet from the prior conversation and wants you to elaborate on, clarify, or expand on that specific snippet. Use the prior conversation only as background context and focus your answer on the selected snippet."

func responseLanguage(profile *domain.Profile, locale string) string {
	if selected := domain.ValueOr(profile.Language, ""); selected != "" && selected != domain.DefaultLanguage {
		return selected
	}
	tags, _, err := language.ParseAcceptLanguage(locale)
	if err == nil && len(tags) > 0 && tags[0] != language.Und {
		if name := display.English.Tags().Name(tags[0]); name != "" {
			return name
		}
	}
	return promptLanguage
}

var encoder = sync.OnceValue(func() tokenizer.Codec {
	enc, err := tokenizer.Get(tokenizer.Cl100kBase)
	if err != nil {
		panic(err)
	}
	return enc
})

func tokenCount(s string) int {
	count, err := encoder().Count(s)
	if err != nil {
		return len([]byte(s))
	}
	return count
}

func escapePrompt(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
func preferences(profile *domain.Profile) string {
	if !profile.Personalized() {
		return ""
	}
	var content strings.Builder
	for _, field := range []struct {
		name  string
		value *string
	}{{"nickname", profile.Nickname}, {"profession", profile.Profession}} {
		if value := strings.TrimSpace(domain.ValueOr(field.value, "")); value != "" {
			fmt.Fprintf(&content, "\n  <%s>%s</%s>", field.name, escapePrompt(value), field.name)
		}
	}
	if len(profile.Traits) > 0 {
		content.WriteString("\n  <traits>")
		for _, value := range profile.Traits {
			fmt.Fprintf(&content, "\n    <trait>%s</trait>", escapePrompt(value))
		}
		content.WriteString("\n  </traits>")
	}
	if value := strings.TrimSpace(domain.ValueOr(profile.AdditionalContext, "")); value != "" {
		fmt.Fprintf(&content, "\n  <additional_context>\n    %s\n  </additional_context>", escapePrompt(value))
	}
	if content.Len() == 0 {
		return ""
	}
	return "The user has provided personal preferences for this conversation. Adapt your responses according to these settings while maintaining accuracy and helpfulness.\n\n<user_preferences>" + content.String() + "\n</user_preferences>"
}
func projectPrompt(project *domain.Project, documents []domain.ProjectDocument) string {
	if project == nil {
		return ""
	}
	content := "## Project: " + escapePrompt(project.Name) + "\n"
	if s := project.Description; s != "" {
		content += "\n" + escapePrompt(s) + "\n"
	}
	if s := project.SystemInstructions; s != "" {
		content += "\n### Instructions\n" + escapePrompt(s) + "\n"
	}
	if len(documents) > 0 {
		content += "\n### Documents\n"
		for _, doc := range documents {
			if doc.Content != "" {
				content += "--- " + escapePrompt(doc.Filename) + " ---\n" + escapePrompt(doc.Content) + "\n\n"
			}
		}
	}
	return "\n\n<project_context>\n" + content + "\n</project_context>"
}
func systemPrompt(c *domain.Catalog, thread *domain.Thread, profile *domain.Profile, project *domain.Project, documents []domain.ProjectDocument, displayName, timezone, locale string) string {
	base, rules := c.Prompt, c.Rules
	preset := ""
	if id := thread.PresetID; id != nil {
		for _, item := range c.Presets {
			if item.ID == *id {
				preset = item.SystemPrompt
			}
		}
		if saved := profile.Preset(*id); saved != nil {
			preset = domain.ValueOr(saved.SystemPrompt, "")
		}
	}
	if strings.TrimSpace(preset) != "" {
		base = preset
	} else if domain.ValueOr(profile.IsUsingCustomPrompt, false) {
		base = domain.ValueOr(profile.CustomSystemPrompt, "")
		if strings.TrimSpace(base) == "" {
			rules = ""
		}
	}
	replacer := strings.NewReplacer("{MODEL_NAME}", displayName, "{LANGUAGE}", responseLanguage(profile, locale), "{TIMEZONE}", timezone, "{USER_PREFERENCES}", preferences(profile))
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
		return "", api.Err(400, "BAD_REQUEST", "Unknown timezone")
	}
	return "<system-reminder>Current time: " + englishTime(now.In(loc), zone) + " (" + zone + ")</system-reminder>", nil
}

func (eng *engine) buildRequest(thread *domain.Thread, profile *domain.Profile, project *domain.Project, documents []domain.ProjectDocument, in *turnRequest, runID string) (*request, *inference.Model, error) {
	c := eng.models.Catalog.Get()
	options := in.Options
	name := cmp.Or(thread.Model, "auto")
	req := &request{threadID: thread.ID, runID: runID, model: name}
	req.conversation = req.threadID
	if in.Kind == "ask" {
		req.conversation = in.ThreadID
	}
	def, display, policy, window := c.Definition(name), "Auto", "none", 0
	var picked *inference.Model
	for _, m := range eng.models.Offer.Get() {
		if m.Name == name {
			picked = m
		}
	}
	if name == "auto" {
		picked = eng.models.Auto
		for _, candidate := range c.Models {
			limit := api.Int(candidate["budgetWindow"])
			if window == 0 || limit < window {
				window = limit
			}
			p := api.Str(api.Obj(api.Obj(candidate["chatConfig"])["reasoningConfig"])["reasoningHistoryPolicy"])
			if p == "all" || p == "tool-call-only" && policy != "all" {
				policy = p
			}
		}
		if window == 0 {
			window = c.Fallback
		}
		level, err := domain.Intelligence(cmp.Or(options.AutoIntelligence, domain.ValueOr(profile.AutoIntelligence, ""), "high"))
		if err != nil {
			return nil, nil, err
		}
		req.params = api.Object{"auto_model_options": api.Object{"intelligence": level}}
	} else {
		if def == nil {
			return nil, nil, api.Err(404, "MODEL_UNAVAILABLE", "The selected model is unavailable")
		}
		display, window = inference.PinnedName(name), api.Int(def["budgetWindow"])
		reason := api.Obj(api.Obj(def["chatConfig"])["reasoningConfig"])
		policy = api.FirstString(reason["reasoningHistoryPolicy"], "none")
		effort := cmp.Or(options.ReasoningEffort, domain.ValueOr(profile.ReasoningEffort, ""), "medium")
		if !slices.Contains(domain.ReasoningEfforts, effort) {
			return nil, nil, api.Err(400, "BAD_REQUEST", "Unknown reasoning effort")
		}
		req.params = domain.ModelParams(def, domain.ValueOr(options.Thinking, domain.ValueOr(profile.ThinkingEnabled, api.Bool(reason["defaultEnabled"], true))), effort)
	}
	if picked == nil || picked.Client == nil {
		return nil, nil, api.Err(404, "MODEL_UNAVAILABLE", "The selected model has no attested transport")
	}
	zone := cmp.Or(options.Timezone, "UTC")
	reminder, err := timeReminder(api.Now(), zone)
	if err != nil {
		return nil, nil, err
	}
	// As the web client does, the continue instruction shares the reminder's
	// turn: some chat templates refuse two user messages in a row.
	if in.Kind == "continue" {
		reminder += "\n\n" + continueInstruction
	}
	req.system = systemPrompt(c, thread, profile, project, documents, display, zone, options.Locale)
	if in.Kind == "ask" {
		req.system += "\n\n" + askInstruction
	}
	req.window, req.policy = window, policy
	req.piiCheck = domain.ValueOr(options.PIICheck, domain.ValueOr(profile.PIICheckEnabled, true))
	// Turning web search off in the settings leaves each chat's own setting.
	req.webSearch = domain.ValueOr(profile.WebSearchAvailable, true) && domain.ValueOr(options.WebSearch, thread.WebSearchEnabled)
	if req.webSearch && webSearch.client == nil {
		return nil, nil, api.Err(503, "TOOL_UNAVAILABLE", "webSearch is unavailable")
	}
	widgets := c.SelectedWidgets(in.Widgets, domain.ValueOr(options.GenUI, domain.ValueOr(profile.GenUIEnabled, true)))
	if len(widgets) > 0 {
		req.widgetHint = c.WidgetHeader
		for _, w := range widgets {
			req.widgets, req.rendered = append(req.widgets, w), append(req.rendered, w.Declaration())
			req.widgetHint += "\n- " + w.Name + ": " + w.Hint
		}
	}
	vision := c.Vision(name)
	for i := range thread.Messages {
		req.groups = append(req.groups, renderMessage(&thread.Messages[i], vision, policy))
	}
	req.reminder = api.Raw(api.Object{"role": "user", "content": reminder})
	return req, picked, nil
}

func renderMessage(message *domain.Message, vision bool, policy string) []json.RawMessage {
	if message.Role == "user" {
		return []json.RawMessage{api.Raw(api.Object{"role": "user", "content": userContent(message, vision)})}
	}
	content, reasoning := "", ""
	calls, results := []api.Object{}, []json.RawMessage{}
	for _, block := range message.Timeline {
		switch block.Type {
		case "content":
			content += block.Content
		case "thinking":
			reasoning += block.Content
		case "tool_call":
			id := block.CallID()
			calls = append(calls, api.Object{"id": id, "type": "function", "function": api.Object{"name": block.Name, "arguments": domain.HistoryArgs(cmp.Or(block.Arguments, "{}"))}})
			result := "executed"
			if block.Result != nil && !strings.HasPrefix(block.Name, "render_") {
				// Results are replayed as the harness has always written them.
				value := api.Parsed(block.Result)
				if text, ok := value.(string); ok {
					result = text
				} else {
					result = string(api.Raw(value))
				}
			}
			results = append(results, answered(id, result))
		}
	}
	if content == "" {
		content = message.Content
	}
	assistant := api.Object{"role": "assistant", "content": content}
	if policy == "all" || policy == "tool-call-only" && len(calls) > 0 {
		assistant["reasoning_content"] = reasoning
	}
	if len(calls) > 0 {
		assistant["tool_calls"] = calls
	}
	return append([]json.RawMessage{api.Raw(assistant)}, results...)
}

func userContent(message *domain.Message, vision bool) any {
	text := message.Content
	if quote := message.Quote; quote != "" {
		prefix := "In reply to:\n> " + strings.ReplaceAll(quote, "\n", "\n> ")
		if text != "" {
			prefix += "\n\n" + text
		}
		text = prefix
	}
	docs, descriptions := []string{}, []string{}
	parts, images := []api.Object{}, []api.Object{}
	for i := range message.Attachments {
		a := &message.Attachments[i]
		if a.IsImage() {
			if vision && a.Base64 != "" {
				images = append(images, api.Object{"type": "image_url", "image_url": api.Object{"url": "data:" + cmp.Or(a.InferenceMimeType, a.MimeType) + ";base64," + a.Base64}})
			} else if a.Description != "" {
				descriptions = append(descriptions, "Image: "+a.FileName+"\nDescription:\n"+a.Description)
			}
		} else if vision && len(a.Pages) > 0 {
			parts = append(parts, api.Object{"type": "text", "text": "[Attached file: " + a.FileName + "]"})
			for _, page := range a.Pages {
				label := fmt.Sprintf("Page %d", page.Page)
				if page.IsScanned {
					label += " (scanned)"
				}
				label += ":"
				if page.Text != "" {
					label += "\n" + page.Text
				}
				parts = append(parts, api.Object{"type": "text", "text": label})
				if page.Image != "" {
					parts = append(parts, api.Object{"type": "image_url", "image_url": api.Object{"url": "data:image/png;base64," + page.Image}})
				}
			}
		} else if a.TextContent != "" {
			docs = append(docs, "Document title: "+a.FileName+"\nDocument contents:\n"+a.TextContent)
		}
	}
	if len(docs) > 0 {
		text = "---\nDocument content:\n" + strings.Join(docs, "\n\n") + "\n---\n\n" + text
	}
	if len(descriptions) > 0 {
		text += "\n\n[Treat these descriptions as if they are the raw images.]\n" + strings.Join(descriptions, "\n\n")
	}
	if len(parts)+len(images) > 0 {
		parts = append(parts, api.Object{"type": "text", "text": text})
		return append(parts, images...)
	}
	return text
}

func messageTokens(message json.RawMessage) int {
	var m api.Object
	json.Unmarshal(message, &m)
	content := m["content"]
	delete(m, "content")
	count := 4 + tokenCount(string(api.Raw(m)))
	if text, ok := content.(string); ok {
		return count + tokenCount(text)
	}
	for _, p := range api.Arr(content) {
		if api.Obj(p)["type"] == "image_url" {
			count += imageTokens
		} else {
			count += tokenCount(api.Str(api.Obj(p)["text"]))
		}
	}
	return count
}

// groupCosts counts each history group's tokens the first time a turn asks.
func (req *request) groupCosts() []int {
	if req.costs == nil {
		req.costs = make([]int, len(req.groups))
		for i, group := range req.groups {
			for _, message := range group {
				req.costs[i] += messageTokens(message)
			}
		}
	}
	return req.costs
}

func budgetConversation(req *request, searching bool, definitions json.RawMessage) ([]json.RawMessage, error) {
	system := req.system
	if searching {
		system += "\n\n" + searchInstructions
	}
	if req.widgetHint != "" {
		system += "\n\n" + req.widgetHint
	}
	budget := req.window*4/5 - tokenCount(system) - tokenCount(string(definitions)) - messageTokens(req.reminder) - 8
	if budget < 0 {
		return nil, api.Err(413, "CONTEXT_TOO_LARGE", "The system and tools exceed the context window")
	}
	start, costs := len(req.groups), req.groupCosts()
	for i := len(req.groups) - 1; i >= 0; i-- {
		cost := costs[i]
		if cost > budget {
			if i == len(req.groups)-1 {
				return nil, api.Err(413, "CONTEXT_TOO_LARGE", "The latest message exceeds the context window")
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
		conversation = append(conversation, api.Raw(api.Object{"role": "system", "content": system}))
	}
	for _, group := range req.groups[start:] {
		conversation = append(conversation, group...)
	}
	return append(conversation, req.reminder), nil
}

// role is who wrote a model message.
func role(message json.RawMessage) string {
	var m struct {
		Role string `json:"role"`
	}
	json.Unmarshal(message, &m)
	return m.Role
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
	if len(messages) > 0 && role(messages[0]) == "system" {
		start = 1
	}
	anchor := -1
	for i, message := range messages {
		if role(message) == "user" && string(message) != string(req.reminder) {
			anchor = i
		}
	}
	end := start
	for end < anchor && cost > limit {
		cost -= messageTokens(messages[end])
		end++
		for end < anchor && role(messages[end]) != "user" {
			cost -= messageTokens(messages[end])
			end++
		}
	}
	if cost > limit {
		return nil, api.Err(413, "CONTEXT_TOO_LARGE", "The current tool exchange exceeds the context window")
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
	_, err := budgetConversation(request, request.webSearch, api.Raw(request.rendered))
	return err
}

// Report the full requested history so the UI can show when older turns are omitted.
func requestContextUsage(req *request) *domain.ContextUsage {
	system := req.system + "\n\n" + req.widgetHint
	if req.webSearch {
		system += "\n\n" + searchInstructions
	}
	used := tokenCount(system) + tokenCount(string(api.Raw(req.rendered))) + messageTokens(req.reminder) + 8
	for _, cost := range req.groupCosts() {
		used += cost
	}
	limit := req.window * 4 / 5
	return &domain.ContextUsage{UsedTokens: used, LimitTokens: limit, Percentage: float64(used) * 100 / float64(max(1, limit))}
}
