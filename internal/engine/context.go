package engine

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"golang.org/x/text/language"
	"golang.org/x/text/language/display"
)

type Request struct {
	refreshAuth                func(context.Context) (auth.Credential, error)
	params                     api.Object
	System, widgetHint, policy string
	window                     int
	Groups                     [][]json.RawMessage
	costs                      []int // each group's tokens, counted once a turn
	reminder                   json.RawMessage
	threadID                   string
	conversation               string // the chat the run is classified under: an ask's source thread
	usageID                    string // the run's id in usage reports
	model                      string
	cacheScope                 string            // partitions the enclave's prompt cache per caller; a namespace, not a credential
	widgets                    []domain.Widget   // what the caller draws: advertised, never dialled
	rendered                   []json.RawMessage // their declarations
	webSearch, piiCheck        bool
	// Groups are the history the model reads: each message after the chat's
	// latest compaction marker, the first carrying the marker's summary.
	// history is every message but the markers, as the safeguards classify
	// the chat. Both are read from messages, the chat as the prompt reads it.
	history  [][]json.RawMessage
	messages []domain.Message
	vision   bool
	loc      *time.Location
}

// cacheScope is the prompt cache a chat's turns share: the chats of one
// client, told apart by the secret it sends, or one chat alone when it sends
// none. The router salts the engine's prompt cache with this scope, so it
// must hold something besides the credential: the client's secret, or the
// chat id. Signed-out callers behind one address hold the same free key, so
// neither is the key alone.
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
func timeReminder(now time.Time, loc *time.Location) string {
	zone := loc.String()
	return "<system-reminder>Current time: " + englishTime(now.In(loc), zone) + " (" + zone + ")</system-reminder>"
}

// sentReminder is when a question was asked, in the zone the current time is
// given in. It is read from the stored time, never the clock, so every turn
// renders the history alike and keeps hitting the prompt cache. A message
// without a readable time has none.
func sentReminder(createdAt string, loc *time.Location) string {
	sent, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return ""
	}
	return "<system-reminder>Sent: " + englishTime(sent.In(loc), loc.String()) + "</system-reminder>"
}

func (eng *Engine) BuildRequest(thread *domain.Thread, profile *domain.Profile, project *domain.Project, documents []domain.ProjectDocument, in *TurnRequest, usageID string) (*Request, *inference.Model, error) {
	c := eng.Models.Catalog.Get()
	options := in.Options
	name := cmp.Or(thread.Model, "auto")
	req := &Request{threadID: thread.ID, usageID: usageID, model: name}
	req.conversation = req.threadID
	if in.Kind == "ask" {
		req.conversation = in.ThreadID
	}
	def, display, policy, window := c.Definition(name), "Auto", "none", 0
	var picked *inference.Model
	for _, m := range eng.Models.Offer.Get() {
		if m.Name == name {
			picked = m
		}
	}
	if name == "auto" {
		picked = eng.Models.Auto
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
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return nil, nil, api.Err(400, "BAD_REQUEST", "Unknown timezone")
	}
	reminder := timeReminder(api.Now(), loc)
	// As the web client does, the continue instruction shares the reminder's
	// turn: some chat templates refuse two user messages in a row.
	if in.Kind == "continue" {
		reminder += "\n\n" + ContinueInstruction
	}
	// An ask's instruction follows the history too, so its prompt starts as
	// the chat it asks about does and reuses that chat's prompt cache.
	if in.Kind == "ask" {
		reminder += "\n\n" + askInstruction
	}
	req.System = systemPrompt(c, thread, profile, project, documents, display, zone, options.Locale)
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
	req.messages, req.vision, req.loc = thread.Messages, c.Vision(name), loc
	req.render()
	req.reminder = api.Raw(api.Object{"role": "user", "content": reminder})
	return req, picked, nil
}

// render reads the chat into the history the model reads, from its latest
// compaction marker on, and into the history the safeguards classify.
func (req *Request) render() {
	req.Groups, req.history, req.costs = nil, nil, nil
	latest := domain.LatestCompaction(req.messages)
	for i := range req.messages {
		m := &req.messages[i]
		if m.Role == domain.CompactionRole {
			continue
		}
		group := RenderMessage(m, req.vision, req.policy, req.loc)
		req.history = append(req.history, group)
		if i > latest {
			req.Groups = append(req.Groups, group)
		}
	}
	if latest >= 0 {
		req.Groups = withSummary(req.Groups, req.messages[latest].Content)
	}
}

// compactedNote tells the model what a compaction marker's summary is.
const compactedNote = "The earlier part of this conversation was compacted into this summary to fit the context window. Treat it as your memory of those messages. Files, images and pages from that part are no longer available; ask the user to share them again if you need their exact content. Do not mention this summary unless the user asks about earlier messages."

// summaryTags are the tags the summary is wrapped in, which its own text
// cannot close.
var summaryTags = regexp.MustCompile(`(?i)<(/?conversation_summary)`)

// withSummary puts a compaction marker's summary at the start of the history
// that follows it: in the question that opens it, so no two user messages
// follow each other, or alone before a reply that opens it.
func withSummary(groups [][]json.RawMessage, summary string) [][]json.RawMessage {
	text := "<conversation_summary>\n" + compactedNote + "\n\n" + summaryTags.ReplaceAllString(summary, "&lt;$1") + "\n</conversation_summary>"
	if len(groups) == 0 || role(groups[0][0]) != "user" {
		return append([][]json.RawMessage{{api.Raw(api.Object{"role": "user", "content": text})}}, groups...)
	}
	question := api.Obj(api.Parsed(groups[0][0]))
	if content, ok := question["content"].(string); ok {
		question["content"] = text + "\n\n" + content
	} else {
		question["content"] = append([]any{api.Object{"type": "text", "text": text}}, api.Arr(question["content"])...)
	}
	first := append([]json.RawMessage{api.Raw(question)}, groups[0][1:]...)
	return append([][]json.RawMessage{first}, groups[1:]...)
}

func RenderMessage(message *domain.Message, vision bool, policy string, loc *time.Location) []json.RawMessage {
	if message.Role == "user" {
		return []json.RawMessage{api.Raw(api.Object{"role": "user", "content": userContent(message, vision, loc)})}
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

func userContent(message *domain.Message, vision bool, loc *time.Location) any {
	text := message.Content
	if quote := message.Quote; quote != "" {
		prefix := "In reply to:\n> " + strings.ReplaceAll(quote, "\n", "\n> ")
		if text != "" {
			prefix += "\n\n" + text
		}
		text = prefix
	}
	if len(message.Answers) > 0 {
		answers := domain.AnswersPrompt(message.Answers)
		if text != "" {
			answers += "\n\n" + text
		}
		text = answers
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
	if sent := sentReminder(message.CreatedAt, loc); sent != "" {
		if text != "" {
			sent += "\n\n"
		}
		text = sent + text
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
	count := 4 + inference.Tokens(string(api.Raw(m)))
	if text, ok := content.(string); ok {
		return count + inference.Tokens(text)
	}
	for _, p := range api.Arr(content) {
		if api.Obj(p)["type"] == "image_url" {
			count += imageTokens
		} else {
			count += inference.Tokens(api.Str(api.Obj(p)["text"]))
		}
	}
	return count
}

// groupCosts counts each history group's tokens the first time a turn asks.
func (req *Request) groupCosts() []int {
	if req.costs == nil {
		req.costs = make([]int, len(req.Groups))
		for i, group := range req.Groups {
			for _, message := range group {
				req.costs[i] += messageTokens(message)
			}
		}
	}
	return req.costs
}

func BudgetConversation(req *Request, searching bool, definitions json.RawMessage) ([]json.RawMessage, error) {
	system := req.System
	if searching {
		system += "\n\n" + searchInstructions
	}
	if req.widgetHint != "" {
		system += "\n\n" + req.widgetHint
	}
	budget := req.window*4/5 - inference.Tokens(system) - inference.Tokens(string(definitions)) - messageTokens(req.reminder) - 8
	if budget < 0 {
		return nil, api.Err(413, "CONTEXT_TOO_LARGE", "The system and tools exceed the context window")
	}
	start, costs := len(req.Groups), req.groupCosts()
	for i := len(req.Groups) - 1; i >= 0; i-- {
		cost := costs[i]
		if cost > budget {
			if i == len(req.Groups)-1 {
				return nil, api.Err(413, "CONTEXT_TOO_LARGE", "The latest message exceeds the context window")
			}
			break
		}
		budget -= cost
		start = i
	}
	// A history window starts with a user message; tool result groups stay intact.
	for start < len(req.Groups) {
		if len(req.Groups[start]) > 0 && strings.Contains(string(req.Groups[start][0]), `"role":"user"`) {
			break
		}
		start++
	}
	conversation := []json.RawMessage{}
	if strings.TrimSpace(system) != "" {
		conversation = append(conversation, api.Raw(api.Object{"role": "system", "content": system}))
	}
	for _, group := range req.Groups[start:] {
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
func trimToolHistory(req *Request, messages []json.RawMessage, defs json.RawMessage) ([]json.RawMessage, error) {
	cost := inference.Tokens(string(defs))
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

func preflightBudget(request *Request) error {
	_, err := BudgetConversation(request, request.webSearch, api.Raw(request.rendered))
	return err
}

// Report the full requested history so the UI can show when older turns are omitted.
func requestContextUsage(req *Request) *domain.ContextUsage {
	system := req.System + "\n\n" + req.widgetHint
	if req.webSearch {
		system += "\n\n" + searchInstructions
	}
	used := inference.Tokens(system) + inference.Tokens(string(api.Raw(req.rendered))) + messageTokens(req.reminder) + 8
	for _, cost := range req.groupCosts() {
		used += cost
	}
	limit := req.window * 4 / 5
	return &domain.ContextUsage{UsedTokens: used, LimitTokens: limit, Percentage: float64(used) * 100 / float64(max(1, limit))}
}
