package main

import (
	"cmp"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
)

//go:embed widgets.json
var widgetJSON []byte

//go:embed presets.json
var presetJSON []byte

//go:embed prompt.json
var promptJSON []byte

// chatPrompt is the system prompt every chat is built from. It is part of the
// measured image, so no service outside the enclave can instruct the model.
type chatPrompt struct {
	SystemPrompt string `json:"systemPrompt"`
	Rules        string `json:"rules"`
	GenUI        struct {
		Header         string   `json:"header"`
		EnabledWidgets []string `json:"enabledWidgets"`
	} `json:"genUI"`
}

// builtInPrompt is the compiled system prompt, read once.
var builtInPrompt = sync.OnceValue(func() chatPrompt {
	var prompt chatPrompt
	if json.Unmarshal(promptJSON, &prompt) != nil || prompt.SystemPrompt == "" {
		panic("invalid compiled system prompt")
	}
	return prompt
})

// preset is a built-in prompt the clients offer.
type preset struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	SystemPrompt string `json:"systemPrompt"`
	Icon         string `json:"icon"`
	BuiltIn      bool   `json:"builtIn"`
}

func builtInPresets() []preset {
	var presets []preset
	json.Unmarshal(presetJSON, &presets)
	return presets
}

type widget struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
	Hint        string          `json:"promptHint"`

	schema *jsonschema.Resolved // what a call's arguments are validated against
}

// widgetCatalog is the compiled widget declarations, read once.
var widgetCatalog = sync.OnceValue(func() []widget {
	var widgets []widget
	if json.Unmarshal(widgetJSON, &widgets) != nil {
		panic("invalid compiled widgets")
	}
	for i := range widgets {
		resolved, err := resolveSchema(widgets[i].Schema)
		if err != nil {
			panic("invalid compiled widget schema: " + err.Error())
		}
		widgets[i].schema = resolved
	}
	return widgets
})

func (w widget) declaration() json.RawMessage {
	return raw(object{"type": "function", "function": object{"name": w.Name, "description": w.Description, "parameters": w.Schema}})
}

type chatCatalog struct {
	Models                      []object
	Deprecated                  []object
	Listed                      []string // every chat model the control plane lists, offered or not
	Prompt, Rules, WidgetHeader string
	EnabledWidgets              []string
	Presets                     []preset
	Fallback                    int // smallest configured window, for Auto while no model is offered
}
type catalogCache struct {
	mu    sync.RWMutex
	value *chatCatalog
}

func (c *catalogCache) get() *chatCatalog      { c.mu.RLock(); defer c.mu.RUnlock(); return c.value }
func (c *catalogCache) set(value *chatCatalog) { c.mu.Lock(); defer c.mu.Unlock(); c.value = value }

func fetchConfig(ctx context.Context, client *http.Client, base, path string, out any) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", base+path, nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("config %s answered %d", path, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(out)
}
func (h *harness) refreshCatalog(ctx context.Context) error {
	h.refreshOffer(ctx)
	client := &http.Client{Timeout: 15 * time.Second}
	var models []object
	if err := fetchConfig(ctx, client, h.controlplane, "/api/config/models", &models); err != nil {
		return err
	}
	c := buildCatalog(models, h.offer.get())
	// The first catalog may offer only Auto; a later one without models keeps the last.
	if len(c.Models) == 0 && h.catalog.get() != nil {
		return errors.New("no configured chat model is offered by the gateway")
	}
	h.catalog.set(c)
	return nil
}
func buildCatalog(config []object, models []*model) *chatCatalog {
	prompt := builtInPrompt()
	c := &chatCatalog{Prompt: prompt.SystemPrompt, Rules: prompt.Rules, WidgetHeader: prompt.GenUI.Header, EnabledWidgets: slices.Clone(prompt.GenUI.EnabledWidgets), Models: []object{}, Presets: builtInPresets()}
	var unpinned []string
	for _, definition := range config {
		if !boolean(definition["chat"], false) || str(definition["type"]) != "chat" {
			continue
		}
		name, repo := str(definition["modelName"]), str(definition["repo"])
		c.Listed = append(c.Listed, name)
		if boolean(definition["deprecated"], false) {
			c.Deprecated = append(c.Deprecated, definition)
			continue
		}
		configured := integer(obj(definition["chatConfig"])["contextWindowTokens"])
		if configured > 0 && (c.Fallback == 0 || configured < c.Fallback) {
			c.Fallback = configured
		}
		if !slices.ContainsFunc(pinned, func(p struct{ name, repo string }) bool { return p.name == name && p.repo == repo }) {
			unpinned = append(unpinned, name)
			continue
		}
		for _, model := range models {
			if model.name == name {
				def := maps.Clone(definition)
				// A prompt must fit both the replica and the window the product configures.
				window := model.context
				if configured > 0 && (window <= 0 || configured < window) {
					window = configured
				}
				def["budgetWindow"] = window
				c.Models = append(c.Models, def)
				break
			}
		}
	}
	if len(unpinned) > 0 {
		slog.Warn("skipping controlplane chat models without a pinned repository", "models", strings.Join(unpinned, ", "))
	}
	return c
}
func (c *chatCatalog) definition(name string) object {
	for _, m := range c.Models {
		if str(m["modelName"]) == name {
			return m
		}
	}
	return nil
}

// displayName is what clients call a configured model, if anything.
func (c *chatCatalog) displayName(name string) string {
	return str(c.definition(name)["name"])
}

// vision reports whether a model reads images. The router gives an Auto turn
// that carries images to a multimodal model.
func (c *chatCatalog) vision(name string) bool {
	return name == "auto" || boolean(c.definition(name)["multimodal"], false)
}

// offers reports whether a chat may run on a model now.
func (c *chatCatalog) offers(name string) bool {
	if name == autoModelName {
		return true
	}
	definition := c.definition(name)
	return definition != nil && !autoOnly(definition)
}

// autoOnly reports whether a model is configured as not paid: it serves free
// keys behind Auto, and no chat chooses it.
func autoOnly(definition object) bool {
	return !boolean(definition["paid"], true)
}

// applyPreset files a chat under a prompt preset, or none, and takes the
// model and web search setting a saved preset chooses. A model that is not
// offered is not taken.
func (c *chatCatalog) applyPreset(t *Thread, profile *Profile, id *string) {
	t.PresetID = id
	if id == nil {
		return
	}
	saved := profile.preset(*id)
	if saved == nil {
		return
	}
	if model := valueOr(saved.Model, ""); model != "" && c.offers(model) {
		t.Model = model
	}
	t.WebSearchEnabled = valueOr(saved.WebSearchEnabled, t.WebSearchEnabled)
}

// retired reports whether the control plane no longer lists a chat model.
func (c *chatCatalog) retired(name string) bool {
	return name != autoModelName && c.definition(name) == nil && !slices.Contains(c.Listed, name)
}

// autoModelName is the model that has the router choose one per turn.
const autoModelName = "auto"

// legacyAutoModels are the Auto tiers of an earlier design, which old chats
// and settings name.
var legacyAutoModels = []string{"auto-smart", "auto-fast"}

func normalizeModel(name string) string {
	if name == "" || slices.Contains(legacyAutoModels, name) {
		return autoModelName
	}
	return name
}

var reasoningEfforts = []string{"low", "medium", "high"}

// intelligenceLevel is one setting of how hard Auto works, as clients label
// it alone and as the model a chat runs on.
type intelligenceLevel struct {
	ID           string `json:"id"`
	Label        string `json:"label"`
	DisplayLabel string `json:"displayLabel"`
}

var intelligenceLevels = []intelligenceLevel{{ID: "instant", Label: "Instant"}, {ID: "low", Label: "Low"}, {ID: "medium", Label: "Med"}, {ID: "high", Label: "High"}, {ID: "extra", Label: "Extra"}, {ID: "max", Label: "Max"}}

// autoLabel is what clients call Auto.
const autoLabel = "Auto"

// autoLevels are Auto's levels, each labeled as the model a chat runs on.
func autoLevels() []intelligenceLevel {
	levels := slices.Clone(intelligenceLevels)
	for i := range levels {
		levels[i].DisplayLabel = autoLabel + " · " + levels[i].Label
	}
	return levels
}

func intelligenceIDs() []string {
	ids := []string{}
	for _, l := range intelligenceLevels {
		ids = append(ids, l.ID)
	}
	return ids
}

func intelligence(level string) (int, error) {
	if level == "" {
		level = "high"
	}
	for i, l := range intelligenceLevels {
		if l.ID == level {
			return i * 20, nil
		}
	}
	return 0, apiErr(400, "BAD_REQUEST", "Unknown Auto intelligence level")
}

// modelConfig is what a client shows of a model the control plane configures.
type modelConfig struct {
	ModelName       string  `json:"modelName"`
	Name            string  `json:"name"`
	NameShort       string  `json:"nameShort"`
	Image           string  `json:"image"`
	Experimental    bool    `json:"experimental"`
	Deprecated      bool    `json:"deprecated"`
	DeprecationDate *string `json:"deprecationDate"`
	ChatConfig      struct {
		DescriptionShort string `json:"descriptionShort"`
		ReasoningConfig  *struct {
			SupportsToggle bool  `json:"supportsToggle"`
			SupportsEffort bool  `json:"supportsEffort"`
			DefaultEnabled *bool `json:"defaultEnabled"`
		} `json:"reasoningConfig"`
	} `json:"chatConfig"`
}

// sessionModel is a chat model as a client offers it, every field ready to
// show. Auto is the entry that has the router choose a model for each turn.
type sessionModel struct {
	ID          string           `json:"id"`
	Label       string           `json:"label"`
	ShortLabel  string           `json:"shortLabel"`
	Icon        string           `json:"icon"`
	Description string           `json:"description"`
	Badges      []lifecycleBadge `json:"badges"`
	Reasoning   *reasoningView   `json:"reasoning"`
	Deprecated  bool             `json:"deprecated"`
	Auto        bool             `json:"auto,omitempty"`
}

type reasoningView struct {
	Toggle         bool `json:"toggle"`
	Effort         bool `json:"effort"`
	DefaultEnabled bool `json:"defaultEnabled"`
}

// lifecycleBadge marks a model that may go away.
type lifecycleBadge struct {
	Kind    string `json:"kind"`
	Label   string `json:"label"`
	Tooltip string `json:"tooltip"`
}

const (
	experimentalTooltip   = "Support and availability are not guaranteed. This model can be deprecated at any time."
	deprecatedTooltip     = "This model is deprecated. An offline date has not been announced."
	deprecatedDateTooltip = "This model will be taken offline on %s."
)

// displayModels are the models a client offers, Auto first.
func (c *chatCatalog) displayModels() []sessionModel {
	models := []sessionModel{{ID: autoModelName, Label: autoLabel, ShortLabel: autoLabel, Badges: []lifecycleBadge{}, Auto: true}}
	for _, m := range append(slices.Clone(c.Models), c.Deprecated...) {
		if autoOnly(m) {
			continue
		}
		var config modelConfig
		readLenient(raw(m), &config)
		display := sessionModel{ID: config.ModelName, Label: config.Name, ShortLabel: cmp.Or(config.NameShort, config.Name), Icon: config.Image, Description: config.ChatConfig.DescriptionShort,
			Deprecated: config.Deprecated, Badges: []lifecycleBadge{}}
		if reason := config.ChatConfig.ReasoningConfig; reason != nil {
			display.Reasoning = &reasoningView{Toggle: reason.SupportsToggle, Effort: reason.SupportsEffort, DefaultEnabled: valueOr(reason.DefaultEnabled, true)}
		}
		if config.Experimental {
			display.Badges = append(display.Badges, lifecycleBadge{"experimental", "Experimental", experimentalTooltip})
		}
		if config.Deprecated {
			tooltip := deprecatedTooltip
			if date := valueOr(config.DeprecationDate, ""); date != "" {
				tooltip = fmt.Sprintf(deprecatedDateTooltip, date)
			}
			display.Badges = append(display.Badges, lifecycleBadge{"deprecated", "Deprecated", tooltip})
		}
		models = append(models, display)
	}
	return models
}
func (c *chatCatalog) selectedWidgets(requested []string, enabled bool) []widget {
	result := []widget{}
	if !enabled {
		return result
	}
	for _, widget := range widgetCatalog() {
		if slices.Contains(c.EnabledWidgets, widget.Name) && slices.Contains(requested, widget.Name) {
			result = append(result, widget)
		}
	}
	return result
}

func modelParams(def object, thinking bool, effort string) object {
	reserved := []string{"model", "messages", "stream", "stream_options", "signal", "reasoning_effort", "web_search_options", "code_execution_options", "pii_check_options", "tools", "tool_choice", "response_format", "user_cache_secret", "auto_model_options"}
	params := object{}
	reason := obj(obj(def["chatConfig"])["reasoningConfig"])
	mode := "enable"
	if boolean(reason["supportsToggle"], false) && !thinking {
		mode = "disable"
	}
	if effort == "" || !boolean(reason["supportsEffort"], false) {
		effort = "medium"
	}
	effort = firstString(obj(reason["effortMap"])[effort], effort)
	block := obj(obj(reason["params"])["/v1/chat/completions"])[mode]
	for k, v := range obj(substituteEffort(block, effort)) {
		if k == "reasoning_effort" || !slices.Contains(reserved, k) {
			params[k] = v
		}
	}
	for k, v := range obj(def["requestParams"]) {
		if !slices.Contains(reserved, k) {
			params[k] = v
		}
	}
	return params
}
func substituteEffort(value any, effort string) any {
	switch v := value.(type) {
	case string:
		if v == "$EFFORT" {
			return effort
		}
		return v
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = substituteEffort(item, effort)
		}
		return out
	case map[string]any:
		out := object{}
		for k, item := range v {
			out[k] = substituteEffort(item, effort)
		}
		return out
	case object:
		out := object{}
		for k, item := range v {
			out[k] = substituteEffort(item, effort)
		}
		return out
	}
	return value
}
