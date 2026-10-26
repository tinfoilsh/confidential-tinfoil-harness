package domain

import (
	"cmp"
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sync"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
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

// BuiltInPrompt is the compiled system prompt, read once.
var BuiltInPrompt = sync.OnceValue(func() chatPrompt {
	var prompt chatPrompt
	if json.Unmarshal(promptJSON, &prompt) != nil || prompt.SystemPrompt == "" {
		panic("invalid compiled system prompt")
	}
	return prompt
})

// Preset is a built-in prompt the clients offer.
type Preset struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	SystemPrompt string `json:"systemPrompt"`
	Icon         string `json:"icon"`
	BuiltIn      bool   `json:"builtIn"`
}

func BuiltInPresets() []Preset {
	var presets []Preset
	json.Unmarshal(presetJSON, &presets)
	return presets
}

type Widget struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
	Hint        string          `json:"promptHint"`

	Resolved *jsonschema.Resolved // what a call's arguments are validated against
}

// WidgetCatalog is the compiled widget declarations, read once.
var WidgetCatalog = sync.OnceValue(func() []Widget {
	var widgets []Widget
	if json.Unmarshal(widgetJSON, &widgets) != nil {
		panic("invalid compiled widgets")
	}
	for i := range widgets {
		resolved, err := resolveSchema(widgets[i].Schema)
		if err != nil {
			panic("invalid compiled widget schema: " + err.Error())
		}
		widgets[i].Resolved = resolved
	}
	return widgets
})

func (w Widget) Declaration() json.RawMessage {
	return api.Raw(api.Object{"type": "function", "function": api.Object{"name": w.Name, "description": w.Description, "parameters": w.Schema}})
}

type Catalog struct {
	Models                      []api.Object
	Deprecated                  []api.Object
	Listed                      []string // every chat model the control plane lists, offered or not
	Prompt, Rules, WidgetHeader string
	EnabledWidgets              []string
	Presets                     []Preset
	Fallback                    int // smallest configured window, for Auto while no model is offered
}

func (c *Catalog) Definition(name string) api.Object {
	for _, m := range c.Models {
		if api.Str(m["modelName"]) == name {
			return m
		}
	}
	return nil
}

// DisplayName is what clients call a configured model, if anything.
func (c *Catalog) DisplayName(name string) string {
	return api.Str(c.Definition(name)["name"])
}

// Vision reports whether a model reads images. The router gives an Auto turn
// that carries images to a multimodal model.
func (c *Catalog) Vision(name string) bool {
	return name == "auto" || api.Bool(c.Definition(name)["multimodal"], false)
}

// Offers reports whether a chat may run on a model now.
func (c *Catalog) Offers(name string) bool {
	if name == AutoModelName {
		return true
	}
	definition := c.Definition(name)
	return definition != nil && !autoOnly(definition)
}

// autoOnly reports whether a model is configured as not paid: it serves free
// keys behind Auto, and no chat chooses it.
func autoOnly(definition api.Object) bool {
	return !api.Bool(definition["paid"], true)
}

// ApplyPreset files a chat under a prompt preset, or none, and takes the
// model and web search setting a saved preset chooses. A model that is not
// offered is not taken.
func (c *Catalog) ApplyPreset(t *Thread, profile *Profile, id *string) {
	t.PresetID = id
	if id == nil {
		return
	}
	saved := profile.Preset(*id)
	if saved == nil {
		return
	}
	if model := ValueOr(saved.Model, ""); model != "" && c.Offers(model) {
		t.Model = model
	}
	t.WebSearchEnabled = ValueOr(saved.WebSearchEnabled, t.WebSearchEnabled)
}

// retired reports whether the control plane no longer lists a chat model.
func (c *Catalog) retired(name string) bool {
	return name != AutoModelName && c.Definition(name) == nil && !slices.Contains(c.Listed, name)
}

// AutoModelName is the model that has the router choose one per turn.
const AutoModelName = "auto"

// legacyAutoModels are the Auto tiers of an earlier design, which old chats
// and settings name.
var legacyAutoModels = []string{"auto-smart", "auto-fast"}

func normalizeModel(name string) string {
	if name == "" || slices.Contains(legacyAutoModels, name) {
		return AutoModelName
	}
	return name
}

var ReasoningEfforts = []string{"low", "medium", "high"}

// IntelligenceLevel is one setting of how hard Auto works, as clients label
// it alone and as the model a chat runs on.
type IntelligenceLevel struct {
	ID           string `json:"id"`
	Label        string `json:"label"`
	DisplayLabel string `json:"displayLabel"`
}

var intelligenceLevels = []IntelligenceLevel{{ID: "instant", Label: "Instant"}, {ID: "low", Label: "Low"}, {ID: "medium", Label: "Med"}, {ID: "high", Label: "High"}, {ID: "extra", Label: "Extra"}, {ID: "max", Label: "Max"}}

// autoLabel is what clients call Auto.
const autoLabel = "Auto"

// AutoLevels are Auto's levels, each labeled as the model a chat runs on.
func AutoLevels() []IntelligenceLevel {
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

func Intelligence(level string) (int, error) {
	if level == "" {
		level = "high"
	}
	for i, l := range intelligenceLevels {
		if l.ID == level {
			return i * 20, nil
		}
	}
	return 0, api.Err(400, "BAD_REQUEST", "Unknown Auto intelligence level")
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

// SessionModel is a chat model as a client offers it, every field ready to
// show. Auto is the entry that has the router choose a model for each turn.
type SessionModel struct {
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
	ExperimentalTooltip   = "Support and availability are not guaranteed. This model can be deprecated at any time."
	deprecatedTooltip     = "This model is deprecated. An offline date has not been announced."
	deprecatedDateTooltip = "This model will be taken offline on %s."
)

// DisplayModels are the models a client offers, Auto first.
func (c *Catalog) DisplayModels() []SessionModel {
	models := []SessionModel{{ID: AutoModelName, Label: autoLabel, ShortLabel: autoLabel, Badges: []lifecycleBadge{}, Auto: true}}
	for _, m := range append(slices.Clone(c.Models), c.Deprecated...) {
		if autoOnly(m) {
			continue
		}
		var config modelConfig
		readLenient(api.Raw(m), &config)
		display := SessionModel{ID: config.ModelName, Label: config.Name, ShortLabel: cmp.Or(config.NameShort, config.Name), Icon: config.Image, Description: config.ChatConfig.DescriptionShort,
			Deprecated: config.Deprecated, Badges: []lifecycleBadge{}}
		if reason := config.ChatConfig.ReasoningConfig; reason != nil {
			display.Reasoning = &reasoningView{Toggle: reason.SupportsToggle, Effort: reason.SupportsEffort, DefaultEnabled: ValueOr(reason.DefaultEnabled, true)}
		}
		if config.Experimental {
			display.Badges = append(display.Badges, lifecycleBadge{"experimental", "Experimental", ExperimentalTooltip})
		}
		if config.Deprecated {
			tooltip := deprecatedTooltip
			if date := ValueOr(config.DeprecationDate, ""); date != "" {
				tooltip = fmt.Sprintf(deprecatedDateTooltip, date)
			}
			display.Badges = append(display.Badges, lifecycleBadge{"deprecated", "Deprecated", tooltip})
		}
		models = append(models, display)
	}
	return models
}
func (c *Catalog) SelectedWidgets(requested []string, enabled bool) []Widget {
	result := []Widget{}
	if !enabled {
		return result
	}
	for _, widget := range WidgetCatalog() {
		if slices.Contains(c.EnabledWidgets, widget.Name) && slices.Contains(requested, widget.Name) {
			result = append(result, widget)
		}
	}
	return result
}

// reasoningSetting is a word a model's reasoning effort is written in. The
// model catalog comes from outside the enclave, and a chat template writes
// these settings into the prompt, so nothing that could be an instruction is
// sent.
var reasoningSetting = regexp.MustCompile(`^[a-z]{1,16}$`)

// ModelParams are the reasoning settings a model's configuration asks for:
// whether it thinks and how hard, and nothing else.
func ModelParams(def api.Object, thinking bool, effort string) api.Object {
	params := api.Object{}
	reason := api.Obj(api.Obj(def["chatConfig"])["reasoningConfig"])
	mode := "enable"
	if api.Bool(reason["supportsToggle"], false) && !thinking {
		mode = "disable"
	}
	if effort == "" || !api.Bool(reason["supportsEffort"], false) {
		effort = "medium"
	}
	effort = api.FirstString(api.Obj(reason["effortMap"])[effort], effort)
	block := api.Obj(substituteEffort(api.Obj(api.Obj(reason["params"])["/v1/chat/completions"])[mode], effort))
	if value, ok := block["reasoning_effort"].(string); ok && reasoningSetting.MatchString(value) {
		params["reasoning_effort"] = value
	}
	switches := api.Object{}
	for k, v := range api.Obj(block["chat_template_kwargs"]) {
		switch value := v.(type) {
		case bool:
			if k == "enable_thinking" || k == "thinking" {
				switches[k] = value
			}
		case string:
			if k == "reasoning_effort" && reasoningSetting.MatchString(value) {
				switches[k] = value
			}
		}
	}
	if len(switches) > 0 {
		params["chat_template_kwargs"] = switches
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
		out := api.Object{}
		for k, item := range v {
			out[k] = substituteEffort(item, effort)
		}
		return out
	case api.Object:
		out := api.Object{}
		for k, item := range v {
			out[k] = substituteEffort(item, effort)
		}
		return out
	}
	return value
}
