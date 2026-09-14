package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

//go:embed widgets.json
var widgetJSON []byte

//go:embed presets.json
var presetJSON []byte

func builtInPresets() []object {
	var presets []object
	json.Unmarshal(presetJSON, &presets)
	return presets
}

type widget struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
	Hint        string          `json:"promptHint"`
	Surface     string          `json:"surface"`
}

func widgetCatalog() []widget {
	var widgets []widget
	if json.Unmarshal(widgetJSON, &widgets) != nil {
		panic("invalid compiled widgets")
	}
	return widgets
}
func (w widget) declaration() json.RawMessage {
	return raw(object{"type": "function", "function": object{"name": w.Name, "description": w.Description, "parameters": w.Schema}})
}

type chatCatalog struct {
	Models                                    []object
	Deprecated                                []object
	Prompt, Rules, WidgetHeader, MemoryPrompt string
	EnabledWidgets                            []string
	Presets                                   []object
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
	client := &http.Client{Timeout: 15 * time.Second}
	var models struct {
		Models []object `json:"models"`
	}
	var prompt object
	if err := fetchConfig(ctx, client, h.controlplane, "/api/config/models", &models); err != nil {
		return err
	}
	if err := fetchConfig(ctx, client, h.controlplane, "/api/config/system-prompt", &prompt); err != nil {
		return err
	}
	c, err := buildCatalog(models.Models, prompt, h.models)
	if err != nil {
		return err
	}
	var memory object
	if err := fetchConfig(ctx, client, h.controlplane, "/api/config/memory-prompt", &memory); err == nil {
		c.MemoryPrompt = str(memory["memoryPrompt"])
	}
	h.catalog.set(c)
	return nil
}
func buildCatalog(config []object, prompt object, models []*model) (*chatCatalog, error) {
	c := &chatCatalog{Prompt: str(prompt["systemPrompt"]), Rules: str(prompt["rules"]), WidgetHeader: str(obj(prompt["genUI"])["header"]), Models: []object{}, Presets: builtInPresets()}
	for _, name := range arr(obj(prompt["genUI"])["enabledWidgets"]) {
		c.EnabledWidgets = append(c.EnabledWidgets, str(name))
	}
	var unpinned []string
	for _, definition := range config {
		if !boolean(definition["chat"], false) || str(definition["type"]) != "chat" {
			continue
		}
		name, repo := str(definition["modelName"]), str(definition["repo"])
		if boolean(definition["deprecated"], false) {
			c.Deprecated = append(c.Deprecated, clone(definition))
			continue
		}
		if !slices.ContainsFunc(pinned, func(p struct{ name, repo string }) bool { return p.name == name && p.repo == repo }) {
			unpinned = append(unpinned, name)
			continue
		}
		for _, model := range models {
			if model.name == name {
				def := clone(definition)
				def["budgetWindow"] = model.context
				c.Models = append(c.Models, def)
				break
			}
		}
	}
	if len(unpinned) > 0 {
		return nil, fmt.Errorf("controlplane chat models have no pinned repository: %s", strings.Join(unpinned, ", "))
	}
	if len(c.Models) == 0 {
		return nil, errors.New("no configured chat model has an attested gateway replica")
	}
	return c, nil
}
func (c *chatCatalog) definition(name string) object {
	for _, m := range c.Models {
		if str(m["modelName"]) == name {
			return m
		}
	}
	return nil
}
func normalizeModel(name string) string {
	if name == "" || name == "auto-smart" || name == "auto-fast" {
		return "auto"
	}
	return name
}

var intelligenceLevels = []object{{"id": "instant", "label": "Instant"}, {"id": "low", "label": "Low"}, {"id": "medium", "label": "Med"}, {"id": "high", "label": "High"}, {"id": "extra", "label": "Extra"}, {"id": "max", "label": "Max"}}

func intelligence(level string) (int, error) {
	if level == "" {
		level = "high"
	}
	for i, l := range intelligenceLevels {
		if l["id"] == level {
			return i * 20, nil
		}
	}
	return 0, apiErr(400, "BAD_REQUEST", "Unknown Auto intelligence level")
}

func (c *chatCatalog) displayModels() []object {
	models := []object{}
	for _, m := range append(slices.Clone(c.Models), c.Deprecated...) {
		display := object{"id": m["modelName"], "reasoning": nil, "deprecationDate": nil}
		for _, field := range []string{"name", "nameShort", "description", "image", "multimodal", "paid", "experimental", "deprecated", "deprecationDate"} {
			if value, ok := m[field]; ok {
				display[field] = value
			}
		}
		cfg := obj(m["chatConfig"])
		display["descriptionShort"], display["attributes"] = cfg["descriptionShort"], cfg["attributes"]
		if reason := obj(cfg["reasoningConfig"]); len(reason) > 0 {
			display["reasoning"] = object{"toggle": boolean(reason["supportsToggle"], false), "effort": boolean(reason["supportsEffort"], false), "defaultEnabled": boolean(reason["defaultEnabled"], true)}
		}
		models = append(models, display)
	}
	return models
}
func (c *chatCatalog) selectedWidgets(requested []any, enabled bool) []widget {
	result := []widget{}
	if !enabled {
		return result
	}
	for _, widget := range widgetCatalog() {
		if slices.Contains(c.EnabledWidgets, widget.Name) && slices.ContainsFunc(requested, func(v any) bool { return str(v) == widget.Name }) {
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
