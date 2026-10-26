package main

import (
	"context"
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

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

type catalogCache struct {
	mu    sync.RWMutex
	value *domain.Catalog
}

func (c *catalogCache) get() *domain.Catalog      { c.mu.RLock(); defer c.mu.RUnlock(); return c.value }
func (c *catalogCache) set(value *domain.Catalog) { c.mu.Lock(); defer c.mu.Unlock(); c.value = value }

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
	var models []api.Object
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
func buildCatalog(config []api.Object, models []*model) *domain.Catalog {
	prompt := domain.BuiltInPrompt()
	c := &domain.Catalog{Prompt: prompt.SystemPrompt, Rules: prompt.Rules, WidgetHeader: prompt.GenUI.Header, EnabledWidgets: slices.Clone(prompt.GenUI.EnabledWidgets), Models: []api.Object{}, Presets: domain.BuiltInPresets()}
	var unpinned []string
	for _, definition := range config {
		if !api.Bool(definition["chat"], false) || api.Str(definition["type"]) != "chat" {
			continue
		}
		name, repo := api.Str(definition["modelName"]), api.Str(definition["repo"])
		c.Listed = append(c.Listed, name)
		if api.Bool(definition["deprecated"], false) {
			c.Deprecated = append(c.Deprecated, definition)
			continue
		}
		configured := api.Int(api.Obj(definition["chatConfig"])["contextWindowTokens"])
		if configured > 0 && (c.Fallback == 0 || configured < c.Fallback) {
			c.Fallback = configured
		}
		if !slices.ContainsFunc(pinned, func(p pin) bool { return p.name == name && p.repo == repo }) {
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
