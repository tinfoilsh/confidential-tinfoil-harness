package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestCatalogRefreshAcceptsControlplaneArray(t *testing.T) {
	definitions := []object{
		{"modelName": "deepseek-v4-1-flash", "repo": "tinfoilsh/confidential-deepseek-v4-1-flash", "type": "chat", "chat": true},
		{"modelName": "glm-5-3", "repo": "tinfoilsh/confidential-glm5-3-nvfp4", "type": "chat", "chat": true},
		{"modelName": "glm-5-3-flash", "repo": "tinfoilsh/confidential-glm5-3-flash", "type": "chat", "chat": true},
		{"modelName": "gemma4-31b", "repo": "tinfoilsh/confidential-gemma4-31b", "type": "chat", "chat": true},
		{"modelName": "deepseek-v4-flash", "repo": "tinfoilsh/confidential-deepseek-v4-flash", "type": "chat", "chat": true, "deprecated": true},
	}
	var mode atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var response any
		switch r.URL.Path {
		case "/api/config/models":
			response = definitions
			switch mode.Load() {
			case 1:
				response = object{"models": definitions}
			case 2:
				changed := clone(definitions[1])
				changed["repo"] = "untrusted/model"
				response = []object{changed}
			case 3:
				response = []object{}
			}
		case "/api/config/system-prompt":
			response = object{"systemPrompt": "Test prompt", "rules": "Test rules", "genUI": object{"enabledWidgets": []string{"render_chart"}}}
		case "/api/config/memory-prompt":
			response = object{"memoryPrompt": "Test memory prompt"}
		default:
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	h := &harness{controlplane: server.URL, models: []*model{
		{name: "glm-5-3", context: 1048576, client: server.Client()},
		{name: "gemma4-31b", context: 262144, client: server.Client()},
	}}
	if err := h.refreshCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	catalog := h.catalog.get()
	if len(catalog.Models) != 2 || catalog.definition("glm-5-3") == nil || catalog.definition("gemma4-31b") == nil {
		t.Fatalf("wrong available models: %v", catalog.Models)
	}
	if catalog.definition("deepseek-v4-1-flash") != nil || catalog.definition("glm-5-3-flash") != nil {
		t.Fatal("advertised a model without an attested replica")
	}
	if integer(catalog.definition("glm-5-3")["budgetWindow"]) != 1048576 || len(catalog.Deprecated) != 1 {
		t.Fatal("lost replica budget or deprecated model metadata")
	}
	if catalog.Prompt != "Test prompt" || catalog.MemoryPrompt != "Test memory prompt" || len(catalog.EnabledWidgets) != 1 {
		t.Fatal("lost prompt configuration")
	}
	for i, name := range []string{"wrong response shape", "untrusted repository", "no active models"} {
		t.Run(name, func(t *testing.T) {
			mode.Store(int32(i + 1))
			if err := h.refreshCatalog(context.Background()); err == nil {
				t.Fatal("invalid catalog accepted")
			}
			if h.catalog.get() != catalog {
				t.Fatal("failed refresh replaced the last valid catalog")
			}
		})
	}
}
