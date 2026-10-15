package main

import (
	"context"
	"encoding/json"
	"io"
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
		case "/catalog":
			io.WriteString(w, `{"glm-5-3": {"repo": "tinfoilsh/confidential-glm5-3-nvfp4", "hosts": ["glm.example"], "context": 1048576},
				"gemma4-31b": {"repo": "tinfoilsh/confidential-gemma4-31b", "hosts": ["gemma.example"], "context": 262144, "vision": true}}`)
			return
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
	h := &harness{gateway: server.URL, controlplane: server.URL}
	if err := h.refreshCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	catalog := h.catalog.get()
	if len(catalog.Models) != 2 || catalog.definition("glm-5-3") == nil || catalog.definition("gemma4-31b") == nil {
		t.Fatalf("wrong available models: %v", catalog.Models)
	}
	if catalog.definition("deepseek-v4-1-flash") != nil || catalog.definition("glm-5-3-flash") != nil {
		t.Fatal("advertised a model the gateway does not offer")
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

func TestHarnessStartsBeforeTheGatewayOffersAModel(t *testing.T) {
	var offered atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/catalog":
			if !offered.Load() {
				io.WriteString(w, `{}`)
				return
			}
			io.WriteString(w, `{"kimi-k3": {"repo": "tinfoilsh/confidential-kimi-k3", "hosts": ["kimi.example"], "context": 262144}}`)
		case "/api/config/models":
			json.NewEncoder(w).Encode([]object{{"modelName": "kimi-k3", "name": "Kimi K3", "repo": "tinfoilsh/confidential-kimi-k3", "type": "chat", "chat": true}})
		case "/api/config/system-prompt":
			json.NewEncoder(w).Encode(object{"systemPrompt": "Test prompt"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	h, _, _ := testChatHarness(t, nil)
	h.gateway, h.controlplane = server.URL, server.URL
	h.offer.models = nil
	h.catalog.set(nil)
	if err := h.refreshCatalog(context.Background()); err != nil {
		t.Fatalf("an empty gateway catalog stopped startup: %v", err)
	}
	var session struct {
		DefaultModel string   `json:"defaultModel"`
		Models       []object `json:"models"`
	}
	response := postRoute(h, "/v1/session", object{}, "")
	if err := json.Unmarshal(response.Body.Bytes(), &session); err != nil || response.Code != 200 || session.DefaultModel != "auto" || len(session.Models) != 0 {
		t.Fatalf("session without offered models: HTTP %d %s", response.Code, response.Body.String())
	}
	offered.Store(true)
	if err := h.refreshCatalog(context.Background()); err != nil || h.catalog.get().definition("kimi-k3") == nil {
		t.Fatalf("a model the gateway now offers was not advertised: %v", err)
	}
}
