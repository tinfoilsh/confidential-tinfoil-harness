package main

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
)

func TestCatalogRefreshAcceptsControlplaneArray(t *testing.T) {
	definitions := []object{
		{"modelName": "deepseek-v4-1-flash", "repo": "tinfoilsh/confidential-deepseek-v4-1-flash", "type": "chat", "chat": true},
		{"modelName": "glm-5-3", "repo": "tinfoilsh/confidential-glm5-3-nvfp4", "type": "chat", "chat": true, "chatConfig": object{"contextWindowTokens": 524288}},
		{"modelName": "glm-5-3-flash", "repo": "tinfoilsh/confidential-glm5-3-flash", "type": "chat", "chat": true},
		{"modelName": "gemma4-31b", "repo": "tinfoilsh/confidential-gemma4-31b", "type": "chat", "chat": true, "chatConfig": object{"contextWindowTokens": 1048576}},
		{"modelName": "llama3-3-70b", "repo": "tinfoilsh/confidential-llama3-3-70b", "type": "chat", "chat": true},
		{"modelName": "new-model", "repo": "tinfoilsh/confidential-new-model", "type": "chat", "chat": true, "chatConfig": object{"contextWindowTokens": 65536}},
		{"modelName": "deepseek-v4-flash", "repo": "tinfoilsh/confidential-deepseek-v4-flash", "type": "chat", "chat": true, "deprecated": true},
	}
	var mode atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var response any
		switch r.URL.Path {
		case "/catalog":
			io.WriteString(w, `{"glm-5-3": {"repo": "tinfoilsh/confidential-glm5-3-nvfp4", "hosts": ["glm.example"], "context": 1048576},
				"gemma4-31b": {"repo": "tinfoilsh/confidential-gemma4-31b", "hosts": ["gemma.example"], "context": 262144, "vision": true},
				"llama3-3-70b": {"repo": "tinfoilsh/confidential-llama3-3-70b", "hosts": ["llama.example"], "context": 131072}}`)
			return
		case "/api/config/models":
			response = definitions
			switch mode.Load() {
			case 1:
				response = object{"models": definitions}
			case 2:
				changed := maps.Clone(definitions[1])
				changed["repo"] = "untrusted/model"
				response = []object{changed}
			case 3:
				response = []object{}
			}
		case "/api/config/system-prompt":
			response = object{"systemPrompt": "Test prompt", "rules": "Test rules", "genUI": object{"enabledWidgets": []string{"render_chart"}}}
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
	if len(catalog.Models) != 3 || catalog.definition("glm-5-3") == nil || catalog.definition("gemma4-31b") == nil || catalog.definition("llama3-3-70b") == nil {
		t.Fatalf("wrong available models: %v", catalog.Models)
	}
	if catalog.definition("deepseek-v4-1-flash") != nil || catalog.definition("glm-5-3-flash") != nil || catalog.definition("new-model") != nil {
		t.Fatal("advertised a model the gateway does not offer or the harness does not pin")
	}
	for name, want := range map[string]int{"glm-5-3": 524288, "gemma4-31b": 262144, "llama3-3-70b": 131072} {
		if got := integer(catalog.definition(name)["budgetWindow"]); got != want {
			t.Errorf("%s budget = %d, want the smaller of the replica and configured windows, %d", name, got, want)
		}
	}
	if catalog.Fallback != 65536 || len(catalog.Deprecated) != 1 {
		t.Fatal("lost the configured fallback window or deprecated model metadata")
	}
	if catalog.Prompt != "Test prompt" || len(catalog.EnabledWidgets) != 1 {
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
		case "/v1/chat/completions":
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
		case "/api/config/models":
			json.NewEncoder(w).Encode([]object{{"modelName": "kimi-k3", "name": "Kimi K3", "repo": "tinfoilsh/confidential-kimi-k3", "type": "chat", "chat": true, "chatConfig": object{"contextWindowTokens": 8192}}})
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
	turn := localInput("", object{})
	turn["options"] = object{"model": "auto", "webSearch": false}
	if events := eventsIn(t, postRoute(h, "/v1/threads/turn", turn, "")); events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatalf("Auto did not run within the configured window: %+v", events[len(events)-1])
	}
	offered.Store(true)
	if err := h.refreshCatalog(context.Background()); err != nil || h.catalog.get().definition("kimi-k3") == nil {
		t.Fatalf("a model the gateway now offers was not advertised: %v", err)
	}
}

func TestSessionModelsAreReadyToDisplay(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	c := h.catalog.get()
	c.Models = []object{
		{"modelName": "kimi-k3", "name": "Kimi K3", "paid": true, "experimental": true, "chatConfig": object{"reasoningConfig": object{"supportsToggle": true}}},
		{"modelName": "gpt-oss-120b", "name": "Free", "paid": false},
	}
	c.Deprecated = []object{{"modelName": "old", "name": "Old", "nameShort": "O", "deprecated": true, "deprecationDate": "2026-12-01"}}
	var session struct {
		Models []sessionModel `json:"models"`
		Auto   autoView       `json:"auto"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", object{}, "").Body.Bytes(), &session)
	if len(session.Models) != 2 || session.Models[0].ID != "kimi-k3" || session.Models[1].ID != "old" {
		t.Fatalf("the session offers %+v", session.Models)
	}
	kimi, old := session.Models[0], session.Models[1]
	if kimi.NameShort != "Kimi K3" || kimi.Attributes == nil || kimi.Reasoning == nil || !kimi.Reasoning.Toggle || !kimi.Reasoning.DefaultEnabled || len(kimi.Badges) != 1 || kimi.Badges[0].Kind != "experimental" {
		t.Fatalf("kimi-k3 is offered as %+v", kimi)
	}
	if old.NameShort != "O" || len(old.Badges) != 1 || old.Badges[0].Tooltip != "This model will be taken offline on 2026-12-01." {
		t.Fatalf("the deprecated model is offered as %+v", old)
	}
	if level := session.Auto.Levels[slices.IndexFunc(session.Auto.Levels, func(l intelligenceLevel) bool { return l.ID == "high" })]; level.Label != "High" || level.DisplayLabel != "Auto · High" {
		t.Fatalf("Auto's high level reads %+v", level)
	}
}
