package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// savedPresets are prompts a user saved: one choosing an offered model with
// web search off, one naming a model no longer offered, and one naming a
// model that serves only Auto.
var savedPresets = []any{
	api.Object{"id": "user:fast", "name": "Fast", "systemPrompt": "Be brief.", "model": "gemma4-31b", "webSearchEnabled": false},
	api.Object{"id": "user:gone", "name": "Gone", "model": "retired-model", "webSearchEnabled": false},
	api.Object{"id": "user:free", "name": "Free", "model": "gpt-oss-120b", "webSearchEnabled": false},
}

// presetHarness offers kimi-k3 and gemma4-31b, and serves gpt-oss-120b
// behind Auto alone. It reports the model each model turn asks for.
func presetHarness(t *testing.T) (*harness, *memoryRows, auth.ContentKey, chan string) {
	t.Helper()
	asked := make(chan string, 16)
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body api.Object
		json.NewDecoder(r.Body).Decode(&body)
		asked <- api.Str(body["model"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	client := h.offer.models[0].client
	h.offer.models = append(h.offer.models, &model{name: "gemma4-31b", context: 8192, client: client}, &model{name: "gpt-oss-120b", context: 8192, client: client})
	c := h.catalog.get()
	c.Models = append(c.Models, api.Object{"modelName": "gemma4-31b", "name": "Gemma", "budgetWindow": 8192, "chatConfig": api.Object{}},
		api.Object{"modelName": "gpt-oss-120b", "name": "Free", "paid": false, "budgetWindow": 8192, "chatConfig": api.Object{}})
	return h, rows, key, asked
}

func presetProfile() api.Object {
	return api.Object{"customPromptPresets": savedPresets, "defaultPromptPresetId": "user:fast", "selectedModel": "kimi-k3"}
}

// storedChat is a stored chat as the harness reads it back.
func storedChat(t *testing.T, rows *memoryRows, key auth.ContentKey, id string) Thread {
	t.Helper()
	row, err := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", id)
	if err != nil {
		t.Fatal(err)
	}
	return mustRead(readThreadRow(row)).Thread
}

func TestANewChatTakesItsPresetsSettingsUnlessItsTurnSetsThem(t *testing.T) {
	h, rows, key, asked := presetHarness(t)
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: api.Raw(presetProfile())}
	for _, tc := range []struct {
		name      string
		presetID  any // nil leaves the preset to the profile's default
		options   api.Object
		model     string
		webSearch bool
	}{
		{"default preset", nil, api.Object{}, "gemma4-31b", false},
		{"turn's model", nil, api.Object{"model": "kimi-k3"}, "kimi-k3", false},
		{"unoffered model", "user:gone", api.Object{}, "kimi-k3", false},
		{"model behind Auto", "user:free", api.Object{}, "kimi-k3", false},
	} {
		in := sendInput(key)
		in["options"] = tc.options
		if tc.presetID != nil {
			in["presetId"] = tc.presetID
		}
		events := finishedRun(t, h, "/v1/threads/turn", in)
		chat := storedChat(t, rows, key, events[0].ThreadID)
		if chat.Model != tc.model || chat.WebSearchEnabled != tc.webSearch || <-asked != tc.model {
			t.Fatalf("%s: the chat started on %s with web search %v", tc.name, chat.Model, chat.WebSearchEnabled)
		}
	}
	// A device chat takes them from the profile its turn carries.
	in := localInput("", nil)
	in["options"], in["profile"] = api.Object{}, presetProfile()
	thread := endedThread(t, h, in, "RUN_FINISHED")
	if thread["model"] != "gemma4-31b" || thread["webSearchEnabled"] != false || thread["presetId"] != "user:fast" || <-asked != "gemma4-31b" {
		t.Fatalf("the device chat started as %v", thread)
	}
}

// A new chat takes the profile's web search setting, which its turn's own
// setting overrides.
func TestANewChatTakesTheProfilesWebSearchSetting(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	serveWebsearch(t, nil)
	for _, tc := range []struct {
		profile bool
		options api.Object
		want    bool
	}{
		{false, api.Object{}, false},
		{true, api.Object{}, true},
		{true, api.Object{"webSearch": false}, false},
	} {
		rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: api.Raw(api.Object{"webSearchEnabled": tc.profile})}
		in := sendInput(key)
		in["options"] = tc.options
		events := finishedRun(t, h, "/v1/threads/turn", in)
		if chat := storedChat(t, rows, key, events[0].ThreadID); chat.WebSearchEnabled != tc.want {
			t.Fatalf("with web search %v in the profile and options %v, the chat started with it %v", tc.profile, tc.options, chat.WebSearchEnabled)
		}
	}
}

// Turning web search off in the settings keeps each chat's own setting, and
// no turn searches until it is turned back on.
func TestTheWebSearchSettingGatesSearchWithoutChangingChats(t *testing.T) {
	var mu sync.Mutex
	var offered []bool
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body api.Object
		json.NewDecoder(r.Body).Decode(&body)
		searches := false
		for _, tool := range api.Arr(body["tools"]) {
			searches = searches || api.Obj(api.Obj(tool)["function"])["name"] == "web_search"
		}
		mu.Lock()
		offered = append(offered, searches)
		mu.Unlock()
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	})
	serveWebsearch(t, nil)
	for _, available := range []bool{false, true} {
		rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: api.Raw(api.Object{"webSearchAvailable": available})}
		in := sendInput(key)
		api.Obj(in["options"])["webSearch"] = true
		events := finishedRun(t, h, "/v1/threads/turn", in)
		if chat := storedChat(t, rows, key, events[0].ThreadID); !chat.WebSearchEnabled {
			t.Fatalf("with web search available %v, the chat stored its own setting as off", available)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(offered, []bool{false, true}) {
		t.Fatalf("the model was offered web search %v", offered)
	}
}

// A profile's model that is no longer offered reads as Auto, and a new chat
// starts on Auto rather than failing.
func TestAnUnofferedSelectedModelReadsAsAuto(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: api.Raw(api.Object{"selectedModel": "retired-model"})}
	if profile := profileOf(t, postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(key)}, "alice")); profile["selectedModel"] != autoModelName {
		t.Fatalf("the profile reads with model %v", profile["selectedModel"])
	}
	in := sendInput(key)
	in["options"] = api.Object{"webSearch": false}
	events := finishedRun(t, h, "/v1/threads/turn", in)
	if chat := storedChat(t, rows, key, events[0].ThreadID); chat.Model != autoModelName {
		t.Fatalf("the chat started on %s", chat.Model)
	}
}

func TestChoosingAPresetTakesTheSettingsTheRequestLeavesOut(t *testing.T) {
	h, rows, key, _ := presetHarness(t)
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: api.Raw(presetProfile())}
	in := sendInput(key)
	in["presetId"] = nil
	id := finishedRun(t, h, "/v1/threads/turn", in)[0].ThreadID
	update := func(patch api.Object) Thread {
		t.Helper()
		patch["key"], patch["id"] = keyBase64(key), id
		if w := postRoute(h, "/v1/threads/update", patch, "alice"); w.Code != 200 {
			t.Fatalf("update %v: HTTP %d %s", patch, w.Code, w.Body.String())
		}
		return storedChat(t, rows, key, id)
	}
	if chat := update(api.Object{"presetId": "user:fast"}); chat.Model != "gemma4-31b" || chat.WebSearchEnabled || valueOr(chat.PresetID, "") != "user:fast" {
		t.Fatalf("choosing the preset left the chat on %s with web search %v", chat.Model, chat.WebSearchEnabled)
	}
	// Naming the chat's own preset again applies only what the request sets.
	if chat := update(api.Object{"title": "Renamed", "presetId": "user:fast", "model": "kimi-k3"}); chat.Model != "kimi-k3" || chat.Title != "Renamed" {
		t.Fatalf("the update's own model was not taken: %+v", chat)
	}
	update(api.Object{"presetId": nil})
	if chat := update(api.Object{"presetId": "user:fast", "webSearchEnabled": true}); chat.Model != "gemma4-31b" || !chat.WebSearchEnabled {
		t.Fatalf("the update's own web search setting was not taken: %s %v", chat.Model, chat.WebSearchEnabled)
	}
	// A model that serves only Auto is never a chat's own.
	if w := postRoute(h, "/v1/threads/update", api.Object{"key": keyBase64(key), "id": id, "model": "gpt-oss-120b"}, "alice"); w.Code != 404 || !strings.Contains(w.Body.String(), "MODEL_UNAVAILABLE") {
		t.Fatalf("choosing a model behind Auto: HTTP %d %s", w.Code, w.Body.String())
	}
}
