package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// savedPresets are prompts a user saved: one choosing an offered model with
// web search off, one naming a model no longer offered, and one naming a
// model that serves only Auto.
var savedPresets = []any{
	object{"id": "user:fast", "name": "Fast", "systemPrompt": "Be brief.", "model": "gemma4-31b", "webSearchEnabled": false},
	object{"id": "user:gone", "name": "Gone", "model": "retired-model", "webSearchEnabled": false},
	object{"id": "user:free", "name": "Free", "model": "gpt-oss-120b", "webSearchEnabled": false},
}

// presetHarness offers kimi-k3 and gemma4-31b, and serves gpt-oss-120b
// behind Auto alone. It reports the model each model turn asks for.
func presetHarness(t *testing.T) (*harness, *memoryRows, contentKey, chan string) {
	t.Helper()
	asked := make(chan string, 16)
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body object
		json.NewDecoder(r.Body).Decode(&body)
		asked <- str(body["model"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	client := h.offer.models[0].client
	h.offer.models = append(h.offer.models, &model{name: "gemma4-31b", context: 8192, client: client}, &model{name: "gpt-oss-120b", context: 8192, client: client})
	c := h.catalog.get()
	c.Models = append(c.Models, object{"modelName": "gemma4-31b", "name": "Gemma", "budgetWindow": 8192, "chatConfig": object{}},
		object{"modelName": "gpt-oss-120b", "name": "Free", "paid": false, "budgetWindow": 8192, "chatConfig": object{}})
	return h, rows, key, asked
}

func presetProfile() object {
	return object{"customPromptPresets": savedPresets, "defaultPromptPresetId": "user:fast", "selectedModel": "kimi-k3"}
}

// storedChat is a stored chat as the harness reads it back.
func storedChat(t *testing.T, rows *memoryRows, key contentKey, id string) Thread {
	t.Helper()
	row, err := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", id)
	if err != nil {
		t.Fatal(err)
	}
	return readThreadRow(row).Thread
}

func TestANewChatTakesItsPresetsSettingsUnlessItsTurnSetsThem(t *testing.T) {
	h, rows, key, asked := presetHarness(t)
	rows.rows[rowMapKey(&principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: raw(presetProfile())}
	for _, tc := range []struct {
		name      string
		presetID  any // nil leaves the preset to the profile's default
		options   object
		model     string
		webSearch bool
	}{
		{"default preset", nil, object{}, "gemma4-31b", false},
		{"turn's model", nil, object{"model": "kimi-k3"}, "kimi-k3", false},
		{"unoffered model", "user:gone", object{}, "kimi-k3", false},
		{"model behind Auto", "user:free", object{}, "kimi-k3", false},
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
	in["options"], in["profile"] = object{}, presetProfile()
	thread := endedThread(t, h, in, "RUN_FINISHED")
	if thread["model"] != "gemma4-31b" || thread["webSearchEnabled"] != false || thread["presetId"] != "user:fast" || <-asked != "gemma4-31b" {
		t.Fatalf("the device chat started as %v", thread)
	}
}

// A new chat takes the profile's web search setting, which its turn's own
// setting overrides.
func TestANewChatTakesTheProfilesWebSearchSetting(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	for _, tc := range []struct {
		profile bool
		options object
		want    bool
	}{
		{false, object{}, false},
		{true, object{"webSearch": false}, false},
	} {
		rows.rows[rowMapKey(&principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: raw(object{"webSearchEnabled": tc.profile})}
		in := sendInput(key)
		in["options"] = tc.options
		events := finishedRun(t, h, "/v1/threads/turn", in)
		if chat := storedChat(t, rows, key, events[0].ThreadID); chat.WebSearchEnabled != tc.want {
			t.Fatalf("with web search %v in the profile and options %v, the chat started with it %v", tc.profile, tc.options, chat.WebSearchEnabled)
		}
	}
}

// A profile's model that is no longer offered reads as Auto, and a new chat
// starts on Auto rather than failing.
func TestAnUnofferedSelectedModelReadsAsAuto(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	rows.rows[rowMapKey(&principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: raw(object{"selectedModel": "retired-model"})}
	if profile := profileOf(t, postRoute(h, "/v1/profile/get", object{"key": key.base64()}, "alice")); profile["selectedModel"] != autoModelName {
		t.Fatalf("the profile reads with model %v", profile["selectedModel"])
	}
	in := sendInput(key)
	in["options"] = object{"webSearch": false}
	events := finishedRun(t, h, "/v1/threads/turn", in)
	if chat := storedChat(t, rows, key, events[0].ThreadID); chat.Model != autoModelName {
		t.Fatalf("the chat started on %s", chat.Model)
	}
}

func TestChoosingAPresetTakesTheSettingsTheRequestLeavesOut(t *testing.T) {
	h, rows, key, _ := presetHarness(t)
	rows.rows[rowMapKey(&principal{ID: "alice"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: raw(presetProfile())}
	in := sendInput(key)
	in["presetId"] = nil
	id := finishedRun(t, h, "/v1/threads/turn", in)[0].ThreadID
	update := func(patch object) Thread {
		t.Helper()
		patch["key"], patch["id"] = key.base64(), id
		if w := postRoute(h, "/v1/threads/update", patch, "alice"); w.Code != 200 {
			t.Fatalf("update %v: HTTP %d %s", patch, w.Code, w.Body.String())
		}
		return storedChat(t, rows, key, id)
	}
	if chat := update(object{"presetId": "user:fast"}); chat.Model != "gemma4-31b" || chat.WebSearchEnabled || valueOr(chat.PresetID, "") != "user:fast" {
		t.Fatalf("choosing the preset left the chat on %s with web search %v", chat.Model, chat.WebSearchEnabled)
	}
	// Naming the chat's own preset again applies only what the request sets.
	if chat := update(object{"title": "Renamed", "presetId": "user:fast", "model": "kimi-k3"}); chat.Model != "kimi-k3" || chat.Title != "Renamed" {
		t.Fatalf("the update's own model was not taken: %+v", chat)
	}
	update(object{"presetId": nil})
	if chat := update(object{"presetId": "user:fast", "webSearchEnabled": true}); chat.Model != "gemma4-31b" || !chat.WebSearchEnabled {
		t.Fatalf("the update's own web search setting was not taken: %s %v", chat.Model, chat.WebSearchEnabled)
	}
	// A model that serves only Auto is never a chat's own.
	if w := postRoute(h, "/v1/threads/update", object{"key": key.base64(), "id": id, "model": "gpt-oss-120b"}, "alice"); w.Code != 404 || !strings.Contains(w.Body.String(), "MODEL_UNAVAILABLE") {
		t.Fatalf("choosing a model behind Auto: HTTP %d %s", w.Code, w.Body.String())
	}
}
