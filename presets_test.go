package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// savedPresets are prompts a user saved: one choosing an offered model with
// web search off, and one naming a model no longer offered.
var savedPresets = []any{
	object{"id": "user:fast", "name": "Fast", "systemPrompt": "Be brief.", "model": "gemma4-31b", "webSearchEnabled": false},
	object{"id": "user:gone", "name": "Gone", "model": "retired-model", "webSearchEnabled": false},
}

// presetHarness offers kimi-k3 and gemma4-31b, and reports the model each
// model turn asks for.
func presetHarness(t *testing.T) (*harness, *memoryRows, contentKey, chan string) {
	t.Helper()
	asked := make(chan string, 16)
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body object
		json.NewDecoder(r.Body).Decode(&body)
		asked <- str(body["model"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.offer.models = append(h.offer.models, &model{name: "gemma4-31b", context: 8192, client: h.offer.models[0].client})
	c := h.catalog.get()
	c.Models = append(c.Models, object{"modelName": "gemma4-31b", "name": "Gemma", "budgetWindow": 8192, "chatConfig": object{}})
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
}
