package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// oldProfile is settings an old client stored: a custom default prompt, an
// earlier Auto tier, and a saved prompt naming a model since retired.
func oldProfile() object {
	return object{"isUsingCustomPrompt": true, "customSystemPrompt": "<system>\nAnswer in French.\n</system>", "selectedModel": "auto-smart", "nickname": "Sam",
		"customPromptPresets": []any{object{"id": "user:old", "name": "Old", "model": "retired-model"}, object{"id": "user:kept", "name": "Kept", "model": "kimi-k3", "webSearchEnabled": false}}}
}

func profileOf(t *testing.T, w interface{ Result() *http.Response }) object {
	t.Helper()
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d %s", resp.StatusCode, body)
	}
	var profile object
	json.Unmarshal(body, &profile)
	if nested, ok := profile["profile"]; ok {
		return obj(nested)
	}
	return profile
}

func presetsByID(profile object) map[string]object {
	presets := map[string]object{}
	for _, preset := range arr(profile["customPromptPresets"]) {
		presets[str(obj(preset)["id"])] = obj(preset)
	}
	return presets
}

func TestAStoredProfileReadsUpgradedAndKeepsTheUpgradeOnceChanged(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &principal{ID: "alice"}
	stored := oldProfile()
	stored["updatedAt"] = "2026-01-02T03:04:05Z"
	rows.rows[rowMapKey(p, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: raw(stored)}
	get := func() object {
		return profileOf(t, postRoute(h, "/v1/profile/get", object{"key": key.base64()}, "alice"))
	}
	profile := get()
	presets := presetsByID(profile)
	migrated := presets[migratedPromptPresetID]
	if profile["defaultPromptPresetId"] != migratedPromptPresetID || profile["isUsingCustomPrompt"] != false || profile["customSystemPrompt"] != "" || profile["selectedModel"] != "auto" ||
		migrated["systemPrompt"] != "<system>\nAnswer in French.\n</system>" || migrated["createdAt"] != float64(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixMilli()) {
		t.Fatalf("the old custom prompt read as %v", profile)
	}
	if _, kept := presets["user:old"]["model"]; kept || presets["user:kept"]["model"] != "kimi-k3" || len(presets) != 3 {
		t.Fatalf("saved prompts read as %v", presets)
	}
	if profile["themeMode"] != "system" || profile["piiCheckEnabled"] != true || profile["nickname"] != "Sam" {
		t.Fatalf("unset settings did not read as their defaults: %v", profile)
	}
	if again := get(); !reflect.DeepEqual(again, profile) {
		t.Fatalf("a second read upgraded differently:\n%v\n%v", again, profile)
	}
	// A profile without a date dates its migrated prompt the same on every read.
	rows.rows[rowMapKey(&principal{ID: "bob"}, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: raw(oldProfile())}
	undated := profileOf(t, postRoute(h, "/v1/profile/get", object{"key": key.base64()}, "bob"))
	if created := presetsByID(undated)[migratedPromptPresetID]["createdAt"]; created != float64(0) {
		t.Fatalf("an undated profile's migrated prompt is dated %v", created)
	}
	// Removing the migrated prompt stores the upgrade, so it does not return.
	w := postRoute(h, "/v1/profile/update", object{"key": key.base64(), "patch": object{"customPromptPresets": []any{presets["user:kept"]}}}, "alice")
	if presets := presetsByID(profileOf(t, w)); len(presets) != 1 {
		t.Fatalf("the update answered with prompts %v", presets)
	}
	if presets := presetsByID(get()); len(presets) != 1 || presets["user:kept"] == nil {
		t.Fatalf("a removed migrated prompt came back: %v", presets)
	}
	row, _ := rows.load(context.Background(), p, key, "profile", "profile")
	if data := rowJSON(row); data["isUsingCustomPrompt"] != false || data["themeMode"] != nil {
		t.Fatalf("the stored profile is %v", data)
	}
}

// A device's settings upgrade as a stored profile reads, and its invalid
// fields are dropped rather than refusing the request.
func TestADeviceProfileUpgradesAsAStoredOneReads(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	var session struct {
		ProfileDefaults object `json:"profileDefaults"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", object{}, "").Body.Bytes(), &session)
	// Thinking left unset follows each model's default, so it has none here.
	if _, thinking := session.ProfileDefaults["thinkingEnabled"]; thinking || session.ProfileDefaults["piiCheckEnabled"] != true || session.ProfileDefaults["selectedModel"] != "auto" {
		t.Fatalf("the session defaults are %v", session.ProfileDefaults)
	}
	if empty := profileOf(t, postRoute(h, "/v1/profile/get", object{"key": key.base64()}, "alice")); !reflect.DeepEqual(empty, session.ProfileDefaults) {
		t.Fatalf("an empty profile reads as\n%v\nnot the session's defaults\n%v", empty, session.ProfileDefaults)
	}
	device := oldProfile()
	device["themeMode"], device["traits"] = "neon", 5
	device["customPromptPresets"] = append(arr(device["customPromptPresets"]), object{"id": "user:unnamed"})
	calls := rows.calls
	upgraded := profileOf(t, postRoute(h, "/v1/profile/upgrade", object{"profile": device}, ""))
	presets := presetsByID(upgraded)
	if upgraded["defaultPromptPresetId"] != migratedPromptPresetID || upgraded["selectedModel"] != "auto" || upgraded["themeMode"] != "system" || len(arr(upgraded["traits"])) != 0 || upgraded["nickname"] != "Sam" {
		t.Fatalf("the device profile upgraded as %v", upgraded)
	}
	if _, kept := presets["user:old"]["model"]; kept || presets["user:unnamed"] != nil || presets[migratedPromptPresetID]["systemPrompt"] != "<system>\nAnswer in French.\n</system>" {
		t.Fatalf("the device's saved prompts upgraded as %v", presets)
	}
	if rows.calls != calls {
		t.Fatalf("the upgrade made %d storage calls", rows.calls-calls)
	}
	if w := postRoute(h, "/v1/profile/upgrade", object{"profile": "none"}, ""); w.Code != 400 {
		t.Fatalf("a profile that is not an object: HTTP %d", w.Code)
	}
}

func TestADeviceTurnDropsInvalidProfileFields(t *testing.T) {
	systems := make(chan string, 1)
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		systems <- str(obj(lastRequest(r)[0])["content"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	in := localInput("", object{"messages": []any{}})
	// A saved prompt without a name is dropped, so a new chat cannot start with it.
	in["profile"] = object{"nickname": "Sam", "themeMode": "neon", "traits": 5, "defaultPromptPresetId": "user:unnamed",
		"customPromptPresets": []any{object{"id": "user:unnamed", "systemPrompt": "Unnamed prompt marker"}}}
	finishedRun(t, h, "/v1/threads/turn", in)
	if system := <-systems; strings.Contains(system, "Unnamed prompt marker") || !strings.Contains(system, "<nickname>Sam</nickname>") {
		t.Fatalf("the device's settings read as %q", system)
	}
}
