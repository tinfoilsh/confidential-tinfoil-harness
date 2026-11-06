package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// oldProfile is settings an old client stored: a custom default prompt, an
// earlier Auto tier, and a saved prompt naming a model since retired.
func oldProfile() api.Object {
	return api.Object{"isUsingCustomPrompt": true, "customSystemPrompt": "<system>\nAnswer in French.\n</system>", "selectedModel": "auto-smart", "nickname": "Sam",
		"customPromptPresets": []any{api.Object{"id": "user:old", "name": "Old", "model": "retired-model"}, api.Object{"id": "user:kept", "name": "Kept", "model": "kimi-k3", "webSearchEnabled": false}}}
}

func profileOf(t *testing.T, w interface{ Result() *http.Response }) api.Object {
	t.Helper()
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP %d %s", resp.StatusCode, body)
	}
	var profile api.Object
	json.Unmarshal(body, &profile)
	if nested, ok := profile["profile"]; ok {
		return api.Obj(nested)
	}
	return profile
}

func presetsByID(profile api.Object) map[string]api.Object {
	presets := map[string]api.Object{}
	for _, preset := range api.Arr(profile["customPromptPresets"]) {
		presets[api.Str(api.Obj(preset)["id"])] = api.Obj(preset)
	}
	return presets
}

func TestAStoredProfileReadsUpgradedAndKeepsTheUpgradeOnceChanged(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	stored := oldProfile()
	stored["updatedAt"] = "2026-01-02T03:04:05Z"
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(stored)}
	get := func() api.Object {
		return profileOf(t, postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(key)}, "alice"))
	}
	profile := get()
	presets := presetsByID(profile)
	migrated := presets[domain.MigratedPromptPresetID]
	if profile["defaultPromptPresetId"] != domain.MigratedPromptPresetID || profile["isUsingCustomPrompt"] != false || profile["customSystemPrompt"] != "" || profile["selectedModel"] != "auto" ||
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
	rows.rows[rowMapKey(&auth.Principal{ID: "bob"}, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(oldProfile())}
	undated := profileOf(t, postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(key)}, "bob"))
	if created := presetsByID(undated)[domain.MigratedPromptPresetID]["createdAt"]; created != float64(0) {
		t.Fatalf("an undated profile's migrated prompt is dated %v", created)
	}
	// Removing the migrated prompt stores the upgrade, so it does not return.
	w := postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "patch": api.Object{"customPromptPresets": []any{presets["user:kept"]}}}, "alice")
	if presets := presetsByID(profileOf(t, w)); len(presets) != 1 {
		t.Fatalf("the update answered with prompts %v", presets)
	}
	if presets := presetsByID(get()); len(presets) != 1 || presets["user:kept"] == nil {
		t.Fatalf("a removed migrated prompt came back: %v", presets)
	}
	row, _ := rows.Load(context.Background(), p, key, "profile", "profile")
	if data := rowJSON(row); data["isUsingCustomPrompt"] != false || data["themeMode"] != nil {
		t.Fatalf("the stored profile is %v", data)
	}
}

// The apps' own profile sync keeps members of its own in the profile row and
// merges it field by field by edit clocks it trusts only while clockVersion
// is the row's revision. A change through the harness keeps those members
// and advances the clock of each field it changes, and only those.
func TestAProfileChangeKeepsTheAppsMembersAndAdvancesTheirClocks(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	stored := api.Object{"nickname": "Sam", "isDarkMode": true, "projectUploadPreference": "ask", "version": 4, "clockVersion": 4,
		"fieldClocks": api.Object{"nickname": api.Object{"v": 7, "w": "web"}, "isDarkMode": api.Object{"v": 9, "w": "ios"}}}
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "4", Data: api.Raw(stored)}
	if w := postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "patch": api.Object{"profession": "Pilot"}}, "alice"); w.Code != 200 {
		t.Fatalf("HTTP %d %s", w.Code, w.Body)
	}
	row, _ := rows.Load(context.Background(), p, key, "profile", "profile")
	data := rowJSON(row)
	if data["isDarkMode"] != true || data["projectUploadPreference"] != "ask" || data["nickname"] != "Sam" || data["profession"] != "Pilot" {
		t.Fatalf("the stored profile is %v", data)
	}
	clocks := api.Obj(data["fieldClocks"])
	want := api.Object{"nickname": map[string]any{"v": float64(7), "w": "web"}, "isDarkMode": map[string]any{"v": float64(9), "w": "ios"}, "profession": map[string]any{"v": float64(10), "w": "harness"}}
	if !reflect.DeepEqual(clocks, want) {
		t.Fatalf("the stored clocks are %v", clocks)
	}
	if revision := strconv.Itoa(api.Int(data["clockVersion"])); revision != row.ETag || data["version"] != data["clockVersion"] {
		t.Fatalf("clocks marked current at %v, version %v, for a row at revision %s", data["clockVersion"], data["version"], row.ETag)
	}
}

// The apps' own profile sync refuses a whole profile when a saved prompt
// lacks its text or numeric dates, so every stored prompt has them.
func TestASavedPromptIsStoredWithWhatTheAppsRequire(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	patch := api.Object{"customPromptPresets": []any{api.Object{"id": "user:bare", "name": "Bare"},
		api.Object{"id": "user:dated", "name": "Dated", "systemPrompt": "Be brief", "createdAt": 1000, "updatedAt": "yesterday"}}}
	if w := postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "patch": patch}, "alice"); w.Code != 200 {
		t.Fatalf("HTTP %d %s", w.Code, w.Body)
	}
	row, _ := rows.Load(context.Background(), p, key, "profile", "profile")
	presets := presetsByID(rowJSON(row))
	bare, dated := presets["user:bare"], presets["user:dated"]
	if created, _ := bare["createdAt"].(float64); bare["description"] != "" || bare["systemPrompt"] != "" || created <= 0 || bare["updatedAt"] != bare["createdAt"] {
		t.Fatalf("a prompt saved with only a name is stored as %v", bare)
	}
	if dated["systemPrompt"] != "Be brief" || dated["createdAt"] != float64(1000) || dated["updatedAt"] != float64(1000) {
		t.Fatalf("a prompt saved with a date that is not a number is stored as %v", dated)
	}
	// A device's copy that leaves the text unset is the same prompt, not a
	// conflicting one.
	device := api.Object{"customPromptPresets": []any{api.Object{"id": "user:bare", "name": "Bare"}}}
	if merged := presetsByID(profileOf(t, postRoute(h, "/v1/profile/merge", api.Object{"key": keyBase64(key), "profile": device}, "alice"))); len(merged) != 2 {
		t.Fatalf("the device's unchanged prompt merged as %v", merged)
	}
}

// Saved prompts and favorites change one at a time on the profile as stored,
// so a client that read the profile before another device changed it, or has
// not read it yet, cannot drop that device's prompts.
func TestPromptEditsApplyToTheStoredProfile(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	var session struct {
		Features map[string]bool `json:"features"`
	}
	if json.Unmarshal(postRoute(h, "/v1/session", api.Object{}, "alice").Body.Bytes(), &session); !session.Features["promptPresetEdits"] {
		t.Fatalf("the session does not offer prompt edits: %v", session.Features)
	}
	p := &auth.Principal{ID: "alice"}
	stored := api.Object{"customPromptPresets": []any{
		api.Object{"id": "user:a", "name": "A", "systemPrompt": "first", "createdAt": 1, "updatedAt": 1},
		api.Object{"id": "user:b", "name": "B", "systemPrompt": "second", "createdAt": 2, "updatedAt": 2}},
		"favoritePromptPresetIds": []any{"user:b", "student"}, "defaultPromptPresetId": "user:b"}
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(stored)}
	edit := func(edits api.Object) api.Object {
		edits["key"] = keyBase64(key)
		return profileOf(t, postRoute(h, "/v1/profile/update", edits, "alice"))
	}
	ids := func(profile api.Object) (ids []string) {
		for _, preset := range api.Arr(profile["customPromptPresets"]) {
			ids = append(ids, api.Str(api.Obj(preset)["id"]))
		}
		return ids
	}
	profile := edit(api.Object{"savePromptPresets": []any{api.Object{"id": "user:c", "name": "C"}, api.Object{"id": "user:a", "name": "A2", "systemPrompt": "edited"}},
		"addFavoritePromptPresetIds": []any{"user:c", "student"}})
	if got := ids(profile); !reflect.DeepEqual(got, []string{"user:a", "user:b", "user:c"}) || presetsByID(profile)["user:a"]["systemPrompt"] != "edited" {
		t.Fatalf("saving a new and an edited prompt left %v", profile["customPromptPresets"])
	}
	if favorites := profile["favoritePromptPresetIds"]; !reflect.DeepEqual(favorites, []any{"user:b", "student", "user:c"}) {
		t.Fatalf("adding favorites left %v", favorites)
	}
	profile = edit(api.Object{"deletePromptPresetIds": []any{"user:b"}, "removeFavoritePromptPresetIds": []any{"student"}})
	row, _ := rows.Load(context.Background(), p, key, "profile", "profile")
	if got := ids(profile); !reflect.DeepEqual(got, []string{"user:a", "user:c"}) || !reflect.DeepEqual(profile["favoritePromptPresetIds"], []any{"user:c"}) || rowJSON(row)["defaultPromptPresetId"] != "" {
		t.Fatalf("removing the default prompt left %v and stored %v", profile, rowJSON(row))
	}
	// An emptied list is stored empty, which the apps' own profile sync reads
	// as cleared rather than as not synced yet.
	edit(api.Object{"deletePromptPresetIds": []any{"user:a", "user:c"}})
	row, _ = rows.Load(context.Background(), p, key, "profile", "profile")
	if presets, stored := rowJSON(row)["customPromptPresets"]; !stored || len(api.Arr(presets)) != 0 {
		t.Fatalf("removing every prompt stored %v", rowJSON(row))
	}
	if w := postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "savePromptPresets": []any{api.Object{"id": "user:d"}}}, "alice"); w.Code != 400 {
		t.Fatalf("a prompt without a name: HTTP %d", w.Code)
	}
}

// An update changes only the members it names, so a client that read a
// prompt before another device set its model and web search does not drop
// them by renaming it, and a null clears a setting.
func TestAPromptUpdateChangesOnlyWhatItNames(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	var session struct {
		Features map[string]bool `json:"features"`
	}
	if json.Unmarshal(postRoute(h, "/v1/session", api.Object{}, "alice").Body.Bytes(), &session); !session.Features["promptPresetUpdates"] {
		t.Fatalf("the session does not offer prompt updates: %v", session.Features)
	}
	p := &auth.Principal{ID: "alice"}
	stored := api.Object{"customPromptPresets": []any{api.Object{"id": "user:a", "name": "A", "systemPrompt": "first", "model": "glm-5-3", "webSearchEnabled": false, "createdAt": 1, "updatedAt": 1}}}
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(stored)}
	update := func(fields api.Object) api.Object {
		profileOf(t, postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "updatePromptPresets": []any{fields}}, "alice"))
		row, _ := rows.Load(context.Background(), p, key, "profile", "profile")
		return presetsByID(rowJSON(row))["user:a"]
	}
	if renamed := update(api.Object{"id": "user:a", "name": "A2", "updatedAt": 5}); renamed["name"] != "A2" || renamed["systemPrompt"] != "first" ||
		renamed["model"] != "glm-5-3" || renamed["webSearchEnabled"] != false || renamed["updatedAt"] != float64(5) {
		t.Fatalf("renaming the prompt stored %v", renamed)
	}
	if cleared := update(api.Object{"id": "user:a", "model": nil, "webSearchEnabled": true}); cleared["model"] != nil || cleared["webSearchEnabled"] != true || cleared["name"] != "A2" {
		t.Fatalf("clearing the model and turning web search on stored %v", cleared)
	}
	// A prompt another device deleted stays deleted.
	if profile := profileOf(t, postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "updatePromptPresets": []any{api.Object{"id": "user:gone", "name": "Back"}}}, "alice")); presetsByID(profile)["user:gone"] != nil {
		t.Fatalf("an update brought back a deleted prompt: %v", profile["customPromptPresets"])
	}
	for _, refused := range []api.Object{{"id": "user:a", "name": ""}, {"id": "user:a", "model": 3}} {
		if w := postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "updatePromptPresets": []any{refused}}, "alice"); w.Code != 400 {
			t.Fatalf("the update %v: HTTP %d", refused, w.Code)
		}
		row, _ := rows.Load(context.Background(), p, key, "profile", "profile")
		if kept := presetsByID(rowJSON(row))["user:a"]; kept["name"] != "A2" || kept["webSearchEnabled"] != true {
			t.Fatalf("the refused update %v stored %v", refused, kept)
		}
	}
}

// A saved prompt naming an earlier design's Auto tier, as an older client may
// still write one, reads as Auto rather than as a retired model.
func TestASavedPromptOnAnEarlierAutoTierReadsAsAuto(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	stored := api.Object{"recordVersion": domain.RecordVersion, "customPromptPresets": []any{api.Object{"id": "user:a", "name": "A", "model": "auto-smart", "createdAt": 1, "updatedAt": 1}}}
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(stored)}
	if preset := presetsByID(profileOf(t, postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(key)}, "alice")))["user:a"]; preset["model"] != domain.AutoModelName {
		t.Fatalf("the prompt read as %v", preset)
	}
}

// A device's settings upgrade as a stored profile reads, and its invalid
// fields are dropped rather than refusing the request.
func TestADeviceProfileUpgradesAsAStoredOneReads(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	var session struct {
		ProfileDefaults api.Object `json:"profileDefaults"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", api.Object{}, "").Body.Bytes(), &session)
	// Thinking left unset follows each model's default, so it has none here.
	if _, thinking := session.ProfileDefaults["thinkingEnabled"]; thinking || session.ProfileDefaults["piiCheckEnabled"] != true || session.ProfileDefaults["selectedModel"] != "auto" {
		t.Fatalf("the session defaults are %v", session.ProfileDefaults)
	}
	if empty := profileOf(t, postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(key)}, "alice")); !reflect.DeepEqual(empty, session.ProfileDefaults) {
		t.Fatalf("an empty profile reads as\n%v\nnot the session's defaults\n%v", empty, session.ProfileDefaults)
	}
	device := oldProfile()
	device["themeMode"], device["traits"] = "neon", 5
	device["customPromptPresets"] = append(api.Arr(device["customPromptPresets"]), api.Object{"id": "user:unnamed"})
	calls := rows.calls
	upgraded := profileOf(t, postRoute(h, "/v1/profile/upgrade", api.Object{"profile": device}, ""))
	presets := presetsByID(upgraded)
	if upgraded["defaultPromptPresetId"] != domain.MigratedPromptPresetID || upgraded["selectedModel"] != "auto" || upgraded["themeMode"] != "system" || len(api.Arr(upgraded["traits"])) != 0 || upgraded["nickname"] != "Sam" {
		t.Fatalf("the device profile upgraded as %v", upgraded)
	}
	if _, kept := presets["user:old"]["model"]; kept || presets["user:unnamed"] != nil || presets[domain.MigratedPromptPresetID]["systemPrompt"] != "<system>\nAnswer in French.\n</system>" {
		t.Fatalf("the device's saved prompts upgraded as %v", presets)
	}
	if rows.calls != calls {
		t.Fatalf("the upgrade made %d storage calls", rows.calls-calls)
	}
	if w := postRoute(h, "/v1/profile/upgrade", api.Object{"profile": "none"}, ""); w.Code != 400 {
		t.Fatalf("a profile that is not an object: HTTP %d", w.Code)
	}
}

func TestADeviceTurnDropsInvalidProfileFields(t *testing.T) {
	systems := make(chan string, 1)
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		systems <- api.Str(api.Obj(lastRequest(r)[0])["content"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	in := localInput("", api.Object{"messages": []any{}})
	// A saved prompt without a name is dropped, so a new chat cannot start with it.
	in["profile"] = api.Object{"nickname": "Sam", "themeMode": "neon", "traits": 5, "defaultPromptPresetId": "user:unnamed",
		"customPromptPresets": []any{api.Object{"id": "user:unnamed", "systemPrompt": "Unnamed prompt marker"}}}
	finishedRun(t, h, "/v1/threads/turn", in)
	if system := <-systems; strings.Contains(system, "Unnamed prompt marker") || !strings.Contains(system, "<nickname>Sam</nickname>") {
		t.Fatalf("the device's settings read as %q", system)
	}
}

// Turning web search off in an old client's settings stays off, apart from
// whether a chat searches.
func TestTheWebSearchSettingOfAnOldProfileIsKept(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(api.Object{"webSearchAvailable": false, "webSearchEnabled": true})}
	get := func(user string) api.Object {
		return profileOf(t, postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(key)}, user))
	}
	if got := get("alice"); got["webSearchAvailable"] != false || got["webSearchEnabled"] != true {
		t.Fatalf("the old profile read as %v", got)
	}
	if got := get("bob"); got["webSearchAvailable"] != true {
		t.Fatalf("an unset web search setting read as %v", got["webSearchAvailable"])
	}
	w := postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "patch": api.Object{"webSearchAvailable": true}}, "alice")
	row, _ := rows.Load(context.Background(), p, key, "profile", "profile")
	if got := profileOf(t, w); got["webSearchAvailable"] != true || rowJSON(row)["webSearchAvailable"] != true {
		t.Fatalf("the update answered %v and stored %v", got, rowJSON(row))
	}
}

// Starting fresh on another device registers a new key and removes the
// profile. A client still holding the old key learns so when it reads the
// profile, rather than being told its key opens the account and having
// every write refused after.
func TestReadingNoProfileRefusesAKeyReplacedOnAnotherDevice(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	replacement := randomKey()
	if w := postRoute(h, "/v1/keys/register", api.Object{"key": keyBase64(replacement), "if_match": "*", "created_via": "start_fresh", "idempotency_key": "reg-1"}, "alice"); w.Code != http.StatusOK {
		t.Fatalf("register: HTTP %d: %s", w.Code, w.Body.String())
	}
	w := postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(key)}, "alice")
	var refused struct {
		Error api.Error `json:"error"`
	}
	json.Unmarshal(w.Body.Bytes(), &refused)
	if w.Code != http.StatusConflict || refused.Error.Code != "KEY_MISMATCH" || refused.Error.KeyID != keyIDOf(replacement) {
		t.Fatalf("reading no profile with the replaced key: HTTP %d: %s", w.Code, w.Body.String())
	}
	if got := profileOf(t, postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(replacement)}, "alice")); got["selectedModel"] != domain.AutoModelName {
		t.Fatalf("reading no profile with the registered key: %v", got)
	}
}
