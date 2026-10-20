package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResponseLanguageUsesTheDeviceUnlessTheProfileChoosesOne(t *testing.T) {
	systems := make(chan string, 1)
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		systems <- str(obj(lastRequest(r)[0])["content"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Bonjour\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.catalog.get().Prompt = "Respond in {LANGUAGE}."
	for _, tc := range []struct{ selected, locale, want string }{{"System", "fr-FR,fr;q=0.9", "French"}, {"German", "fr-FR", "German"}, {"System", "", "English"}} {
		in := localInput("", nil)
		in["profile"] = object{"language": tc.selected}
		r := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(raw(in)))
		r.Header.Set("Authorization", "Bearer clerk-alice")
		r.Header.Set("Accept-Language", tc.locale)
		w := httptest.NewRecorder()
		h.routes().ServeHTTP(w, r)
		eventsIn(t, w)
		if len(systems) != 1 {
			t.Fatal("the turn never reached the model")
		}
		if system := <-systems; !strings.Contains(system, tc.want) {
			t.Fatalf("%s/%s: %q", tc.selected, tc.locale, system)
		}
	}
}

func TestExplicitSessionRefreshRechecksTheSubscription(t *testing.T) {
	var subscribed atomic.Bool
	h, _ := subscriberHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == chatTokenPath && !subscribed.Load() {
			w.WriteHeader(http.StatusPaymentRequired)
			return
		}
		if r.URL.Path == chatTokenPath {
			json.NewEncoder(w).Encode(object{"key": "paid-token", "expires_at": time.Now().Add(time.Hour).Format(time.RFC3339), "rate_limit": object{"max_input_tokens": 10000, "input_tokens_used": 0, "input_tokens_remaining": 10000}})
			return
		}
		if r.URL.Path != "/api/keys/chat" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(object{"key": "free_previous-plan", "expires_at": time.Now().Add(12 * time.Hour).Format(time.RFC3339), "is_free_tier": true, "rate_limit": object{"max_requests": 5, "remaining": 0}})
	})
	for _, paid := range []bool{false, true, false} {
		subscribed.Store(paid)
		result := profileOf(t, postRoute(h, "/v1/session", object{"refreshCredentials": true}, "alice"))
		want := "free_daily"
		if paid {
			want = "hourly"
		}
		if obj(result["rateLimit"])["kind"] != want {
			t.Fatalf("paid=%v: %v", paid, result["rateLimit"])
		}
	}
}

func TestMergingDeviceSettingsPreservesBothPromptLibraries(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &principal{ID: "alice"}
	local := object{"nickname": "Sam", "themeMode": "light", "customPromptPresets": []any{object{"id": "user:same", "name": "Editor", "systemPrompt": "Local editing rules"}, object{"id": "user:local", "name": "Research", "systemPrompt": "Local research rules"}}, "defaultPromptPresetId": "user:same", "favoritePromptPresetIds": []string{"user:same"}}
	first := profileOf(t, postRoute(h, "/v1/profile/merge", object{"key": key.base64(), "profile": local}, "alice"))
	if first["nickname"] != "Sam" || len(presetsByID(first)) != 2 || first["defaultPromptPresetId"] != "user:same" {
		t.Fatalf("first cloud settings: %v", first)
	}
	remote := object{"nickname": "Cloud Sam", "themeMode": "dark", "customPromptPresets": []any{object{"id": "user:same", "name": "Editor", "systemPrompt": "Cloud editing rules"}}}
	rows.rows[rowMapKey(p, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: raw(remote)}
	merged := profileOf(t, postRoute(h, "/v1/profile/merge", object{"key": key.base64(), "profile": local, "changedFields": []string{"nickname"}}, "alice"))
	presets := presetsByID(merged)
	if merged["themeMode"] != "dark" || merged["nickname"] != "Sam" || len(presets) != 3 || presets["user:same"]["systemPrompt"] != "Cloud editing rules" || presets[str(merged["defaultPromptPresetId"])]["systemPrompt"] != "Local editing rules" {
		t.Fatalf("merged settings: %v", merged)
	}
	again := profileOf(t, postRoute(h, "/v1/profile/merge", object{"key": key.base64(), "profile": local}, "alice"))
	if !reflect.DeepEqual(presetsByID(again), presets) {
		t.Fatalf("a retry duplicated prompts: %v", again)
	}
	stored, _ := rows.load(context.Background(), p, key, "profile", "profile")
	if len(readProfile(stored.Data).CustomPromptPresets) != 3 {
		t.Fatal("merge was not stored")
	}
}

func TestASelectionCanBeSentAloneAndAskedAbout(t *testing.T) {
	requests := make(chan []any, 1)
	h, rows, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- lastRequest(r)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"An explanation\"}}]}\n\ndata: [DONE]\n\n")
	})
	for _, kind := range []string{"send", "ask"} {
		in := localInput("", priorThread())
		in["kind"], in["quote"], in["content"] = kind, "Selected passage", ""
		finishedRun(t, h, "/v1/threads/turn", in)
		if len(requests) != 1 {
			t.Fatal("the selection never reached the model")
		}
		prompt := <-requests
		if !strings.Contains(string(raw(prompt)), "Selected passage") || kind == "ask" && !strings.Contains(str(obj(prompt[0])["content"]), "clarify") {
			t.Fatalf("%s: %s", kind, raw(prompt))
		}
	}
	if rows.calls != 0 {
		t.Fatal("device questions used sync")
	}
}

func TestAProjectImageBecomesContextForItsChats(t *testing.T) {
	var descriptions atomic.Int32
	requests := make(chan string, 1)
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		if in["stream"] == false {
			descriptions.Add(1)
			content := arr(obj(arr(in["messages"])[0])["content"])
			if len(content) != 2 || !strings.HasPrefix(str(obj(obj(content[1])["image_url"])["url"]), "data:image/jpeg;base64,") {
				t.Errorf("image description request: %v", content)
			}
			json.NewEncoder(w).Encode(object{"choices": []object{{"message": object{"content": "A red plan with three labeled rooms."}}}})
			return
		}
		requests <- str(obj(arr(in["messages"])[0])["content"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Three rooms\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.offer.models[0].vision = true
	rows.rows["user:alice/project/plan"] = storedRow{ID: "plan", ETag: "1", Data: raw(Project{ID: "plan", Name: "House"})}
	var encodedImage bytes.Buffer
	if err := png.Encode(&encodedImage, image.NewRGBA(image.Rect(0, 0, 20, 10))); err != nil {
		t.Fatal(err)
	}
	w := uploadRoute(h, "/v1/projects/documents/upload", "plan.png", encodedImage.Bytes(), map[string]string{"key": key.base64(), "projectId": "plan"}, "alice")
	var document ProjectDocument
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &document) != nil || document.Content != "A red plan with three labeled rooms." {
		t.Fatalf("project image: %d %s", w.Code, w.Body.String())
	}
	in := sendInput(key)
	in["projectId"] = "plan"
	finishedRun(t, h, "/v1/threads/turn", in)
	if len(requests) != 1 {
		t.Fatal("the project turn never reached the model")
	}
	if system := <-requests; descriptions.Load() != 1 || !strings.Contains(system, document.Content) {
		t.Fatalf("project image was not reusable context: %q", system)
	}
}

func TestACloudRunUsesItsStorageCredentialForTheFinalSave(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	var grantCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		if r.URL.Path == "/v1/runs/authorize" {
			if r.Header.Get("Authorization") != "Bearer clerk-alice" || in["key"] != key.base64() || str(in["thread_id"]) == "" {
				t.Errorf("invalid delegation request")
			}
			grantCalls.Add(1)
			json.NewEncoder(w).Encode(object{"token": "scoped-storage", "expires_at": nowUTC().Add(time.Hour)})
			return
		}
		if r.Header.Get("Authorization") != "Bearer scoped-storage" {
			w.WriteHeader(401)
			return
		}
		p := &principal{ID: "alice"}
		switch r.URL.Path {
		case "/v1/sync/pull":
			items := []object{}
			for _, value := range arr(in["ids"]) {
				id := str(value)
				row, err := rows.load(r.Context(), p, key, str(in["scope"]), id)
				if err != nil {
					items = append(items, object{"id": id, "ok": false, "code": "NOT_FOUND"})
					continue
				}
				items = append(items, object{"id": id, "ok": true, "etag": row.ETag, "plaintext": base64.StdEncoding.EncodeToString(row.Data)})
			}
			json.NewEncoder(w).Encode(object{"items": items})
		case "/v1/sync/push":
			plain, _ := base64.StdEncoding.DecodeString(str(in["plaintext"]))
			etag, err := rows.push(r.Context(), p, key, str(in["scope"]), &storedRow{ID: str(in["id"]), ETag: str(in["if_match"]), Data: plain}, "write")
			if err != nil {
				w.WriteHeader(409)
				return
			}
			json.NewEncoder(w).Encode(object{"etag": etag})
		default:
			t.Errorf("unexpected storage route %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	h.storeRows = &syncClient{URL: server.URL, Client: server.Client()}
	in := sendInput(key)
	events := finishedRun(t, h, "/v1/threads/turn", in)
	row, err := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", str(in["threadId"]))
	if err != nil || grantCalls.Load() != 1 || eventOfType(events, "RUN_FINISHED") == nil || len(readThreadRow(row).Messages) != 2 || readThreadRow(row).Messages[1].Content != "hello" {
		t.Fatalf("reply was not stored with the run credential: %v", err)
	}
}

func TestRemoteChangesReadOnlyStorageMetadata(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	var mu sync.Mutex
	revisions := map[string]string{"chat": "1", "profile": "1", "project": "1", "project_document": "1"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer clerk-alice" {
			t.Errorf("missing caller identity")
		}
		if r.URL.Path == "/v1/sync/revision-summary" {
			json.NewEncoder(w).Encode(object{"current_revision": revisions["chat"]})
			return
		}
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		if r.URL.Path != "/v1/sync/list-status" || in["limit"] != float64(1) || in["direction"] != "desc" {
			t.Errorf("unexpected metadata request: %s %v", r.URL.Path, in)
		}
		scope := str(in["scope"])
		json.NewEncoder(w).Encode(object{"updates": []object{{"id": scope, "etag": revisions[scope]}}})
	}))
	defer server.Close()
	h.syncAPI = &syncClient{URL: server.URL, Client: server.Client()}
	read := func() object {
		t.Helper()
		w := postRoute(h, "/v1/changes", object{}, "alice")
		var result object
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || str(result["chats"]) == "" || str(result["profile"]) == "" {
			t.Fatalf("change tokens: %d %s", w.Code, w.Body.String())
		}
		return result
	}
	first := read()
	if !reflect.DeepEqual(first, read()) {
		t.Fatal("unchanged metadata changed its tokens")
	}
	mu.Lock()
	revisions["profile"] = "2"
	mu.Unlock()
	second := read()
	if first["profile"] == second["profile"] || first["chats"] != second["chats"] || first["projects"] != second["projects"] {
		t.Fatalf("profile changes: %v -> %v", first, second)
	}
	mu.Lock()
	revisions["project_document"] = "2"
	mu.Unlock()
	third := read()
	if third["projects"] == second["projects"] || rows.calls != 0 {
		t.Fatal("document changes were missed or full rows were read")
	}
}

func TestRenamingAChatPreservesItsConversationActivity(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	rows.rows["user:alice/chat/activity"] = storedRow{ID: "activity", ETag: "1", Data: raw(Thread{ID: "activity", CreatedAt: "2026-01-01T00:00:00Z", Messages: []Message{{ID: "latest", Role: "user", CreatedAt: "2026-03-01T00:00:00Z", Content: "Latest"}, {ID: "older", Role: "assistant", CreatedAt: "2026-02-01T00:00:00Z", Content: "Older"}}})}
	w := postRoute(h, "/v1/threads/update", object{"id": "activity", "key": key.base64(), "title": "Renamed"}, "alice")
	var summary threadSummary
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &summary) != nil || summary.Title != "Renamed" || summary.ActivityAt != "2026-03-01T00:00:00Z" || summary.UpdatedAt == nil || *summary.UpdatedAt == summary.ActivityAt {
		t.Fatalf("rename activity: %d %s", w.Code, w.Body.String())
	}
}
