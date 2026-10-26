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

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

func TestResponseLanguageUsesTheDeviceUnlessTheProfileChoosesOne(t *testing.T) {
	systems := make(chan string, 1)
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		systems <- api.Str(api.Obj(lastRequest(r)[0])["content"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Bonjour\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.catalog.get().Prompt = "Respond in {LANGUAGE}."
	for _, tc := range []struct{ selected, locale, want string }{{"System", "fr-FR,fr;q=0.9", "French"}, {"German", "fr-FR", "German"}, {"System", "", "English"}} {
		in := localInput("", nil)
		in["profile"] = api.Object{"language": tc.selected}
		r := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(api.Raw(in)))
		signIn(r, "alice")
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

func TestMergingDeviceSettingsPreservesBothPromptLibraries(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &principal{ID: "alice"}
	local := api.Object{"nickname": "Sam", "themeMode": "light", "customPromptPresets": []any{api.Object{"id": "user:same", "name": "Editor", "systemPrompt": "Local editing rules"}, api.Object{"id": "user:local", "name": "Research", "systemPrompt": "Local research rules"}}, "defaultPromptPresetId": "user:same", "favoritePromptPresetIds": []string{"user:same"}}
	first := profileOf(t, postRoute(h, "/v1/profile/merge", api.Object{"key": keyBase64(key), "profile": local}, "alice"))
	if first["nickname"] != "Sam" || len(presetsByID(first)) != 2 || first["defaultPromptPresetId"] != "user:same" {
		t.Fatalf("first cloud settings: %v", first)
	}
	remote := api.Object{"nickname": "Cloud Sam", "themeMode": "dark", "customPromptPresets": []any{api.Object{"id": "user:same", "name": "Editor", "systemPrompt": "Cloud editing rules"}}}
	rows.rows[rowMapKey(p, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: api.Raw(remote)}
	merged := profileOf(t, postRoute(h, "/v1/profile/merge", api.Object{"key": keyBase64(key), "profile": local, "changedFields": []string{"nickname"}}, "alice"))
	presets := presetsByID(merged)
	if merged["themeMode"] != "dark" || merged["nickname"] != "Sam" || len(presets) != 3 || presets["user:same"]["systemPrompt"] != "Cloud editing rules" || presets[api.Str(merged["defaultPromptPresetId"])]["systemPrompt"] != "Local editing rules" {
		t.Fatalf("merged settings: %v", merged)
	}
	again := profileOf(t, postRoute(h, "/v1/profile/merge", api.Object{"key": keyBase64(key), "profile": local}, "alice"))
	if !reflect.DeepEqual(presetsByID(again), presets) {
		t.Fatalf("a retry duplicated prompts: %v", again)
	}
	local["defaultPromptPresetId"] = nil
	local["favoritePromptPresetIds"] = []string{}
	cleared := profileOf(t, postRoute(h, "/v1/profile/merge", api.Object{"key": keyBase64(key), "profile": local, "changedFields": []string{"defaultPromptPresetId", "favoritePromptPresetIds"}}, "alice"))
	if len(api.Arr(cleared["favoritePromptPresetIds"])) != 0 {
		t.Fatal("the device's un-favorite was lost")
	}
	if api.Str(cleared["defaultPromptPresetId"]) != "" {
		t.Fatalf("clearing the device default was lost: %v", cleared["defaultPromptPresetId"])
	}
	stored, _ := rows.load(context.Background(), p, key, "profile", "profile")
	if mustRead(readProfile(stored.Data)).DefaultPromptPresetID != nil {
		t.Fatal("the cleared default was not stored")
	}
	if len(mustRead(readProfile(stored.Data)).CustomPromptPresets) != 3 {
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
		if !strings.Contains(string(api.Raw(prompt)), "Selected passage") || kind == "ask" && !strings.Contains(api.Str(api.Obj(prompt[0])["content"]), "clarify") {
			t.Fatalf("%s: %s", kind, api.Raw(prompt))
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
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		if in["stream"] == false {
			descriptions.Add(1)
			content := api.Arr(api.Obj(api.Arr(in["messages"])[0])["content"])
			if len(content) != 2 || !strings.HasPrefix(api.Str(api.Obj(api.Obj(content[1])["image_url"])["url"]), "data:image/jpeg;base64,") {
				t.Errorf("image description request: %v", content)
			}
			json.NewEncoder(w).Encode(api.Object{"choices": []api.Object{{"message": api.Object{"content": "A red plan with three labeled rooms."}}}})
			return
		}
		requests <- api.Str(api.Obj(api.Arr(in["messages"])[0])["content"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Three rooms\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.offer.models[0].vision = true
	rows.rows["user:alice/project/plan"] = storedRow{ID: "plan", ETag: "1", Data: api.Raw(Project{ID: "plan", Name: "House"})}
	var encodedImage bytes.Buffer
	if err := png.Encode(&encodedImage, image.NewRGBA(image.Rect(0, 0, 20, 10))); err != nil {
		t.Fatal(err)
	}
	w := uploadRoute(h, "/v1/projects/documents/upload", "plan.png", encodedImage.Bytes(), map[string]string{"key": keyBase64(key), "projectId": "plan"}, "alice")
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
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		if r.URL.Path == "/v1/runs/authorize" {
			if r.Header.Get("Authorization") != "Bearer clerk-alice" || in["key"] != keyBase64(key) || api.Str(in["thread_id"]) == "" {
				t.Errorf("invalid delegation request")
			}
			grantCalls.Add(1)
			json.NewEncoder(w).Encode(api.Object{"token": "scoped-storage", "expires_at": api.Now().Add(time.Hour)})
			return
		}
		if r.Header.Get("Authorization") != "Bearer scoped-storage" {
			w.WriteHeader(401)
			return
		}
		p := &principal{ID: "alice"}
		switch r.URL.Path {
		case "/v1/sync/pull":
			items := []api.Object{}
			for _, value := range api.Arr(in["ids"]) {
				id := api.Str(value)
				row, err := rows.load(r.Context(), p, key, api.Str(in["scope"]), id)
				if err != nil {
					items = append(items, api.Object{"id": id, "ok": false, "code": "NOT_FOUND"})
					continue
				}
				items = append(items, api.Object{"id": id, "ok": true, "etag": row.ETag, "plaintext": base64.StdEncoding.EncodeToString(row.Data)})
			}
			json.NewEncoder(w).Encode(api.Object{"items": items})
		case "/v1/sync/push":
			plain, _ := base64.StdEncoding.DecodeString(api.Str(in["plaintext"]))
			etag, err := rows.push(r.Context(), p, key, api.Str(in["scope"]), &storedRow{ID: api.Str(in["id"]), ETag: api.Str(in["if_match"]), Data: plain}, "write")
			if err != nil {
				w.WriteHeader(409)
				return
			}
			json.NewEncoder(w).Encode(api.Object{"etag": etag})
		default:
			t.Errorf("unexpected storage route %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	h.storeRows = &syncClient{URL: server.URL, Client: server.Client()}
	in := sendInput(key)
	events := finishedRun(t, h, "/v1/threads/turn", in)
	row, err := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", api.Str(in["threadId"]))
	if err != nil || grantCalls.Load() != 1 || eventOfType(events, "RUN_FINISHED") == nil || len(mustRead(readThreadRow(row)).Messages) != 2 || mustRead(readThreadRow(row)).Messages[1].Content != "hello" {
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
			json.NewEncoder(w).Encode(api.Object{"current_revision": revisions["chat"]})
			return
		}
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		scope := api.Str(in["scope"])
		direction := "desc"
		if scope == "profile" {
			direction = "asc"
		}
		if r.URL.Path != "/v1/sync/list-status" || in["limit"] != float64(1) || in["direction"] != direction {
			t.Errorf("unexpected metadata request: %s %v", r.URL.Path, in)
		}
		json.NewEncoder(w).Encode(api.Object{"updates": []api.Object{{"id": scope, "etag": revisions[scope]}}})
	}))
	defer server.Close()
	h.syncAPI = &syncClient{URL: server.URL, Client: server.Client()}
	read := func() api.Object {
		t.Helper()
		w := postRoute(h, "/v1/changes", api.Object{}, "alice")
		var result api.Object
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || api.Str(result["chats"]) == "" || api.Str(result["profile"]) == "" {
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
	rows.rows["user:alice/chat/activity"] = storedRow{ID: "activity", ETag: "1", Data: api.Raw(Thread{ID: "activity", CreatedAt: "2026-01-01T00:00:00Z", Messages: []Message{{ID: "latest", Role: "user", CreatedAt: "2026-03-01T00:00:00Z", Content: "Latest"}, {ID: "older", Role: "assistant", CreatedAt: "2026-02-01T00:00:00Z", Content: "Older"}}})}
	w := postRoute(h, "/v1/threads/update", api.Object{"id": "activity", "key": keyBase64(key), "title": "Renamed"}, "alice")
	var summary threadSummary
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &summary) != nil || summary.Title != "Renamed" || summary.ActivityAt != "2026-03-01T00:00:00Z" || summary.UpdatedAt == nil || *summary.UpdatedAt == summary.ActivityAt {
		t.Fatalf("rename activity: %d %s", w.Code, w.Body.String())
	}
}

type countedRunGrants struct {
	*memoryRows
	grants atomic.Int32
}

func (s *countedRunGrants) authorizeRun(ctx context.Context, p *principal, key contentKey, id string, project *string) (storageGrant, error) {
	s.grants.Add(1)
	return s.memoryRows.authorizeRun(ctx, p, key, id, project)
}

func TestAQueuedTurnRefreshesStorageAccessBeforeStarting(t *testing.T) {
	model, release := heldModel()
	h, rows, key := testChatHarness(t, model)
	t.Cleanup(release)
	grants := &countedRunGrants{memoryRows: rows}
	h.storeRows = grants
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	in := sendInput(key)
	first, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](in))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, first.log, "TEXT_MESSAGE_CHUNK")
	next := sendInput(key)
	next["threadId"], next["content"] = in["threadId"], "Queued question"
	queued, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](next))
	if err != nil {
		t.Fatal(err)
	}
	h.live.mu.Lock()
	queued.principal.SyncExpires = api.Now()
	h.live.mu.Unlock()
	release()
	<-queued.finished
	row, err := rows.load(context.Background(), p, key, "chat", api.Str(in["threadId"]))
	if err != nil || len(mustRead(readThreadRow(row)).Messages) != 4 || mustRead(readThreadRow(row)).Messages[2].Content != "Queued question" || mustRead(readThreadRow(row)).Messages[3].Content == "" || grants.grants.Load() != 3 {
		t.Fatalf("queued reply did not renew and save: %v, grants=%d, row=%+v", err, grants.grants.Load(), row)
	}
}
