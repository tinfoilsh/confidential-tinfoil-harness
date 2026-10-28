package server

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
)

func TestResponseLanguageUsesTheDeviceUnlessTheProfileChoosesOne(t *testing.T) {
	systems := make(chan string, 1)
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		systems <- api.Str(api.Obj(lastRequest(r)[0])["content"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Bonjour\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.models.Catalog.Get().Prompt = "Respond in {LANGUAGE}."
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
	p := &auth.Principal{ID: "alice"}
	local := api.Object{"nickname": "Sam", "themeMode": "light", "customPromptPresets": []any{api.Object{"id": "user:same", "name": "Editor", "systemPrompt": "Local editing rules"}, api.Object{"id": "user:local", "name": "Research", "systemPrompt": "Local research rules"}}, "defaultPromptPresetId": "user:same", "favoritePromptPresetIds": []string{"user:same"}}
	first := profileOf(t, postRoute(h, "/v1/profile/merge", api.Object{"key": keyBase64(key), "profile": local}, "alice"))
	if first["nickname"] != "Sam" || len(presetsByID(first)) != 2 || first["defaultPromptPresetId"] != "user:same" {
		t.Fatalf("first cloud settings: %v", first)
	}
	remote := api.Object{"nickname": "Cloud Sam", "themeMode": "dark", "customPromptPresets": []any{api.Object{"id": "user:same", "name": "Editor", "systemPrompt": "Cloud editing rules"}}}
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(remote)}
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
	stored, _ := rows.Load(context.Background(), p, key, "profile", "profile")
	if mustRead(domain.ReadProfile(stored.Data)).DefaultPromptPresetID != nil {
		t.Fatal("the cleared default was not stored")
	}
	if len(mustRead(domain.ReadProfile(stored.Data)).CustomPromptPresets) != 3 {
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
		t.Fatal("device questions used the cloud")
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
	h.models.Offer.Get()[0].Vision = true
	rows.rows["user:alice/project/plan"] = domain.StoredRow{ID: "plan", ETag: "1", Data: api.Raw(domain.Project{ID: "plan", Name: "House"})}
	var encodedImage bytes.Buffer
	if err := png.Encode(&encodedImage, image.NewRGBA(image.Rect(0, 0, 20, 10))); err != nil {
		t.Fatal(err)
	}
	w := uploadRoute(h, "/v1/projects/documents/upload", "plan.png", encodedImage.Bytes(), map[string]string{"key": keyBase64(key), "projectId": "plan"}, "alice")
	var document domain.ProjectDocument
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

func TestRemoteChangesReadOnlyStorageMetadata(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	for _, seeded := range []struct{ scope, id string }{{"chat", "c1"}, {"profile", "profile"}, {"project", "p1"}, {"project_document", "p1/d1"}} {
		rows.cp.SetBlob(seeded.scope, seeded.id, cpstub.Blob{ETag: 1, Body: []byte("sealed")})
	}
	read := func() api.Object {
		t.Helper()
		w := postRoute(h, "/v1/changes", api.Object{}, "alice")
		var result api.Object
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || api.Str(result["chats"]) == "" || api.Str(result["profile"]) == "" {
			t.Fatalf("change tokens: %d %s", w.Code, w.Body.String())
		}
		return result
	}
	revise := func(scope, id string) {
		rows.cp.Mu.Lock()
		defer rows.cp.Mu.Unlock()
		rows.cp.Blobs[cpstub.Key(scope, id)].ETag++
	}
	first := read()
	if !reflect.DeepEqual(first, read()) {
		t.Fatal("unchanged metadata changed its tokens")
	}
	revise("profile", "profile")
	second := read()
	if first["profile"] == second["profile"] || first["chats"] != second["chats"] || first["projects"] != second["projects"] {
		t.Fatalf("profile changes: %v -> %v", first, second)
	}
	revise("project_document", "p1/d1")
	third := read()
	if third["projects"] == second["projects"] || third["profile"] != second["profile"] || third["chats"] != second["chats"] {
		t.Fatalf("document changes: %v -> %v", second, third)
	}
	rows.cp.Mu.Lock()
	rows.cp.SourceRevision++
	rows.cp.Mu.Unlock()
	fourth := read()
	if fourth["chats"] == third["chats"] || fourth["projects"] != third["projects"] || rows.calls != 0 {
		t.Fatalf("chat changes were missed or full rows were read: %v -> %v after %d row reads", third, fourth, rows.calls)
	}
}

func TestRenamingAChatPreservesItsConversationActivity(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	rows.rows["user:alice/chat/activity"] = domain.StoredRow{ID: "activity", ETag: "1", Data: api.Raw(domain.Thread{ID: "activity", CreatedAt: "2026-01-01T00:00:00Z", Messages: []domain.Message{{ID: "latest", Role: "user", CreatedAt: "2026-03-01T00:00:00Z", Content: "Latest"}, {ID: "older", Role: "assistant", CreatedAt: "2026-02-01T00:00:00Z", Content: "Older"}}})}
	w := postRoute(h, "/v1/threads/update", api.Object{"id": "activity", "key": keyBase64(key), "title": "Renamed"}, "alice")
	var summary domain.ThreadSummary
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &summary) != nil || summary.Title != "Renamed" || summary.ActivityAt != "2026-03-01T00:00:00Z" || summary.UpdatedAt == nil || *summary.UpdatedAt == summary.ActivityAt {
		t.Fatalf("rename activity: %d %s", w.Code, w.Body.String())
	}
}

type countedRunGrants struct {
	*memoryRows
	grants atomic.Int32
}

func (s *countedRunGrants) AuthorizeRun(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, project *string) (*auth.StorageGrant, error) {
	s.grants.Add(1)
	return s.memoryRows.AuthorizeRun(ctx, p, key, id, project)
}

func TestAQueuedTurnRefreshesStorageAccessBeforeStarting(t *testing.T) {
	model, release := heldModel()
	h, rows, key := testChatHarness(t, model)
	t.Cleanup(release)
	grants := &countedRunGrants{memoryRows: rows}
	h.store.Rows = grants
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	in := sendInput(key)
	first, err := h.engine.AcceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[engine.TurnRequest](in))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, first.Log, "TEXT_MESSAGE_CHUNK")
	next := sendInput(key)
	next["threadId"], next["content"] = in["threadId"], "Queued question"
	queued, err := h.engine.AcceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[engine.TurnRequest](next))
	if err != nil {
		t.Fatal(err)
	}
	h.engine.Live.Locked(func() { queued.Principal.Grant.ExpiresAt = api.Now() })
	release()
	<-queued.Finished
	row, err := rows.Load(context.Background(), p, key, "chat", api.Str(in["threadId"]))
	if err != nil || len(mustRead(domain.ReadThreadRow(row)).Messages) != 4 || mustRead(domain.ReadThreadRow(row)).Messages[2].Content != "Queued question" || mustRead(domain.ReadThreadRow(row)).Messages[3].Content == "" || grants.grants.Load() != 3 {
		t.Fatalf("queued reply did not renew and save: %v, grants=%d, row=%+v", err, grants.grants.Load(), row)
	}
}
