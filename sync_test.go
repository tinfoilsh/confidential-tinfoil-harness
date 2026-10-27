package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/archives"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

type syncTestTransport func(*http.Request) (*http.Response, error)

func (f syncTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func syncTestResponse(status int, body any) (*http.Response, error) {
	return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(api.Raw(body))), Header: http.Header{"Content-Type": {"application/json"}}}, nil
}

func TestGetProjectLoadsDocumentsAcrossSyncPages(t *testing.T) {
	type page struct {
		ids  []string
		next string
	}
	for _, tc := range []struct {
		name    string
		pages   map[string]page
		cursors []string
		docs    []string
	}{
		{
			name:    "new project without documents",
			pages:   map[string]page{"": {}},
			cursors: []string{""},
		},
		{
			name: "documents after pages from other projects",
			pages: map[string]page{
				"":     {ids: []string{"other/doc-1", "project-main-extra/doc-2"}, next: "next"},
				"next": {ids: []string{"project-main/doc-1", "other/doc-3"}, next: "last"},
				"last": {ids: []string{"project-main/doc-2"}},
			},
			cursors: []string{"", "next", "last"},
			docs:    []string{"doc-1", "doc-2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, key := testChatHarness(t, nil)
			var cursors, pulled []string
			h.store.Rows = &store.Client{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
				var in api.Object
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Errorf("decode sync request: %v", err)
					return syncTestResponse(400, api.Object{})
				}
				switch r.URL.Path {
				case "/v1/sync/list-status":
					// Match the sync service's actual filter contract.
					if api.Str(in["project_id"]) != "" && in["scope"] != "chat" {
						return syncTestResponse(400, api.Object{"code": "BAD_REQUEST", "message": "project_id filter is only valid for chat scope"})
					}
					if in["scope"] != "project_document" {
						t.Errorf("listed scope %v, want project_document", in["scope"])
						return syncTestResponse(400, api.Object{})
					}
					cursor := api.Str(in["cursor"])
					cursors = append(cursors, cursor)
					p, ok := tc.pages[cursor]
					if !ok {
						t.Errorf("unexpected cursor %q", cursor)
						return syncTestResponse(400, api.Object{})
					}
					updates := []api.Object{}
					for _, id := range p.ids {
						updates = append(updates, api.Object{"id": id})
					}
					return syncTestResponse(200, api.Object{"updates": updates, "next_cursor": p.next})
				case "/v1/sync/pull":
					items := []api.Object{}
					for _, value := range api.Arr(in["ids"]) {
						id := api.Str(value)
						var data api.Object
						switch in["scope"] {
						case "project":
							if id != "project-main" {
								t.Errorf("unexpected project %q", id)
							}
							data = api.Object{"name": "New project"}
						case "project_document":
							doc, ok := strings.CutPrefix(id, "project-main/")
							if !ok {
								t.Errorf("pulled another project's document %q", id)
								return syncTestResponse(409, api.Object{"code": "KEY_MISMATCH"})
							}
							pulled = append(pulled, doc)
							data = api.Object{"filename": doc + ".txt", "content": "Project context"}
						default:
							t.Errorf("unexpected pull scope %v", in["scope"])
							return syncTestResponse(400, api.Object{})
						}
						items = append(items, api.Object{"id": id, "ok": true, "etag": "1", "plaintext": base64.StdEncoding.EncodeToString(api.Raw(data))})
					}
					return syncTestResponse(200, api.Object{"items": items})
				default:
					t.Errorf("unexpected sync path %q", r.URL.Path)
					return syncTestResponse(404, api.Object{})
				}
			})}}
			response := postRoute(h, "/v1/projects/get", api.Object{"id": "project-main", "key": keyBase64(key)}, "alice")
			if response.Code != 200 {
				t.Fatalf("open project: HTTP %d: %s", response.Code, response.Body.String())
			}
			var result struct {
				ID        string `json:"id"`
				Documents []struct {
					ID        string `json:"id"`
					ProjectID string `json:"projectId"`
				} `json:"documents"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.ID != "project-main" || len(result.Documents) != len(tc.docs) {
				t.Fatalf("unexpected project response: %s", response.Body.String())
			}
			for i, doc := range result.Documents {
				if doc.ID != tc.docs[i] || doc.ProjectID != "project-main" {
					t.Errorf("unexpected document: %+v", doc)
				}
			}
			if !slices.Equal(cursors, tc.cursors) || !slices.Equal(pulled, tc.docs) {
				t.Errorf("wrong pagination or document pulls: cursors=%v pulled=%v", cursors, pulled)
			}
		})
	}
}

func TestSyncListPreservesChatProjectFilter(t *testing.T) {
	client := &store.Client{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
		var in api.Object
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/v1/sync/list-status" || in["scope"] != "chat" || in["project_id"] != "project-main" || in["cursor"] != "before" || in["direction"] != "desc" {
			t.Errorf("incorrect chat listing: path=%s request=%v", r.URL.Path, in)
		}
		return syncTestResponse(200, api.Object{"updates": []api.Object{}, "next_cursor": "after"})
	})}}
	page, err := client.List(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, randomKey(), "chat", "before", "project-main", 50)
	if err != nil || page.Next != "after" {
		t.Fatalf("list project chats: page=%+v err=%v", page, err)
	}
}

func TestListPullsInBatchesAndListsUnreadableRows(t *testing.T) {
	ids := []string{}
	for i := range store.PullBatch + 5 {
		ids = append(ids, "row-"+strconv.Itoa(i))
	}
	batches, retried := []int{}, 0
	client := &store.Client{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("X-Sync-Protocol") != "3" {
			return syncTestResponse(426, api.Object{"code": "SYNC_PROTOCOL_UPGRADE_REQUIRED"})
		}
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		if r.URL.Path == "/v1/sync/list-status" {
			updates := []api.Object{}
			for _, id := range ids {
				updates = append(updates, api.Object{"id": id})
			}
			return syncTestResponse(200, api.Object{"updates": updates})
		}
		requested := api.Arr(in["ids"])
		batches = append(batches, len(requested))
		items := []api.Object{}
		for _, value := range requested {
			id := api.Str(value)
			switch {
			case id == "row-3":
				items = append(items, api.Object{"id": id, "ok": false, "code": "UNKNOWN_KEY", "key_id": "retired"})
			case id == "row-7" && retried == 0:
				retried++
				items = append(items, api.Object{"id": id, "ok": false, "code": "NETWORK"})
			case id == "row-busy":
				items = append(items, api.Object{"id": id, "ok": false, "code": "BUSY"})
			default:
				items = append(items, api.Object{"id": id, "ok": true, "etag": "1", "plaintext": base64.StdEncoding.EncodeToString(api.Raw(api.Object{"title": id}))})
			}
		}
		return syncTestResponse(200, api.Object{"items": items})
	})}}
	page, err := client.List(context.Background(), &auth.Principal{ID: "alice", JWT: "jwt"}, randomKey(), "chat", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(batches, []int{store.PullBatch, 1, 5}) {
		t.Fatalf("pull batches: %v", batches)
	}
	titles := []string{}
	for _, row := range page.Rows {
		titles = append(titles, mustRead(domain.ReadThreadRow(&row)).Title)
	}
	if len(page.Rows) != store.PullBatch+4 || !slices.Contains(titles, "row-7") || slices.Contains(titles, "row-3") || !slices.Equal(slices.Sorted(maps.Keys(page.Unreadable)), []string{"row-3"}) {
		t.Fatalf("rows %v, unreadable %v", titles, page.Unreadable)
	}
	if _, err := client.Load(context.Background(), &auth.Principal{ID: "alice", JWT: "jwt"}, randomKey(), "chat", "row-3"); api.AsError(err).Code != "KEY_MISMATCH" {
		t.Fatalf("loading an unreadable row: %v", err)
	}
	// Sync answered for the row with a code the harness does not know, not
	// with a refusal of the request, so loading it may be tried again.
	if _, err := client.Load(context.Background(), &auth.Principal{ID: "alice", JWT: "jwt"}, randomKey(), "chat", "row-busy"); api.AsError(err).Action != api.ActionRetry || api.AsError(err).Status != 0 {
		t.Fatalf("loading a row sync could not read: %+v", api.AsError(err))
	}
}

// Sync finds a row's revision, retries a race and deletes rows no key can
// open. Favorites cleanup may read the profile; deletion never opens the chat.
func TestDeletionsLeaveTheWorkToSync(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	var requests []api.Object
	pages := map[string]api.Object{"": {"updates": []api.Object{{"id": "one"}, {"id": "sealed-to-a-retired-key"}}, "next_cursor": "more"}, "more": {"updates": []api.Object{{"id": "two"}}}}
	h.store.API = &store.Client{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		in["path"] = r.URL.Path
		requests = append(requests, in)
		switch r.URL.Path {
		case "/v1/sync/pull":
			if in["scope"] != "profile" {
				t.Fatalf("deletion opened a chat: %v", in)
			}
			requests = requests[:len(requests)-1]
			return syncTestResponse(200, api.Object{"items": []api.Object{{"id": "profile", "ok": false, "code": "NOT_FOUND"}}})
		case "/v1/sync/list-status":
			return syncTestResponse(200, pages[api.Str(in["cursor"])])
		case "/v1/sync/delete-all-projects":
			return syncTestResponse(200, api.Object{"ok": true, "deleted": 4})
		case "/v1/sync/delete":
			return syncTestResponse(200, api.Object{"ok": true})
		}
		return syncTestResponse(404, api.Object{"code": "NOT_FOUND"})
	})}}
	h.store.Rows = h.store.API
	deleted := func(scope, id string) api.Object {
		return api.Object{"path": "/v1/sync/delete", "scope": scope, "id": id, "key": keyBase64(key), "if_match": ""}
	}
	for _, tc := range []struct {
		path string
		in   api.Object
		want []api.Object
	}{
		{"/v1/threads/delete", api.Object{"id": "missing"}, []api.Object{deleted("chat", "missing")}},
		{"/v1/threads/delete", api.Object{"id": "transferred", "revision": "copied-revision"}, []api.Object{{"path": "/v1/sync/delete", "scope": "chat", "id": "transferred", "key": keyBase64(key), "if_match": "copied-revision"}}},
		{"/v1/projects/delete", api.Object{"id": "project-1"}, []api.Object{deleted("project", "project-1")}},
		{"/v1/threads/delete-all", api.Object{"projectId": "project-1"}, []api.Object{
			{"path": "/v1/sync/list-status", "scope": "chat", "project_id": "project-1", "cursor": "", "limit": float64(store.ListStatusPage), "direction": "desc"},
			{"path": "/v1/sync/list-status", "scope": "chat", "project_id": "project-1", "cursor": "more", "limit": float64(store.ListStatusPage), "direction": "desc"},
			deleted("chat", "one"), deleted("chat", "sealed-to-a-retired-key"), deleted("chat", "two")}},
		{"/v1/projects/delete-all", api.Object{}, []api.Object{{"path": "/v1/sync/delete-all-projects", "key": keyBase64(key)}}},
	} {
		requests = nil
		tc.in["key"] = keyBase64(key)
		w := postRoute(h, tc.path, tc.in, "alice")
		for _, request := range requests {
			if api.Str(request["idempotency_key"]) == "" && request["path"] != "/v1/sync/list-status" {
				t.Fatalf("%s: sync was asked %v without an idempotency key", tc.path, request)
			}
			delete(request, "idempotency_key")
		}
		if w.Code != 200 || !reflect.DeepEqual(api.Parsed(api.Raw(requests)), api.Parsed(api.Raw(tc.want))) {
			t.Fatalf("%s: HTTP %d %s, sync was asked\n%v\nwant\n%v", tc.path, w.Code, w.Body.String(), requests, tc.want)
		}
	}
	if w := postRoute(h, "/v1/threads/delete-all", api.Object{"key": keyBase64(key)}, ""); w.Code != 401 {
		t.Fatalf("anonymous bulk deletion accepted: %d", w.Code)
	}
}

// The rows the harness writes name the format they are in, which a later
// format tells apart from rows older clients wrote.
func TestStoredRowsNameTheirFormat(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	finishedRun(t, h, "/v1/threads/turn", sendInput(key))
	if w := postRoute(h, "/v1/profile/update", api.Object{"key": keyBase64(key), "patch": api.Object{"nickname": "Sam"}}, "alice"); w.Code != 200 {
		t.Fatalf("profile: HTTP %d %s", w.Code, w.Body)
	}
	var project domain.Project
	if w := postRoute(h, "/v1/projects/create", api.Object{"key": keyBase64(key), "name": "Trip"}, "alice"); w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &project) != nil {
		t.Fatalf("project: HTTP %d %s", w.Code, w.Body)
	}
	if w := uploadRoute(h, "/v1/projects/documents/upload", "notes.txt", []byte("Pack light"), map[string]string{"key": keyBase64(key), "projectId": project.ID}, "alice"); w.Code != 200 {
		t.Fatalf("document: HTTP %d %s", w.Code, w.Body)
	}
	rows.mu.Lock()
	defer rows.mu.Unlock()
	written := map[string]bool{}
	for name, row := range rows.rows {
		var stored struct {
			RecordVersion int `json:"recordVersion"`
		}
		json.Unmarshal(row.Data, &stored)
		if stored.RecordVersion != domain.RecordVersion {
			t.Errorf("%s was stored in format %d", name, stored.RecordVersion)
		}
		written[strings.Split(name, "/")[1]] = true
	}
	for _, scope := range []string{"chat", "profile", "project", "project_document"} {
		if !written[scope] {
			t.Errorf("no %s row was stored", scope)
		}
	}
}

// A record is read by the format it names: the current one as written, and
// one older clients wrote, naming none, through the legacy reader. One naming
// another format is refused, as is one in the current format leaving out or
// nulling a member it requires, or giving one a value of another type; a
// member the format does not name, which another client of the same rows
// may have added, is read past. A list or a
// backup leaves out what it cannot read; a project's turns read every
// document, so a project with one it cannot read is refused.
func TestRecordsAreReadByTheFormatTheyName(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	current := func(data api.Object) {
		data["recordVersion"], data["titleState"], data["model"], data["presetId"], data["pinned"], data["webSearchEnabled"] = domain.RecordVersion, "manual", "auto", nil, false, true
		for _, message := range api.Arr(data["messages"]) {
			api.Obj(message)["createdAt"] = "2026-01-01T00:00:00Z"
		}
	}
	formats := map[string]func(api.Object){
		"legacy":  func(api.Object) {},
		"current": current,
		"later":   func(data api.Object) { data["recordVersion"] = domain.RecordVersion + 1 },
		"extra": func(data api.Object) {
			current(data)
			data["mood"] = "sunny"
		},
		"incomplete": func(data api.Object) {
			current(data)
			delete(data, "webSearchEnabled")
		},
		"nulled": func(data api.Object) {
			current(data)
			data["messages"] = nil
		},
		"mistyped": func(data api.Object) {
			current(data)
			data["pinned"] = "yes"
		},
	}
	// A member another client of the same rows added does not make a record
	// unreadable; a missing or nulled one the format requires does.
	readable := map[string]bool{"legacy": true, "current": true, "extra": true}
	refused := func(w *httptest.ResponseRecorder) bool {
		e := api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["error"])
		return w.Code == http.StatusConflict && e["code"] == "RECORD_UNSUPPORTED" && e["action"] == "stop"
	}
	for format, edit := range formats {
		storedThread(rows, "alice", format)
		row := rows.rows[rowMapKey(p, "chat", format)]
		editRow(&row, func(data api.Object) {
			data["id"] = format
			edit(data)
		})
		rows.rows[rowMapKey(p, "chat", format)] = row
		w := postRoute(h, "/v1/threads/get", api.Object{"key": keyBase64(key), "id": format}, "alice")
		if readable[format] && w.Code != http.StatusOK || !readable[format] && !refused(w) {
			t.Errorf("opening the %s chat: HTTP %d %s", format, w.Code, w.Body)
		}
		id := domain.RowID()
		thread := api.Object{"id": id, "title": "On my device", "messages": []any{api.Object{"id": "m1", "role": "user", "content": "hi"}}}
		edit(thread)
		w = postRoute(h, "/v1/threads/turn", localInput(id, thread), "alice")
		// A transcript its client sends with a member of another type is the
		// request's fault, refused before any record is read.
		if format == "mistyped" {
			if w.Code != http.StatusBadRequest {
				t.Errorf("a turn of the mistyped device chat: HTTP %d %s", w.Code, w.Body)
			}
		} else if readable[format] && w.Code != http.StatusOK || !readable[format] && !refused(w) {
			t.Errorf("a turn of the %s device chat: HTTP %d %s", format, w.Code, w.Body)
		}
	}
	var list struct {
		Threads []domain.ThreadSummary `json:"threads"`
	}
	json.Unmarshal(postRoute(h, "/v1/threads/list", api.Object{"key": keyBase64(key)}, "alice").Body.Bytes(), &list)
	listed := []string{}
	for _, thread := range list.Threads {
		listed = append(listed, thread.ID)
	}
	if slices.Sort(listed); !slices.Equal(listed, []string{"current", "extra", "legacy"}) {
		t.Errorf("the list holds %v", listed)
	}
	rows.rows[rowMapKey(p, "project", "later")] = domain.StoredRow{ID: "later", ETag: "1", Data: api.Raw(api.Object{"name": "Trip", "recordVersion": domain.RecordVersion + 1})}
	if w := postRoute(h, "/v1/projects/get", api.Object{"key": keyBase64(key), "id": "later"}, "alice"); !refused(w) {
		t.Errorf("opening a project in a later format: HTTP %d %s", w.Code, w.Body)
	}
	trip := "trip"
	rows.rows[rowMapKey(p, "project", trip)] = domain.StoredRow{ID: trip, ETag: "1", Data: api.Raw(api.Object{"name": "Trip"})}
	for doc, data := range map[string]api.Object{"notes": {"filename": "notes.md"}, "later": {"filename": "later.md", "recordVersion": domain.RecordVersion + 1}} {
		rows.rows[rowMapKey(p, "project_document", trip+"/"+doc)] = domain.StoredRow{ID: trip + "/" + doc, ETag: "1", Data: api.Raw(data), Project: &trip, ProjectSet: true}
	}
	if w := postRoute(h, "/v1/projects/get", api.Object{"key": keyBase64(key), "id": trip}, "alice"); !refused(w) {
		t.Errorf("opening a project with a document in a later format: HTTP %d %s", w.Code, w.Body)
	}
	storedThread(rows, "alice", "trip-chat")
	row := rows.rows[rowMapKey(p, "chat", "trip-chat")]
	row.Project, row.ProjectSet = &trip, true
	rows.rows[rowMapKey(p, "chat", "trip-chat")] = row
	if w := postRoute(h, "/v1/threads/turn", continueInput(key, "trip-chat"), "alice"); !refused(w) {
		t.Errorf("a turn in a project with a document in a later format: HTTP %d %s", w.Code, w.Body)
	}
	w := archiveRoute(h, "/v1/export", "alice", optionsPart(api.Object{"format": "native", "key": keyBase64(key)}))
	files, _ := unzipped(t, w.Body.Bytes())
	var manifest archives.BackupManifest
	json.Unmarshal(files[archives.ManifestPath], &manifest)
	if left := (archives.BackupOmission{Kind: "project_document", SourceID: "later", ParentSourceID: trip, Category: "invalid", Reason: "row_unreadable"}); w.Code != http.StatusOK || manifest.Counts.ProjectDocuments != 1 || !slices.Contains(manifest.Omissions, left) {
		t.Errorf("backing up a project with a document in a later format: HTTP %d %+v", w.Code, manifest)
	}
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(api.Object{"nickname": "Sam", "recordVersion": domain.RecordVersion + 1})}
	if w := postRoute(h, "/v1/profile/get", api.Object{"key": keyBase64(key)}, "alice"); !refused(w) {
		t.Errorf("opening a profile in a later format: HTTP %d %s", w.Code, w.Body)
	}
}

// pagedDocuments serves a project's documents in the pages a test scripts,
// as sync lists them, and every other scope from the rows.
type pagedDocuments struct {
	*memoryRows
	pages []store.Page
}

func (s *pagedDocuments) List(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, cursor, project string, limit int) (store.Page, error) {
	if scope != "project_document" {
		return s.memoryRows.List(ctx, p, key, scope, cursor, project, limit)
	}
	page, _ := strconv.Atoi(cmp.Or(cursor, "0"))
	return s.pages[page], nil
}

// A document sync cannot open is handled as one this harness cannot read:
// opening its project fails with sync's reason, and a backup leaves it out
// and says so. A page that lists a document again holds its newer copy.
func TestADocumentSyncCannotOpenIsNeverLeftOutUnsaid(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	trip := "trip"
	rows.rows[rowMapKey(p, "project", trip)] = domain.StoredRow{ID: trip, ETag: "1", Data: api.Raw(api.Object{"name": "Trip"})}
	doc := func(id, filename string) domain.StoredRow {
		return domain.StoredRow{ID: trip + "/" + id, ETag: "1", Data: api.Raw(api.Object{"filename": filename}), Project: &trip, ProjectSet: true}
	}
	h.store.Rows = &pagedDocuments{memoryRows: rows, pages: []store.Page{
		{Rows: []domain.StoredRow{doc("notes", "draft.md"), doc("map", "map.md")}, Next: "1"},
		{Rows: []domain.StoredRow{doc("notes", "notes.md")}, Unreadable: map[string]error{trip + "/map": api.Err(409, "KEY_MISMATCH", "sealed to another key")}},
	}}
	w := postRoute(h, "/v1/projects/get", api.Object{"key": keyBase64(key), "id": trip}, "alice")
	if e := api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["error"]); w.Code != http.StatusConflict || e["code"] != "KEY_MISMATCH" || e["action"] != "unlock" {
		t.Errorf("opening the project: HTTP %d %s", w.Code, w.Body)
	}
	w = archiveRoute(h, "/v1/export", "alice", optionsPart(api.Object{"format": "native", "key": keyBase64(key)}))
	files, _ := unzipped(t, w.Body.Bytes())
	var manifest archives.BackupManifest
	json.Unmarshal(files[archives.ManifestPath], &manifest)
	var notes archives.BackupDocument
	json.Unmarshal(files[archives.BackupDocumentPath(trip, "notes")], &notes)
	left := archives.BackupOmission{Kind: "project_document", SourceID: "map", ParentSourceID: trip, Category: "invalid", Reason: "row_unreadable"}
	if manifest.Counts.ProjectDocuments != 1 || notes.Filename != "notes.md" || !reflect.DeepEqual(manifest.Omissions, []archives.BackupOmission{left}) {
		t.Errorf("backup: %+v, notes %+v", manifest, notes)
	}
}
