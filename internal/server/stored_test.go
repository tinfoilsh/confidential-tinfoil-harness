package server

import (
	"cmp"
	"context"
	"encoding/json"
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
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// A deletion names a row and never opens it: a chat sealed to a retired
// key goes too. Deleting a project's chats lists every id before any is
// deleted; deleting every project is one step of the control plane's,
// which leaves the chats.
func TestDeletionsNeverOpenTheRowsTheyRemove(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	project := "project-1"
	rows.rows[rowMapKey(p, "project", project)] = domain.StoredRow{ID: project, ETag: "1", Data: api.Raw(api.Object{"name": "Trip"})}
	for _, id := range []string{"one", "two", "sealed-to-a-retired-key", "other"} {
		storedThread(rows, "alice", id)
		row := rows.rows[rowMapKey(p, "chat", id)]
		if id != "other" {
			row.Project, row.ProjectSet = &project, true
		}
		rows.rows[rowMapKey(p, "chat", id)] = row
	}
	rows.sealed[rowMapKey(p, "chat", "sealed-to-a-retired-key")] = randomKey()
	for _, tc := range []struct {
		path string
		in   api.Object
		left []string
	}{
		{"/v1/threads/delete", api.Object{"id": "missing"}, []string{"one", "other", "sealed-to-a-retired-key", "two"}},
		{"/v1/threads/delete", api.Object{"id": "sealed-to-a-retired-key"}, []string{"one", "other", "two"}},
		{"/v1/projects/delete", api.Object{"id": project}, []string{"one", "other", "two"}},
		{"/v1/threads/delete-all", api.Object{"projectId": project}, []string{"other"}},
	} {
		tc.in["key"] = keyBase64(key)
		w := postRoute(h, tc.path, tc.in, "alice")
		if w.Code != 200 || !slices.Equal(storedIDs(rows, p, "chat"), tc.left) {
			t.Fatalf("%s %v: HTTP %d %s, leaving %v, want %v", tc.path, tc.in["id"], w.Code, w.Body.String(), storedIDs(rows, p, "chat"), tc.left)
		}
	}
	if left := storedIDs(rows, p, "project"); len(left) != 0 {
		t.Fatalf("the project is still there: %v", left)
	}
	if w := postRoute(h, "/v1/threads/delete-all", api.Object{"key": keyBase64(key)}, ""); w.Code != 401 {
		t.Fatalf("anonymous bulk deletion accepted: %d", w.Code)
	}
	// The control plane deletes every project and document in one step,
	// without any being opened, and the chats stay.
	for _, seeded := range []struct{ scope, id string }{{"project", "p1"}, {"project", "p2"}, {"project_document", "p1/d1"}, {"chat", "c1"}} {
		rows.cp.SetBlob(seeded.scope, seeded.id, cpstub.Blob{ETag: 1, Body: []byte("sealed")})
	}
	w := postRoute(h, "/v1/projects/delete-all", api.Object{"key": keyBase64(key)}, "alice")
	rows.cp.Mu.Lock()
	left := slices.Sorted(maps.Keys(rows.cp.Blobs))
	rows.cp.Mu.Unlock()
	if w.Code != 200 || api.Obj(api.Parsed(w.Body.Bytes()))["deleted"] != float64(2) || !reflect.DeepEqual(left, []string{"chat/c1"}) {
		t.Fatalf("delete all projects: HTTP %d %s, leaving %v", w.Code, w.Body.String(), left)
	}
}

// storedIDs are the ids of a user's rows of a scope, in order.
func storedIDs(rows *memoryRows, p *auth.Principal, scope string) []string {
	ids, _ := rows.IDs(context.Background(), p, scope, "")
	slices.Sort(ids)
	return ids
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
// as the control plane lists them, and every other scope from the rows.
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

// A document sealed to another key is handled as one this harness cannot
// read: opening its project fails with the reason, and a backup leaves it
// out and says so. A page that lists a document again holds its newer copy.
func TestADocumentTheKeyCannotOpenIsNeverLeftOutUnsaid(t *testing.T) {
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
