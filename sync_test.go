package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type syncTestTransport func(*http.Request) (*http.Response, error)

func (f syncTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func syncTestResponse(status int, body any) (*http.Response, error) {
	return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(raw(body))), Header: http.Header{"Content-Type": {"application/json"}}}, nil
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
			h.storeRows = &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
				var in object
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Errorf("decode sync request: %v", err)
					return syncTestResponse(400, object{})
				}
				switch r.URL.Path {
				case "/v1/sync/list-status":
					// Match the sync service's actual filter contract.
					if str(in["project_id"]) != "" && in["scope"] != "chat" {
						return syncTestResponse(400, object{"code": "BAD_REQUEST", "message": "project_id filter is only valid for chat scope"})
					}
					if in["scope"] != "project_document" {
						t.Errorf("listed scope %v, want project_document", in["scope"])
						return syncTestResponse(400, object{})
					}
					cursor := str(in["cursor"])
					cursors = append(cursors, cursor)
					p, ok := tc.pages[cursor]
					if !ok {
						t.Errorf("unexpected cursor %q", cursor)
						return syncTestResponse(400, object{})
					}
					updates := []object{}
					for _, id := range p.ids {
						updates = append(updates, object{"id": id})
					}
					return syncTestResponse(200, object{"updates": updates, "next_cursor": p.next})
				case "/v1/sync/pull":
					items := []object{}
					for _, value := range arr(in["ids"]) {
						id := str(value)
						var data object
						switch in["scope"] {
						case "project":
							if id != "project-main" {
								t.Errorf("unexpected project %q", id)
							}
							data = object{"name": "New project"}
						case "project_document":
							doc, ok := strings.CutPrefix(id, "project-main/")
							if !ok {
								t.Errorf("pulled another project's document %q", id)
								return syncTestResponse(409, object{"code": "KEY_MISMATCH"})
							}
							pulled = append(pulled, doc)
							data = object{"filename": doc + ".txt", "content": "Project context"}
						default:
							t.Errorf("unexpected pull scope %v", in["scope"])
							return syncTestResponse(400, object{})
						}
						items = append(items, object{"id": id, "ok": true, "etag": "1", "plaintext": base64.StdEncoding.EncodeToString(raw(data))})
					}
					return syncTestResponse(200, object{"items": items})
				default:
					t.Errorf("unexpected sync path %q", r.URL.Path)
					return syncTestResponse(404, object{})
				}
			})}}
			response := postRoute(h, "/v1/projects/get", object{"id": "project-main", "key": key.base64()}, "alice")
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
	client := &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
		var in object
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/v1/sync/list-status" || in["scope"] != "chat" || in["project_id"] != "project-main" || in["cursor"] != "before" || in["direction"] != "desc" {
			t.Errorf("incorrect chat listing: path=%s request=%v", r.URL.Path, in)
		}
		return syncTestResponse(200, object{"updates": []object{}, "next_cursor": "after"})
	})}}
	page, err := client.list(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, randomKey(), "chat", "before", "project-main", 50)
	if err != nil || page.Next != "after" {
		t.Fatalf("list project chats: page=%+v err=%v", page, err)
	}
}

func TestListPullsInBatchesAndListsUnreadableRows(t *testing.T) {
	ids := []string{}
	for i := range pullBatch + 5 {
		ids = append(ids, "row-"+strconv.Itoa(i))
	}
	batches, retried := []int{}, 0
	client := &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("X-Sync-Protocol") != "3" {
			return syncTestResponse(426, object{"code": "SYNC_PROTOCOL_UPGRADE_REQUIRED"})
		}
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		if r.URL.Path == "/v1/sync/list-status" {
			updates := []object{}
			for _, id := range ids {
				updates = append(updates, object{"id": id})
			}
			return syncTestResponse(200, object{"updates": updates})
		}
		requested := arr(in["ids"])
		batches = append(batches, len(requested))
		items := []object{}
		for _, value := range requested {
			id := str(value)
			switch {
			case id == "row-3":
				items = append(items, object{"id": id, "ok": false, "code": "UNKNOWN_KEY", "key_id": "retired"})
			case id == "row-7" && retried == 0:
				retried++
				items = append(items, object{"id": id, "ok": false, "code": "NETWORK"})
			default:
				items = append(items, object{"id": id, "ok": true, "etag": "1", "plaintext": base64.StdEncoding.EncodeToString(raw(object{"title": id}))})
			}
		}
		return syncTestResponse(200, object{"items": items})
	})}}
	page, err := client.list(context.Background(), &principal{ID: "alice", JWT: "jwt"}, randomKey(), "chat", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(batches, []int{pullBatch, 1, 5}) {
		t.Fatalf("pull batches: %v", batches)
	}
	titles := []string{}
	for _, row := range page.Rows {
		titles = append(titles, readThreadRow(&row).Title)
	}
	if len(page.Rows) != pullBatch+4 || !slices.Contains(titles, "row-7") || slices.Contains(titles, "row-3") || !slices.Equal(page.Unreadable, []string{"row-3"}) {
		t.Fatalf("rows %v, unreadable %v", titles, page.Unreadable)
	}
	if _, err := client.load(context.Background(), &principal{ID: "alice", JWT: "jwt"}, randomKey(), "chat", "row-3"); asAPIError(err).Code != "KEY_MISMATCH" {
		t.Fatalf("loading an unreadable row: %v", err)
	}
}

// Sync finds a row's revision, retries a race and deletes rows no key can
// open, so a deletion opens nothing and names no revision.
func TestDeletionsLeaveTheWorkToSync(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	var requests []object
	pages := map[string]object{"": {"updates": []object{{"id": "one"}, {"id": "sealed-to-a-retired-key"}}, "next_cursor": "more"}, "more": {"updates": []object{{"id": "two"}}}}
	h.syncAPI = &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		in["path"] = r.URL.Path
		requests = append(requests, in)
		switch r.URL.Path {
		case "/v1/sync/list-status":
			return syncTestResponse(200, pages[str(in["cursor"])])
		case "/v1/sync/delete-all-projects":
			return syncTestResponse(200, object{"ok": true, "deleted": 4})
		case "/v1/sync/delete":
			return syncTestResponse(200, object{"ok": true})
		}
		return syncTestResponse(404, object{"code": "NOT_FOUND"})
	})}}
	h.storeRows = h.syncAPI
	deleted := func(scope, id string) object {
		return object{"path": "/v1/sync/delete", "scope": scope, "id": id, "key": key.base64(), "if_match": ""}
	}
	for _, tc := range []struct {
		path string
		in   object
		want []object
	}{
		{"/v1/threads/delete", object{"id": "missing"}, []object{deleted("chat", "missing")}},
		{"/v1/projects/delete", object{"id": "project-1"}, []object{deleted("project", "project-1")}},
		{"/v1/threads/delete-all", object{"projectId": "project-1"}, []object{
			{"path": "/v1/sync/list-status", "scope": "chat", "project_id": "project-1", "cursor": "", "limit": float64(listStatusPage), "direction": "desc"},
			{"path": "/v1/sync/list-status", "scope": "chat", "project_id": "project-1", "cursor": "more", "limit": float64(listStatusPage), "direction": "desc"},
			deleted("chat", "one"), deleted("chat", "sealed-to-a-retired-key"), deleted("chat", "two")}},
		{"/v1/projects/delete-all", object{}, []object{{"path": "/v1/sync/delete-all-projects", "key": key.base64()}}},
	} {
		requests = nil
		tc.in["key"] = key.base64()
		w := postRoute(h, tc.path, tc.in, "alice")
		for _, request := range requests {
			if str(request["idempotency_key"]) == "" && request["path"] != "/v1/sync/list-status" {
				t.Fatalf("%s: sync was asked %v without an idempotency key", tc.path, request)
			}
			delete(request, "idempotency_key")
		}
		if w.Code != 200 || !reflect.DeepEqual(decodeValue(raw(requests)), decodeValue(raw(tc.want))) {
			t.Fatalf("%s: HTTP %d %s, sync was asked\n%v\nwant\n%v", tc.path, w.Code, w.Body.String(), requests, tc.want)
		}
	}
	if w := postRoute(h, "/v1/threads/delete-all", object{"key": key.base64()}, ""); w.Code != 401 {
		t.Fatalf("anonymous bulk deletion accepted: %d", w.Code)
	}
}
