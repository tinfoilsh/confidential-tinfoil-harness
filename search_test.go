package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"slices"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

func TestSearchRefreshesResultsAfterACompletedRebuild(t *testing.T) {
	const queryPath = "/v1/search/query"
	const reindexPath = "/v1/search/reindex"
	const limit = 7
	for _, tc := range []struct {
		name          string
		needsReindex  bool
		job           api.Object
		initialIDs    []string
		stillIndexing bool
		wantID        string
		wantIndexing  bool
		wantPaths     []string
	}{
		{name: "healthy index", initialIDs: []string{"indexed"}, wantID: "indexed", wantPaths: []string{queryPath}},
		{name: "completed rebuild", needsReindex: true, job: api.Object{"status": "completed"}, wantID: "indexed", wantPaths: []string{queryPath, reindexPath, queryPath}},
		{name: "running rebuild", needsReindex: true, job: api.Object{"status": "running"}, initialIDs: []string{"partial"}, wantID: "partial", wantIndexing: true, wantPaths: []string{queryPath, reindexPath}},
		{name: "resumable checkpoint", needsReindex: true, job: api.Object{"status": "completed", "partial": true}, initialIDs: []string{"partial"}, wantID: "partial", wantIndexing: true, wantPaths: []string{queryPath, reindexPath}},
		{name: "index invalidated again", needsReindex: true, job: api.Object{"status": "completed"}, stillIndexing: true, wantID: "indexed", wantIndexing: true, wantPaths: []string{queryPath, reindexPath, queryPath}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, rows, key := testChatHarness(t, nil)
			created := "2026-10-22T12:00:00Z"
			for _, id := range []string{"partial", "indexed"} {
				rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", id)] = storedRow{ID: id, ETag: "1", Data: api.Raw(Thread{
					ID: id, Title: id + " conversation", TitleState: "manual", CreatedAt: created, UpdatedAt: created, Model: "gpt-oss-120b",
					Messages: []Message{{ID: id + "-message", Role: "user", Content: "Find this conversation", CreatedAt: created}},
				})}
			}
			var paths []string
			queries := 0
			h.syncAPI = &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
				paths = append(paths, r.URL.Path)
				var in api.Object
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Fatal(err)
				}
				if r.Header.Get("Authorization") != "Bearer clerk-alice" {
					t.Errorf("search used the wrong account: %q", r.Header.Get("Authorization"))
				}
				switch r.URL.Path {
				case queryPath:
					queries++
					if want := (api.Object{"key": keyBase64(key), "query": "needle", "limit": float64(limit)}); !reflect.DeepEqual(in, want) {
						t.Errorf("search payload: %v, want %v", in, want)
					}
					ids, indexing := tc.initialIDs, tc.needsReindex
					if queries > 1 {
						ids, indexing = []string{"indexed"}, tc.stillIndexing
					}
					results := []api.Object{}
					for _, id := range ids {
						results = append(results, api.Object{"id": id})
					}
					return syncTestResponse(http.StatusOK, api.Object{"results": results, "needs_reindex": indexing})
				case reindexPath:
					if want := api.Parsed(api.Raw(api.Object{"keys": []api.Object{{"key": keyBase64(key)}}})); !reflect.DeepEqual(api.Parsed(api.Raw(in)), want) {
						t.Errorf("rebuild payload: %v, want %v", in, want)
					}
					return syncTestResponse(http.StatusOK, tc.job)
				default:
					t.Errorf("unexpected sync request: %s", r.URL.Path)
					return syncTestResponse(http.StatusNotFound, api.Object{"code": "NOT_FOUND"})
				}
			})}}
			w := postRoute(h, "/v1/threads/search", api.Object{"key": keyBase64(key), "query": "needle", "limit": limit}, "alice")
			if w.Code != http.StatusOK {
				t.Fatalf("search: HTTP %d: %s", w.Code, w.Body.String())
			}
			var got struct {
				Results  []threadSummary `json:"results"`
				Indexing bool            `json:"indexing"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			want := []threadSummary{{ID: tc.wantID, Title: tc.wantID + " conversation", TitleState: "manual", CreatedAt: &created, UpdatedAt: &created, ActivityAt: created, Model: "gpt-oss-120b", MessageCount: 1}}
			if !reflect.DeepEqual(got.Results, want) || got.Indexing != tc.wantIndexing {
				t.Errorf("search returned %s, want results %s with indexing=%v", w.Body.String(), api.Raw(want), tc.wantIndexing)
			}
			if !slices.Equal(paths, tc.wantPaths) {
				t.Errorf("sync requests: %v, want %v", paths, tc.wantPaths)
			}
		})
	}
}

func TestSearchReportsRebuildAndRefreshFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		job        api.Object
		refreshErr bool
		wantStatus int
		wantCode   string
		wantCalls  int
	}{
		{name: "failed rebuild", job: api.Object{"status": "failed", "error": "embedding failed", "partial": true}, wantStatus: http.StatusBadGateway, wantCode: "SEARCH_INDEX_FAILED", wantCalls: 2},
		{name: "failed refreshed query", job: api.Object{"status": "completed"}, refreshErr: true, wantStatus: http.StatusConflict, wantCode: "KEY_MISMATCH", wantCalls: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, key := testChatHarness(t, nil)
			calls, queries := 0, 0
			h.syncAPI = &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				switch r.URL.Path {
				case "/v1/search/query":
					queries++
					if queries > 1 && tc.refreshErr {
						return syncTestResponse(http.StatusConflict, api.Object{"code": "KEY_MISMATCH"})
					}
					return syncTestResponse(http.StatusOK, api.Object{"results": []api.Object{}, "needs_reindex": true})
				case "/v1/search/reindex":
					return syncTestResponse(http.StatusOK, tc.job)
				default:
					t.Errorf("unexpected sync request: %s", r.URL.Path)
					return syncTestResponse(http.StatusNotFound, api.Object{"code": "NOT_FOUND"})
				}
			})}}
			w := postRoute(h, "/v1/threads/search", api.Object{"key": keyBase64(key), "query": "needle"}, "alice")
			var got api.Object
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.wantStatus || api.Obj(got["error"])["code"] != tc.wantCode || got["results"] != nil || calls != tc.wantCalls {
				t.Fatalf("search answered HTTP %d %s after %d sync requests", w.Code, w.Body.String(), calls)
			}
		})
	}
}

// A search shows the chats it found in their order, and counts the ones that
// match but cannot be opened instead of failing on them. A chat deleted since
// it was indexed is neither. A chat sync could not fetch in time fails the
// search, which may find it when run again.
func TestSearchCountsTheChatsItCannotOpen(t *testing.T) {
	stored := func(id string, version int) string {
		return base64.StdEncoding.EncodeToString(api.Raw(api.Object{"id": id, "title": id, "titleState": "manual", "model": "auto", "presetId": nil, "pinned": false, "webSearchEnabled": true, "messages": []any{}, "recordVersion": version}))
	}
	for _, tc := range []struct {
		name      string
		slow      string
		wantCode  int
		wantFound []string
	}{
		{name: "every chat fetched", wantCode: http.StatusOK, wantFound: []string{"second", "first"}},
		{name: "a chat not fetched in time", slow: "second", wantCode: http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, key := testChatHarness(t, nil)
			pulls := 0
			h.syncAPI = &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
				var in api.Object
				json.NewDecoder(r.Body).Decode(&in)
				if r.URL.Path == "/v1/search/query" {
					results := []api.Object{}
					for _, id := range []string{"second", "gone", "sealed", "broken", "later", "first"} {
						results = append(results, api.Object{"id": id})
					}
					return syncTestResponse(http.StatusOK, api.Object{"results": results})
				}
				pulls++
				items := []api.Object{}
				for _, value := range api.Arr(in["ids"]) {
					switch id := api.Str(value); id {
					case tc.slow:
						items = append(items, api.Object{"id": id, "ok": false, "code": "NETWORK"})
					case "gone":
						items = append(items, api.Object{"id": id, "ok": false, "code": "NOT_FOUND"})
					case "sealed":
						items = append(items, api.Object{"id": id, "ok": false, "code": "UNKNOWN_KEY", "key_id": "retired"})
					case "broken":
						items = append(items, api.Object{"id": id, "ok": false, "code": "BAD_REQUEST"})
					case "later":
						items = append(items, api.Object{"id": id, "ok": true, "etag": "1", "plaintext": stored(id, recordVersion+1)})
					default:
						items = append(items, api.Object{"id": id, "ok": true, "etag": "1", "plaintext": stored(id, recordVersion)})
					}
				}
				// Sync answers in an order of its own.
				slices.Reverse(items)
				return syncTestResponse(http.StatusOK, api.Object{"items": items})
			})}}
			h.storeRows = h.syncAPI
			w := postRoute(h, "/v1/threads/search", api.Object{"key": keyBase64(key), "query": "needle"}, "alice")
			var got struct {
				Results    []threadSummary `json:"results"`
				Unreadable int             `json:"unreadable"`
				Error      api.Object      `json:"error"`
			}
			json.Unmarshal(w.Body.Bytes(), &got)
			found := []string{}
			for _, result := range got.Results {
				found = append(found, result.ID)
			}
			if tc.slow != "" {
				if w.Code != tc.wantCode || got.Error["code"] != "UPSTREAM_REFUSED" || got.Error["action"] != "retry" || pulls != 2 {
					t.Fatalf("search answered HTTP %d %s after %d pulls", w.Code, w.Body, pulls)
				}
				return
			}
			if w.Code != tc.wantCode || !slices.Equal(found, tc.wantFound) || got.Unreadable != 3 || pulls != 1 {
				t.Fatalf("search answered HTTP %d %s after %d pulls", w.Code, w.Body, pulls)
			}
		})
	}
}
