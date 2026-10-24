package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"slices"
	"testing"
)

func TestSearchRefreshesResultsAfterACompletedRebuild(t *testing.T) {
	const queryPath = "/v1/search/query"
	const reindexPath = "/v1/search/reindex"
	const limit = 7
	for _, tc := range []struct {
		name          string
		needsReindex  bool
		job           object
		initialIDs    []string
		stillIndexing bool
		wantID        string
		wantIndexing  bool
		wantPaths     []string
	}{
		{name: "healthy index", initialIDs: []string{"indexed"}, wantID: "indexed", wantPaths: []string{queryPath}},
		{name: "completed rebuild", needsReindex: true, job: object{"status": "completed"}, wantID: "indexed", wantPaths: []string{queryPath, reindexPath, queryPath}},
		{name: "running rebuild", needsReindex: true, job: object{"status": "running"}, initialIDs: []string{"partial"}, wantID: "partial", wantIndexing: true, wantPaths: []string{queryPath, reindexPath}},
		{name: "resumable checkpoint", needsReindex: true, job: object{"status": "completed", "partial": true}, initialIDs: []string{"partial"}, wantID: "partial", wantIndexing: true, wantPaths: []string{queryPath, reindexPath}},
		{name: "index invalidated again", needsReindex: true, job: object{"status": "completed"}, stillIndexing: true, wantID: "indexed", wantIndexing: true, wantPaths: []string{queryPath, reindexPath, queryPath}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, rows, key := testChatHarness(t, nil)
			created := "2026-10-22T12:00:00Z"
			for _, id := range []string{"partial", "indexed"} {
				rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", id)] = storedRow{ID: id, ETag: "1", Data: raw(Thread{
					ID: id, Title: id + " conversation", TitleState: "manual", CreatedAt: created, UpdatedAt: created, Model: "gpt-oss-120b",
					Messages: []Message{{ID: id + "-message", Role: "user", Content: "Find this conversation", CreatedAt: created}},
				})}
			}
			var paths []string
			queries := 0
			h.syncAPI = &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
				paths = append(paths, r.URL.Path)
				var in object
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Fatal(err)
				}
				if r.Header.Get("Authorization") != "Bearer clerk-alice" {
					t.Errorf("search used the wrong account: %q", r.Header.Get("Authorization"))
				}
				switch r.URL.Path {
				case queryPath:
					queries++
					if want := (object{"key": key.base64(), "query": "needle", "limit": float64(limit)}); !reflect.DeepEqual(in, want) {
						t.Errorf("search payload: %v, want %v", in, want)
					}
					ids, indexing := tc.initialIDs, tc.needsReindex
					if queries > 1 {
						ids, indexing = []string{"indexed"}, tc.stillIndexing
					}
					results := []object{}
					for _, id := range ids {
						results = append(results, object{"id": id})
					}
					return syncTestResponse(http.StatusOK, object{"results": results, "needs_reindex": indexing})
				case reindexPath:
					if want := decodeValue(raw(object{"keys": []object{{"key": key.base64()}}})); !reflect.DeepEqual(decodeValue(raw(in)), want) {
						t.Errorf("rebuild payload: %v, want %v", in, want)
					}
					return syncTestResponse(http.StatusOK, tc.job)
				default:
					t.Errorf("unexpected sync request: %s", r.URL.Path)
					return syncTestResponse(http.StatusNotFound, object{"code": "NOT_FOUND"})
				}
			})}}
			w := postRoute(h, "/v1/threads/search", object{"key": key.base64(), "query": "needle", "limit": limit}, "alice")
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
				t.Errorf("search returned %s, want results %s with indexing=%v", w.Body.String(), raw(want), tc.wantIndexing)
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
		job        object
		refreshErr bool
		wantStatus int
		wantCode   string
		wantCalls  int
	}{
		{name: "failed rebuild", job: object{"status": "failed", "error": "embedding failed", "partial": true}, wantStatus: http.StatusBadGateway, wantCode: "SEARCH_INDEX_FAILED", wantCalls: 2},
		{name: "failed refreshed query", job: object{"status": "completed"}, refreshErr: true, wantStatus: http.StatusConflict, wantCode: "KEY_MISMATCH", wantCalls: 3},
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
						return syncTestResponse(http.StatusConflict, object{"code": "KEY_MISMATCH"})
					}
					return syncTestResponse(http.StatusOK, object{"results": []object{}, "needs_reindex": true})
				case "/v1/search/reindex":
					return syncTestResponse(http.StatusOK, tc.job)
				default:
					t.Errorf("unexpected sync request: %s", r.URL.Path)
					return syncTestResponse(http.StatusNotFound, object{"code": "NOT_FOUND"})
				}
			})}}
			w := postRoute(h, "/v1/threads/search", object{"key": key.base64(), "query": "needle"}, "alice")
			var got object
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.wantStatus || obj(got["error"])["code"] != tc.wantCode || got["results"] != nil || calls != tc.wantCalls {
				t.Fatalf("search answered HTTP %d %s after %d sync requests", w.Code, w.Body.String(), calls)
			}
		})
	}
}
