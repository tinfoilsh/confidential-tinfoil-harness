package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/search"
)

const searchTestCreated = "2026-10-22T12:00:00Z"

// searchTestThread is a stored chat with one message of text.
func searchTestThread(id, text string) domain.Thread {
	return domain.Thread{
		ID: id, Title: id + " conversation", TitleState: "manual", CreatedAt: searchTestCreated, UpdatedAt: searchTestCreated, Model: "gpt-oss-120b",
		Messages: []domain.Message{{ID: id + "-message", Role: "user", Content: text, CreatedAt: searchTestCreated}},
	}
}

// searchTestSummary is how a search lists searchTestThread.
func searchTestSummary(id string) domain.ThreadSummary {
	created := searchTestCreated
	return domain.ThreadSummary{ID: id, Title: id + " conversation", TitleState: "manual", CreatedAt: &created, UpdatedAt: &created, ActivityAt: created, Model: "gpt-oss-120b", MessageCount: 1}
}

// seedChat stores a chat for alice without indexing it: the row, which
// the control plane also knows at its revision, and the source revision
// its write advanced.
func seedChat(rows *memoryRows, id, text string) (data []byte, revision int64) {
	p := &auth.Principal{ID: "alice"}
	data = api.Raw(searchTestThread(id, text))
	rows.mu.Lock()
	rows.rows[rowMapKey(p, "chat", id)] = domain.StoredRow{ID: id, ETag: "1", Data: data}
	rows.mu.Unlock()
	rows.cp.SetBlob("chat", id, cpstub.Blob{ETag: 1})
	rows.cp.Mu.Lock()
	rows.cp.SourceRevision++
	revision = rows.cp.SourceRevision
	rows.cp.Mu.Unlock()
	return data, revision
}

// storeChat stores a chat for alice as a push does: indexed at the
// revision its write left, while the harness holds its text and key.
func storeChat(t *testing.T, h *harness, rows *memoryRows, key auth.ContentKey, id, text string) {
	t.Helper()
	data, revision := seedChat(rows, id, text)
	if err := h.search.ChatStored(context.Background(), &auth.Principal{ID: "alice"}, key, id, data, "1", api.Now(), revision); err != nil {
		t.Fatalf("index %s: %v", id, err)
	}
}

// searchHarness is a chat harness whose key is the one the control plane
// publishes alice's search index under.
func searchHarness(t *testing.T) (*harness, *memoryRows, auth.ContentKey) {
	t.Helper()
	h, rows, key := testChatHarness(t, nil)
	rows.cp.Mu.Lock()
	rows.cp.CurrentKeyID = keyIDOf(key)
	rows.cp.Mu.Unlock()
	return h, rows, key
}

// searchFor is alice's search for query, as the route answers it.
func searchFor(t *testing.T, h *harness, key auth.ContentKey, query string) (results []domain.ThreadSummary, indexing bool, w *httptest.ResponseRecorder) {
	t.Helper()
	w = postRoute(h, "/v1/threads/search", api.Object{"key": keyBase64(key), "query": query, "limit": 7}, "alice")
	if w.Code != http.StatusOK {
		return nil, false, w
	}
	var got struct {
		Results  []domain.ThreadSummary `json:"results"`
		Indexing bool                   `json:"indexing"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got.Results, got.Indexing, w
}

// rebuildEnded waits for alice's rebuild to end and is its status.
func rebuildEnded(t *testing.T, h *harness) jobs.Status[search.RebuildReport] {
	t.Helper()
	job, started := h.search.Rebuilds.Start(context.Background(), "alice", func(context.Context, func(search.RebuildReport)) (search.RebuildReport, error) {
		return search.RebuildReport{}, nil
	})
	if started {
		t.Fatal("no rebuild was running for alice")
	}
	select {
	case <-job.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the rebuild did not end")
	}
	return job.Status()
}

func TestSearchAnswersFromAHealthyIndex(t *testing.T) {
	h, rows, key := searchHarness(t)
	storeChat(t, h, rows, key, "indexed", "find this needle in the haystack")
	storeChat(t, h, rows, key, "other", "nothing of note")
	results, indexing, w := searchFor(t, h, key, "needle")
	if w.Code != http.StatusOK || indexing || len(results) == 0 || !reflect.DeepEqual(results[0], searchTestSummary("indexed")) {
		t.Fatalf("search answered HTTP %d %s, want the chat with the word first and no indexing", w.Code, w.Body.String())
	}
}

func TestSearchRebuildsAnIndexThatDoesNotCoverTheChats(t *testing.T) {
	h, rows, key := searchHarness(t)
	// Chats stored before any index was built.
	for _, id := range []string{"partial", "indexed"} {
		seedChat(rows, id, "find this needle")
	}

	// The first search starts the rebuild and answers what the index has.
	results, indexing, w := searchFor(t, h, key, "needle")
	if w.Code != http.StatusOK || !indexing || len(results) != 0 {
		t.Fatalf("search over no index answered HTTP %d %s, want nothing and indexing under way", w.Code, w.Body.String())
	}
	if status := rebuildEnded(t, h); status.State != jobs.Complete || status.Report.Partial || status.Report.TotalIndexed != 2 {
		t.Fatalf("the rebuild = %+v, want complete over both chats", status)
	}
	// The next search answers from the rebuilt index.
	results, indexing, w = searchFor(t, h, key, "needle")
	ids := []string{}
	for _, result := range results {
		ids = append(ids, result.ID)
	}
	slices.Sort(ids)
	if w.Code != http.StatusOK || indexing || !slices.Equal(ids, []string{"indexed", "partial"}) {
		t.Fatalf("search after the rebuild answered HTTP %d %s, want both chats and no indexing", w.Code, w.Body.String())
	}
}

func TestSearchReportsIndexingWhileARebuildRunsOrEndedPartial(t *testing.T) {
	h, rows, key := searchHarness(t)
	storeChat(t, h, rows, key, "partial", "find this needle")
	// A later chat the index does not cover yet.
	seedChat(rows, "indexed", "find this needle")
	// A rebuild within an expired budget stops before its first page.
	h.search.Rebuilds = jobs.New[search.RebuildReport](-time.Second, search.RebuildRetention)

	results, indexing, w := searchFor(t, h, key, "needle")
	if w.Code != http.StatusOK || !indexing || len(results) != 1 || results[0].ID != "partial" {
		t.Fatalf("search during the rebuild answered HTTP %d %s, want the chat indexed so far and indexing under way", w.Code, w.Body.String())
	}
	if status := rebuildEnded(t, h); status.State != jobs.Complete || !status.Report.Partial {
		t.Fatalf("the rebuild = %+v, want it ended partial", status)
	}
	if _, indexing, w := searchFor(t, h, key, "needle"); w.Code != http.StatusOK || !indexing {
		t.Fatalf("search after a partial rebuild answered HTTP %d %s, want indexing still under way", w.Code, w.Body.String())
	}
}

func TestSearchReportsAFailedRebuild(t *testing.T) {
	h, rows, key := searchHarness(t)
	storeChat(t, h, rows, key, "indexed", "find this needle")
	// A chat sealed under another key, which the rebuild cannot read.
	seedChat(rows, "sealed", "find this needle")
	rows.sealed[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "sealed")] = randomKey()

	if _, indexing, w := searchFor(t, h, key, "needle"); w.Code != http.StatusOK || !indexing {
		t.Fatalf("search answered HTTP %d %s, want indexing under way", w.Code, w.Body.String())
	}
	if status := rebuildEnded(t, h); status.State != jobs.Failed {
		t.Fatalf("the rebuild = %+v, want it failed over the chat it cannot read", status)
	}
	_, _, w := searchFor(t, h, key, "needle")
	var got api.Object
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusBadGateway || api.Obj(got["error"])["code"] != "SEARCH_INDEX_FAILED" || got["results"] != nil {
		t.Fatalf("search after the failed rebuild answered HTTP %d %s", w.Code, w.Body.String())
	}
	// The failure was reported; the next search starts over.
	if _, indexing, w := searchFor(t, h, key, "needle"); w.Code != http.StatusOK || !indexing {
		t.Fatalf("search after the failure was reported answered HTTP %d %s, want a rebuild under way", w.Code, w.Body.String())
	}
}

// A search shows the chats it found in their order, and counts the ones that
// match but cannot be opened instead of failing on them. A chat deleted since
// it was indexed is neither. A chat the store could not fetch in time fails
// the search, which may find it when run again.
func TestSearchCountsTheChatsItCannotOpen(t *testing.T) {
	for _, tc := range []struct {
		name      string
		unfetched string
		wantCode  int
	}{
		{name: "every chat fetched", wantCode: http.StatusOK},
		{name: "a chat not fetched in time", unfetched: "second", wantCode: http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, rows, key := searchHarness(t)
			p := &auth.Principal{ID: "alice"}
			for _, id := range []string{"first", "second", "gone", "sealed", "broken", "later"} {
				storeChat(t, h, rows, key, id, "find this needle")
			}
			// Since they were indexed: one chat was deleted, one is sealed
			// under another key, one is no longer a chat, and one was
			// written by a newer harness.
			delete(rows.rows, rowMapKey(p, "chat", "gone"))
			rows.sealed[rowMapKey(p, "chat", "sealed")] = randomKey()
			broken := rows.rows[rowMapKey(p, "chat", "broken")]
			broken.Data = []byte("[]")
			rows.rows[rowMapKey(p, "chat", "broken")] = broken
			later := rows.rows[rowMapKey(p, "chat", "later")]
			editRow(&later, func(data api.Object) { data["recordVersion"] = domain.RecordVersion + 1 })
			rows.rows[rowMapKey(p, "chat", "later")] = later
			rows.unfetched = tc.unfetched

			results, indexing, w := searchFor(t, h, key, "needle")
			if tc.unfetched != "" {
				var got api.Object
				json.Unmarshal(w.Body.Bytes(), &got)
				if w.Code != tc.wantCode || api.Obj(got["error"])["code"] != "UPSTREAM_REFUSED" || api.Obj(got["error"])["action"] != "retry" {
					t.Fatalf("search answered HTTP %d %s", w.Code, w.Body)
				}
				return
			}
			var got struct {
				Unreadable int `json:"unreadable"`
			}
			json.Unmarshal(w.Body.Bytes(), &got)
			found := []string{}
			for _, result := range results {
				found = append(found, result.ID)
			}
			slices.Sort(found)
			if w.Code != tc.wantCode || indexing || !slices.Equal(found, []string{"first", "second"}) || got.Unreadable != 3 {
				t.Fatalf("search answered HTTP %d %s", w.Code, w.Body)
			}
		})
	}
}
