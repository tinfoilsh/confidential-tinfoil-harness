package store

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// countChatReads counts the chats the harness opens from the control plane.
func (h *rowsHarness) countChatReads() *atomic.Int32 {
	var reads atomic.Int32
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	h.cp.CaptureHeaders = func(r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/sync/blob/chat/") {
			reads.Add(1)
		}
	}
	return &reads
}

// listed is the first page of chats in the order the list shows them,
// each by its title and project.
func (h *rowsHarness) listed(t *testing.T, p *auth.Principal, key auth.ContentKey) []string {
	t.Helper()
	summaries, _, err := h.store.ThreadSummaries(context.Background(), p, key, "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for _, summary := range summaries {
		out = append(out, summary.Title+" in "+domain.ValueOr(summary.ProjectID, "no project"))
	}
	return out
}

func TestAListingOpensOnlyTheChatsChangedSinceItsLastRead(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	chat := func(id, title string) []byte {
		return []byte(`{"id":"` + id + `","title":"` + title + `","createdAt":"2026-01-01T00:00:00Z","messages":[]}`)
	}
	project := "p1"
	for _, id := range []string{"c1", "c2", "c3"} {
		if _, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: id, Data: chat(id, "First "+id), Project: &project}, "create-"+id); err != nil {
			t.Fatal(err)
		}
	}
	reads := h.countChatReads()
	// The chats are equally recent, so the control plane lists them by id,
	// last first.
	want := []string{"First c3 in p1", "First c2 in p1", "First c1 in p1"}
	if got := h.listed(t, h.p, h.key); !reflect.DeepEqual(got, want) || reads.Load() != 3 {
		t.Fatalf("first listing = %v after %d reads, want %v after 3", got, reads.Load(), want)
	}
	if got := h.listed(t, h.p, h.key); !reflect.DeepEqual(got, want) || reads.Load() != 3 {
		t.Fatalf("listing unchanged chats again = %v after %d reads, want no more reads", got, reads.Load())
	}

	// Another writer changes c2, and the control plane moves c1 out of a
	// deleted project without a new revision.
	if _, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "c2", ETag: "1", Data: chat("c2", "Second c2"), Project: &project}, "other-writer"); err != nil {
		t.Fatal(err)
	}
	h.cp.Mu.Lock()
	h.cp.Blobs["chat/c1"].ProjectID = nil
	h.cp.Mu.Unlock()
	want[1], want[2] = "Second c2 in p1", "First c1 in no project"
	if got := h.listed(t, h.p, h.key); !reflect.DeepEqual(got, want) || reads.Load() != 4 {
		t.Fatalf("listing after changes = %v after %d reads, want %v after reading c2 alone", got, reads.Load(), want)
	}

	// A change the harness writes is listed without reading the chat again.
	if _, err := h.store.MutateThread(ctx, h.p, h.key, "c3", func(t *domain.Thread) error { t.Title = "Renamed c3"; return nil }); err != nil {
		t.Fatal(err)
	}
	afterWrite := reads.Load()
	want[0] = "Renamed c3 in p1"
	if got := h.listed(t, h.p, h.key); !reflect.DeepEqual(got, want) || reads.Load() != afterWrite {
		t.Fatalf("listing after a write = %v after %d more reads, want %v after none", got, reads.Load()-afterWrite, want)
	}

	// What one caller's key opened is never listed for another key or user.
	if got := h.listed(t, h.p, randomKey()); len(got) != 0 {
		t.Fatalf("another key was listed %v", got)
	}
	if got := h.listed(t, &auth.Principal{ID: "user_other", JWT: "clerk-other"}, h.key); len(got) != 0 {
		t.Fatalf("another user was listed %v", got)
	}
}

func TestAKeptSummarysRunHintLapsesWithTheRun(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.push(t, "chat", "c1", `{"id":"c1","title":"Running","messages":[]}`)
	const runLeft = 200 * time.Millisecond
	expires := api.Now().Add(runLeft)
	if _, err := h.store.MutateThread(ctx, h.p, h.key, "c1", func(t *domain.Thread) error {
		t.ActiveRun = &domain.ActiveRun{RunID: "r1", Instance: "i1", ExpiresAt: expires.Format(time.RFC3339Nano)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reads := h.countChatReads()
	time.Sleep(time.Until(expires))
	summaries, _, err := h.store.ThreadSummaries(ctx, h.p, h.key, "", "", 10)
	if err != nil || len(summaries) != 1 || summaries[0].ActiveRun != nil || reads.Load() != 0 {
		t.Fatalf("listing after the run's deadline = %+v (%v) after %d reads, want the kept summary without its hint", summaries, err, reads.Load())
	}
}
