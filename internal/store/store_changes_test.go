package store

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
)

// A client keeps the tokens it was given and compares them with the next
// ones, so what each digests cannot change under it.
func TestChangeTokensDigestTheLedgersEdgeAsClientsHoldThem(t *testing.T) {
	h := newRowsHarness(t)
	at := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	h.cp.SetBlob("profile", "profile", cpstub.Blob{ETag: 3, Body: []byte("sealed"), UpdatedAt: at})
	h.cp.SetBlob("project", "p1", cpstub.Blob{ETag: 1, Body: []byte("sealed"), UpdatedAt: at})
	h.cp.SetBlob("project", "p2", cpstub.Blob{ETag: 2, Body: []byte("sealed"), UpdatedAt: at.Add(time.Minute)})
	h.cp.Mu.Lock()
	h.cp.SourceRevision = 42
	h.cp.Mu.Unlock()
	tokens, err := h.store.StorageChanges(context.Background(), h.p)
	if err != nil {
		t.Fatal(err)
	}
	profile := api.HashHex([]byte(`{"updates":[{"id":"profile","etag":"3","updated_at":"2026-01-02T03:04:05.000000006Z"}],"deletes":null}`))
	projects := api.HashHex([]byte(`{"updates":[{"id":"p2","etag":"2","updated_at":"2026-01-02T03:05:05.000000006Z"}],"deletes":null}`))
	documents := api.HashHex([]byte(`{"updates":null,"deletes":null}`))
	want := map[string]any{"chats": "42", "profile": profile, "projects": api.HashHex([]byte(projects + documents))}
	if got := map[string]any(api.Obj(api.Parsed(api.Raw(tokens)))); !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens = %v, want %v", got, want)
	}
}
