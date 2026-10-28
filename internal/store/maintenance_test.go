package store

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets/bucketstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
)

// bucketsRefusing puts buckets behind a proxy that refuses the delete of
// the objects named.
func (h *rowsHarness) bucketsRefusing(t *testing.T, deletesOf ...string) {
	t.Helper()
	target, err := url.Parse(h.bkURL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorLog = log.New(io.Discard, "", 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && slices.ContainsFunc(deletesOf, func(id string) bool { return strings.HasSuffix(r.URL.Path, "/"+id) }) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	h.store.Buckets = buckets.NewClient(server.URL, testBucket, nil)
}

// purges are the attachments still queued for deletion.
func (h *rowsHarness) purges() []string {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	ids := []string{}
	for _, purge := range h.cp.AttachmentPurges {
		ids = append(ids, purge.AttachmentID)
	}
	return ids
}

func TestThePurgeSweepDeletesWhatNoChatNamesAndAcknowledgesOnlyWhatItDeleted(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	// The chat's last write names one of its three attachments; the
	// other two are unreferenced, and a fourth chat's is still named.
	h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, Body: []byte("sealed"), AttachmentRefs: []string{"att-kept"}})
	for _, id := range []string{"att-kept", "att-dropped", "att-stuck"} {
		h.seedAttachment(t, "c1", id, []byte("bytes of "+id))
	}
	h.bucketsRefusing(t, "att-stuck")
	deleted, err := h.store.purgeUnreferenced(ctx)
	if deleted != 1 || err == nil || !strings.Contains(err.Error(), "att-stuck") {
		t.Fatalf("sweep = (%d, %v), want one deleted and the refused delete reported", deleted, err)
	}
	if h.bk.Has("att-dropped") || !h.bk.Has("att-kept") || !h.bk.Has("att-stuck") {
		t.Fatalf("buckets holds dropped=%v kept=%v stuck=%v, want only the unreferenced one buckets allowed deleted", h.bk.Has("att-dropped"), h.bk.Has("att-kept"), h.bk.Has("att-stuck"))
	}
	h.cp.Mu.Lock()
	_, keptIndexed := h.cp.AttachmentIndex["att-kept"]
	_, droppedIndexed := h.cp.AttachmentIndex["att-dropped"]
	h.cp.Mu.Unlock()
	if !keptIndexed || droppedIndexed || !slices.Equal(h.purges(), []string{"att-stuck"}) {
		t.Fatalf("index kept=%v dropped=%v, queue %v, want the named attachment indexed and the refused delete still queued", keptIndexed, droppedIndexed, h.purges())
	}
	// A later sweep, once the lease has lapsed and buckets allows the
	// delete, finishes it.
	h.store.Buckets = h.rows.Buckets
	h.cp.LapsePurgeLeases()
	if deleted, err := h.store.purgeUnreferenced(ctx); deleted != 1 || err != nil || h.bk.Has("att-stuck") || len(h.purges()) != 0 {
		t.Fatalf("the later sweep = (%d, %v), buckets holds the attachment=%v, queue %v", deleted, err, h.bk.Has("att-stuck"), h.purges())
	}
}

func TestThePendingAndOrphanSweepsDeleteWhatTheControlPlaneReleases(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	tenant, _ := buckets.TenantForUser(h.p.ID)
	for _, id := range []string{"att-pending", "att-orphan", "att-live"} {
		h.bk.Put(id, bucketstub.Item{Tenant: tenant, Value: []byte("bytes"), EncryptionKeys: [][]byte{make([]byte, attachmentKeySize)}})
	}
	h.cp.Mu.Lock()
	h.cp.PendingAttachmentWrites = []controlplane.PendingAttachmentRow{{AttachmentID: "att-pending", ChatID: "c1", ClerkUserID: h.p.ID}}
	h.cp.OrphanAttachments = []controlplane.OrphanAttachmentRow{{AttachmentID: "att-orphan", ClerkUserID: h.p.ID}}
	h.cp.AttachmentIndex["att-orphan"], h.cp.AttachmentIndex["att-live"] = "gone", "c1"
	h.cp.Mu.Unlock()
	if deleted, err := h.store.releasePending(ctx); deleted != 1 || err != nil || h.bk.Has("att-pending") {
		t.Fatalf("the pending sweep = (%d, %v), buckets holds the write=%v", deleted, err, h.bk.Has("att-pending"))
	}
	if deleted, err := h.store.dropOrphans(ctx); deleted != 1 || err != nil || h.bk.Has("att-orphan") {
		t.Fatalf("the orphan sweep = (%d, %v), buckets holds the orphan=%v", deleted, err, h.bk.Has("att-orphan"))
	}
	h.cp.Mu.Lock()
	_, orphanIndexed := h.cp.AttachmentIndex["att-orphan"]
	released, orphans := len(h.cp.PendingAttachmentWrites), len(h.cp.OrphanAttachments)
	h.cp.Mu.Unlock()
	if orphanIndexed || released != 0 || orphans != 0 || !h.bk.Has("att-live") {
		t.Fatalf("after the sweeps: orphan indexed=%v, %d pending and %d orphans left, live attachment held=%v", orphanIndexed, released, orphans, h.bk.Has("att-live"))
	}
	if deleted, err := h.store.releasePending(ctx); deleted != 0 || err != nil {
		t.Fatalf("a sweep with nothing released = (%d, %v)", deleted, err)
	}
}
