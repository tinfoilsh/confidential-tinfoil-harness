package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// sealV2 seals a row in the current format under key, as a push does.
func (h *rowsHarness) sealV2(t *testing.T, key auth.ContentKey, scope, id, plaintext string) []byte {
	t.Helper()
	envelope, err := seal.Encrypt(key[:], []byte(plaintext), seal.AAD{KeyIDHex: keyID(key), Scope: seal.Scope(scope), ID: aadID(scope, id), ClerkUserID: h.p.ID})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

// registered makes kid the caller's registered current key.
func (h *rowsHarness) registered(kid string) {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	h.cp.CurrentKeyID = kid
}

// resealEnded waits for the caller's reseal to end, then asks for it
// again, which answers with the ended job's report. A repeated start finds
// the job the caller has.
func (h *rowsHarness) resealEnded(t *testing.T, keys []auth.ContentKey, target auth.ContentKey) ResealReport {
	t.Helper()
	job, started := h.store.Reseals.Start(context.Background(), h.p.ID, func(context.Context, func(ResealReport)) (ResealReport, error) { return ResealReport{}, nil })
	if started {
		t.Fatal("no reseal was running for the caller")
	}
	<-job.Done()
	report, err := h.store.Reseal(context.Background(), h.p, keys, target)
	if err != nil {
		t.Fatalf("reseal after it ended: %v", err)
	}
	return report
}

func TestResealSealsRowsAnOlderKeyOrNoneSealedUnderTheTarget(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	old := randomKey()
	h.registered(h.kid)
	rows := map[[2]string]string{
		{"profile", "profile"}:        `{"theme":"dark"}`,
		{"chat", "c1"}:                `{"id":"c1","messages":[]}`,
		{"project", "p1"}:             `{"name":"House"}`,
		{"project_document", "p1/d1"}: `{"content":"Context"}`,
	}
	h.cp.SetBlob("profile", "profile", cpstub.Blob{ETag: 1, Body: sealLegacyV1(t, old, []byte(rows[[2]string{"profile", "profile"}]))})
	h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, KeyID: keyID(old), Body: h.sealV2(t, old, "chat", "c1", rows[[2]string{"chat", "c1"}])})
	h.cp.SetBlob("project", "p1", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(rows[[2]string{"project", "p1"}]))})
	h.cp.SetBlob("project_document", "p1/d1", cpstub.Blob{ETag: 1, KeyID: keyID(old), Body: h.sealV2(t, old, "project_document", "p1/d1", rows[[2]string{"project_document", "p1/d1"}])})
	h.push(t, "chat", "c2", `{"id":"c2"}`)

	keys := []auth.ContentKey{old, h.key}
	first, err := h.store.Reseal(ctx, h.p, keys, h.key)
	if err != nil || first.Status != jobs.Running && first.Status != jobs.Complete {
		t.Fatalf("reseal = %+v (%v)", first, err)
	}
	if report := h.resealEnded(t, keys, h.key); report != (ResealReport{Status: jobs.Complete, Migrated: 4}) {
		t.Fatalf("report = %+v, want every row of every scope resealed and nothing left", report)
	}
	for row, plaintext := range rows {
		scope, id := row[0], row[1]
		if stored := h.stored(scope, id); stored.ETag != 2 || stored.KeyID != h.kid {
			t.Errorf("%s/%s = %+v, want one reseal under %s", scope, id, stored, h.kid)
		}
		if opened := h.opened(t, scope, id); string(opened) != plaintext {
			t.Errorf("%s/%s opens to %s, want %s", scope, id, opened, plaintext)
		}
	}
	if stored := h.stored("chat", "c2"); stored.ETag != 1 {
		t.Fatalf("a row already under the target was written again: %+v", stored)
	}
}

func TestResealBlocksARowNoKeyOpensAndRecordsIt(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.registered(h.kid)
	h.cp.SetBlob("chat", "locked", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, randomKey(), []byte(`{}`))})
	h.cp.SetBlob("chat", "open", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(`{"id":"open"}`))})
	keys := []auth.ContentKey{h.key}
	if _, err := h.store.Reseal(ctx, h.p, keys, h.key); err != nil {
		t.Fatal(err)
	}
	if report := h.resealEnded(t, keys, h.key); report != (ResealReport{Status: jobs.Complete, Migrated: 1, Blocked: 1}) {
		t.Fatalf("report = %+v, want one row resealed and one blocked", report)
	}
	h.cp.Mu.Lock()
	recorded := h.cp.MigrationFailures["chat/locked"]
	h.cp.Mu.Unlock()
	if recorded != 1 || seal.Detect(h.stored("chat", "locked").Body) != seal.VersionV0 || h.stored("chat", "locked").ETag != 1 {
		t.Fatalf("the locked row was recorded %d times and is %+v, want it recorded once and left as it was", recorded, h.stored("chat", "locked"))
	}
	if stored := h.stored("chat", "open"); stored.KeyID != h.kid {
		t.Fatalf("the row the key opens was not resealed: %+v", stored)
	}
}

func TestASecondResealWhileOneRunsJoinsIt(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.registered(h.kid)
	h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(`{"id":"c1"}`))})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var listings atomic.Int32
	h.cp.Mu.Lock()
	h.cp.CaptureHeaders = func(r *http.Request) {
		switch r.URL.Path {
		case "/api/sync/needs-migration":
			listings.Add(1)
		case controlplane.RewrapPath:
			once.Do(func() { close(entered) })
			<-release
		}
	}
	h.cp.Mu.Unlock()
	keys := []auth.ContentKey{h.key}
	first, err := h.store.Reseal(ctx, h.p, keys, h.key)
	if err != nil || first.Status != jobs.Running {
		t.Fatalf("the first call = %+v (%v), want a running job", first, err)
	}
	<-entered
	if second, err := h.store.Reseal(ctx, h.p, keys, h.key); err != nil || second.Status != jobs.Running {
		t.Fatalf("the second call = %+v (%v), want the running job", second, err)
	}
	close(release)
	if report := h.resealEnded(t, keys, h.key); report != (ResealReport{Status: jobs.Complete, Migrated: 1}) {
		t.Fatalf("report = %+v", report)
	}
	// One job lists each scope once; a second would have listed them again.
	if listings.Load() != int32(len(resealScopes)) {
		t.Fatalf("the control plane listed rows %d times, want %d for one job", listings.Load(), len(resealScopes))
	}
}

func TestResealCountsARowAnotherWriterSealedUnderTheTarget(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.registered(h.kid)
	h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(`{"id":"c1"}`))})
	// Between the read and the reseal, another writer seals the row
	// under the target; the reseal at the old revision is refused.
	var swapped sync.Once
	replacement := h.sealV2(t, h.key, "chat", "c1", `{"id":"c1","v":2}`)
	h.cp.Mu.Lock()
	h.cp.CaptureHeaders = func(r *http.Request) {
		if r.URL.Path != controlplane.RewrapPath {
			return
		}
		swapped.Do(func() {
			h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 2, KeyID: h.kid, Body: replacement})
		})
	}
	h.cp.Mu.Unlock()
	keys := []auth.ContentKey{h.key}
	if _, err := h.store.Reseal(ctx, h.p, keys, h.key); err != nil {
		t.Fatal(err)
	}
	if report := h.resealEnded(t, keys, h.key); report != (ResealReport{Status: jobs.Complete, Migrated: 1}) {
		t.Fatalf("report = %+v, want the row counted as resealed", report)
	}
	h.cp.Mu.Lock()
	recorded := len(h.cp.MigrationFailures)
	h.cp.Mu.Unlock()
	if stored := h.stored("chat", "c1"); stored.ETag != 2 || recorded != 0 {
		t.Fatalf("the row is at revision %d with %d failures recorded, want the other writer's revision kept and no failure", stored.ETag, recorded)
	}
}

// A reseal the caller's credential cuts short between pages still reports
// what the pages it did count: the rows resealed, and the rows the control
// plane said were left.
func TestResealCutShortReportsWhatItsLastPageCounted(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.registered(h.kid)
	for i := 0; i <= resealPage; i++ {
		id := fmt.Sprintf("c%03d", i)
		h.cp.SetBlob("chat", id, cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(`{"id":"`+id+`"}`))})
	}
	h.cp.Mu.Lock()
	// After the profile's listing and the chats' first, the chats' second
	// listing, which would name the row the first page left, is refused.
	h.cp.NeedsMigrationFailures = map[int]int{2: http.StatusUnauthorized}
	h.cp.Mu.Unlock()
	keys := []auth.ContentKey{h.key}
	if _, err := h.store.Reseal(ctx, h.p, keys, h.key); err != nil {
		t.Fatal(err)
	}
	if report := h.resealEnded(t, keys, h.key); report != (ResealReport{Status: jobs.Complete, Migrated: resealPage, Remaining: 1, Partial: true}) {
		t.Fatalf("report = %+v, want the first page resealed, one row left and the reseal partial", report)
	}
}

func TestResealRefusesATargetThatIsNotTheRegisteredKey(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	var listings atomic.Int32
	h.cp.Mu.Lock()
	h.cp.CaptureHeaders = func(r *http.Request) {
		if r.URL.Path == "/api/sync/needs-migration" {
			listings.Add(1)
		}
	}
	h.cp.Mu.Unlock()
	keys := []auth.ContentKey{h.key}
	_, err := h.store.Reseal(ctx, h.p, keys, h.key)
	if e := api.AsError(err); e.Code != "KEY_MISMATCH" || e.KeyID != "" {
		t.Fatalf("reseal with no key registered: %+v, want KEY_MISMATCH naming no key", e)
	}
	other := keyID(randomKey())
	h.registered(other)
	_, err = h.store.Reseal(ctx, h.p, keys, h.key)
	if e := api.AsError(err); e.Code != "KEY_MISMATCH" || e.KeyID != other {
		t.Fatalf("reseal under another registered key: %+v, want KEY_MISMATCH naming %s", e, other)
	}
	if listings.Load() != 0 {
		t.Fatal("a refused reseal listed rows")
	}
}

func TestResealMovesAChatsLegacyAttachmentToBuckets(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.registered(h.kid)
	attID := "123e4567-e89b-12d3-a456-426614174000"
	attKey := make([]byte, legacyAttachmentKeySize)
	rand.Read(attKey)
	chat := `{"id":"c1","messages":[{"id":"m1","role":"user","attachments":[{"id":"` + attID + `","type":"image","encryptionKey":"` + base64.StdEncoding.EncodeToString(attKey) + `"}]}]}`
	h.cp.Mu.Lock()
	h.cp.LegacyAttachments = map[string][]byte{attID: sealLegacyAttachment(t, attKey, []byte("the image"))}
	h.cp.Mu.Unlock()
	h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, Body: sealLegacyV1(t, h.key, []byte(chat))})
	keys := []auth.ContentKey{h.key}
	if _, err := h.store.Reseal(ctx, h.p, keys, h.key); err != nil {
		t.Fatal(err)
	}
	if report := h.resealEnded(t, keys, h.key); report != (ResealReport{Status: jobs.Complete, Migrated: 1}) {
		t.Fatalf("report = %+v", report)
	}
	if string(h.opened(t, "chat", "c1")) != chat {
		t.Fatal("the chat was not resealed as it was")
	}
	item, ok := h.bk.Item(attID)
	if !ok || !bytes.Equal(item.Value, []byte("the image")) || len(item.EncryptionKeys) != 1 || !bytes.Equal(item.EncryptionKeys[0], attKey) {
		t.Fatalf("buckets item = %+v (present=%v), want the attachment under the key the chat embeds", item, ok)
	}
	h.cp.Mu.Lock()
	indexed := h.cp.AttachmentIndex[attID]
	h.cp.Mu.Unlock()
	if indexed != "c1" {
		t.Fatalf("the attachment is indexed under %q, want c1", indexed)
	}
}
