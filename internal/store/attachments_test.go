package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets/bucketstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// seedAttachment holds an attachment as the harness stores one: its bytes
// in buckets under the caller's tenant and key, and its index entry under
// the chat. Returns the key as a chat embeds it.
func (h *rowsHarness) seedAttachment(t *testing.T, chat, id string, data []byte) string {
	t.Helper()
	tenant, err := buckets.TenantForUser(h.p.ID)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, attachmentKeySize)
	rand.Read(key)
	h.bk.Put(id, bucketstub.Item{Tenant: tenant, Value: data, EncryptionKeys: [][]byte{key}})
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	h.cp.AttachmentIndex[id], h.cp.AttachmentOwners[id] = chat, h.p.ID
	return base64.StdEncoding.EncodeToString(key)
}

func TestPutAttachmentStoresOnceUnderAnIdentityDerivedFromTheUpload(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	data := []byte("the attachment's bytes")
	id, key, err := h.store.PutAttachment(ctx, h.p, "chat-1", "att_client", data)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	// The id and key are a stored format: every attachment the control
	// plane indexes was derived this way, so the derivation cannot move.
	if id != "731ff64a558f3da1814546a65ddcc43b3412" || key != "85n3muQp1OYTr9zUE0otPzIRQpqXPjHieXlCxkpBpoc=" || !domain.EnclaveAttachmentID.MatchString(id) {
		t.Fatalf("derived id=%s key=%s", id, key)
	}
	rawKey, _ := base64.StdEncoding.DecodeString(key)
	tenant, _ := buckets.TenantForUser(h.p.ID)
	item, ok := h.bk.Item(id)
	if !ok || item.Tenant != tenant || !bytes.Equal(item.Value, data) || len(item.EncryptionKeys) != 1 || !bytes.Equal(item.EncryptionKeys[0], rawKey) {
		t.Fatalf("buckets item = %+v (present=%v), want the bytes under the caller's tenant and the returned key", item, ok)
	}
	h.cp.Mu.Lock()
	chat, owner := h.cp.AttachmentIndex[id], h.cp.AttachmentOwners[id]
	h.cp.Mu.Unlock()
	if chat != "chat-1" || owner != h.p.ID {
		t.Fatalf("indexed under chat %q for %q, want chat-1 for the caller", chat, owner)
	}
	if again, againKey, err := h.store.PutAttachment(ctx, h.p, "chat-1", "att_client", data); err != nil || again != id || againKey != key || len(h.bk.Tokens()) != 1 {
		t.Fatalf("the same upload again: id=%s key=%s err=%v over %d objects, want the same id and key over one", again, againKey, err, len(h.bk.Tokens()))
	}
	if other, _, err := h.store.PutAttachment(ctx, h.p, "chat-2", "att_client", data); err != nil || other == id {
		t.Fatalf("the same upload under another chat: id=%s err=%v, want another id", other, err)
	}
	if changed, _, err := h.store.PutAttachment(ctx, h.p, "chat-1", "att_client", []byte("other bytes")); err != nil || changed == id {
		t.Fatalf("the same client id over other bytes: id=%s err=%v, want another id", changed, err)
	}
	if read, err := h.store.GetAttachment(ctx, h.p, id, key); err != nil || !bytes.Equal(read, data) {
		t.Fatalf("get after put = %q (%v)", read, err)
	}
}

func TestPutAttachmentRemovesBytesWhoseIndexTheControlPlaneRefused(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	data := []byte("bytes")
	id, _, _ := attachmentMaterials("att_client", "chat-1", h.p.ID, data)
	h.cp.Mu.Lock()
	h.cp.RegisterAttachmentIndexFailures[id] = []int{http.StatusForbidden, http.StatusServiceUnavailable}
	h.cp.Mu.Unlock()
	// A refusal proves the control plane holds no row for the bytes, so
	// they go; a failure leaves them for the sweep, which may find a row.
	if _, _, err := h.store.PutAttachment(ctx, h.p, "chat-1", "att_client", data); codeOf(err) != "UPSTREAM_REFUSED" || h.bk.Has(id) {
		t.Fatalf("after a refused registration: %v, bytes kept=%v", err, h.bk.Has(id))
	}
	if _, _, err := h.store.PutAttachment(ctx, h.p, "chat-1", "att_client", data); codeOf(err) != "UPSTREAM_REFUSED" || !h.bk.Has(id) {
		t.Fatalf("after a failed registration: %v, bytes kept=%v", err, h.bk.Has(id))
	}
	h.cp.Mu.Lock()
	_, indexed := h.cp.AttachmentIndex[id]
	h.cp.Mu.Unlock()
	if indexed {
		t.Fatal("the attachment was indexed by a registration the control plane did not accept")
	}
}

func TestPutAttachmentWaitsOutAPurgeOfTheEarlierCopy(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	data := []byte("bytes")
	id, _, _ := attachmentMaterials("att_client", "chat-1", h.p.ID, data)
	h.cp.Mu.Lock()
	h.cp.ReserveAttachmentPurges[id] = 1
	h.cp.Mu.Unlock()
	started := time.Now()
	stored, _, err := h.store.PutAttachment(ctx, h.p, "chat-1", "att_client", data)
	if err != nil || stored != id || !h.bk.Has(id) {
		t.Fatalf("put during a purge: id=%s err=%v stored=%v, want the bytes stored once the purge is over", stored, err, h.bk.Has(id))
	}
	if time.Since(started) < reserveWaitStep {
		t.Fatal("the reservation was not waited out")
	}
}

func TestGetAttachmentRefusesTheWrongKeyAndAnUnknownId(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	id := "123e4567-e89b-12d3-a456-426614174000"
	key := h.seedAttachment(t, "chat-1", id, []byte("bytes"))
	if _, err := h.store.GetAttachment(ctx, h.p, id, base64.StdEncoding.EncodeToString(make([]byte, attachmentKeySize))); codeOf(err) != "ATTACHMENT_UNSUPPORTED" || api.AsError(err).HTTP != 422 {
		t.Fatalf("get with another key: %v, want ATTACHMENT_UNSUPPORTED", err)
	}
	if _, err := h.store.GetAttachment(ctx, h.p, id, "short"); codeOf(err) != "ATTACHMENT_UNSUPPORTED" {
		t.Fatalf("get with a key that is not one: %v, want ATTACHMENT_UNSUPPORTED", err)
	}
	if _, err := h.store.GetAttachment(ctx, h.p, "223e4567-e89b-12d3-a456-426614174000", key); codeOf(err) != "THREAD_NOT_FOUND" || api.AsError(err).HTTP != 404 {
		t.Fatalf("get of an unknown id: %v, want 404 THREAD_NOT_FOUND", err)
	}
	// The id and the key are the whole proof of access: a share's reader
	// reads the owner's attachment without being anyone.
	if data, err := h.store.GetAttachment(ctx, &auth.Principal{Anonymous: true}, id, key); err != nil || string(data) != "bytes" {
		t.Fatalf("an anonymous get with the key = %q (%v)", data, err)
	}
}

func TestGetAttachmentOpensOneOnlyTheControlPlaneHoldsForItsOwner(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	id := "323e4567-e89b-12d3-a456-426614174000"
	key := make([]byte, legacyAttachmentKeySize)
	rand.Read(key)
	h.cp.Mu.Lock()
	h.cp.LegacyAttachments = map[string][]byte{id: sealLegacyAttachment(t, key, []byte("an old photo"))}
	h.cp.Mu.Unlock()
	embedded := base64.StdEncoding.EncodeToString(key)
	if data, err := h.store.GetAttachment(ctx, h.p, id, embedded); err != nil || string(data) != "an old photo" {
		t.Fatalf("the owner's get = %q (%v)", data, err)
	}
	if _, err := h.store.GetAttachment(ctx, h.p, id, base64.StdEncoding.EncodeToString(make([]byte, legacyAttachmentKeySize))); codeOf(err) != "ATTACHMENT_UNSUPPORTED" {
		t.Fatalf("the owner's get with another key: %v, want ATTACHMENT_UNSUPPORTED", err)
	}
	if _, err := h.store.GetAttachment(ctx, &auth.Principal{Anonymous: true}, id, embedded); codeOf(err) != "THREAD_NOT_FOUND" {
		t.Fatalf("an anonymous get of an attachment only the control plane holds: %v, want THREAD_NOT_FOUND", err)
	}
	h.cp.Mu.Lock()
	h.cp.GoneAttachments = map[string]bool{id: true}
	h.cp.Mu.Unlock()
	if _, err := h.store.GetAttachment(ctx, h.p, id, embedded); codeOf(err) != "THREAD_NOT_FOUND" {
		t.Fatalf("get of a legacy attachment already moved: %v, want THREAD_NOT_FOUND", err)
	}
}

func TestARunStoresAttachmentsUnderItsChatAlone(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	run := *h.p
	run.Grant = &auth.StorageGrant{ChatID: "chat-1", KeyID: h.kid, ExpiresAt: api.Now().Add(grantLifetime)}
	id, key, err := h.store.PutAttachment(ctx, &run, "chat-1", "att_client", []byte("bytes"))
	if err != nil {
		t.Fatalf("a run's put under its chat: %v", err)
	}
	if _, _, err := h.store.PutAttachment(ctx, &run, "chat-2", "att_client", []byte("bytes")); !errors.Is(err, errRunScope) {
		t.Fatalf("a run's put under another chat: %v, want a refusal of the grant's scope", err)
	}
	export := *h.p
	export.Grant = &auth.StorageGrant{Operation: "export", KeyID: h.kid, ExpiresAt: api.Now().Add(grantLifetime)}
	if data, err := h.store.GetAttachment(ctx, &export, id, key); err != nil || string(data) != "bytes" {
		t.Fatalf("an export's get = %q (%v)", data, err)
	}
	if _, _, err := h.store.PutAttachment(ctx, &export, "chat-1", "att_other", []byte("bytes")); !errors.Is(err, errRunScope) {
		t.Fatalf("an export's put: %v, want a refusal of the grant's scope", err)
	}
}
