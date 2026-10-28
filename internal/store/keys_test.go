package store

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
)

// testBundle is a bundle of the shape the registry stores, under a
// credential.
func testBundle(credential string) KeyBundleInput {
	return KeyBundleInput{CredentialID: credential, KEKIV: strings.Repeat("0", bundleIVHexLen), EncryptedKeys: strings.Repeat("a", bundleWrappedKeyHexLen)}
}

// bundles are the credential ids the control plane holds under a key.
func (h *rowsHarness) bundles(kid string) []string {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	return slices.Sorted(maps.Keys(h.cp.Bundles[kid]))
}

func TestRegisterKeyThenCurrentShowsTheKeyWithItsBundle(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	before, err := h.store.CurrentKey(ctx, h.p)
	if err != nil || before.KeyID != nil || before.Bundles == nil || len(before.Bundles) != 0 || before.HasData {
		t.Fatalf("current key of a new account = %+v (%v), want no key, no bundles and no data", before, err)
	}
	bundle := testBundle("cred-1")
	id, err := h.store.RegisterKey(ctx, h.p, h.key, "*", "passkey", "reg-1", &bundle)
	if err != nil || id != h.kid {
		t.Fatalf("register: id=%s err=%v, want %s", id, err, h.kid)
	}
	current, err := h.store.CurrentKey(ctx, h.p)
	if err != nil || current.KeyID == nil || *current.KeyID != h.kid || current.ETag != "1" || current.CreatedVia != "passkey" || current.CreatedAt == "" || current.HasData {
		t.Fatalf("current key after register = %+v (%v), want %s at etag 1 made by a passkey", current, err, h.kid)
	}
	held, ok := current.Bundles["cred-1"]
	if !ok || held.CredentialID != "cred-1" || held.KEKIV != bundle.KEKIV || held.EncryptedKeys != bundle.EncryptedKeys || held.CreatedAt == "" || held.UpdatedAt != held.CreatedAt {
		t.Fatalf("bundle = %+v (present=%v), want the registered one with when it was made", held, ok)
	}
	// Rows the key did not seal show as data the caller must deal with.
	h.cp.SetBlob("chat", "old", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(`{}`))})
	if current, err := h.store.CurrentKey(ctx, h.p); err != nil || !current.HasData {
		t.Fatalf("current key over a legacy row = %+v (%v), want has_data", current, err)
	}
}

func TestRegisterOverARegisteredKeyNeedsItsETagAndNamesIt(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	if _, err := h.store.RegisterKey(ctx, h.p, h.key, "*", "passkey", "reg-1", nil); err != nil {
		t.Fatal(err)
	}
	other := randomKey()
	for name, ifMatch := range map[string]string{"as if none": "*", "at a stale etag": "7"} {
		_, err := h.store.RegisterKey(ctx, h.p, other, ifMatch, "manual", "reg-"+name, nil)
		if e := api.AsError(err); e.Code != "KEY_MISMATCH" || e.KeyID != h.kid {
			t.Errorf("register %s: %+v, want KEY_MISMATCH naming %s", name, e, h.kid)
		}
	}
	id, err := h.store.RegisterKey(ctx, h.p, other, "1", "manual", "reg-2", nil)
	if err != nil || id != keyID(other) {
		t.Fatalf("register at the current etag: id=%s err=%v", id, err)
	}
	if current, err := h.store.CurrentKey(ctx, h.p); err != nil || *current.KeyID != keyID(other) || current.ETag != "2" {
		t.Fatalf("current key after the change = %+v (%v), want the new key at etag 2", current, err)
	}
	if _, err := h.store.RegisterKey(ctx, h.p, other, "1", "manual", "reg-3", nil); codeOf(err) != "KEY_MISMATCH" {
		t.Fatalf("register at the etag before the change: %v, want KEY_MISMATCH", err)
	}
}

func TestAPasskeyKeyOverLegacyRowsIsRefusedWhileARecoveryKeyIsAccepted(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.cp.SetBlob("chat", "old", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(`{}`))})
	// A new key would strand the legacy row; a recovered one claims to open it.
	_, err := h.store.RegisterKey(ctx, h.p, h.key, "*", "passkey", "reg-1", nil)
	if e := api.AsError(err); e.Code != "KEY_MISMATCH" || e.KeyID != "" {
		t.Fatalf("a passkey key over legacy rows: %+v, want KEY_MISMATCH naming no key", e)
	}
	if current, err := h.store.CurrentKey(ctx, h.p); err != nil || current.KeyID != nil || !current.HasData {
		t.Fatalf("after the refusal = %+v (%v), want no key and has_data", current, err)
	}
	if _, err := h.store.RegisterKey(ctx, h.p, h.key, "*", "recovery", "reg-2", nil); err != nil {
		t.Fatalf("a recovery key over legacy rows: %v", err)
	}
	if current, err := h.store.CurrentKey(ctx, h.p); err != nil || *current.KeyID != h.kid || !current.HasData {
		t.Fatalf("after the recovery = %+v (%v), want the key with the legacy row still to reseal", current, err)
	}
}

func TestStartFreshWipesRowsAndTheirAttachmentsLeaveBuckets(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.push(t, "chat", "c1", `{"id":"c1"}`)
	h.push(t, "project", "p1", `{"name":"p1"}`)
	attID := "123e4567-e89b-12d3-a456-426614174000"
	h.seedAttachment(t, "c1", attID, []byte("bytes"))
	fresh := randomKey()
	if _, err := h.store.RegisterKey(ctx, h.p, fresh, "*", "passkey", "reg-1", nil); codeOf(err) != "KEY_MISMATCH" {
		t.Fatalf("a new key over rows under another: %v, want KEY_MISMATCH", err)
	}
	bundle := testBundle("cred-fresh")
	if _, err := h.store.RegisterKey(ctx, h.p, fresh, "*", "start_fresh", "reg-2", &bundle); err != nil {
		t.Fatalf("start fresh: %v", err)
	}
	h.cp.Mu.Lock()
	left, indexed := slices.Sorted(maps.Keys(h.cp.Blobs)), len(h.cp.AttachmentIndex)
	h.cp.Mu.Unlock()
	if len(left) != 0 || indexed != 0 || h.bk.Has(attID) {
		t.Fatalf("after start fresh: rows=%v indexed=%d attachment kept=%v, want everything gone", left, indexed, h.bk.Has(attID))
	}
	if current, err := h.store.CurrentKey(ctx, h.p); err != nil || *current.KeyID != keyID(fresh) || current.HasData || len(current.Bundles) != 1 {
		t.Fatalf("current key after start fresh = %+v (%v)", current, err)
	}
}

func TestAddThenRemoveABundle(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	first := testBundle("cred-1")
	if _, err := h.store.RegisterKey(ctx, h.p, h.key, "*", "passkey", "reg-1", &first); err != nil {
		t.Fatal(err)
	}
	if err := h.store.AddKeyBundle(ctx, h.p, h.key, h.kid, testBundle("cred-2"), "add-1"); err != nil {
		t.Fatalf("add bundle: %v", err)
	}
	if got := h.bundles(h.kid); !slices.Equal(got, []string{"cred-1", "cred-2"}) {
		t.Fatalf("bundles after add = %v", got)
	}
	if err := h.store.RemoveKeyBundle(ctx, h.p, h.key, h.kid, "cred-1", "remove-1"); err != nil {
		t.Fatalf("remove bundle: %v", err)
	}
	if got := h.bundles(h.kid); !slices.Equal(got, []string{"cred-2"}) {
		t.Fatalf("bundles after remove = %v", got)
	}
	other := randomKey()
	if err := h.store.AddKeyBundle(ctx, h.p, other, keyID(other), testBundle("cred-3"), "add-2"); codeOf(err) != "KEY_MISMATCH" {
		t.Fatalf("add under a key that is not the current one: %v, want KEY_MISMATCH", err)
	}
}

func TestABundleWhoseKeyDoesNotMatchItsIdIsRefusedBeforeTheControlPlane(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	if _, err := h.store.RegisterKey(ctx, h.p, h.key, "*", "passkey", "reg-1", nil); err != nil {
		t.Fatal(err)
	}
	calls := 0
	h.cp.Mu.Lock()
	h.cp.CaptureHeaders = func(*http.Request) { calls++ }
	h.cp.Mu.Unlock()
	if err := h.store.AddKeyBundle(ctx, h.p, randomKey(), h.kid, testBundle("cred-x"), "add-1"); codeOf(err) != "BAD_REQUEST" {
		t.Fatalf("add with another key: %v, want BAD_REQUEST", err)
	}
	if err := h.store.RemoveKeyBundle(ctx, h.p, randomKey(), h.kid, "cred-x", "remove-1"); codeOf(err) != "BAD_REQUEST" {
		t.Fatalf("remove with another key: %v, want BAD_REQUEST", err)
	}
	upper := testBundle("cred-y")
	upper.KEKIV = strings.Repeat("A", bundleIVHexLen)
	if err := h.store.AddKeyBundle(ctx, h.p, h.key, h.kid, upper, "add-2"); codeOf(err) != "BAD_REQUEST" {
		t.Fatalf("add a bundle outside the stored shape: %v, want BAD_REQUEST", err)
	}
	if calls != 0 || len(h.bundles(h.kid)) != 0 {
		t.Fatalf("the control plane was called %d times and holds %v", calls, h.bundles(h.kid))
	}
}
