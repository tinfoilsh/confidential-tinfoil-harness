package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// keyIDOf is the id a content key is registered under.
func keyIDOf(key auth.ContentKey) string {
	return seal.KeyIDHex(mustRead(seal.DeriveKeyID(key[:])))
}

// testBundle is a passkey bundle of the shape the registry stores.
func testBundle(credential string) api.Object {
	return api.Object{"credential_id": credential, "kek_iv": strings.Repeat("0", 24), "encrypted_keys": strings.Repeat("a", 96)}
}

// sealedByAnOlderClient seals a row as the oldest clients did, without a
// key id.
func sealedByAnOlderClient(t *testing.T, key auth.ContentKey, plaintext string) []byte {
	t.Helper()
	nonce, ciphertext, err := seal.GCMSeal(key[:], []byte(plaintext), nil)
	if err != nil {
		t.Fatal(err)
	}
	return api.Raw(api.Object{"iv": base64.StdEncoding.EncodeToString(nonce), "data": base64.StdEncoding.EncodeToString(ciphertext)})
}

func currentKeyOf(t *testing.T, h *harness, user string) store.KeyState {
	t.Helper()
	w := postRoute(h, "/v1/keys/current", api.Object{}, user)
	if w.Code != http.StatusOK {
		t.Fatalf("current key: HTTP %d: %s", w.Code, w.Body.String())
	}
	var state store.KeyState
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestRegisteringAKeyShowsItAsTheCurrentOne(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	if before := currentKeyOf(t, h, "alice"); before.KeyID != nil || before.Bundles == nil || len(before.Bundles) != 0 || before.HasData {
		t.Fatalf("a new account's key = %+v, want none", before)
	}
	w := postRoute(h, "/v1/keys/register", api.Object{"key": keyBase64(key), "if_match": "*", "created_via": "passkey", "idempotency_key": "reg-1", "initial_bundle": testBundle("cred-1")}, "alice")
	var registered struct {
		OK    bool   `json:"ok"`
		KeyID string `json:"key_id"`
	}
	json.Unmarshal(w.Body.Bytes(), &registered)
	if w.Code != http.StatusOK || !registered.OK || registered.KeyID != keyIDOf(key) {
		t.Fatalf("register: HTTP %d: %s", w.Code, w.Body.String())
	}
	current := currentKeyOf(t, h, "alice")
	if current.KeyID == nil || *current.KeyID != keyIDOf(key) || current.ETag != "1" || current.CreatedVia != "passkey" || current.HasData {
		t.Fatalf("current key after register = %+v", current)
	}
	if bundle, ok := current.Bundles["cred-1"]; !ok || bundle.EncryptedKeys != strings.Repeat("a", 96) || bundle.CreatedAt == "" {
		t.Fatalf("bundle = %+v (present=%v)", bundle, ok)
	}
	// Another key over the registered one is refused, naming it.
	w = postRoute(h, "/v1/keys/register", api.Object{"key": keyBase64(randomKey()), "if_match": "*", "created_via": "passkey", "idempotency_key": "reg-2"}, "alice")
	var refused struct {
		Error api.Error `json:"error"`
	}
	json.Unmarshal(w.Body.Bytes(), &refused)
	if w.Code != http.StatusConflict || refused.Error.Code != "KEY_MISMATCH" || refused.Error.KeyID != keyIDOf(key) {
		t.Fatalf("another key: HTTP %d: %s", w.Code, w.Body.String())
	}
	if w := postRoute(h, "/v1/keys/register", api.Object{"key": keyBase64(key), "if_match": "*", "created_via": "passkey"}, "alice"); w.Code != http.StatusBadRequest {
		t.Fatalf("a register without an idempotency key: HTTP %d: %s", w.Code, w.Body.String())
	}
	if w := postRoute(h, "/v1/keys/current", api.Object{}, ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("a signed-out caller's current key: HTTP %d: %s", w.Code, w.Body.String())
	}
}

func TestMigrateAllResealsStoredRowsUnderTheRegisteredKey(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	rows.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, Body: sealedByAnOlderClient(t, key, `{"id":"c1","messages":[]}`)})
	reseal := api.Object{"keys": []api.Object{{"key": keyBase64(key)}}, "target": api.Object{"key": keyBase64(key)}}
	w := postRoute(h, "/v1/keys/migrate-all", reseal, "alice")
	var refused struct {
		Error api.Error `json:"error"`
	}
	json.Unmarshal(w.Body.Bytes(), &refused)
	if w.Code != http.StatusConflict || refused.Error.Code != "KEY_MISMATCH" {
		t.Fatalf("a reseal under a key that is not registered: HTTP %d: %s", w.Code, w.Body.String())
	}
	// A key asserted over rows older clients stored is a recovered one.
	if w := postRoute(h, "/v1/keys/register", api.Object{"key": keyBase64(key), "if_match": "*", "created_via": "recovery", "idempotency_key": "reg-1"}, "alice"); w.Code != http.StatusOK {
		t.Fatalf("register: HTTP %d: %s", w.Code, w.Body.String())
	}
	w = postRoute(h, "/v1/keys/migrate-all", reseal, "alice")
	var report store.ResealReport
	json.Unmarshal(w.Body.Bytes(), &report)
	if w.Code != http.StatusOK || report.Status != jobs.Running && report.Status != jobs.Complete {
		t.Fatalf("migrate-all: HTTP %d: %s", w.Code, w.Body.String())
	}
	// A repeated start finds the caller's job, which the next call reports on.
	job, started := h.store.Reseals.Start(context.Background(), "alice", func(context.Context, func(store.ResealReport)) (store.ResealReport, error) {
		return store.ResealReport{}, nil
	})
	if started {
		t.Fatal("the route started no reseal")
	}
	<-job.Done()
	w = postRoute(h, "/v1/keys/migrate-all", reseal, "alice")
	json.Unmarshal(w.Body.Bytes(), &report)
	if w.Code != http.StatusOK || report != (store.ResealReport{Status: jobs.Complete, Migrated: 1}) {
		t.Fatalf("migrate-all after the reseal: HTTP %d: %s", w.Code, w.Body.String())
	}
	rows.cp.Mu.Lock()
	stored := *rows.cp.Blobs["chat/c1"]
	rows.cp.Mu.Unlock()
	if stored.ETag != 2 || stored.KeyID != keyIDOf(key) || seal.Detect(stored.Body) != seal.VersionV2 {
		t.Fatalf("the stored chat = %+v, want it resealed once under %s", stored, keyIDOf(key))
	}
	if current := currentKeyOf(t, h, "alice"); current.HasData {
		t.Fatal("rows remain under another key after the reseal")
	}
}
