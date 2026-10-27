package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets/bucketstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

const (
	testBucket = "test-attachments-bucket"
	testSecret = "test-enclave-secret"
)

// rowsHarness is a Rows, and the Store over it, on a stub of the control
// plane and of buckets, for one caller with one content key.
type rowsHarness struct {
	cp    *cpstub.Store
	bk    *bucketstub.Store
	rows  *Rows
	store *Store
	p     *auth.Principal
	key   auth.ContentKey
	kid   string
}

func newRowsHarness(t *testing.T) *rowsHarness {
	t.Helper()
	cp := cpstub.New()
	cp.Secret = testSecret
	cpServer := httptest.NewServer(cp)
	t.Cleanup(cpServer.Close)
	bk := bucketstub.NewStore()
	mux := http.NewServeMux()
	mux.HandleFunc("/"+testBucket+"/{key}", bk.Handle)
	bkServer := httptest.NewServer(mux)
	t.Cleanup(bkServer.Close)
	key := randomKey()
	rows := &Rows{Controlplane: controlplane.NewClient(cpServer.URL, nil, controlplane.WithServiceSecret(testSecret)), Buckets: buckets.NewClient(bkServer.URL, testBucket, nil)}
	return &rowsHarness{
		cp:    cp,
		bk:    bk,
		rows:  rows,
		store: &Store{Rows: rows, Controlplane: rows.Controlplane, Buckets: rows.Buckets},
		p:     &auth.Principal{ID: "user_abc", JWT: "clerk-abc"},
		key:   key,
		kid:   keyID(key),
	}
}

func randomKey() auth.ContentKey {
	var key auth.ContentKey
	rand.Read(key[:])
	return key
}

func (h *rowsHarness) push(t *testing.T, scope, id, data string) string {
	t.Helper()
	etag, err := h.rows.Push(context.Background(), h.p, h.key, scope, &domain.StoredRow{ID: id, Data: []byte(data)}, "nonce-"+scope+"-"+id)
	if err != nil {
		t.Fatalf("push %s/%s: %v", scope, id, err)
	}
	return etag
}

// stored is the control plane's copy of a row.
func (h *rowsHarness) stored(scope, id string) cpstub.Blob {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	blob := h.cp.Blobs[cpstub.Key(scope, id)]
	if blob == nil {
		return cpstub.Blob{}
	}
	return *blob
}

// opened decrypts the control plane's copy of a row under the harness's key.
func (h *rowsHarness) opened(t *testing.T, scope, id string) []byte {
	t.Helper()
	blob := h.stored(scope, id)
	if seal.Detect(blob.Body) != seal.VersionV2 {
		t.Fatalf("%s/%s is not sealed in the current format: %q", scope, id, blob.Body)
	}
	dec, err := seal.DecryptV2(blob.Body, []seal.Key{{Bytes: h.key[:], KeyIDHex: h.kid}}, seal.AAD{Scope: seal.Scope(scope), ID: id, ClerkUserID: h.p.ID})
	if err != nil {
		t.Fatalf("open %s/%s: %v", scope, id, err)
	}
	return dec.Plaintext
}

// sealLegacyV0 seals a row as the oldest clients did: {iv, data} JSON.
func sealLegacyV0(t *testing.T, key auth.ContentKey, plaintext []byte) []byte {
	t.Helper()
	nonce, ciphertext, err := seal.GCMSeal(key[:], plaintext, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]string{"iv": base64.StdEncoding.EncodeToString(nonce), "data": base64.StdEncoding.EncodeToString(ciphertext)})
	return body
}

// sealLegacyV1 seals a row as the web app did before key ids: the nonce,
// then AES-GCM over the gzipped JSON.
func sealLegacyV1(t *testing.T, key auth.ContentKey, plaintext []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write(plaintext); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	nonce, ciphertext, err := seal.GCMSeal(key[:], compressed.Bytes(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return append(nonce, ciphertext...)
}

// sealLegacyAttachment seals an attachment as the web app did: the nonce,
// then AES-GCM over the bytes, with no AAD.
func sealLegacyAttachment(t *testing.T, key, plaintext []byte) []byte {
	t.Helper()
	nonce, ciphertext, err := seal.GCMSeal(key, plaintext, nil)
	if err != nil {
		t.Fatal(err)
	}
	return append(nonce, ciphertext...)
}

func codeOf(err error) string {
	if err == nil {
		return ""
	}
	return api.AsError(err).Code
}

func TestPushThenPullRoundTripsARowAndRefusesATamperedOne(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	plaintext := `{"id":"c1","title":"Hello","messages":[]}`
	project := "project-1"
	etag, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "c1", Data: []byte(plaintext), Project: &project}, "nonce-1")
	if err != nil || etag != "1" {
		t.Fatalf("push: etag=%q err=%v", etag, err)
	}
	if stored := h.stored("chat", "c1"); bytes.Contains(stored.Body, []byte("Hello")) || stored.KeyID != h.kid {
		t.Fatalf("the control plane holds the row unsealed or under another key: %+v", stored)
	}
	rows, failed, err := h.rows.Pull(ctx, h.p, h.key, "chat", []string{"c1"})
	if err != nil || len(failed) != 0 || len(rows) != 1 {
		t.Fatalf("pull: rows=%+v failed=%v err=%v", rows, failed, err)
	}
	if row := rows[0]; row.ID != "c1" || row.ETag != "1" || string(row.Data) != plaintext || !row.ProjectSet || row.Project == nil || *row.Project != project {
		t.Fatalf("pulled row = %+v, want the pushed plaintext at revision 1 under %s", row, project)
	}

	// A profile is written under the merge protocol the control plane
	// requires of every writer of an existing profile.
	h.cp.Mu.Lock()
	h.cp.MinimumProfileSyncProtocol = profileSyncProtocol
	h.cp.Mu.Unlock()
	h.push(t, "profile", "profile", `{"settings":{}}`)
	if _, err := h.rows.Push(ctx, h.p, h.key, "profile", &domain.StoredRow{ID: "profile", ETag: "1", Data: []byte(`{"settings":{"a":1}}`)}, "nonce-profile-2"); err != nil {
		t.Fatalf("profile overwrite: %v", err)
	}

	h.cp.Mu.Lock()
	body := h.cp.Blobs["chat/c1"].Body
	var envelope map[string]any
	json.Unmarshal(body, &envelope)
	ciphertext, _ := base64.StdEncoding.DecodeString(envelope["ct"].(string))
	ciphertext[len(ciphertext)/2] ^= 0x01
	envelope["ct"] = base64.StdEncoding.EncodeToString(ciphertext)
	h.cp.Blobs["chat/c1"].Body, _ = json.Marshal(envelope)
	h.cp.Mu.Unlock()
	rows, failed, err = h.rows.Pull(ctx, h.p, h.key, "chat", []string{"c1"})
	if err != nil || len(rows) != 0 || codeOf(failed["c1"]) != "UPSTREAM_REFUSED" {
		t.Fatalf("a tampered row was served: rows=%+v failed=%v err=%v", rows, failed, err)
	}
}

func TestPushAtAStaleRevisionIsAConflictThatKeepsTheCurrentRow(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.push(t, "chat", "c1", `{"v":1}`)
	if etag, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "c1", ETag: "1", Data: []byte(`{"v":2}`)}, "nonce-2"); err != nil || etag != "2" {
		t.Fatalf("second push: etag=%q err=%v", etag, err)
	}
	_, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "c1", ETag: "1", Data: []byte(`{"v":3}`)}, "nonce-3")
	if codeOf(err) != "REVISION_CONFLICT" {
		t.Fatalf("push at a stale revision: %v, want REVISION_CONFLICT", err)
	}
	if _, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "c1", Data: []byte(`{"v":3}`)}, "nonce-4"); codeOf(err) != "REVISION_CONFLICT" {
		t.Fatalf("create over an existing row: %v, want REVISION_CONFLICT", err)
	}
	row, err := h.rows.Load(ctx, h.p, h.key, "chat", "c1")
	if err != nil || row.ETag != "2" || string(row.Data) != `{"v":2}` {
		t.Fatalf("after the refused pushes the row is %+v (%v), want revision 2 as pushed", row, err)
	}
}

func TestPullWithAnotherKeyNamesTheSealingKey(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.push(t, "chat", "c1", `{"x":1}`)
	other := randomKey()
	rows, failed, err := h.rows.Pull(ctx, h.p, other, "chat", []string{"c1"})
	if err != nil || len(rows) != 0 {
		t.Fatalf("pull with another key opened the row: rows=%+v err=%v", rows, err)
	}
	if e := api.AsError(failed["c1"]); e.Code != "KEY_MISMATCH" || e.KeyID != h.kid {
		t.Fatalf("failure = %+v, want KEY_MISMATCH naming %s", e, h.kid)
	}
	if _, err := h.rows.AuthorizeRun(ctx, h.p, other, "c1", nil); codeOf(err) != "KEY_MISMATCH" {
		t.Fatalf("a run was granted a chat its key cannot open: %v", err)
	}
}

func TestPullResealsALegacyRowAndMovesItsAttachmentUnderTheRegisteredKey(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	attID := "123e4567-e89b-12d3-a456-426614174000"
	attKey := make([]byte, legacyAttachmentKeySize)
	rand.Read(attKey)
	attPlaintext := []byte("a tiny image's bytes")
	chat := `{"id":"c1","createdAt":"2025-02-03T04:05:06Z","messages":[{"id":"m1","role":"user","attachments":[{"id":"` + attID + `","type":"image","base64":"dGh1bWI=","key":"` + base64.StdEncoding.EncodeToString(attKey) + `"}]}]}`
	h.cp.Mu.Lock()
	h.cp.CurrentKeyID = h.kid
	h.cp.LegacyAttachments = map[string][]byte{attID: sealLegacyAttachment(t, attKey, attPlaintext)}
	h.cp.Mu.Unlock()
	h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, Body: sealLegacyV1(t, h.key, []byte(chat))})

	rows, failed, err := h.rows.Pull(ctx, h.p, h.key, "chat", []string{"c1", "c1"})
	if err != nil || len(failed) != 0 || len(rows) != 1 {
		t.Fatalf("pull: rows=%+v failed=%v err=%v", rows, failed, err)
	}
	if rows[0].ETag != "2" || string(rows[0].Data) != chat {
		t.Fatalf("row = %+v, want the legacy chat at the resealed revision 2", rows[0])
	}
	if stored := h.stored("chat", "c1"); stored.ETag != 2 || stored.KeyID != h.kid {
		t.Fatalf("stored row = %+v, want one reseal under %s", stored, h.kid)
	}
	var resealed map[string]any
	if err := json.Unmarshal(h.opened(t, "chat", "c1"), &resealed); err != nil {
		t.Fatal(err)
	}
	att := resealed["messages"].([]any)[0].(map[string]any)["attachments"].([]any)[0].(map[string]any)
	if att["encryptionKey"] != base64.StdEncoding.EncodeToString(attKey) || att["key"] != att["encryptionKey"] || att["base64"] != "dGh1bWI=" {
		t.Fatalf("resealed attachment = %v, want its key under both names and its thumbnail kept", att)
	}
	item, ok := h.bk.Item(attID)
	if !ok || !bytes.Equal(item.Value, attPlaintext) || len(item.EncryptionKeys) != 1 || !bytes.Equal(item.EncryptionKeys[0], attKey) {
		t.Fatalf("buckets item = %+v (present=%v), want the attachment under its own key", item, ok)
	}
	h.cp.Mu.Lock()
	indexed, legacyLeft, backfills := h.cp.AttachmentIndex[attID], h.cp.LegacyAttachments[attID], h.cp.ActivityBackfills
	h.cp.Mu.Unlock()
	if indexed != "c1" || legacyLeft != nil {
		t.Fatalf("attachment index=%q legacy=%v, want the attachment indexed under c1 and its legacy bytes gone", indexed, legacyLeft)
	}
	if want := []cpstub.ActivityBackfill{{ID: "c1", ETag: "2", ActivityAt: "2025-02-03T04:05:06Z"}}; !reflect.DeepEqual(backfills, want) {
		t.Fatalf("activity backfills = %+v, want %+v at the resealed revision", backfills, want)
	}
}

func TestPullKeepsALegacyRowWhoseAttachmentClaimFails(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	attID := "223e4567-e89b-12d3-a456-426614174999"
	attKey := make([]byte, legacyAttachmentKeySize)
	rand.Read(attKey)
	chat := `{"id":"c1","messages":[{"attachments":[{"id":"` + attID + `","type":"image","encryptionKey":"` + base64.StdEncoding.EncodeToString(attKey) + `"}]}]}`
	h.cp.Mu.Lock()
	h.cp.CurrentKeyID = h.kid
	h.cp.Secret = "another-secret"
	h.cp.LegacyAttachments = map[string][]byte{attID: sealLegacyAttachment(t, attKey, []byte("bytes"))}
	h.cp.Mu.Unlock()
	h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(chat))})

	row, err := h.rows.Load(ctx, h.p, h.key, "chat", "c1")
	if err != nil || row.ETag != "1" || string(row.Data) != chat {
		t.Fatalf("row = %+v (%v), want the legacy chat read as it is", row, err)
	}
	if h.bk.Has(attID) || seal.Detect(h.stored("chat", "c1").Body) != seal.VersionV0 {
		t.Fatal("an attachment whose claim the control plane did not sign moved, or its chat was resealed without it")
	}
}

func TestPullLeavesALegacyRowAsItIsUnderAnotherRegisteredKey(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	chat := `{"id":"c1","createdAt":"2025-02-03T04:05:06Z","messages":[]}`
	h.cp.SetBlob("chat", "c1", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, h.key, []byte(chat))})
	h.cp.Mu.Lock()
	h.cp.CurrentKeyID = keyID(randomKey())
	h.cp.Mu.Unlock()

	row, err := h.rows.Load(ctx, h.p, h.key, "chat", "c1")
	if err != nil || row.ETag != "1" || string(row.Data) != chat {
		t.Fatalf("row = %+v (%v), want the legacy chat at revision 1", row, err)
	}
	h.cp.Mu.Lock()
	stored, backfills := *h.cp.Blobs["chat/c1"], h.cp.ActivityBackfills
	h.cp.Mu.Unlock()
	if seal.Detect(stored.Body) != seal.VersionV0 {
		t.Fatalf("the row was resealed under a key that is not the registered one: %q", stored.Body)
	}
	if want := []cpstub.ActivityBackfill{{ID: "c1", ETag: "1", ActivityAt: "2025-02-03T04:05:06Z"}}; !reflect.DeepEqual(backfills, want) {
		t.Fatalf("activity backfills = %+v, want %+v", backfills, want)
	}
}

func TestChatPushReportsItsAttachmentsAndActivity(t *testing.T) {
	h := newRowsHarness(t)
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	chat := `{"id":"chat-refs","createdAt":"2026-01-01T00:00:00.000Z","messages":[` +
		`{"role":"user","content":"a","timestamp":"2026-01-01T00:00:00.000Z","attachments":[` +
		`{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","type":"image","encryptionKey":"` + key + `"},` +
		`{"id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","type":"document","fileSize":1.5,"key":"` + key + `"},` +
		`{"id":"local-only","type":"image","base64":"AQID"}]},` +
		`{"role":"assistant","content":"b","timestamp":"2026-02-03T04:05:06.789Z","attachments":[` +
		`{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","type":"image","encryptionKey":"` + key + `"}]}]}`
	h.push(t, "chat", "chat-refs", chat)
	h.push(t, "chat", "chat-empty", `{"id":"chat-empty","messages":[]}`)
	h.push(t, "chat", "chat-unreadable", `{"id":"chat-unreadable","messages":{"attachments":[]}}`)
	h.push(t, "project", "project-1", chat)

	stored := h.stored("chat", "chat-refs")
	if want := []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}; !reflect.DeepEqual(stored.AttachmentRefs, want) {
		t.Fatalf("attachment refs = %v, want the uploaded attachments once each: %v", stored.AttachmentRefs, want)
	}
	if want := time.Date(2026, 2, 3, 4, 5, 6, 789_000_000, time.UTC); stored.ActivityAt == nil || !stored.ActivityAt.Equal(want) {
		t.Fatalf("activity = %v, want the newest message's %s", stored.ActivityAt, want)
	}
	// A chat with no uploaded attachments says so, which tells the control
	// plane every blob under it is dead; one whose references cannot be
	// read says nothing, which keeps them all.
	if empty := h.stored("chat", "chat-empty"); empty.AttachmentRefs == nil || len(empty.AttachmentRefs) != 0 || empty.ActivityAt != nil {
		t.Fatalf("empty chat: refs=%v activity=%v", empty.AttachmentRefs, empty.ActivityAt)
	}
	if unreadable := h.stored("chat", "chat-unreadable"); unreadable.AttachmentRefs != nil {
		t.Fatalf("a chat whose references cannot be read reported %v", unreadable.AttachmentRefs)
	}
	if project := h.stored("project", "project-1"); project.AttachmentRefs != nil || project.ActivityAt != nil {
		t.Fatalf("a project reported refs=%v activity=%v", project.AttachmentRefs, project.ActivityAt)
	}
}

func TestRemoveRetriesARaceAndTakesAChatsAttachmentsWithIt(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	attID := "123e4567-e89b-12d3-a456-426614174222"
	h.push(t, "chat", "c2", `{"id":"c2","messages":[]}`)
	tenant, err := buckets.TenantForUser(h.p.ID)
	if err != nil {
		t.Fatal(err)
	}
	h.bk.Put(attID, bucketstub.Item{Tenant: tenant, Value: []byte("payload"), EncryptionKeys: [][]byte{make([]byte, 32)}})
	raced := 0
	h.cp.Mu.Lock()
	h.cp.AttachmentIndex = map[string]string{attID: "c2"}
	h.cp.BeforeDeleteBlob = func(scope, id string) {
		// Another writer changes the chat between the read and the delete.
		if raced == 0 {
			raced++
			h.cp.Blobs["chat/c2"].ETag++
		}
	}
	h.cp.Mu.Unlock()

	if err := h.rows.Remove(ctx, h.p, h.key, "chat", "c2"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	h.cp.Mu.Lock()
	_, present := h.cp.Blobs["chat/c2"]
	h.cp.Mu.Unlock()
	if present || raced != 1 || h.bk.Has(attID) {
		t.Fatalf("after remove: present=%v raced=%d attachment kept=%v", present, raced, h.bk.Has(attID))
	}
	if err := h.rows.Remove(ctx, h.p, h.key, "chat", "c2"); err != nil {
		t.Fatalf("removing a missing row: %v, want it gone", err)
	}

	h.push(t, "chat", "c3", `{"id":"c3"}`)
	if err := h.rows.RemoveRevision(ctx, h.p, h.key, "c3", "7"); codeOf(err) != "REVISION_CONFLICT" {
		t.Fatalf("remove at another revision: %v, want REVISION_CONFLICT", err)
	}
	if err := h.rows.RemoveRevision(ctx, h.p, h.key, "c3", "1"); err != nil {
		t.Fatalf("remove at the current revision: %v", err)
	}
	if _, err := h.rows.Load(ctx, h.p, h.key, "chat", "c3"); api.AsError(err).HTTP != 404 {
		t.Fatalf("after remove at its revision the chat is still there: %v", err)
	}
}

func TestARunsGrantCoversItsChatAndItsProjectsContext(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	project := "project"
	if _, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "target", Data: []byte(`{"id":"target","projectId":"stale"}`), Project: &project}, "seed"); err != nil {
		t.Fatal(err)
	}
	h.push(t, "project", "project", `{"name":"House"}`)
	h.push(t, "project_document", "project/doc", `{"content":"Context"}`)
	h.push(t, "project_document", "other/doc", `{"content":"Not ours"}`)
	grant, err := h.rows.AuthorizeRun(ctx, h.p, h.key, "target", nil)
	if err != nil || grant.ChatID != "target" || grant.ProjectID != "project" || grant.KeyID != h.kid || !grant.ExpiresAt.After(api.Now().Add(grantLifetime-time.Minute)) {
		t.Fatalf("grant = %+v (%v), want the stored chat's project under this key for an hour", grant, err)
	}
	if fresh, err := h.rows.AuthorizeRun(ctx, h.p, h.key, "new-chat", &project); err != nil || fresh.ProjectID != project {
		t.Fatalf("a new chat's grant = %+v (%v), want the project it names", fresh, err)
	}
	run := *h.p
	run.Grant = grant
	saved := `{"id":"target","messages":[{"role":"assistant","content":"Finished"}]}`
	if _, err := h.rows.Push(ctx, &run, h.key, "chat", &domain.StoredRow{ID: "target", ETag: "1", Data: []byte(saved)}, "save"); err != nil {
		t.Fatalf("the run cannot save its chat: %v", err)
	}
	if docs, err := h.rows.List(ctx, &run, h.key, "project_document", "", "project", 10); err != nil || len(docs.Rows) != 1 || docs.Rows[0].ID != "project/doc" {
		t.Fatalf("the run's context = %+v (%v), want its project's one document", docs, err)
	}
	if _, err := h.rows.Load(ctx, &run, h.key, "project", "project"); err != nil {
		t.Fatalf("the run cannot read its project: %v", err)
	}
	other := randomKey()
	for name, attempt := range map[string]func() error{
		"push another chat": func() error {
			_, err := h.rows.Push(ctx, &run, h.key, "chat", &domain.StoredRow{ID: "other", Data: []byte(saved)}, "escape")
			return err
		},
		"push the profile": func() error {
			_, err := h.rows.Push(ctx, &run, h.key, "profile", &domain.StoredRow{ID: "profile", Data: []byte(`{}`)}, "escape")
			return err
		},
		"push under another key": func() error {
			_, err := h.rows.Push(ctx, &run, other, "chat", &domain.StoredRow{ID: "target", ETag: "2", Data: []byte(saved)}, "escape")
			return err
		},
		"pull another chat": func() error {
			_, _, err := h.rows.Pull(ctx, &run, h.key, "chat", []string{"target", "other"})
			return err
		},
		"pull another project's document": func() error {
			_, _, err := h.rows.Pull(ctx, &run, h.key, "project_document", []string{"other/doc"})
			return err
		},
		"list chats": func() error {
			_, err := h.rows.List(ctx, &run, h.key, "chat", "", "", 10)
			return err
		},
		"list another project's documents": func() error {
			_, err := h.rows.List(ctx, &run, h.key, "project_document", "", "other", 10)
			return err
		},
		"remove its chat": func() error { return h.rows.Remove(ctx, &run, h.key, "chat", "target") },
	} {
		if err := attempt(); !errors.Is(err, errRunScope) {
			t.Errorf("%s: %v, want a refusal of the grant's scope", name, err)
		}
	}
	if row, err := h.rows.Load(ctx, h.p, h.key, "chat", "target"); err != nil || row.ETag != "2" || string(row.Data) != saved {
		t.Fatalf("the refused operations changed the chat: %+v (%v)", row, err)
	}
	ended := *h.p
	expired := *grant
	expired.ExpiresAt = api.Now().Add(-time.Second)
	ended.Grant = &expired
	if _, err := h.rows.Load(ctx, &ended, h.key, "chat", "target"); !errors.Is(err, errRunScope) {
		t.Fatalf("a grant that ended still reads: %v", err)
	}
}

func TestAnOperationsGrantCoversWhatItIsFor(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.push(t, "project", "project", `{"name":"House"}`)
	h.push(t, "chat", "existing", `{"id":"existing"}`)
	grant, err := h.rows.AuthorizeStorage(ctx, h.p, h.key, "project-upload", "project")
	if err != nil || grant.Operation != "project-upload" || grant.ProjectID != "project" || grant.KeyID != h.kid {
		t.Fatalf("grant = %+v (%v)", grant, err)
	}
	if _, err := h.rows.AuthorizeStorage(ctx, h.p, h.key, "project-upload", "a/b"); codeOf(err) != "BAD_REQUEST" {
		t.Fatalf("an upload to an invalid project was granted: %v", err)
	}
	upload := *h.p
	upload.Grant = grant
	project := "project"
	document := &domain.StoredRow{ID: "project/doc", Data: []byte(`{"projectId":"project","filename":"photo.png"}`), Project: &project, ProjectSet: true}
	etag, err := h.rows.Push(ctx, &upload, h.key, "project_document", document, "new-document")
	if err != nil {
		t.Fatalf("save document: %v", err)
	}
	otherProject := "other"
	for name, change := range map[string]func(*domain.StoredRow){
		"another project's document": func(r *domain.StoredRow) { r.ID = "other/doc" },
		"a path out of the project":  func(r *domain.StoredRow) { r.ID = "project/../other/doc" },
		"an overwrite":               func(r *domain.StoredRow) { r.ETag = etag },
		"a move to another project":  func(r *domain.StoredRow) { r.Project = &otherProject },
	} {
		modified := *document
		change(&modified)
		if _, err := h.rows.Push(ctx, &upload, h.key, "project_document", &modified, "escape-"+name); !errors.Is(err, errRunScope) {
			t.Errorf("%s: %v, want a refusal of the grant's scope", name, err)
		}
	}
	if _, err := h.rows.Push(ctx, &upload, h.key, "chat", &domain.StoredRow{ID: "existing", ETag: "1", Data: []byte(`{}`)}, "escape-chat"); !errors.Is(err, errRunScope) {
		t.Errorf("a chat push: %v, want a refusal of the grant's scope", err)
	}
	if row, err := h.rows.Load(ctx, &upload, h.key, "project_document", "project/doc"); err != nil || row.ETag != etag || !bytes.Equal(row.Data, document.Data) {
		t.Fatalf("the document was changed, or the upload cannot read it: %+v (%v)", row, err)
	}
	if _, err := h.rows.Load(ctx, &upload, h.key, "chat", "existing"); !errors.Is(err, errRunScope) {
		t.Fatalf("an upload read a chat: %v", err)
	}

	export, imported := *h.p, *h.p
	export.Grant, _ = h.rows.AuthorizeStorage(ctx, h.p, h.key, "export", "")
	imported.Grant, _ = h.rows.AuthorizeStorage(ctx, h.p, h.key, "import", "")
	if page, err := h.rows.List(ctx, &export, h.key, "chat", "", "", 10); err != nil || len(page.Rows) != 1 || page.Rows[0].ID != "existing" {
		t.Fatalf("an export's read of the chats = %+v (%v)", page, err)
	}
	if _, err := h.rows.Push(ctx, &export, h.key, "chat", &domain.StoredRow{ID: "new", Data: []byte(`{}`)}, "export-write"); !errors.Is(err, errRunScope) {
		t.Fatalf("an export wrote a chat: %v", err)
	}
	if _, err := h.rows.Load(ctx, &imported, h.key, "chat", "existing"); !errors.Is(err, errRunScope) {
		t.Fatalf("an import read a chat: %v", err)
	}
}

func TestListPagesNewestFirstAndSelectsAProjectsRows(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	p1, p2 := "p1", "p2"
	for i, chat := range []struct {
		id      string
		project *string
	}{{"c1", &p1}, {"c2", &p2}, {"c3", &p1}} {
		if _, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: chat.id, Data: []byte(`{"id":"` + chat.id + `"}`), Project: chat.project}, "nonce-"+chat.id); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	ids := func(page Page) []string {
		var out []string
		for _, row := range page.Rows {
			out = append(out, row.ID)
		}
		return out
	}
	first, err := h.rows.List(ctx, h.p, h.key, "chat", "", "", 2)
	if err != nil || !reflect.DeepEqual(ids(first), []string{"c3", "c2"}) || first.Next == "" {
		t.Fatalf("first page = %v next=%q (%v), want the two newest chats and a next page", ids(first), first.Next, err)
	}
	second, err := h.rows.List(ctx, h.p, h.key, "chat", first.Next, "", 2)
	if err != nil || !reflect.DeepEqual(ids(second), []string{"c1"}) || second.Next != "" {
		t.Fatalf("second page = %v next=%q (%v), want the oldest chat and no more", ids(second), second.Next, err)
	}
	if page, err := h.rows.List(ctx, h.p, h.key, "chat", "", "p1", 10); err != nil || !reflect.DeepEqual(ids(page), []string{"c3", "c1"}) {
		t.Fatalf("a project's chats = %v (%v), want c3 and c1", ids(page), err)
	}
	if all, err := h.rows.IDs(ctx, h.p, "chat", ""); err != nil || !reflect.DeepEqual(all, []string{"c3", "c2", "c1"}) {
		t.Fatalf("every chat id = %v (%v)", all, err)
	}

	h.push(t, "project_document", "p1/d1", `{"n":1}`)
	h.push(t, "project_document", "p2/d2", `{"n":2}`)
	h.push(t, "project_document", "p1/d3", `{"n":3}`)
	if page, err := h.rows.List(ctx, h.p, h.key, "project_document", "", "p1", 10); err != nil || !reflect.DeepEqual(ids(page), []string{"p1/d3", "p1/d1"}) {
		t.Fatalf("a project's documents = %v (%v), want p1's two", ids(page), err)
	}

	// A page lists a row it cannot open rather than failing on it.
	h.cp.SetBlob("chat", "c4", cpstub.Blob{ETag: 1, Body: sealLegacyV0(t, randomKey(), []byte(`{}`))})
	page, err := h.rows.List(ctx, h.p, h.key, "chat", "", "", 10)
	if err != nil || len(page.Rows) != 3 || codeOf(page.Unreadable["c4"]) != "KEY_MISMATCH" {
		t.Fatalf("page with a row under another key: rows=%v unreadable=%v err=%v", ids(page), page.Unreadable, err)
	}
}

func TestAnAbsentRowIsNotAnErrorForPullAndIs404ForLoad(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.push(t, "chat", "c1", `{"id":"c1"}`)
	rows, failed, err := h.rows.Pull(ctx, h.p, h.key, "chat", []string{"missing", "c1"})
	if err != nil || len(failed) != 0 || len(rows) != 1 || rows[0].ID != "c1" {
		t.Fatalf("pull with an absent id: rows=%+v failed=%v err=%v", rows, failed, err)
	}
	if _, err := h.rows.Load(ctx, h.p, h.key, "chat", "missing"); api.AsError(err).HTTP != 404 || codeOf(err) != "THREAD_NOT_FOUND" {
		t.Fatalf("load of an absent id: %v, want 404 THREAD_NOT_FOUND", err)
	}
}

func TestPullFetchesAFailedRowOnceMoreThenReportsIt(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.push(t, "chat", "c1", `{"id":"c1"}`)
	h.push(t, "chat", "c2", `{"id":"c2"}`)
	h.cp.Mu.Lock()
	h.cp.GetBlobFailures["chat/c1"] = []int{http.StatusServiceUnavailable}
	h.cp.GetBlobFailures["chat/c2"] = []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable}
	h.cp.Mu.Unlock()
	rows, failed, err := h.rows.Pull(ctx, h.p, h.key, "chat", []string{"c1", "c2"})
	if err != nil || len(rows) != 1 || rows[0].ID != "c1" || !errors.Is(failed["c2"], errRowNotFetched) {
		t.Fatalf("rows=%+v failed=%v err=%v, want c1 fetched on the second try and c2 reported", rows, failed, err)
	}
}

func TestPushMapsWhatTheControlPlaneRefuses(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	h.cp.Mu.Lock()
	h.cp.PutBlobFailures["chat/lost"] = controlplane.PutBlobMaxAttempts
	h.cp.Mu.Unlock()
	if _, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "lost", Data: []byte(`{}`)}, "lost"); codeOf(err) != "SYNC_OUTCOME_UNKNOWN" {
		t.Fatalf("a push whose answer was lost: %v, want SYNC_OUTCOME_UNKNOWN", err)
	}
	if _, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "huge", Data: append([]byte(`{"pad":"`), make([]byte, seal.MaxPlaintextBytes)...)}, "huge"); codeOf(err) != "THREAD_TOO_LARGE" {
		t.Fatalf("an oversized push: %v, want THREAD_TOO_LARGE", err)
	}
	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]any{"code": controlplane.StatusMissingAttachment, "missing_attachments": []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}})
	}))
	t.Cleanup(missing.Close)
	refusing := &Rows{Controlplane: controlplane.NewClient(missing.URL, nil)}
	_, err := refusing.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "c1", Data: []byte(`{}`)}, "missing")
	if e := api.AsError(err); e.Code != "MISSING_ATTACHMENT" || !reflect.DeepEqual(e.Missing, []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}) {
		t.Fatalf("a push naming an attachment the control plane lacks: %+v, want MISSING_ATTACHMENT naming it", e)
	}
	if !strings.Contains(err.Error(), "attachments") {
		t.Fatalf("error message %q does not say what is missing", err.Error())
	}
}
