package server

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/archives"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets/bucketstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// storeAttachment holds an attachment as the harness stores one: its bytes
// in buckets under the owner's tenant and the given key, and its index
// entry under the chat.
func (s *memoryRows) storeAttachment(user, chat, id string, key, data []byte) {
	tenant, err := buckets.TenantForUser(user)
	if err != nil {
		panic(err)
	}
	s.bk.Put(id, bucketstub.Item{Tenant: tenant, Value: data, EncryptionKeys: [][]byte{key}})
	s.cp.Mu.Lock()
	defer s.cp.Mu.Unlock()
	s.cp.AttachmentIndex[id], s.cp.AttachmentOwners[id] = chat, user
}

// seedAttachment stores data as an attachment of user's chat under an id
// and key of its own, and is that id and the key as a chat embeds it.
func seedAttachment(rows *memoryRows, user, chat string, data []byte) (id, key string) {
	sum := sha256.Sum256(append([]byte(chat+"\x00seed\x00"), data...))
	id = hex.EncodeToString(sum[:18])
	rows.storeAttachment(user, chat, id, sum[:], data)
	return id, base64.StdEncoding.EncodeToString(sum[:])
}

// attachmentBytes is the bytes stored under an attachment, which its key
// opens; a test fails when the harness stored none.
func attachmentBytes(t *testing.T, rows *memoryRows, a domain.Attachment) []byte {
	t.Helper()
	item, ok := rows.bk.Item(a.ID)
	key, err := base64.StdEncoding.DecodeString(a.EncryptionKey)
	if !ok || err != nil || len(item.EncryptionKeys) != 1 || !bytes.Equal(item.EncryptionKeys[0], key) {
		t.Fatalf("no attachment stored under %+v", a)
	}
	return item.Value
}

// attachmentsStored is how many attachments buckets holds.
func attachmentsStored(rows *memoryRows) int { return len(rows.bk.Tokens()) }

// indexedUnder is the chat the control plane indexes an attachment under.
func (s *memoryRows) indexedUnder(id string) string {
	s.cp.Mu.Lock()
	defer s.cp.Mu.Unlock()
	return s.cp.AttachmentIndex[id]
}

// sharedPayload opens a share as its reader does: the ciphertext the
// harness uploaded, with the key its link carries.
func sharedPayload(t *testing.T, link string, ciphertext []byte) []byte {
	t.Helper()
	_, key, ok := strings.Cut(link, "#v2:")
	plaintext, err := seal.OpenShare(key, ciphertext)
	if !ok || err != nil {
		t.Fatalf("the share at %s cannot be opened: %v", link, err)
	}
	return plaintext
}

// shareStore is a control plane that keeps the shares uploaded to it, by
// chat, and serves them back to readers.
type shareStore struct {
	mu     sync.Mutex
	shares map[string][]byte
}

func fakeShares(t *testing.T, h *harness) *shareStore {
	t.Helper()
	s := &shareStore{shares: map[string][]byte{}}
	cp := httptest.NewServer(s)
	t.Cleanup(cp.Close)
	h.atControlplane(cp.URL)
	h.archives.CPClient = &http.Client{Transport: &auth.CallerTransport{Inner: cp.Client().Transport}}
	return s
}

func (s *shareStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := strings.TrimPrefix(r.URL.Path, "/api/shares/")
	switch r.Method {
	case http.MethodPut:
		s.shares[id], _ = io.ReadAll(r.Body)
	case http.MethodGet:
		if body, ok := s.shares[id]; ok {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

// uploaded is the share of a chat as it was uploaded, and how many
// shares were.
func (s *shareStore) uploaded(id string) ([]byte, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shares[id], len(s.shares)
}

// fakeSync serves the sync enclave's import routes, which require protocol
// 3, and runs the import jobs they start.
type fakeSync struct {
	mu       sync.Mutex
	received map[string][]api.Object
	server   *httptest.Server
	imports  map[string]*fakeImport
}

func newFakeSync(t *testing.T, h *harness) *fakeSync {
	t.Helper()
	f := &fakeSync{received: map[string][]api.Object{}, imports: map[string]*fakeImport{}}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Sync-Protocol") != "3" {
			w.WriteHeader(426)
			io.WriteString(w, `{"ok":false,"code":"SYNC_PROTOCOL_UPGRADE_REQUIRED"}`)
			return
		}
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.received[r.URL.Path] = append(f.received[r.URL.Path], in)
		switch r.URL.Path {
		case "/v1/import/create", "/v1/import/upload", "/v1/import/start", "/v1/import/status":
			status, out := f.importStep(t, h, r, in)
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(out)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(f.server.Close)
	h.store.API = &store.Client{URL: f.server.URL, Client: f.server.Client()}
	return f
}

func TestPublicAttachmentPropagatesMissingBlob(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	w := postRoute(h, "/v1/attachments/get-public", api.Object{"id": "missing", "attKey": keyBase64(key)}, "")
	var response struct {
		Error api.Error `json:"error"`
	}
	json.Unmarshal(w.Body.Bytes(), &response)
	if w.Code != http.StatusNotFound || response.Error.Code != "THREAD_NOT_FOUND" {
		t.Fatalf("missing public blob: HTTP %d %s", w.Code, w.Body.String())
	}
}

func (f *fakeSync) requests(path string) []api.Object {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]api.Object(nil), f.received[path]...)
}

func capturePrompt(prompt *string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		*prompt = string(b)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}
}

// An upload is converted and forgotten: the harness that runs the turn has
// never seen it, and stores what the turn sends under the new chat.
func TestStoredUploadIsOffloadedUnderItsThread(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	uploader, _, _ := testChatHarness(t, nil)
	upload := uploadRoute(uploader, "/v1/attachments/upload", "notes.txt", []byte("remember the blue door"), nil, "alice")
	if upload.Code != 200 {
		t.Fatalf("upload failed: %d %s", upload.Code, upload.Body.String())
	}
	var body struct {
		Attachment api.Object `json:"attachment"`
	}
	json.Unmarshal(upload.Body.Bytes(), &body)
	sent := body.Attachment
	if sent["kind"] != "document" || sent["fileName"] != "notes.txt" || sent["textContent"] != "remember the blue door" || api.Int(sent["size"]) != len("remember the blue door") || !strings.HasPrefix(api.Str(sent["id"]), "att_") {
		t.Fatalf("upload descriptor: %v", sent)
	}
	in := sendInput(key)
	in["attachments"] = []any{sent}
	events := finishedRun(t, h, "/v1/threads/turn", in)
	thread := events[0].ThreadID
	if !strings.Contains(prompt, "remember the blue door") {
		t.Fatal("the document did not reach the model")
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", thread)
	stored := api.Obj(api.Arr(api.Obj(api.Arr(rowJSON(row)["messages"])[0])["attachments"])[0])
	if !domain.EnclaveAttachmentID.MatchString(api.Str(stored["id"])) || api.Str(stored["encryptionKey"]) == "" || stored["storageThreadId"] != thread {
		t.Fatalf("row does not reference the stored blob: %v", stored)
	}
	held := mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments[0]
	if payload := attachmentBytes(t, rows, held); attachmentsStored(rows) != 1 || string(payload) != `{"textContent":"remember the blue door"}` || rows.indexedUnder(held.ID) != thread {
		t.Fatalf("blob is not the document payload under its thread: %s, %d stored, indexed under %q", payload, attachmentsStored(rows), rows.indexedUnder(held.ID))
	}
	for _, field := range []string{"textContent", "pages", "base64", "legacyId"} {
		if _, ok := stored[field]; ok {
			t.Fatalf("row kept %s: %v", field, stored)
		}
	}
	// An edit names the attachment the chat stores by its id alone.
	edit := continueInput(key, thread)
	edit["kind"], edit["messageId"], edit["content"] = "edit", mustRead(domain.ReadThreadRow(row)).Messages[0].ID, "look again"
	edit["attachments"] = []any{api.Object{"id": "att_unknown"}}
	if w := postRoute(h, "/v1/threads/turn", edit, "alice"); w.Code != 404 {
		t.Fatalf("an edit naming an unknown attachment: HTTP %d", w.Code)
	}
	edit["clientRequestId"], edit["attachments"] = uuid.NewString(), []any{api.Object{"id": stored["id"]}}
	finishedRun(t, h, "/v1/threads/turn", edit)
	row, _ = rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", thread)
	kept := mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments[0]
	if kept.ID != stored["id"] || kept.EncryptionKey != stored["encryptionKey"] || attachmentsStored(rows) != 1 {
		t.Fatalf("the edit did not keep the stored attachment: %+v after %d blobs", kept, attachmentsStored(rows))
	}
}

func storedThread(rows *memoryRows, user, id string, attachments ...api.Object) {
	values := []any{}
	for _, a := range attachments {
		values = append(values, a)
	}
	rows.rows[rowMapKey(&auth.Principal{ID: user}, "chat", id)] = domain.StoredRow{ID: id, ETag: "1", Data: api.Raw(api.Object{"title": "Old", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z", "messages": []any{
		api.Object{"id": "m1", "role": "user", "content": "look at this", "attachments": values},
		api.Object{"id": "m2", "role": "assistant", "content": "ok", "timeline": []any{api.Object{"id": "b1", "type": "content", "content": "ok"}}},
	}})}
}

func continueInput(key auth.ContentKey, thread string) api.Object {
	in := sendInput(key)
	in["threadId"] = thread
	return in
}

func TestOffloadedDocumentIsReadBackForEachTurn(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	id, blobKey := seedAttachment(rows, "alice", "chat-1", []byte(`{"textContent":"the quarterly numbers"}`))
	storedThread(rows, "alice", "chat-1", api.Object{"id": id, "type": "document", "fileName": "report.pdf", "encryptionKey": blobKey})
	eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	if !strings.Contains(prompt, "the quarterly numbers") {
		t.Fatal("the offloaded document was not read back")
	}
	if attachmentsStored(rows) != 1 {
		t.Fatal("a document already stored under its thread was stored again")
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	stored := mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments[0]
	if stored.ID != id || stored.TextContent != "" {
		t.Fatalf("the stored reference changed: %+v", stored)
	}
}

func TestMissingDocumentIsDescribedInsteadOfFailingTheTurn(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	storedThread(rows, "alice", "chat-1", api.Object{"id": strings.Repeat("a", 36), "type": "document", "fileName": "gone.pdf", "encryptionKey": keyBase64(randomKey())})
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	if events[len(events)-1].Type != "RUN_FINISHED" || !strings.Contains(prompt, "gone.pdf is no longer available") {
		t.Fatalf("a deleted document blocked the chat: %s", prompt)
	}
}

func TestInlineDocumentIsOffloadedUnderItsThread(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	// The web and iOS apps kept a document's content in the row before storing it as a blob.
	clientID := "3f2b8c1e-5d4a-4e6b-9c7d-1a2b3c4d5e6f"
	storedThread(rows, "alice", "chat-1", api.Object{"id": clientID, "type": "document", "fileName": "deck.pdf", "textContent": "slide text", "pages": nil})
	eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	stored := api.Obj(api.Arr(api.Obj(api.Arr(rowJSON(row)["messages"])[0])["attachments"])[0])
	if !domain.EnclaveAttachmentID.MatchString(api.Str(stored["id"])) || api.Str(stored["encryptionKey"]) == "" || stored["storageThreadId"] != "chat-1" || stored["textContent"] != nil {
		t.Fatalf("row does not reference the stored blob: %v", stored)
	}
	held := mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments[0]
	if payload := attachmentBytes(t, rows, held); attachmentsStored(rows) != 1 || string(payload) != `{"textContent":"slide text"}` || rows.indexedUnder(held.ID) != "chat-1" {
		t.Fatalf("not stored once as a payload under the thread: %s, %d stored", payload, attachmentsStored(rows))
	}
	if _, ok := stored["pages"]; ok {
		t.Fatal("a null field was stored")
	}
	if !strings.Contains(prompt, "slide text") {
		t.Fatal("the document did not reach the model")
	}
}

func TestMissingAttachmentsAreRepairedBeforeRetrying(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	docID, docKey := seedAttachment(rows, "alice", "another-chat", []byte(`{"textContent":"filed elsewhere"}`))
	imageID := strings.Repeat("b", 36)
	storedThread(rows, "alice", "chat-1",
		api.Object{"id": docID, "type": "document", "fileName": "notes.md", "encryptionKey": docKey},
		api.Object{"id": imageID, "type": "image", "fileName": "photo.png", "encryptionKey": keyBase64(randomKey()), "thumbnailBase64": "thumb"})
	refused := 0
	rows.refuse = func(row *domain.StoredRow) error {
		for _, a := range mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments {
			if a.ID == docID || a.ID == imageID && a.EncryptionKey != "" {
				refused++
				return api.DownstreamError(409, []byte(`{"ok":false,"code":"MISSING_ATTACHMENT","missing_attachments":["`+docID+`","`+imageID+`"]}`))
			}
		}
		return nil
	}
	if _, err := h.updateThread(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, key, requestOf[updateThreadRequest](api.Object{"id": "chat-1", "title": "Renamed"})); err != nil {
		t.Fatal(err)
	}
	if refused != 1 {
		t.Fatalf("expected one refused push, got %d", refused)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	chat := mustRead(domain.ReadThreadRow(row))
	doc, image := chat.Messages[0].Attachments[0], chat.Messages[0].Attachments[1]
	if doc.ID == docID || doc.StorageThreadID != "chat-1" || rows.indexedUnder(doc.ID) != "chat-1" || string(attachmentBytes(t, rows, doc)) != `{"textContent":"filed elsewhere"}` {
		t.Fatalf("the document was not stored under its chat: %+v", doc)
	}
	if image.EncryptionKey != "" || image.Thumbnail != "thumb" || chat.Title != "Renamed" {
		t.Fatalf("the unreachable image was not retired to its thumbnail: %+v", image)
	}
}

func TestLegacyKeyedAttachmentIsReadFromTheControlPlane(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	legacyKey := bytes.Repeat([]byte{7}, 32)
	block, _ := aes.NewCipher(legacyKey)
	gcm, _ := cipher.NewGCM(block)
	nonce := bytes.Repeat([]byte{1}, gcm.NonceSize())
	sealed := append(nonce, gcm.Seal(nil, nonce, []byte("old photo"), nil)...)
	// The web and iOS apps stored images at the control plane itself before
	// the harness stored them.
	rows.cp.Mu.Lock()
	rows.cp.LegacyAttachments = map[string][]byte{"legacy-1": sealed}
	rows.cp.Mu.Unlock()
	storedThread(rows, "alice", "chat-1", api.Object{"id": "legacy-1", "type": "image", "fileName": "photo.png", "description": "a photo", "key": base64.StdEncoding.EncodeToString(legacyKey)})
	eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	stored := mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments[0]
	if stored.Key != "" || !domain.EnclaveAttachmentID.MatchString(stored.ID) {
		t.Fatalf("legacy reference kept: %+v", stored)
	}
	if payload := attachmentBytes(t, rows, stored); attachmentsStored(rows) != 1 || string(payload) != "old photo" || rows.indexedUnder(stored.ID) != "chat-1" {
		t.Fatalf("legacy attachment was not moved under its thread: stored %q, %d objects", payload, attachmentsStored(rows))
	}
}

func TestDocumentDownloadReturnsExtractedText(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	id, blobKey := seedAttachment(rows, "alice", "chat-1", []byte(`{"pages":[{"page":1,"text":"page one","image":"","is_scanned":false}]}`))
	storedThread(rows, "alice", "chat-1", api.Object{"id": id, "type": "document", "fileName": "scan.pdf", "encryptionKey": blobKey})
	w := postRoute(h, "/v1/attachments/get", api.Object{"key": keyBase64(key), "id": id, "threadId": "chat-1"}, "alice")
	if w.Code != 200 || w.Body.String() != "page one" {
		t.Fatalf("download was not the extracted text: %d %q", w.Code, w.Body.String())
	}
	if w.Header().Get("Content-Type") != attach.OpaqueContentType || w.Header().Get("Content-Disposition") != "" {
		t.Fatalf("download headers disclose attachment metadata: %v", w.Header())
	}
	// Headers travel outside the sealed body.
	for name, values := range w.Header() {
		if joined := strings.Join(values, " "); strings.Contains(joined, "scan") || strings.Contains(joined, "markdown") || strings.Contains(joined, "pdf") {
			t.Fatalf("header %s describes the file: %q", name, joined)
		}
	}
}

func TestForkCopiesAChatThroughItsMessage(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	project := "project-1"
	imageID, imageKey := seedAttachment(rows, "alice", "chat-1", []byte("png bytes"))
	messages := []any{
		api.Object{"id": "m1", "role": "user", "content": "plan a trip", "attachments": []any{api.Object{"id": imageID, "type": "image", "fileName": "map.png", "mimeType": "image/png", "encryptionKey": imageKey, "thumbnailBase64": "thumb"}}},
		api.Object{"id": "m2", "role": "assistant", "content": "first, pack"}, api.Object{"id": "m3", "role": "user", "content": "go on"}}
	source := domain.StoredRow{ID: "chat-1", ETag: "4", Project: &project, ProjectSet: true, Data: api.Raw(api.Object{"title": "Trip", "titleState": "generated", "createdAt": "2026-01-01T00:00:00Z", "presetId": "builtin:tutor", "model": "kimi-k3", "webSearchEnabled": false,
		"pinned": true, "contextUsage": api.Object{"usedTokens": 900}, "harnessRequests": api.Object{"nonce": api.Object{"runId": "run", "digest": "digest"}}, "messages": messages})}
	rows.rows[rowMapKey(p, "chat", "chat-1")] = source
	for _, tc := range []struct {
		in   api.Object
		code int
	}{{api.Object{"threadId": "chat-1", "messageId": "m4"}, 404}, {api.Object{"threadId": "chat-1"}, 400}} {
		tc.in["key"] = keyBase64(key)
		if w := postRoute(h, "/v1/threads/fork", tc.in, "alice"); w.Code != tc.code || len(rows.rows) != 1 || attachmentsStored(rows) != 1 {
			t.Fatalf("%v: HTTP %d, want %d without a fork", tc.in, w.Code, tc.code)
		}
	}
	w := postRoute(h, "/v1/threads/fork", api.Object{"key": keyBase64(key), "threadId": "chat-1", "messageId": "m2"}, "alice")
	var summary api.Object
	json.Unmarshal(w.Body.Bytes(), &summary)
	target := api.Str(summary["id"])
	created, err := time.Parse(time.RFC3339Nano, api.Str(summary["createdAt"]))
	if w.Code != 200 || !regexp.MustCompile(`^\d{13}_[0-9a-f-]{36}$`).MatchString(target) || err != nil || time.Since(created) > time.Minute {
		t.Fatalf("fork: HTTP %d %s", w.Code, w.Body.String())
	}
	if summary["title"] != "Trip (fork)" || summary["pinned"] != false || summary["activeRun"] != nil || summary["contextUsage"] != nil {
		t.Fatalf("fork summary: %v", summary)
	}
	row, err := rows.Load(context.Background(), p, key, "chat", target)
	if err != nil || row.ETag != "1" || !row.ProjectSet || domain.ValueOr(row.Project, "") != project {
		t.Fatalf("the fork's row = %+v (%v), want a new row under the source's project", row, err)
	}
	fork := mustRead(domain.ReadThreadRow(row))
	if fork.Pinned || fork.ContextUsage != nil || fork.ActiveRun != nil || fork.TitleState != "manual" || rowJSON(row)["harnessRequests"] != nil {
		t.Fatalf("the fork kept the source's bookkeeping: %s", row.Data)
	}
	if domain.ValueOr(fork.PresetID, "") != "builtin:tutor" || len(fork.Messages) != 2 || fork.Messages[0].ID != "m1" || fork.Messages[1].ID != "m2" || fork.CreatedAt != api.Str(summary["createdAt"]) {
		t.Fatalf("the fork lost the conversation: %v", row.Data)
	}
	// The fork's attachment is its own copy, under the fork with a key of
	// its own, and the source keeps what it had.
	copied := fork.Messages[0].Attachments[0]
	if copied.ID == imageID || copied.EncryptionKey == imageKey || copied.StorageThreadID != target || copied.Thumbnail != "thumb" || copied.Base64 != "" {
		t.Fatalf("the fork's attachment = %+v", copied)
	}
	if attachmentsStored(rows) != 2 || string(attachmentBytes(t, rows, copied)) != "png bytes" || rows.indexedUnder(copied.ID) != target || rows.indexedUnder(imageID) != "chat-1" {
		t.Fatalf("attachments after the fork: %d stored, the copy indexed under %q", attachmentsStored(rows), rows.indexedUnder(copied.ID))
	}
	if kept := rows.rows[rowMapKey(p, "chat", "chat-1")]; !reflect.DeepEqual(kept, source) {
		t.Fatalf("the source changed: %+v", kept)
	}
}

func TestForkTitlesNameTheirSource(t *testing.T) {
	for title, want := range map[string]string{"Trip (fork)": "Trip (fork)", "": "Untitled (fork)"} {
		if got := forkTitle(title); got != want {
			t.Errorf("forkTitle(%q) = %q, want %q", title, got, want)
		}
	}
}

func TestALocalChatIsSharedFromItsTranscript(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	type upload struct {
		method, path, auth, format string
		body                       []byte
	}
	uploads := make(chan upload, 4)
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		uploads <- upload{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Format-Version"), body}
	}))
	defer cp.Close()
	h.atControlplane(cp.URL)
	h.archives.CPClient = &http.Client{Transport: &auth.CallerTransport{Inner: cp.Client().Transport}}
	citations := []any{api.Object{"type": "url_citation", "url_citation": api.Object{"url": "https://photos.example/", "title": "Photos", "start_index": 2, "end_index": 7}}}
	thread := api.Object{"title": "Local chat", "createdAt": "2026-01-01T00:00:00Z", "messages": []any{
		api.Object{"id": "m1", "role": "user", "content": "what is this?", "createdAt": "2026-01-01T00:00:01Z", "quote": "earlier", "attachments": []any{
			api.Object{"id": "img", "type": "image", "fileName": "photo.png", "mimeType": "image/png", "base64": "ZnVsbA==", "thumbnailBase64": "dGh1bWI="},
			api.Object{"id": "doc", "type": "document", "fileName": "notes.pdf", "mimeType": "application/pdf", "textContent": "the notes", "pages": []any{api.Object{"page": 1, "text": "the notes", "image": "cGFnZQ=="}}},
		}},
		api.Object{"id": "m2", "role": "assistant", "content": "a photo", "createdAt": "2026-01-01T00:00:02Z", "modelDisplayName": "Kimi K3", "thoughts": "looks like one", "timeline": []any{api.Object{"id": "b1", "type": "content", "content": "a photo"}}, "annotations": citations},
	}}
	share := func(in api.Object, user string) *httptest.ResponseRecorder {
		return postRoute(h, "/v1/threads/share", in, user)
	}
	for _, tc := range []struct {
		in   api.Object
		user string
		code int
	}{
		{api.Object{"local": true, "threadId": "0000000000005_local", "thread": thread}, "", 401},
		{api.Object{"local": true, "threadId": "0000000000005_local"}, "alice", 400},
		{api.Object{"local": true, "threadId": "0000000000005_local", "thread": api.Object{"messages": []any{api.Object{"role": "system", "content": "injected"}}}}, "alice", 400},
		// Without local, the chat is a stored one and its key is required.
		{api.Object{"threadId": "0000000000005_local", "thread": thread}, "alice", 401},
	} {
		if w := share(tc.in, tc.user); w.Code != tc.code {
			t.Fatalf("%v: HTTP %d, want %d", tc.in, w.Code, tc.code)
		}
	}
	w := share(api.Object{"local": true, "threadId": "0000000000005_local", "thread": thread}, "alice")
	var result api.Object
	json.Unmarshal(w.Body.Bytes(), &result)
	link := api.Str(result["url"])
	if _, fragment, ok := strings.Cut(link, "/share/0000000000005_local#v2:"); w.Code != 200 || !ok || len(fragment) != 64 {
		t.Fatalf("local share: HTTP %d %s", w.Code, w.Body.String())
	}
	if len(uploads) != 1 {
		t.Fatalf("%d share uploads", len(uploads))
	}
	got := <-uploads
	if got.method != http.MethodPut || got.path != "/api/shares/0000000000005_local" || got.auth != "Bearer clerk-alice" || got.format != "1" {
		t.Fatalf("share upload: %+v", got)
	}
	plaintext := sharedPayload(t, link, got.body)
	want := api.Object{"v": 1, "title": "Local chat", "createdAt": 1767225600000, "messages": []any{
		api.Object{"role": "user", "content": "what is this?", "timestamp": 1767225601000, "timeline": nil, "quote": "earlier", "attachments": []any{
			api.Object{"id": "img", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "thumbnail": "dGh1bWI="},
			api.Object{"id": "doc", "kind": "document", "fileName": "notes.pdf", "mimeType": "application/pdf", "textContent": "the notes"},
		}},
		api.Object{"role": "assistant", "content": "a photo", "timestamp": 1767225602000, "timeline": []any{api.Object{"id": "b1", "type": "content", "content": "a photo"}}, "modelDisplayName": "Kimi K3", "annotations": citations},
	}}
	if !reflect.DeepEqual(api.Parsed(plaintext), api.Parsed(api.Raw(want))) {
		t.Fatalf("the share holds %s", plaintext)
	}
	// The opened share lists the reply's citations as its sources.
	if opened := mustRead(archives.SharedThreadView("0000000000005_local", plaintext)).Messages[1].Annotations; !reflect.DeepEqual(api.Parsed(api.Raw(opened)), api.Parsed(api.Raw(citations))) {
		t.Fatalf("the opened share cites %s", api.Raw(opened))
	}
	if attachmentsStored(rows) != 0 || rows.calls != 0 {
		t.Fatalf("a local share stored %d attachments and made %d storage calls", attachmentsStored(rows), rows.calls)
	}
}

func TestAStoredChatIsSharedFromItsRow(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	storedThread(rows, "alice", "chat-1")
	shares := fakeShares(t, h)
	w := postRoute(h, "/v1/threads/share", api.Object{"key": keyBase64(key), "threadId": "chat-1"}, "alice")
	var result api.Object
	json.Unmarshal(w.Body.Bytes(), &result)
	link := api.Str(result["url"])
	uploaded, count := shares.uploaded("chat-1")
	if w.Code != 200 || count != 1 {
		t.Fatalf("share: HTTP %d %s after %d uploads", w.Code, w.Body.String(), count)
	}
	shared := api.Obj(api.Parsed(sharedPayload(t, link, uploaded)))
	if messages := api.Arr(shared["messages"]); shared["title"] != "Old" || len(messages) != 2 || api.Obj(messages[0])["content"] != "look at this" || api.Obj(messages[1])["content"] != "ok" {
		t.Fatalf("shared %v", shared)
	}
	// A reader opens the share with the link alone, and nothing else does.
	_, fragment, _ := strings.Cut(link, "#")
	var opened domain.SharedThread
	if w := postRoute(h, "/v1/shares/open", api.Object{"shareId": "chat-1", "fragment": fragment}, ""); w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &opened) != nil || opened.Title != "Old" || len(opened.Messages) != 2 || domain.ValueOr(opened.Messages[0].Content, "") != "look at this" {
		t.Fatalf("open: HTTP %d %s", w.Code, w.Body.String())
	}
	if w := postRoute(h, "/v1/shares/open", api.Object{"shareId": "chat-1", "fragment": "v2:" + strings.Repeat("0", 64)}, ""); w.Code != 400 || !strings.Contains(w.Body.String(), "BAD_REQUEST") {
		t.Fatalf("open with another key: HTTP %d %s", w.Code, w.Body.String())
	}
}

func TestAChatTheProfileListsReadsAsPinned(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &auth.Principal{ID: "alice"}
	storedThread(rows, "alice", "chat-1")
	storedThread(rows, "alice", "chat-2")
	rows.rows[rowMapKey(p, "profile", "profile")] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(api.Object{"pinnedChatIds": []any{"chat-1"}})}
	pinned := func() map[string]any {
		t.Helper()
		var page struct {
			Threads []api.Object `json:"threads"`
		}
		json.Unmarshal(postRoute(h, "/v1/threads/list", api.Object{"key": keyBase64(key)}, "alice").Body.Bytes(), &page)
		out := map[string]any{}
		for _, thread := range page.Threads {
			out[api.Str(thread["id"])] = thread["pinned"]
		}
		return out
	}
	if got := pinned(); got["chat-1"] != true || got["chat-2"] != false || rows.rows[rowMapKey(p, "chat", "chat-1")].ETag != "1" {
		t.Fatalf("listed pins %v; the read wrote the chat: %v", got, rows.rows[rowMapKey(p, "chat", "chat-1")].ETag != "1")
	}
	update := func(id string, pin bool) {
		t.Helper()
		if w := postRoute(h, "/v1/threads/update", api.Object{"key": keyBase64(key), "id": id, "pinned": pin}, "alice"); w.Code != 200 {
			t.Fatalf("pinning %s: HTTP %d %s", id, w.Code, w.Body.String())
		}
	}
	update("chat-2", true)
	if pins := mustRead(domain.ReadProfile(rows.rows[rowMapKey(p, "profile", "profile")].Data)).PinnedChatIDs; !slices.Contains(pins, "chat-2") {
		t.Fatal("pinning a chat did not update the complete favorites index")
	}
	update("chat-1", false)
	if got := pinned(); got["chat-1"] != false || got["chat-2"] != true {
		t.Fatalf("after pinning chat-2 and unpinning chat-1 the list shows %v", got)
	}
	var page struct {
		Pinned []domain.ThreadSummary `json:"pinned"`
	}
	json.Unmarshal(postRoute(h, "/v1/threads/list", api.Object{"key": keyBase64(key)}, "alice").Body.Bytes(), &page)
	if len(page.Pinned) != 1 || page.Pinned[0].ID != "chat-2" {
		t.Fatalf("favorites: %+v", page.Pinned)
	}
	if w := postRoute(h, "/v1/threads/delete", api.Object{"key": keyBase64(key), "id": "chat-2"}, "alice"); w.Code != 200 {
		t.Fatalf("delete favorite: %s", w.Body.String())
	}
	if pins := mustRead(domain.ReadProfile(rows.rows[rowMapKey(p, "profile", "profile")].Data)).PinnedChatIDs; len(pins) != 0 {
		t.Fatalf("deleted favorite still indexed: %v", pins)
	}
}

// A legacy image moved into buckets under the same id and key, as a read
// of its chat moves it, is read from there by the key the chat still
// embeds under its old name.
func TestAPromotedLegacyImageIsReadFromBuckets(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	legacyKey := bytes.Repeat([]byte{7}, 32)
	rows.storeAttachment("alice", "chat-1", "legacy-2", legacyKey, []byte("promoted photo"))
	rows.cp.Mu.Lock()
	rows.cp.GoneAttachments = map[string]bool{"legacy-2": true}
	rows.cp.Mu.Unlock()
	storedThread(rows, "alice", "chat-1", api.Object{"id": "legacy-2", "type": "image", "fileName": "photo.png", "mimeType": "image/png", "key": base64.StdEncoding.EncodeToString(legacyKey)})
	w := postRoute(h, "/v1/attachments/get", api.Object{"key": keyBase64(key), "id": "legacy-2", "threadId": "chat-1"}, "alice")
	if w.Code != 200 || w.Body.String() != "promoted photo" {
		t.Fatalf("a promoted legacy image: HTTP %d %s", w.Code, w.Body.String())
	}
}

// A document whose blob sync never held loses its reference, and each turn
// tells the model it is gone without storing that in the chat.
func TestARetiredDocumentIsDescribedOnlyToTheModel(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	storedThread(rows, "alice", "chat-1", api.Object{"id": "legacy-doc", "type": "document", "fileName": "old.pdf", "encryptionKey": keyBase64(randomKey())})
	finishedRun(t, h, "/v1/threads/turn", continueInput(key, "chat-1"))
	if !strings.Contains(prompt, "old.pdf is no longer available") {
		t.Fatalf("the model was not told the document is gone: %s", prompt)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	stored := mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments[0]
	if stored.EncryptionKey != "" || stored.TextContent != "" || stored.FileName != "old.pdf" {
		t.Fatalf("the retired document was stored as %+v", stored)
	}
}

// A share id names one share, however it is spelled, since the control
// plane answers this host on routes it answers no one else.
func TestOpeningAShareNamesOnlyThatShare(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	paths := make(chan string, 2)
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.EscapedPath()
		w.WriteHeader(http.StatusNotFound)
	}))
	defer cp.Close()
	h.atControlplane(cp.URL)
	h.archives.CPClient = cp.Client()
	fragment := "v2:" + strings.Repeat("ab", 32)
	if w := postRoute(h, "/v1/shares/open", api.Object{"shareId": "..", "fragment": fragment}, ""); w.Code != 400 {
		t.Fatalf("a dot segment share id: HTTP %d", w.Code)
	}
	postRoute(h, "/v1/shares/open", api.Object{"shareId": "..%2Fshim%2Frouter-config", "fragment": fragment}, "")
	if got := <-paths; got != "/api/shares/..%252Fshim%252Frouter-config" {
		t.Fatalf("the control plane was asked for %s", got)
	}
}
