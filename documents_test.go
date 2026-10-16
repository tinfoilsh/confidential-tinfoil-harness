package main

import (
	"archive/zip"
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
	"strings"
	"sync"
	"testing"
)

type fakeBlob struct {
	chat, key string
	data      []byte
}

// fakeSync serves the attachment routes the way the sync enclave does: ids and
// keys derive from the idempotency key and chat, and protocol 3 is required.
type fakeSync struct {
	mu     sync.Mutex
	blobs  map[string]fakeBlob
	puts   []object
	server *httptest.Server
}

func newFakeSync(t *testing.T, h *harness) *fakeSync {
	t.Helper()
	f := &fakeSync{blobs: map[string]fakeBlob{}}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Sync-Protocol") != "3" {
			w.WriteHeader(426)
			io.WriteString(w, `{"ok":false,"code":"SYNC_PROTOCOL_UPGRADE_REQUIRED"}`)
			return
		}
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/v1/attachment/put":
			data, _ := base64.StdEncoding.DecodeString(str(in["plaintext"]))
			sum := sha256.Sum256([]byte(str(in["idempotency_key"]) + "\x00" + str(in["chat_id"])))
			id, key := hex.EncodeToString(sum[:18]), base64.StdEncoding.EncodeToString(sum[:])
			f.blobs[id] = fakeBlob{chat: str(in["chat_id"]), key: key, data: data}
			f.puts = append(f.puts, in)
			json.NewEncoder(w).Encode(object{"ok": true, "id": id, "att_key": key})
		case "/v1/attachment/get":
			blob, ok := f.blobs[str(in["id"])]
			if !ok {
				w.WriteHeader(404)
				io.WriteString(w, `{"ok":false,"code":"NOT_FOUND"}`)
				return
			}
			if blob.key != str(in["att_key"]) {
				w.WriteHeader(400)
				io.WriteString(w, `{"ok":false,"code":"BAD_REQUEST"}`)
				return
			}
			json.NewEncoder(w).Encode(object{"ok": true, "plaintext": base64.StdEncoding.EncodeToString(blob.data)})
		case "/v1/share/seal":
			json.NewEncoder(w).Encode(object{"ok": true, "share_key": strings.Repeat("ab", 32), "ciphertext": base64.StdEncoding.EncodeToString([]byte("sealed"))})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(f.server.Close)
	h.syncAPI = &syncClient{URL: f.server.URL, Client: f.server.Client()}
	return f
}

func (f *fakeSync) seed(chat string, data []byte) (string, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sum := sha256.Sum256(append([]byte(chat+"\x00seed\x00"), data...))
	id, key := hex.EncodeToString(sum[:18]), base64.StdEncoding.EncodeToString(sum[:])
	f.blobs[id] = fakeBlob{chat: chat, key: key, data: data}
	return id, key
}

func (f *fakeSync) putCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.puts)
}

func capturePrompt(prompt *string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		*prompt = string(b)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}
}

func TestStoredUploadIsOffloadedUnderItsThread(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	fake := newFakeSync(t, h)
	upload := uploadRoute(h, "/v1/attachments/upload", "notes.txt", []byte("remember the blue door"), map[string]string{"key": key.base64()}, "alice")
	if upload.Code != 200 {
		t.Fatalf("upload failed: %d %s", upload.Code, upload.Body.String())
	}
	var view object
	json.Unmarshal(upload.Body.Bytes(), &view)
	if fake.putCount() != 0 {
		t.Fatal("an upload was stored before its thread existed")
	}
	in := sendInput(key)
	in["attachments"] = []any{view["id"]}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	thread := events[0].ThreadID
	if !strings.Contains(prompt, "remember the blue door") {
		t.Fatal("the document did not reach the model")
	}
	if fake.putCount() != 1 {
		t.Fatalf("expected one blob, got %d", fake.putCount())
	}
	put := fake.puts[0]
	payload, _ := base64.StdEncoding.DecodeString(str(put["plaintext"]))
	if put["chat_id"] != thread || string(payload) != `{"textContent":"remember the blue door"}` || put["idempotency_key"] != attachmentIdempotencyKey(thread, str(view["id"]), payload) {
		t.Fatalf("blob is not the document payload under its thread: %v %s", put, payload)
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", thread)
	stored := obj(arr(obj(arr(row.Data["messages"])[0])["attachments"])[0])
	if !enclaveAttachmentID.MatchString(str(stored["id"])) || str(stored["encryptionKey"]) == "" || stored["storageThreadId"] != thread {
		t.Fatalf("row does not reference the stored blob: %v", stored)
	}
	for _, field := range []string{"textContent", "pages", "base64", "legacyId"} {
		if _, ok := stored[field]; ok {
			t.Fatalf("row kept %s: %v", field, stored)
		}
	}
}

func storedThread(rows *memoryRows, user, id string, attachments ...object) {
	values := []any{}
	for _, a := range attachments {
		values = append(values, a)
	}
	rows.rows[rowMapKey(&principal{ID: user}, "chat", id)] = storedRow{ID: id, ETag: "1", Data: object{"title": "Old", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z", "messages": []any{
		object{"id": "m1", "role": "user", "content": "look at this", "attachments": values},
		object{"id": "m2", "role": "assistant", "content": "ok", "timeline": []any{object{"id": "b1", "type": "content", "content": "ok"}}},
	}}}
}

func continueInput(key contentKey, thread string) object {
	in := sendInput(key)
	in["threadId"] = thread
	return in
}

func TestOffloadedDocumentIsReadBackForEachTurn(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	fake := newFakeSync(t, h)
	id, blobKey := fake.seed("chat-1", []byte(`{"textContent":"the quarterly numbers"}`))
	storedThread(rows, "alice", "chat-1", object{"id": id, "type": "document", "fileName": "report.pdf", "encryptionKey": blobKey})
	eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	if !strings.Contains(prompt, "the quarterly numbers") {
		t.Fatal("the offloaded document was not read back")
	}
	if fake.putCount() != 0 {
		t.Fatal("a document already stored under its thread was stored again")
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "chat-1")
	stored := obj(arr(obj(arr(row.Data["messages"])[0])["attachments"])[0])
	if stored["id"] != id || stored["textContent"] != nil {
		t.Fatalf("the stored reference changed: %v", stored)
	}
}

func TestMissingDocumentIsDescribedInsteadOfFailingTheTurn(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	newFakeSync(t, h)
	storedThread(rows, "alice", "chat-1", object{"id": strings.Repeat("a", 36), "type": "document", "fileName": "gone.pdf", "encryptionKey": "key"})
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	if events[len(events)-1].Type != "RUN_FINISHED" || !strings.Contains(prompt, "gone.pdf is no longer available") {
		t.Fatalf("a deleted document blocked the chat: %s", prompt)
	}
}

func TestInlineDocumentIsOffloadedUnderItsThread(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	fake := newFakeSync(t, h)
	// The web and iOS apps kept a document's content in the row before storing it as a blob.
	clientID := "3f2b8c1e-5d4a-4e6b-9c7d-1a2b3c4d5e6f"
	storedThread(rows, "alice", "chat-1", object{"id": clientID, "type": "document", "fileName": "deck.pdf", "textContent": "slide text", "pages": nil})
	eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	if fake.putCount() != 1 {
		t.Fatalf("expected the document to be stored once, got %d", fake.putCount())
	}
	payload, _ := base64.StdEncoding.DecodeString(str(fake.puts[0]["plaintext"]))
	if fake.puts[0]["chat_id"] != "chat-1" || string(payload) != `{"textContent":"slide text"}` {
		t.Fatalf("not stored as a payload under the thread: %s", payload)
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "chat-1")
	stored := obj(arr(obj(arr(row.Data["messages"])[0])["attachments"])[0])
	if !enclaveAttachmentID.MatchString(str(stored["id"])) || str(stored["encryptionKey"]) == "" || stored["storageThreadId"] != "chat-1" || stored["textContent"] != nil {
		t.Fatalf("row does not reference the stored blob: %v", stored)
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
	fake := newFakeSync(t, h)
	docID, docKey := fake.seed("another-chat", []byte(`{"textContent":"filed elsewhere"}`))
	imageID := strings.Repeat("b", 36)
	storedThread(rows, "alice", "chat-1",
		object{"id": docID, "type": "document", "fileName": "notes.md", "encryptionKey": docKey},
		object{"id": imageID, "type": "image", "fileName": "photo.png", "encryptionKey": "gone", "thumbnailBase64": "thumb"})
	refused := 0
	rows.refuse = func(row *storedRow) error {
		for _, a := range arr(obj(arr(row.Data["messages"])[0])["attachments"]) {
			if id := str(obj(a)["id"]); id == docID || id == imageID && obj(a)["encryptionKey"] != nil {
				refused++
				return downstreamError(409, []byte(`{"ok":false,"code":"MISSING_ATTACHMENT","missing_attachments":["`+docID+`","`+imageID+`"]}`))
			}
		}
		return nil
	}
	if _, err := h.updateThread(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, key, object{"id": "chat-1", "title": "Renamed"}); err != nil {
		t.Fatal(err)
	}
	if refused != 1 {
		t.Fatalf("expected one refused push, got %d", refused)
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "chat-1")
	attachments := arr(obj(arr(row.Data["messages"])[0])["attachments"])
	doc, image := obj(attachments[0]), obj(attachments[1])
	if doc["id"] == docID || doc["storageThreadId"] != "chat-1" || fake.blobs[str(doc["id"])].chat != "chat-1" {
		t.Fatalf("the document was not stored under its chat: %v", doc)
	}
	if image["encryptionKey"] != nil || image["thumbnailBase64"] != "thumb" || row.Data["title"] != "Renamed" {
		t.Fatalf("the unreachable image was not retired to its thumbnail: %v", image)
	}
}

func TestLegacyKeyedAttachmentIsReadFromTheControlPlane(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	legacyKey := bytes.Repeat([]byte{7}, 32)
	block, _ := aes.NewCipher(legacyKey)
	gcm, _ := cipher.NewGCM(block)
	nonce := bytes.Repeat([]byte{1}, gcm.NonceSize())
	sealed := append(nonce, gcm.Seal(nil, nonce, []byte("old photo"), nil)...)
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/storage/attachment/legacy-1" {
			w.WriteHeader(404)
			return
		}
		w.Write(sealed)
	}))
	defer cp.Close()
	h.controlplane = cp.URL
	// The web and iOS apps stored images at the control plane before the sync enclave.
	storedThread(rows, "alice", "chat-1", object{"id": "legacy-1", "type": "image", "fileName": "photo.png", "description": "a photo", "key": base64.StdEncoding.EncodeToString(legacyKey)})
	eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	if fake.putCount() != 1 {
		t.Fatalf("legacy attachment was not moved under its thread: puts=%d", fake.putCount())
	}
	if payload, _ := base64.StdEncoding.DecodeString(str(fake.puts[0]["plaintext"])); fake.puts[0]["chat_id"] != "chat-1" || string(payload) != "old photo" {
		t.Fatalf("stored %q under %v", payload, fake.puts[0]["chat_id"])
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "chat-1")
	stored := obj(arr(obj(arr(row.Data["messages"])[0])["attachments"])[0])
	if stored["key"] != nil || !enclaveAttachmentID.MatchString(str(stored["id"])) {
		t.Fatalf("legacy reference kept: %v", stored)
	}
}

func TestDocumentDownloadReturnsExtractedText(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	id, blobKey := fake.seed("chat-1", []byte(`{"pages":[{"page":1,"text":"page one","image":"","is_scanned":false}]}`))
	storedThread(rows, "alice", "chat-1", object{"id": id, "type": "document", "fileName": "scan.pdf", "encryptionKey": blobKey})
	w := postRoute(h, "/v1/attachments/get", object{"key": key.base64(), "id": id, "threadId": "chat-1"}, "alice")
	if w.Code != 200 || w.Body.String() != "page one" || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/markdown") || !strings.Contains(w.Header().Get("Content-Disposition"), "scan.md") {
		t.Fatalf("download was not the extracted text: %d %q %v", w.Code, w.Body.String(), w.Header())
	}
}

func TestBackupListsUnavailableBlobsAsOmissions(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	newFakeSync(t, h)
	storedThread(rows, "alice", "chat-1", object{"id": strings.Repeat("c", 36), "type": "image", "fileName": "lost.png", "mimeType": "image/png", "encryptionKey": "gone"})
	var archive bytes.Buffer
	if err := h.writeBackup(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, key, object{}, &archive); err != nil {
		t.Fatal(err)
	}
	reader, _ := zip.NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	var manifest object
	for _, file := range reader.File {
		if file.Name == "manifest.json" {
			f, _ := file.Open()
			json.NewDecoder(f).Decode(&manifest)
			f.Close()
		}
	}
	omissions := arr(manifest["omissions"])
	if manifest["complete"] != false || len(omissions) != 1 || obj(omissions[0])["source_id"] != strings.Repeat("c", 36) || obj(omissions[0])["parent_source_id"] != "chat-1" || len(arr(manifest["warnings"])) != 1 {
		t.Fatalf("the unavailable image was not listed: %v", manifest)
	}
	referenceCheck(t, object{"backup": base64.StdEncoding.EncodeToString(archive.Bytes())})
}

func TestShareIsKeyedByItsChatAndOwnerSession(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	newFakeSync(t, h)
	storedThread(rows, "alice", "chat-1")
	var put *http.Request
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		put = r
	}))
	defer cp.Close()
	h.controlplane, h.cpClient = cp.URL, &http.Client{Transport: &callerAuth{inner: cp.Client().Transport}}
	result, err := h.shareThread(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, key, object{"threadId": "chat-1"})
	if err != nil {
		t.Fatal(err)
	}
	if put == nil || put.Method != http.MethodPut || put.URL.Path != "/api/shares/chat-1" || put.Header.Get("Authorization") != "Bearer clerk-alice" || put.Header.Get("X-Format-Version") != "1" {
		t.Fatalf("share upload: %+v", put)
	}
	if !strings.HasSuffix(str(obj(result)["url"]), "/share/chat-1#v2:"+strings.Repeat("ab", 32)) {
		t.Fatalf("share link: %v", result)
	}
}

func TestDeleteAllIncludesRowsTheKeyCannotOpen(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	storedThread(rows, "alice", "readable")
	storedThread(rows, "alice", "sealed-to-a-retired-key")
	rows.unreadable = map[string]bool{"sealed-to-a-retired-key": true}
	result, err := h.deleteAllThreads(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, key, object{})
	if err != nil {
		t.Fatal(err)
	}
	if obj(result)["deleted"] != 2 || len(rows.rows) != 0 {
		t.Fatalf("delete-all left %d rows: %v", len(rows.rows), result)
	}
}

func TestPinsFollowTheProfileWhenMirroringFails(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	storedThread(rows, "alice", "chat-1")
	storedThread(rows, "alice", "chat-2")
	rows.rows[rowMapKey(p, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: object{"pinnedChatIds": []any{"chat-1", "chat-2"}}}
	pushes := 0
	rows.refuse = func(row *storedRow) error {
		pushes++
		return apiErr(503, "UPSTREAM_REFUSED", "sync is down")
	}
	page := []storedRow{}
	for _, id := range []string{"chat-1", "chat-2"} {
		row, _ := rows.load(context.Background(), p, key, "chat", id)
		page = append(page, *row)
	}
	if err := h.syncPins(context.Background(), p, key, page); err != nil {
		t.Fatal(err)
	}
	// One failed write stops mirroring, and the read still shows the profile's pins.
	if pushes != 1 || page[0].Data["pinned"] != true || page[1].Data["pinned"] != true {
		t.Fatalf("pushes %d, pins %v %v", pushes, page[0].Data["pinned"], page[1].Data["pinned"])
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	page[0].Data["pinned"] = false
	if err := h.syncPins(cancelled, p, key, page[:1]); err == nil {
		t.Fatal("a cancelled read reported success")
	}
}
