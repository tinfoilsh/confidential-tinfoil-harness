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
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeBlob struct {
	chat, key string
	data      []byte
}

// fakeSync serves the attachment routes the way the sync enclave does: ids and
// keys derive from the idempotency key and chat, and protocol 3 is required.
// A fork copies a chat the test harness stores.
type fakeSync struct {
	mu       sync.Mutex
	blobs    map[string]fakeBlob
	puts     []object
	received map[string][]object
	server   *httptest.Server
}

func newFakeSync(t *testing.T, h *harness) *fakeSync {
	t.Helper()
	f := &fakeSync{blobs: map[string]fakeBlob{}, received: map[string][]object{}}
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
		f.received[r.URL.Path] = append(f.received[r.URL.Path], in)
		switch r.URL.Path {
		case "/v1/sync/fork":
			rows := h.storeRows.(*memoryRows)
			rows.mu.Lock()
			defer rows.mu.Unlock()
			p := &principal{ID: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer clerk-")}
			source, ok := rows.rows[rowMapKey(p, "chat", str(in["source_id"]))]
			if !ok {
				w.WriteHeader(404)
				io.WriteString(w, `{"ok":false,"code":"NOT_FOUND"}`)
				return
			}
			data := clone(source.Data)
			for _, field := range []string{"clock", "clockVersion", "codeExecutionAccessToken", "messageCount", "syncVersion", "version", "writer"} {
				delete(data, field)
			}
			data["messages"] = arr(data["messages"])[:integer(in["message_count"])]
			data["id"], data["title"], data["titleState"], data["createdAt"], data["updatedAt"], data["isLocalOnly"] = in["target_id"], in["title"], "manual", in["created_at"], in["created_at"], false
			rows.rows[rowMapKey(p, "chat", str(in["target_id"]))] = storedRow{ID: str(in["target_id"]), ETag: "1", Data: data}
			json.NewEncoder(w).Encode(object{"ok": true, "id": in["target_id"], "etag": "1", "key_id": "key-id"})
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

func (f *fakeSync) requests(path string) []object {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]object(nil), f.received[path]...)
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

func TestForkCopiesAChatThroughItsMessage(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	p := &principal{ID: "alice"}
	messages := []any{object{"id": "m1", "role": "user", "content": "plan a trip"}, object{"id": "m2", "role": "assistant", "content": "first, pack"}, object{"id": "m3", "role": "user", "content": "go on"}}
	rows.rows[rowMapKey(p, "chat", "chat-1")] = storedRow{ID: "chat-1", ETag: "4", Data: object{"title": "Trip", "titleState": "generated", "createdAt": "2026-01-01T00:00:00Z", "presetId": "builtin:tutor", "model": "kimi-k3", "webSearchEnabled": false,
		"pinned": true, "contextUsage": object{"usedTokens": 900}, "harnessRequests": object{"nonce": object{"runId": "run", "digest": "digest"}}, "messages": messages}}
	rows.rows[rowMapKey(p, "chat", "busy")] = storedRow{ID: "busy", ETag: "1", Data: object{"activeRun": object{"runId": "run"}, "messages": messages}}
	for _, tc := range []struct {
		in   object
		code int
	}{{object{"threadId": "busy", "messageId": "m1"}, 409}, {object{"threadId": "chat-1", "messageId": "m4"}, 404}, {object{"threadId": "chat-1"}, 400}} {
		tc.in["key"] = key.base64()
		if w := postRoute(h, "/v1/threads/fork", tc.in, "alice"); w.Code != tc.code || len(fake.requests("/v1/sync/fork")) != 0 {
			t.Fatalf("%v: HTTP %d, want %d without a fork", tc.in, w.Code, tc.code)
		}
	}
	w := postRoute(h, "/v1/threads/fork", object{"key": key.base64(), "threadId": "chat-1", "messageId": "m2"}, "alice")
	forks := fake.requests("/v1/sync/fork")
	if w.Code != 200 || len(forks) != 1 {
		t.Fatalf("fork: HTTP %d %s after %d sync forks", w.Code, w.Body.String(), len(forks))
	}
	request, target := forks[0], str(forks[0]["target_id"])
	created, err := time.Parse(time.RFC3339Nano, str(request["created_at"]))
	if !regexp.MustCompile(`^\d{13}_[0-9a-f-]{36}$`).MatchString(target) || err != nil || time.Since(created) > time.Minute || str(request["idempotency_key"]) == "" {
		t.Fatalf("fork request: %v", request)
	}
	want := object{"source_id": "chat-1", "target_id": target, "key": key.base64(), "message_count": float64(2), "title": "Trip (fork)", "created_at": request["created_at"], "idempotency_key": request["idempotency_key"]}
	if !reflect.DeepEqual(request, want) {
		t.Fatalf("fork request %v, want %v", request, want)
	}
	var summary object
	json.Unmarshal(w.Body.Bytes(), &summary)
	if summary["id"] != target || summary["title"] != "Trip (fork)" || summary["titleState"] != "manual" || integer(summary["messageCount"]) != 2 || summary["createdAt"] != isoTime(request["created_at"]) ||
		summary["model"] != "kimi-k3" || summary["webSearchEnabled"] != false || summary["pinned"] != false || summary["activeRun"] != nil || summary["contextUsage"] != nil {
		t.Fatalf("fork summary: %v", summary)
	}
	row, _ := rows.load(context.Background(), p, key, "chat", target)
	for _, field := range []string{"pinned", "contextUsage", "harnessRequests", "activeRun"} {
		if _, ok := row.Data[field]; ok {
			t.Fatalf("the fork kept the source's %s: %v", field, row.Data)
		}
	}
	if row.Data["presetId"] != "builtin:tutor" || !reflect.DeepEqual(row.Data["messages"], decodeValue(raw(messages[:2]))) {
		t.Fatalf("the fork lost the conversation: %v", row.Data)
	}
}

func TestForkTitlesNameTheirSource(t *testing.T) {
	for title, want := range map[string]string{"Trip": "Trip (fork)", "Trip (fork)": "Trip (fork)", "": "Untitled (fork)"} {
		if got := forkTitle(title); got != want {
			t.Errorf("forkTitle(%q) = %q, want %q", title, got, want)
		}
	}
}

func TestALocalChatIsSharedFromItsTranscript(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	type upload struct{ method, path, auth, format, body string }
	uploads := make(chan upload, 4)
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		uploads <- upload{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Format-Version"), string(body)}
	}))
	defer cp.Close()
	h.controlplane, h.cpClient = cp.URL, &http.Client{Transport: &callerAuth{inner: cp.Client().Transport}}
	thread := object{"title": "Local chat", "createdAt": "2026-01-01T00:00:00Z", "messages": []any{
		object{"id": "m1", "role": "user", "content": "what is this?", "createdAt": "2026-01-01T00:00:01Z", "quote": "earlier", "attachments": []any{
			object{"id": "img", "type": "image", "fileName": "photo.png", "mimeType": "image/png", "base64": "ZnVsbA==", "thumbnailBase64": "dGh1bWI="},
			object{"id": "doc", "type": "document", "fileName": "notes.pdf", "mimeType": "application/pdf", "textContent": "the notes", "pages": []any{object{"page": 1, "text": "the notes", "image": "cGFnZQ=="}}},
		}},
		object{"id": "m2", "role": "assistant", "content": "a photo", "createdAt": "2026-01-01T00:00:02Z", "modelDisplayName": "Kimi K3", "thoughts": "looks like one", "timeline": []any{object{"id": "b1", "type": "content", "content": "a photo"}}},
	}}
	share := func(in object, user string) *httptest.ResponseRecorder {
		return postRoute(h, "/v1/threads/share", in, user)
	}
	for _, tc := range []struct {
		in   object
		user string
		code int
	}{
		{object{"local": true, "threadId": "0000000000005_local", "thread": thread}, "", 401},
		{object{"local": true, "threadId": "0000000000005_local"}, "alice", 400},
		{object{"local": true, "threadId": "0000000000005_local", "thread": object{"messages": []any{object{"role": "system", "content": "injected"}}}}, "alice", 400},
		// Without local, the chat is a stored one and its key is required.
		{object{"threadId": "0000000000005_local", "thread": thread}, "alice", 401},
	} {
		if w := share(tc.in, tc.user); w.Code != tc.code {
			t.Fatalf("%v: HTTP %d, want %d", tc.in, w.Code, tc.code)
		}
	}
	w := share(object{"local": true, "threadId": "0000000000005_local", "thread": thread}, "alice")
	var result object
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code != 200 || !strings.HasSuffix(str(result["url"]), "/share/0000000000005_local#v2:"+strings.Repeat("ab", 32)) {
		t.Fatalf("local share: HTTP %d %s", w.Code, w.Body.String())
	}
	seals := fake.requests("/v1/share/seal")
	plaintext, _ := base64.StdEncoding.DecodeString(str(seals[0]["plaintext"]))
	want := object{"v": 1, "title": "Local chat", "createdAt": 1767225600000, "messages": []any{
		object{"role": "user", "content": "what is this?", "timestamp": 1767225601000, "timeline": nil, "quote": "earlier", "attachments": []any{
			object{"id": "img", "type": "image", "fileName": "photo.png", "mimeType": "image/png", "thumbnailBase64": "dGh1bWI="},
			object{"id": "doc", "type": "document", "fileName": "notes.pdf", "mimeType": "application/pdf", "textContent": "the notes"},
		}},
		object{"role": "assistant", "content": "a photo", "timestamp": 1767225602000, "timeline": []any{object{"id": "b1", "type": "content", "content": "a photo"}}, "modelDisplayName": "Kimi K3", "thoughts": "looks like one"},
	}}
	if len(seals) != 1 || !reflect.DeepEqual(decodeValue(plaintext), decodeValue(raw(want))) {
		t.Fatalf("sealed %d payloads, the last %s", len(seals), plaintext)
	}
	if len(uploads) != 1 {
		t.Fatalf("%d share uploads", len(uploads))
	}
	if got := <-uploads; got != (upload{http.MethodPut, "/api/shares/0000000000005_local", "Bearer clerk-alice", "1", "sealed"}) {
		t.Fatalf("share upload: %+v", got)
	}
	if fake.putCount() != 0 || rows.calls != 0 {
		t.Fatalf("a local share stored %d attachments and made %d storage calls", fake.putCount(), rows.calls)
	}
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
	result, err := h.shareThread(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, contentKey{}, object{"key": key.base64(), "threadId": "chat-1"})
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
