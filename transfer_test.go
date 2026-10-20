package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestTranscriptInlinesAttachmentsAndDetachesKeys(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	imageID, imageKey := fake.seed("chat-1", []byte("png bytes"))
	docID, docKey := fake.seed("chat-1", []byte(`{"textContent":"the plan"}`))
	storedThread(rows, "alice", "chat-1",
		object{"id": imageID, "type": "image", "fileName": "photo.png", "mimeType": "image/png", "encryptionKey": imageKey, "thumbnailBase64": "thumb", "storageThreadId": "chat-1"},
		object{"id": docID, "type": "document", "fileName": "plan.pdf", "encryptionKey": docKey, "pageCount": 2})
	row := rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", "chat-1")]
	editRow(&row, func(data object) {
		data["projectId"], data["harnessRequests"], data["clock"] = "project-1", object{"nonce": object{}}, 9
	})
	rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", "chat-1")] = row
	w := postRoute(h, "/v1/threads/transcript", object{"key": key.base64(), "threadId": "chat-1"}, "alice")
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		Thread object `json:"thread"`
	}
	json.Unmarshal(w.Body.Bytes(), &out)
	attachments := arr(obj(arr(out.Thread["messages"])[0])["attachments"])
	image, doc := obj(attachments[0]), obj(attachments[1])
	if image["base64"] != base64.StdEncoding.EncodeToString([]byte("png bytes")) || image["thumbnailBase64"] != "thumb" || doc["textContent"] != "the plan" {
		t.Fatalf("content not inlined: %v", attachments)
	}
	for _, a := range []object{image, doc} {
		for _, field := range []string{"encryptionKey", "key", "storageThreadId", "pageCount"} {
			if _, ok := a[field]; ok {
				t.Fatalf("a device copy kept %s: %v", field, a)
			}
		}
	}
	for _, field := range []string{"projectId", "harnessRequests", "activeRun", "clock"} {
		if out.Thread[field] != nil {
			t.Fatalf("a device copy kept %s: %v", field, out.Thread)
		}
	}
	if out.Thread["title"] != "Old" || len(arr(out.Thread["messages"])) != 2 {
		t.Fatalf("transcript lost the chat: %v", out.Thread)
	}
}

func TestTranscriptRefusesToLoseContent(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	newFakeSync(t, h)
	storedThread(rows, "alice", "gone", object{"id": strings.Repeat("d", 36), "type": "image", "fileName": "lost.png", "encryptionKey": "gone"})
	if w := postRoute(h, "/v1/threads/transcript", object{"key": key.base64(), "threadId": "gone"}, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "ATTACHMENTS_UNAVAILABLE") {
		t.Fatalf("a chat with an unreadable attachment moved: %d %s", w.Code, w.Body.String())
	}
}

func TestImportStoresTheDeviceChatUnderItsID(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	thread := object{"title": "From the device", "titleState": "manual", "createdAt": "2026-02-01T00:00:00Z", "messages": []any{
		object{"id": "m1", "role": "user", "content": "see these", "attachments": []any{
			object{"id": "local-image", "type": "image", "fileName": "a.png", "mimeType": "image/png", "base64": base64.StdEncoding.EncodeToString([]byte("image bytes")), "thumbnailBase64": "thumb"},
			object{"id": "local-doc", "type": "document", "fileName": "b.txt", "textContent": "doc text"},
		}},
		object{"id": "m2", "role": "assistant", "content": "seen"},
	}, "activeRun": object{"runId": "forged"}}
	in := object{"key": key.base64(), "threadId": "0000000000005_device", "thread": thread}
	w := postRoute(h, "/v1/threads/import", in, "alice")
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var summary object
	json.Unmarshal(w.Body.Bytes(), &summary)
	if summary["id"] != "0000000000005_device" || summary["title"] != "From the device" || summary["activeRun"] != nil {
		t.Fatalf("summary: %v", summary)
	}
	row, err := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "0000000000005_device")
	if err != nil {
		t.Fatal(err)
	}
	attachments := arr(obj(arr(rowJSON(row)["messages"])[0])["attachments"])
	for _, value := range attachments {
		a := obj(value)
		if !enclaveAttachmentID.MatchString(str(a["id"])) || a["storageThreadId"] != "0000000000005_device" || a["base64"] != nil || a["textContent"] != nil {
			t.Fatalf("attachment not stored under the chat: %v", a)
		}
	}
	if fake.putCount() != 2 || string(fake.blobs[str(obj(attachments[0])["id"])].data) != "image bytes" || string(fake.blobs[str(obj(attachments[1])["id"])].data) != `{"textContent":"doc text"}` {
		t.Fatalf("blobs: %d", fake.putCount())
	}
	if w := postRoute(h, "/v1/threads/import", in, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "THREAD_EXISTS") {
		t.Fatalf("an import overwrote a cloud chat: %d %s", w.Code, w.Body.String())
	}
}

func TestImportMayReuseADeletedChatID(t *testing.T) {
	var metadata object
	client := &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		metadata = obj(in["metadata"])
		if in["if_match"] != nil {
			t.Errorf("a restore must be a create: %v", in["if_match"])
		}
		return syncTestResponse(200, object{"ok": true, "etag": "1"})
	})}}
	h, _, key := testChatHarness(t, nil)
	h.storeRows = client
	if _, err := h.importThread(context.Background(), &principal{ID: "alice", JWT: "jwt"}, key, object{"threadId": "chat-1", "thread": object{"messages": []any{}}}); err != nil {
		t.Fatal(err)
	}
	if metadata["restoreDeleted"] != true {
		t.Fatalf("metadata: %v", metadata)
	}
	row := &storedRow{ID: "chat-1", Data: raw(object{"messages": []any{}})}
	client.push(context.Background(), &principal{ID: "alice", JWT: "jwt"}, key, "chat", row, "nonce")
	if _, ok := metadata["restoreDeleted"]; ok {
		t.Fatal("an ordinary push asked to restore a deleted chat")
	}
}
