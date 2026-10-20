package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
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
	if image["base64"] != base64.StdEncoding.EncodeToString([]byte("png bytes")) || image["thumbnail"] != "thumb" || doc["textContent"] != "the plan" {
		t.Fatalf("content not inlined: %v", attachments)
	}
	for _, a := range []object{image, doc} {
		for _, field := range []string{"encryptionKey", "key", "storageThreadId"} {
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

func TestTranscriptWaitsForALiveReply(t *testing.T) {
	model, release := heldModel()
	h, _, key := testChatHarness(t, model)
	t.Cleanup(release)
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	run, err := h.acceptTurn(context.Background(), p, key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.log, "TEXT_MESSAGE_CHUNK")
	in := object{"key": key.base64(), "threadId": run.thread.id}
	w := postRoute(h, "/v1/threads/transcript", in, "alice")
	if w.Code != 409 || !strings.Contains(w.Body.String(), "RUN_IN_FLIGHT") {
		t.Fatalf("a live reply could be left behind: %d %s", w.Code, w.Body.String())
	}
	release()
	<-run.finished
	w = postRoute(h, "/v1/threads/transcript", in, "alice")
	var out struct {
		Thread Thread `json:"thread"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out.Thread.Messages) != 2 || out.Thread.Messages[1].Content == "" {
		t.Fatalf("finished reply not transferred: %d %s", w.Code, w.Body.String())
	}
}

func TestTransferDeletionPreservesRepliesStartedAfterTheCopy(t *testing.T) {
	model, release := heldModel()
	h, rows, key := testChatHarness(t, model)
	t.Cleanup(release)
	in := sendInput(key)
	id := str(in["threadId"])
	storedThread(rows, "alice", id)
	copy := postRoute(h, "/v1/threads/transcript", object{"key": key.base64(), "threadId": id}, "alice")
	var copied struct {
		Revision string `json:"revision"`
	}
	if copy.Code != 200 || json.Unmarshal(copy.Body.Bytes(), &copied) != nil || copied.Revision == "" {
		t.Fatalf("copy: %s", copy.Body.String())
	}
	run, err := h.acceptTurn(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, key, requestOf[turnRequest](in))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.log, "TEXT_MESSAGE_CHUNK")
	deletion := object{"key": key.base64(), "id": id, "revision": copied.Revision}
	if w := postRoute(h, "/v1/threads/delete", deletion, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "RUN_IN_FLIGHT") {
		t.Fatalf("active transfer: %d %s", w.Code, w.Body.String())
	}
	release()
	<-run.finished
	if w := postRoute(h, "/v1/threads/delete", deletion, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "REVISION_CONFLICT") {
		t.Fatalf("stale transfer: %d %s", w.Code, w.Body.String())
	}
	row, err := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", id)
	if err != nil || len(readThreadRow(row).Messages) != 4 || readThreadRow(row).Messages[3].Content == "" {
		t.Fatalf("reply lost: %v %+v", err, row)
	}
	deletion["revision"] = row.ETag
	if w := postRoute(h, "/v1/threads/delete", deletion, "alice"); w.Code != 200 {
		t.Fatalf("fresh transfer: %d %s", w.Code, w.Body.String())
	}
	if _, err := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", id); asAPIError(err).HTTP != 404 {
		t.Fatal("transferred chat remains in the cloud")
	}
}

func TestFavoriteReturnsWithAChatMovedBackToTheCloud(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	const id = "0000000000005_favorite"
	storedThread(rows, "alice", id)
	profileKey := "user:alice/profile/profile"
	rows.rows[profileKey] = storedRow{ID: "profile", ETag: "1", Data: raw(object{"pinnedChatIds": []string{id}})}
	copy := postRoute(h, "/v1/threads/transcript", object{"key": key.base64(), "threadId": id}, "alice")
	var copied struct {
		Thread   Thread `json:"thread"`
		Revision string `json:"revision"`
	}
	if copy.Code != 200 || json.Unmarshal(copy.Body.Bytes(), &copied) != nil || !copied.Thread.Pinned {
		t.Fatalf("favorite copy: %d %s", copy.Code, copy.Body.String())
	}
	if w := postRoute(h, "/v1/threads/delete", object{"key": key.base64(), "id": id, "revision": copied.Revision}, "alice"); w.Code != 200 {
		t.Fatalf("transfer: %s", w.Body.String())
	}
	if len(readProfile(rows.rows[profileKey].Data).PinnedChatIDs) != 0 {
		t.Fatal("device chat left in the cloud favorites index")
	}
	if w := postRoute(h, "/v1/threads/import", object{"key": key.base64(), "threadId": id, "thread": copied.Thread}, "alice"); w.Code != 200 {
		t.Fatalf("return to cloud: %s", w.Body.String())
	}
	w := postRoute(h, "/v1/threads/list", object{"key": key.base64()}, "alice")
	var page struct {
		Pinned []threadSummary `json:"pinned"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Pinned) != 1 || page.Pinned[0].ID != id || !page.Pinned[0].Pinned {
		t.Fatalf("favorite not restored: %s", w.Body.String())
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
	attachments := readThreadRow(row).Messages[0].Attachments
	for _, a := range attachments {
		if !enclaveAttachmentID.MatchString(a.ID) || a.StorageThreadID != "0000000000005_device" || a.Base64 != "" || a.TextContent != "" {
			t.Fatalf("attachment not stored under the chat: %+v", a)
		}
	}
	if fake.putCount() != 2 || string(fake.blobs[attachments[0].ID].data) != "image bytes" || string(fake.blobs[attachments[1].ID].data) != `{"textContent":"doc text"}` {
		t.Fatalf("blobs: %d", fake.putCount())
	}
	if w := postRoute(h, "/v1/threads/import", in, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "THREAD_EXISTS") {
		t.Fatalf("an import overwrote a cloud chat: %d %s", w.Code, w.Body.String())
	}
}

// An archive import's status names its job and its failure as the client
// reads them.
func TestImportStatusReadsAsTheClientNamesIt(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	f := newFakeSync(t, h)
	f.imports["job-1"] = &fakeImport{status: object{"status": "failed", "imported": 0, "failed": 1, "total": 1, "job_id": "job-1", "failure_reason": "invalid_archive", "failure_detail": "Not a zip archive"}}
	w := postRoute(h, "/v1/import/status", object{"jobId": "job-1"}, "alice")
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var status object
	json.Unmarshal(w.Body.Bytes(), &status)
	want := object{"status": "failed", "imported": float64(0), "failed": float64(1), "total": float64(1), "jobId": "job-1", "failureReason": "invalid_archive", "failureDetail": "Not a zip archive"}
	if !reflect.DeepEqual(status, want) {
		t.Fatalf("the status reads as %v, want %v", status, want)
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
	if _, err := h.importThread(context.Background(), &principal{ID: "alice", JWT: "jwt"}, key, requestOf[threadImportRequest](object{"threadId": "chat-1", "thread": object{"messages": []any{}}})); err != nil {
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
