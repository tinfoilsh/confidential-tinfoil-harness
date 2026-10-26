package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func TestTranscriptInlinesAttachmentsAndDetachesKeys(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	imageID, imageKey := fake.seed("chat-1", []byte("png bytes"))
	docID, docKey := fake.seed("chat-1", []byte(`{"textContent":"the plan"}`))
	storedThread(rows, "alice", "chat-1",
		api.Object{"id": imageID, "type": "image", "fileName": "photo.png", "mimeType": "image/png", "encryptionKey": imageKey, "thumbnailBase64": "thumb", "storageThreadId": "chat-1"},
		api.Object{"id": docID, "type": "document", "fileName": "plan.pdf", "encryptionKey": docKey, "pageCount": 2})
	row := rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")]
	editRow(&row, func(data api.Object) {
		data["projectId"], data["harnessRequests"], data["clock"] = "project-1", api.Object{"nonce": api.Object{}}, 9
	})
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")] = row
	w := postRoute(h, "/v1/threads/transcript", api.Object{"key": keyBase64(key), "threadId": "chat-1"}, "alice")
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		Thread api.Object `json:"thread"`
	}
	json.Unmarshal(w.Body.Bytes(), &out)
	attachments := api.Arr(api.Obj(api.Arr(out.Thread["messages"])[0])["attachments"])
	image, doc := api.Obj(attachments[0]), api.Obj(attachments[1])
	if image["base64"] != base64.StdEncoding.EncodeToString([]byte("png bytes")) || image["thumbnail"] != "thumb" || doc["textContent"] != "the plan" {
		t.Fatalf("content not inlined: %v", attachments)
	}
	for _, a := range []api.Object{image, doc} {
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
	if out.Thread["title"] != "Old" || len(api.Arr(out.Thread["messages"])) != 2 {
		t.Fatalf("transcript lost the chat: %v", out.Thread)
	}
}

func TestTranscriptRefusesToLoseContent(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	newFakeSync(t, h)
	storedThread(rows, "alice", "gone", api.Object{"id": strings.Repeat("d", 36), "type": "image", "fileName": "lost.png", "encryptionKey": "gone"})
	if w := postRoute(h, "/v1/threads/transcript", api.Object{"key": keyBase64(key), "threadId": "gone"}, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "ATTACHMENTS_UNAVAILABLE") {
		t.Fatalf("a chat with an unreadable attachment moved: %d %s", w.Code, w.Body.String())
	}
}

func TestTranscriptWaitsForALiveReply(t *testing.T) {
	model, release := heldModel()
	h, _, key := testChatHarness(t, model)
	t.Cleanup(release)
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	run, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.log, "TEXT_MESSAGE_CHUNK")
	in := api.Object{"key": keyBase64(key), "threadId": run.thread.id}
	w := postRoute(h, "/v1/threads/transcript", in, "alice")
	if w.Code != 409 || !strings.Contains(w.Body.String(), "RUN_IN_FLIGHT") {
		t.Fatalf("a live reply could be left behind: %d %s", w.Code, w.Body.String())
	}
	release()
	<-run.finished
	w = postRoute(h, "/v1/threads/transcript", in, "alice")
	var out struct {
		Thread domain.Thread `json:"thread"`
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
	id := api.Str(in["threadId"])
	storedThread(rows, "alice", id)
	copy := postRoute(h, "/v1/threads/transcript", api.Object{"key": keyBase64(key), "threadId": id}, "alice")
	var copied struct {
		Revision string `json:"revision"`
	}
	if copy.Code != 200 || json.Unmarshal(copy.Body.Bytes(), &copied) != nil || copied.Revision == "" {
		t.Fatalf("copy: %s", copy.Body.String())
	}
	run, err := h.acceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), key, requestOf[turnRequest](in))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.log, "TEXT_MESSAGE_CHUNK")
	deletion := api.Object{"key": keyBase64(key), "id": id, "revision": copied.Revision}
	if w := postRoute(h, "/v1/threads/delete", deletion, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "RUN_IN_FLIGHT") {
		t.Fatalf("active transfer: %d %s", w.Code, w.Body.String())
	}
	release()
	<-run.finished
	if w := postRoute(h, "/v1/threads/delete", deletion, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "REVISION_CONFLICT") {
		t.Fatalf("stale transfer: %d %s", w.Code, w.Body.String())
	}
	row, err := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", id)
	if err != nil || len(mustRead(domain.ReadThreadRow(row)).Messages) != 4 || mustRead(domain.ReadThreadRow(row)).Messages[3].Content == "" {
		t.Fatalf("reply lost: %v %+v", err, row)
	}
	deletion["revision"] = row.ETag
	if w := postRoute(h, "/v1/threads/delete", deletion, "alice"); w.Code != 200 {
		t.Fatalf("fresh transfer: %d %s", w.Code, w.Body.String())
	}
	if _, err := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", id); api.AsError(err).HTTP != 404 {
		t.Fatal("transferred chat remains in the cloud")
	}
}

func TestFavoriteReturnsWithAChatMovedBackToTheCloud(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	const id = "0000000000005_favorite"
	storedThread(rows, "alice", id)
	profileKey := "user:alice/profile/profile"
	rows.rows[profileKey] = domain.StoredRow{ID: "profile", ETag: "1", Data: api.Raw(api.Object{"pinnedChatIds": []string{id}})}
	copy := postRoute(h, "/v1/threads/transcript", api.Object{"key": keyBase64(key), "threadId": id}, "alice")
	var copied struct {
		Thread   domain.Thread `json:"thread"`
		Revision string        `json:"revision"`
	}
	if copy.Code != 200 || json.Unmarshal(copy.Body.Bytes(), &copied) != nil || !copied.Thread.Pinned {
		t.Fatalf("favorite copy: %d %s", copy.Code, copy.Body.String())
	}
	if w := postRoute(h, "/v1/threads/delete", api.Object{"key": keyBase64(key), "id": id, "revision": copied.Revision}, "alice"); w.Code != 200 {
		t.Fatalf("transfer: %s", w.Body.String())
	}
	if len(mustRead(domain.ReadProfile(rows.rows[profileKey].Data)).PinnedChatIDs) != 0 {
		t.Fatal("device chat left in the cloud favorites index")
	}
	if w := postRoute(h, "/v1/threads/import", api.Object{"key": keyBase64(key), "threadId": id, "thread": copied.Thread}, "alice"); w.Code != 200 {
		t.Fatalf("return to cloud: %s", w.Body.String())
	}
	w := postRoute(h, "/v1/threads/list", api.Object{"key": keyBase64(key)}, "alice")
	var page struct {
		Pinned []domain.ThreadSummary `json:"pinned"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Pinned) != 1 || page.Pinned[0].ID != id || !page.Pinned[0].Pinned {
		t.Fatalf("favorite not restored: %s", w.Body.String())
	}
}

func TestImportStoresTheDeviceChatUnderItsID(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	thread := api.Object{"title": "From the device", "titleState": "manual", "createdAt": "2026-02-01T00:00:00Z", "messages": []any{
		api.Object{"id": "m1", "role": "user", "content": "see these", "attachments": []any{
			api.Object{"id": "local-image", "type": "image", "fileName": "a.png", "mimeType": "image/png", "base64": base64.StdEncoding.EncodeToString([]byte("image bytes")), "thumbnailBase64": "thumb"},
			api.Object{"id": "local-doc", "type": "document", "fileName": "b.txt", "textContent": "doc text"},
		}},
		api.Object{"id": "m2", "role": "assistant", "content": "seen"},
	}, "activeRun": api.Object{"runId": "forged"}}
	in := api.Object{"key": keyBase64(key), "threadId": "0000000000005_device", "thread": thread}
	w := postRoute(h, "/v1/threads/import", in, "alice")
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var summary api.Object
	json.Unmarshal(w.Body.Bytes(), &summary)
	if summary["id"] != "0000000000005_device" || summary["title"] != "From the device" || summary["activeRun"] != nil {
		t.Fatalf("summary: %v", summary)
	}
	row, err := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "0000000000005_device")
	if err != nil {
		t.Fatal(err)
	}
	attachments := mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments
	for _, a := range attachments {
		if !domain.EnclaveAttachmentID.MatchString(a.ID) || a.StorageThreadID != "0000000000005_device" || a.Base64 != "" || a.TextContent != "" {
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
	f.imports["job-1"] = &fakeImport{status: api.Object{"status": "failed", "imported": 0, "failed": 1, "total": 1, "job_id": "job-1", "failure_reason": "invalid_archive", "failure_detail": "Not a zip archive"}}
	w := postRoute(h, "/v1/import/status", api.Object{"jobId": "job-1"}, "alice")
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	var status api.Object
	json.Unmarshal(w.Body.Bytes(), &status)
	want := api.Object{"status": "failed", "imported": float64(0), "failed": float64(1), "total": float64(1), "jobId": "job-1", "failureReason": "invalid_archive", "failureDetail": "Not a zip archive"}
	if !reflect.DeepEqual(status, want) {
		t.Fatalf("the status reads as %v, want %v", status, want)
	}
}

func TestImportMayReuseADeletedChatID(t *testing.T) {
	var metadata api.Object
	client := &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		metadata = api.Obj(in["metadata"])
		if in["if_match"] != nil {
			t.Errorf("a restore must be a create: %v", in["if_match"])
		}
		return syncTestResponse(200, api.Object{"ok": true, "etag": "1"})
	})}}
	h, _, key := testChatHarness(t, nil)
	h.storeRows = client
	if _, err := h.importThread(context.Background(), &auth.Principal{ID: "alice", JWT: "jwt"}, key, requestOf[threadImportRequest](api.Object{"threadId": "chat-1", "thread": api.Object{"messages": []any{}}})); err != nil {
		t.Fatal(err)
	}
	if metadata["restoreDeleted"] != true {
		t.Fatalf("metadata: %v", metadata)
	}
	row := &domain.StoredRow{ID: "chat-1", Data: api.Raw(api.Object{"messages": []any{}})}
	client.push(context.Background(), &auth.Principal{ID: "alice", JWT: "jwt"}, key, "chat", row, "nonce")
	if _, ok := metadata["restoreDeleted"]; ok {
		t.Fatal("an ordinary push asked to restore a deleted chat")
	}
}
