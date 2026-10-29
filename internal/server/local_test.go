package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
)

func localInput(threadID string, thread api.Object) api.Object {
	in := api.Object{"threadId": threadID, "clientRequestId": uuid.NewString(), "kind": "send", "local": true, "content": "second question", "options": api.Object{"model": "kimi-k3", "webSearch": false}}
	if threadID == "" {
		in["threadId"] = domain.RowID()
	}
	if thread != nil {
		in["thread"] = thread
	}
	return in
}

func priorThread() api.Object {
	return api.Object{"title": "Kept title", "createdAt": "2026-01-01T00:00:00Z", "messages": []any{
		api.Object{"id": "m1", "role": "user", "content": "first question"},
		api.Object{"id": "m2", "role": "assistant", "content": "first answer", "timeline": []any{api.Object{"id": "b1", "type": "content", "content": "first answer"}}},
	}}
}

func eventOfType(events []event, kind string) *event {
	for i := range events {
		if events[i].Type == kind {
			return &events[i]
		}
	}
	return nil
}

func TestLocalTurnUsesClientTranscriptWithoutStorage(t *testing.T) {
	var prompt []any
	h, rows, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var payload api.Object
		json.NewDecoder(r.Body).Decode(&payload)
		prompt = api.Arr(payload["messages"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"second answer\"}}]}\n\ndata: [DONE]\n\n")
	})
	thread := priorThread()
	// Fields the harness owns must not be taken from the client's copy.
	thread["activeRun"], thread["harnessHiddenCount"] = api.Object{"runId": "forged"}, 2
	in := localInput("0000000000001_local", thread)
	in["profile"] = api.Object{"nickname": "Sam", "themeMode": "dark", "fieldFromANewerClient": true}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if rows.calls != 0 {
		t.Fatalf("a local turn used storage %d times", rows.calls)
	}
	if events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatalf("run did not finish: %+v", events[len(events)-1])
	}
	contents := []string{}
	for _, message := range prompt {
		contents = append(contents, api.Str(api.Obj(message)["role"])+":"+api.Str(api.Obj(message)["content"]))
	}
	if len(contents) != 5 || !strings.Contains(contents[0], "<nickname>Sam</nickname>") || contents[1] != "user:first question" || contents[2] != "assistant:first answer" || contents[3] != "user:second question" || !strings.Contains(contents[4], "system-reminder") {
		t.Fatalf("prompt does not continue the local transcript: %q", contents)
	}
	snapshot := eventOfType(events, "MESSAGES_SNAPSHOT")
	if snapshot == nil {
		t.Fatal("no message snapshot")
	}
	ids := []string{}
	for _, message := range api.Arr(snapshot.Messages) {
		ids = append(ids, api.Str(api.Obj(message)["id"]))
	}
	if len(ids) != 3 || ids[0] != "m1" || ids[1] != "m2" || ids[2] == "" {
		t.Fatalf("snapshot does not keep the client's message ids: %q", ids)
	}
	finished := api.Obj(api.Parsed(events[len(events)-1].Metadata))
	replies := api.Arr(finished["messages"])
	if len(replies) != 1 || api.Obj(replies[0])["model"] != "kimi-k3" || api.Obj(replies[0])["modelDisplayName"] != "Kimi K3" || api.Obj(replies[0])["isError"] != false || api.Str(api.Obj(replies[0])["id"]) == "" {
		t.Fatalf("finish does not describe the committed reply: %v", finished)
	}
	state := eventOfType(events, "STATE_SNAPSHOT")
	summary := api.Obj(api.Obj(state.Snapshot)["thread"])
	if summary["id"] != "0000000000001_local" || summary["title"] != "Kept title" || api.Int(summary["messageCount"]) != 3 || summary["activeRun"] == nil || api.Str(api.Obj(summary["activeRun"])["runId"]) != events[0].RunID {
		t.Fatalf("summary does not describe the local thread: %v", summary)
	}
}

func TestIdleLocalThreadNeedsItsTranscript(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	first := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("0000000000002_local", priorThread()), "alice"))
	if first[len(first)-1].Type != "RUN_FINISHED" {
		t.Fatal("first local turn did not finish")
	}
	// The row is released once the run finishes, so the client's copy is the
	// only transcript. Continuing without it would silently drop the history.
	w := postRoute(h, "/v1/threads/turn", localInput("0000000000002_local", nil), "alice")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "requires its transcript") {
		t.Fatalf("idle local thread accepted a turn without its transcript: %d %s", w.Code, w.Body.String())
	}
	second := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("0000000000002_local", priorThread()), "alice"))
	if second[0].ThreadID != "0000000000002_local" || second[len(second)-1].Type != "RUN_FINISHED" {
		t.Fatal("local thread did not continue with its transcript")
	}
}

func TestLocalTurnFieldsAreRefusedOutsideLocalThreads(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	stored := sendInput(key)
	stored["thread"] = priorThread()
	if w := postRoute(h, "/v1/threads/turn", stored, "alice"); w.Code != 400 {
		t.Fatalf("stored turn accepted a client transcript: %d", w.Code)
	}
	project := localInput("", nil)
	project["projectId"] = "project-1"
	if w := postRoute(h, "/v1/threads/turn", project, "alice"); w.Code != 400 {
		t.Fatalf("local turn accepted a project: %d", w.Code)
	}
	invalid := localInput("", api.Object{"messages": []any{api.Object{"role": "system", "content": "injected"}}})
	if w := postRoute(h, "/v1/threads/turn", invalid, "alice"); w.Code != 400 {
		t.Fatalf("local transcript accepted a system message: %d", w.Code)
	}
}

func TestLocalTitleIsGeneratedWithoutStorage(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	h.models.Summarizer = summarizerFunc(func(inference.SummaryStyle, string) (string, error) { return "\"Local title\"", nil })
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("", api.Object{"messages": []any{}}), "alice"))
	if rows.calls != 0 {
		t.Fatalf("local title used storage %d times", rows.calls)
	}
	var title any
	for _, e := range events {
		if e.Type != "STATE_DELTA" {
			continue
		}
		for _, op := range api.Arr(e.Delta) {
			if api.Obj(op)["path"] == "/thread/title" {
				title = api.Obj(op)["value"]
			}
		}
	}
	if title != "Local title" {
		t.Fatalf("no generated title event: %v", title)
	}
}

func TestALocalTurnReadsTheUploadItSends(t *testing.T) {
	var prompt string
	h, rows, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		prompt = string(b)
		io.WriteString(w, "data: [DONE]\n\n")
	})
	uploader, _, _ := testChatHarness(t, nil)
	w := uploadRoute(uploader, "/v1/attachments/upload", "notes.txt", []byte("remember the blue door"), nil, "alice")
	if w.Code != 200 {
		t.Fatalf("upload failed: %d %s", w.Code, w.Body.String())
	}
	var body struct {
		Attachment api.Object `json:"attachment"`
	}
	json.Unmarshal(w.Body.Bytes(), &body)
	in := localInput("0000000000006_local", api.Object{"messages": []any{}})
	in["attachments"] = []any{body.Attachment}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if events[len(events)-1].Type != "RUN_FINISHED" || !strings.Contains(prompt, "remember the blue door") {
		t.Fatalf("local turn did not read its upload: %s", prompt)
	}
	if rows.calls != 0 {
		t.Fatalf("local upload used storage %d times", rows.calls)
	}
}

func TestAnonymousLocalTranscriptMayOmitImageBytes(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	thread := api.Object{"messages": []any{
		api.Object{"id": "m1", "role": "user", "content": "what is this?", "attachments": []any{api.Object{"id": "img-1", "type": "image", "fileName": "photo.png", "mimeType": "image/png"}}},
		api.Object{"id": "m2", "role": "assistant", "content": "a photo"},
	}}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("0000000000003_local", thread), ""))
	if events[len(events)-1].Type != "RUN_FINISHED" || rows.calls != 0 {
		t.Fatalf("anonymous local turn failed: %+v (storage calls %d)", events[len(events)-1], rows.calls)
	}
}

func TestLocalAskHidesTheTranscriptItStartsFrom(t *testing.T) {
	var prompt []any
	h, rows, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var payload api.Object
		json.NewDecoder(r.Body).Decode(&payload)
		prompt = api.Arr(payload["messages"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"side answer\"}}]}\n\ndata: [DONE]\n\n")
	})
	in := localInput("0000000000004_local", priorThread())
	in["kind"], in["quote"], in["ephemeral"] = "ask", "first answer", true
	delete(in, "content")
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if rows.calls != 0 || events[0].ThreadID == "0000000000004_local" {
		t.Fatalf("a local ask must start a new temporary thread without storage: %v (storage calls %d)", events[0].ThreadID, rows.calls)
	}
	snapshot := eventOfType(events, "MESSAGES_SNAPSHOT")
	if messages := api.Arr(snapshot.Messages); len(messages) != 1 || api.Obj(messages[0])["content"] != "first answer" {
		t.Fatalf("the ask shows the conversation it was asked from: %v", snapshot.Messages)
	}
	summary := api.Obj(api.Obj(eventOfType(events, "STATE_SNAPSHOT").Snapshot)["thread"])
	if api.Int(summary["messageCount"]) != 1 {
		t.Fatalf("hidden messages are counted: %v", summary)
	}
	// The model still reads the conversation the question refers to.
	if len(prompt) < 4 || api.Str(api.Obj(prompt[1])["content"]) != "first question" {
		t.Fatalf("the model lost the asked-about conversation: %v", prompt)
	}
	// Without the conversation there is nothing to ask about.
	delete(in, "thread")
	in["clientRequestId"] = uuid.NewString()
	if w := postRoute(h, "/v1/threads/turn", in, "alice"); w.Code != 400 || !strings.Contains(w.Body.String(), "requires its transcript") {
		t.Fatalf("an ask without its conversation: HTTP %d %s", w.Code, w.Body.String())
	}
}

// endedThread posts a device chat's turn and returns the thread its run's
// terminal event carries.
func endedThread(t *testing.T, h *harness, in api.Object, terminal string) api.Object {
	t.Helper()
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	last := events[len(events)-1]
	if last.Type != terminal {
		t.Fatalf("the run ended with %+v", last)
	}
	return api.Obj(api.Obj(api.Parsed(last.Metadata))["thread"])
}

// A device chat's run ends with the whole thread its client stores, which
// the client sends back to continue the chat.
func TestADeviceChatRunReturnsItsUpdatedThread(t *testing.T) {
	var down atomic.Bool
	h, rows, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"an answer\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.models.Summarizer = summarizerFunc(func(inference.SummaryStyle, string) (string, error) { return "Device title", nil })
	in := localInput("", api.Object{"messages": []any{}})
	id := api.Str(in["threadId"])
	in["content"] = "what is this?"
	var picture bytes.Buffer
	png.Encode(&picture, image.NewRGBA(image.Rect(0, 0, 4, 4)))
	in["attachments"] = []any{api.Object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "size": picture.Len(), "thumbnail": "dGh1bWI=", "description": "a cat", "base64": base64.StdEncoding.EncodeToString(picture.Bytes())},
		api.Object{"id": "att_scan", "kind": "document", "fileName": "scan.pdf", "mimeType": "application/pdf", "size": 3, "pages": []any{api.Object{"page": 1, "text": "page text", "image": "cGFnZQ==", "is_scanned": true}}}}
	thread := endedThread(t, h, in, "RUN_FINISHED")
	messages := api.Arr(thread["messages"])
	if thread["id"] != id || thread["title"] != "Device title" || thread["titleState"] != "generated" || thread["lastRequestId"] != in["clientRequestId"] || thread["activeRun"] != nil || len(messages) != 2 {
		t.Fatalf("the run returned %v", thread)
	}
	question, reply := api.Obj(messages[0]), api.Obj(messages[1])
	photo := api.Obj(api.Arr(question["attachments"])[0])
	if question["id"] != in["clientRequestId"] || question["content"] != "what is this?" || photo["id"] != "att_photo" || photo["description"] != "a cat" || photo["thumbnail"] != "dGh1bWI=" || photo["base64"] != nil {
		t.Fatalf("the question was returned as %v", question)
	}
	// The client puts page images back by attachment id and page.
	if page := api.Obj(api.Arr(api.Obj(api.Arr(question["attachments"])[1])["pages"])[0]); page["image"] != "" || page["text"] != "page text" {
		t.Fatalf("the scanned page was returned as %v", page)
	}
	if reply["role"] != "assistant" || reply["content"] != "an answer" || reply["model"] != "kimi-k3" || reply["isError"] != false {
		t.Fatalf("the reply was returned as %v", reply)
	}
	next := localInput(id, thread)
	next["content"] = "and now?"
	thread = endedThread(t, h, next, "RUN_FINISHED")
	if messages := api.Arr(thread["messages"]); len(messages) != 4 || api.Obj(messages[2])["id"] != next["clientRequestId"] || api.Obj(api.Arr(api.Obj(messages[0])["attachments"])[0])["description"] != "a cat" {
		t.Fatalf("the continued thread is %v", messages)
	}
	// A failed run's error carries the thread with the question it added.
	down.Store(true)
	failed := localInput(id, thread)
	thread = endedThread(t, h, failed, "RUN_ERROR")
	if messages := api.Arr(thread["messages"]); len(messages) != 5 || api.Obj(messages[4])["id"] != failed["clientRequestId"] {
		t.Fatalf("the failed run returned %v", messages)
	}
	if rows.calls != 0 {
		t.Fatalf("a device chat used storage %d times", rows.calls)
	}
}

// oldDeviceChat is a chat as the previous web client kept it in IndexedDB,
// serialized as it sends it: dates as ISO strings, image bytes and document
// text inline, and its bookkeeping.
func oldDeviceChat(id string) api.Object {
	return api.Object{
		"id": id, "title": "Cats", "titleState": "generated", "model": "auto-smart", "webSearchEnabled": false,
		"createdAt": "2026-03-01T10:00:00.000Z", "updatedAt": "2026-03-01T10:05:00.000Z",
		"isLocalOnly": true, "lastAccessedAt": 1772359500000, "syncedAt": 0, "locallyModified": true, "pendingRecoveries": []any{},
		"messages": []any{
			api.Object{"role": "user", "content": "What is in this picture?", "timestamp": "2026-03-01T10:00:01.000Z", "attachments": []any{
				api.Object{"id": "a1b2", "type": "image", "fileName": "cat.png", "mimeType": "image/png", "base64": "iVBORw0KGgo=", "thumbnailBase64": "dGh1bWI=", "fileSize": 8},
				api.Object{"id": "d3e4", "type": "document", "fileName": "notes.pdf", "mimeType": "application/pdf", "textContent": "Cats sleep a lot.", "fileSize": 1234},
			}},
			api.Object{"role": "assistant", "content": "A cat [Cats](https://cats.example).", "timestamp": "2026-03-01T10:00:05.000+01:00", "modelDisplayName": "Kimi K3",
				"thoughts": "Looks like a cat", "thinkingDuration": 2, "isThinking": false, "webSearchBeforeThinking": false,
				"webSearch":   api.Object{"query": "cat", "status": "completed", "sources": []any{api.Object{"url": "https://cats.example", "title": "Cats", "snippet": "about cats"}}},
				"annotations": []any{api.Object{"type": "url_citation", "url_citation": api.Object{"url": "https://cats.example", "title": "Cats", "start_index": 3, "end_index": 7}}}},
			api.Object{"role": "user", "content": "And this?", "timestamp": "2026-03-01T10:01:00.000Z", "documentContent": "legacy doc text", "imageData": []any{api.Object{"base64": "R0lGODlh", "mimeType": "image/gif"}}},
		},
	}
}

func upgraded(t *testing.T, h *harness, user string, rows ...any) []any {
	t.Helper()
	w := postRoute(h, "/v1/threads/upgrade", api.Object{"threads": rows}, user)
	if w.Code != 200 {
		t.Fatalf("upgrade: HTTP %d %s", w.Code, w.Body.String())
	}
	var body struct {
		Threads []any `json:"threads"`
	}
	json.Unmarshal(w.Body.Bytes(), &body)
	return body.Threads
}

// The web client upgrades the chats its previous version kept on the device
// through the harness's one reader of old chats, and stores what it returns.
// A signed-out tab upgrades its own with the free key it holds.
func TestOldDeviceChatsUpgradeIntoThreads(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	id := "8207544722379_4fe58a7b-eece-4f57-b098-d0c3cec90006"
	threads := upgraded(t, h, "", oldDeviceChat(id), api.Object{"id": "second", "messages": []any{}})
	if len(threads) != 2 || api.Obj(threads[1])["id"] != "second" {
		t.Fatalf("the threads came back as %v", threads)
	}
	m := func(index int) string { return id + "-message-" + strconv.Itoa(index) }
	want := api.Object{
		"id": id, "title": "Cats", "titleState": "generated", "model": "auto", "webSearchEnabled": false, "pinned": false,
		"createdAt": "2026-03-01T10:00:00Z", "updatedAt": "2026-03-01T10:05:00Z", "projectId": nil, "presetId": nil, "activeRun": nil, "recordVersion": 1,
		"messages": []any{
			api.Object{"id": m(0), "role": "user", "content": "What is in this picture?", "createdAt": "2026-03-01T10:00:01Z", "attachments": []any{
				api.Object{"id": "a1b2", "kind": "image", "fileName": "cat.png", "mimeType": "image/png", "thumbnail": "dGh1bWI=", "size": 8},
				api.Object{"id": "d3e4", "kind": "document", "fileName": "notes.pdf", "mimeType": "application/pdf", "textContent": "Cats sleep a lot.", "size": 1234},
			}},
			api.Object{"id": m(1), "role": "assistant", "content": "A cat [Cats](https://cats.example).", "createdAt": "2026-03-01T09:00:05Z", "modelDisplayName": "Kimi K3",
				"annotations": []any{api.Object{"type": "url_citation", "url_citation": api.Object{"url": "https://cats.example", "title": "Cats", "start_index": 3, "end_index": 7}}},
				"timeline": []any{
					api.Object{"id": m(1) + "-thinking", "type": "thinking", "content": "Looks like a cat", "duration": 2},
					api.Object{"id": m(1) + "-search", "toolCallId": m(1) + "-search", "type": "tool_call", "name": "web_search", "arguments": `{"query":"cat"}`, "complete": true,
						"result":  api.Object{"query": "cat", "results": []any{api.Object{"url": "https://cats.example", "title": "Cats", "snippet": "about cats"}}},
						"display": api.Object{"query": "cat", "status": "completed", "sources": []any{api.Object{"url": "https://cats.example", "title": "Cats", "snippet": "about cats"}}}},
					api.Object{"id": m(1) + "-content", "type": "content", "content": "A cat [Cats](https://cats.example)."},
				}},
			api.Object{"id": m(2), "role": "user", "content": "And this?", "createdAt": "2026-03-01T10:01:00Z", "attachments": []any{
				api.Object{"id": m(2) + "-attachment-0", "kind": "document", "fileName": "Attached document", "mimeType": "", "textContent": "legacy doc text"},
				api.Object{"id": m(2) + "-attachment-1", "kind": "image", "fileName": "image-0", "mimeType": "image/gif"},
			}},
		},
	}
	if !reflect.DeepEqual(threads[0], api.Parsed(api.Raw(want))) {
		t.Fatalf("upgraded\n%s\nwant\n%s", api.Raw(threads[0]), api.Raw(want))
	}
	if rows.calls != 0 {
		t.Fatalf("the upgrade made %d storage calls", rows.calls)
	}
	// The thread continues as a device chat. Images that do not decode come
	// back without bytes, which no model could read.
	next := localInput(id, api.Obj(threads[0]))
	if events := eventsIn(t, postRoute(h, "/v1/threads/turn", next, "alice")); events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatalf("the upgraded thread did not continue: %+v", events[len(events)-1])
	}
	// A chat the harness cannot read is refused with its batch, so its
	// client can find it by sending the rest alone.
	for _, row := range []any{api.Object{"messages": []any{}}, api.Object{"id": "bad", "messages": []any{api.Object{"role": "system", "content": "x"}}}, "not a chat"} {
		if w := postRoute(h, "/v1/threads/upgrade", api.Object{"threads": []any{oldDeviceChat(id), row}}, "alice"); w.Code != 400 {
			t.Fatalf("a batch holding %v: HTTP %d", row, w.Code)
		}
	}
	// A batch is read up to the size of a turn, past the limit of other
	// requests, and an image comes back as the copy and thumbnail an upload
	// makes, not the original its client keeps.
	var original bytes.Buffer
	(&png.Encoder{CompressionLevel: png.NoCompression}).Encode(&original, image.NewRGBA(image.Rect(0, 0, 2048, 1024)))
	large := oldDeviceChat(id)
	api.Obj(api.Arr(api.Obj(api.Arr(large["messages"])[0])["attachments"])[0])["base64"] = base64.StdEncoding.EncodeToString(original.Bytes())
	if original.Len() <= api.MaxRequestBody {
		t.Fatalf("the large image holds only %d bytes", original.Len())
	}
	got := api.Obj(api.Arr(api.Obj(api.Arr(api.Obj(upgraded(t, h, "alice", large)[0])["messages"])[0])["attachments"])[0])
	if resized, thumbnail := decodedImage(api.Str(got["base64"])), decodedImage(api.Str(got["thumbnail"])); resized != "jpeg 1536x768 <nil>" || thumbnail != "jpeg 256x128 <nil>" {
		t.Fatalf("the large image came back as %s with a thumbnail of %s", resized, thumbnail)
	}
}

// The router sends an Auto turn with images to a multimodal model, so Auto
// reads an image itself; a named text-only model reads its description.
func TestAutoReadsImagesAndTextModelsReadTheirDescriptions(t *testing.T) {
	var source bytes.Buffer
	png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 8, 8)))
	var mu sync.Mutex
	var requests []api.Object
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body api.Object
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		requests = append(requests, body)
		mu.Unlock()
		if body["stream"] == false {
			json.NewEncoder(w).Encode(api.Object{"choices": []api.Object{{"message": api.Object{"content": "a small square"}}}})
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.models.Offer.Set(append(h.models.Offer.Get(), &inference.Model{Name: "gemma4-31b", Vision: true, Context: 8192, Client: h.models.Offer.Get()[0].Client}), h.models.Offer.SDK())
	for _, name := range []string{"auto", "kimi-k3"} {
		requests = nil
		in := localInput("", api.Object{})
		api.Obj(in["options"])["model"] = name
		in["attachments"] = []any{api.Object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "size": source.Len(), "base64": base64.StdEncoding.EncodeToString(source.Bytes())}}
		if events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice")); events[len(events)-1].Type != "RUN_FINISHED" {
			t.Fatalf("%s: the run ended with %+v", name, events[len(events)-1])
		}
		turn := api.Arr(requests[len(requests)-1]["messages"])
		question := api.Obj(turn[len(turn)-2])["content"]
		read := strings.Contains(string(api.Raw(question)), `"type":"image_url"`)
		described := strings.Contains(api.Str(question), "a small square")
		if name == "auto" && (!read || len(requests) != 1) || name != "auto" && (read || !described || len(requests) != 2) {
			t.Fatalf("%s: the model was asked %v after %d requests", name, question, len(requests))
		}
	}
}

// A client's chats share a prompt cache through the secret it sends, which
// nothing outside the enclaves can tell from its credential. Sent without
// one, a chat has a cache of its own, whether its caller is signed in or one
// of the visitors behind one address who hold the same free key. A one-off
// request such as an image description gets one of its own.
func TestChatsShareAPromptCacheOnlyThroughTheClientsSecret(t *testing.T) {
	var source bytes.Buffer
	png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 8, 8)))
	var mu sync.Mutex
	var scopes []string
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body api.Object
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		scopes = append(scopes, api.Str(body["user_cache_secret"]))
		mu.Unlock()
		if body["stream"] == false {
			json.NewEncoder(w).Encode(api.Object{"choices": []api.Object{{"message": api.Object{"content": "a small square"}}}})
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.models.Offer.Set(append(h.models.Offer.Get(), &inference.Model{Name: "gemma4-31b", Vision: true, Context: 8192, Client: h.models.Offer.Get()[0].Client}), h.models.Offer.SDK())
	scopesOf := func(in api.Object, user string) []string {
		t.Helper()
		mu.Lock()
		scopes = nil
		mu.Unlock()
		if events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, user)); events[len(events)-1].Type != "RUN_FINISHED" {
			t.Fatalf("the run ended with %+v", events[len(events)-1])
		}
		mu.Lock()
		defer mu.Unlock()
		for _, scope := range scopes {
			if scope == "" {
				t.Fatalf("a model request named no cache scope: %q", scopes)
			}
		}
		return slices.Clone(scopes)
	}
	chat := domain.RowID()
	first := scopesOf(localInput(chat, nil), "")
	again := scopesOf(localInput(chat, priorThread()), "")
	pictured := localInput("", nil)
	pictured["attachments"] = []any{api.Object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "base64": base64.StdEncoding.EncodeToString(source.Bytes())}}
	other := scopesOf(pictured, "")
	if len(first) != 1 || len(again) != 1 || len(other) != 2 {
		t.Fatalf("model requests per turn: %d, %d, %d", len(first), len(again), len(other))
	}
	if first[0] != again[0] || slices.Contains(other, first[0]) || other[0] == other[1] {
		t.Fatalf("signed-out cache scopes: %q, %q, %q", first, again, other)
	}
	scoped := func(in api.Object, secret string) api.Object {
		api.Obj(in["options"])["cacheSecret"] = secret
		return in
	}
	secret, another := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	visitor := append(scopesOf(scoped(localInput("", nil), secret), ""), scopesOf(scoped(localInput("", nil), secret), "")...)
	alice := append(scopesOf(scoped(sendInput(key), secret), "alice"), scopesOf(scoped(sendInput(key), secret), "alice")...)
	if visitor[0] != visitor[1] || alice[0] != alice[1] {
		t.Fatalf("chats sent with one client secret do not share a prompt cache: %q, %q", visitor, alice)
	}
	unscoped := append(scopesOf(sendInput(key), "alice"), scopesOf(sendInput(key), "alice")...)
	if unscoped[0] == unscoped[1] || slices.Contains(unscoped, alice[0]) || visitor[0] == alice[0] || scopesOf(scoped(sendInput(key), another), "alice")[0] == alice[0] {
		t.Fatalf("prompt caches shared beyond the client secret: %q, %q, %q", unscoped, alice, visitor)
	}
	if w := postRoute(h, "/v1/threads/turn", scoped(sendInput(key), "not hex"), "alice"); w.Code != 400 {
		t.Fatalf("a malformed cache secret was accepted: %d", w.Code)
	}
}
