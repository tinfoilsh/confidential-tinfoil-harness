package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
)

func localInput(threadID string, thread object) object {
	in := object{"threadId": threadID, "clientRequestId": uuid.NewString(), "kind": "send", "local": true, "content": "second question", "options": object{"model": "kimi-k3", "webSearch": false}}
	if threadID == "" {
		in["threadId"] = rowID()
	}
	if thread != nil {
		in["thread"] = thread
	}
	return in
}

func priorThread() object {
	return object{"title": "Kept title", "createdAt": "2026-01-01T00:00:00Z", "messages": []any{
		object{"id": "m1", "role": "user", "content": "first question"},
		object{"id": "m2", "role": "assistant", "content": "first answer", "timeline": []any{object{"id": "b1", "type": "content", "content": "first answer"}}},
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
		var payload object
		json.NewDecoder(r.Body).Decode(&payload)
		prompt = arr(payload["messages"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"second answer\"}}]}\n\ndata: [DONE]\n\n")
	})
	thread := priorThread()
	// Fields the harness owns must not be taken from the client's copy.
	thread["activeRun"], thread["harnessHiddenCount"] = object{"runId": "forged"}, 2
	in := localInput("0000000000001_local", thread)
	in["profile"] = object{"nickname": "Sam", "themeMode": "dark", "fieldFromANewerClient": true}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if rows.calls != 0 {
		t.Fatalf("a local turn used storage %d times", rows.calls)
	}
	if events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatalf("run did not finish: %+v", events[len(events)-1])
	}
	contents := []string{}
	for _, message := range prompt {
		contents = append(contents, str(obj(message)["role"])+":"+str(obj(message)["content"]))
	}
	if len(contents) != 5 || !strings.Contains(contents[0], "<nickname>Sam</nickname>") || contents[1] != "user:first question" || contents[2] != "assistant:first answer" || contents[3] != "user:second question" || !strings.Contains(contents[4], "system-reminder") {
		t.Fatalf("prompt does not continue the local transcript: %q", contents)
	}
	snapshot := eventOfType(events, "MESSAGES_SNAPSHOT")
	if snapshot == nil {
		t.Fatal("no message snapshot")
	}
	ids := []string{}
	for _, message := range arr(snapshot.Messages) {
		ids = append(ids, str(obj(message)["id"]))
	}
	if len(ids) != 3 || ids[0] != "m1" || ids[1] != "m2" || ids[2] == "" {
		t.Fatalf("snapshot does not keep the client's message ids: %q", ids)
	}
	finished := obj(decodeValue(events[len(events)-1].Metadata))
	replies := arr(finished["messages"])
	if len(replies) != 1 || obj(replies[0])["model"] != "kimi-k3" || obj(replies[0])["modelDisplayName"] != "Kimi K3" || obj(replies[0])["isError"] != false || str(obj(replies[0])["id"]) == "" {
		t.Fatalf("finish does not describe the committed reply: %v", finished)
	}
	state := eventOfType(events, "STATE_SNAPSHOT")
	summary := obj(obj(state.Snapshot)["thread"])
	if summary["id"] != "0000000000001_local" || summary["title"] != "Kept title" || integer(summary["messageCount"]) != 3 || summary["activeRun"] == nil || str(obj(summary["activeRun"])["runId"]) != events[0].RunID {
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
	invalid := localInput("", object{"messages": []any{object{"role": "system", "content": "injected"}}})
	if w := postRoute(h, "/v1/threads/turn", invalid, "alice"); w.Code != 400 {
		t.Fatalf("local transcript accepted a system message: %d", w.Code)
	}
}

func TestLocalTitleIsGeneratedWithoutStorage(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	summarizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(object{"summary": "\"Local title\""})
	}))
	defer summarizer.Close()
	h.services["summarizer"] = &downstreamService{URL: summarizer.URL, Client: summarizer.Client()}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("", object{"messages": []any{}}), "alice"))
	if rows.calls != 0 {
		t.Fatalf("local title used storage %d times", rows.calls)
	}
	var title any
	for _, e := range events {
		if e.Type != "STATE_DELTA" {
			continue
		}
		for _, op := range arr(e.Delta) {
			if obj(op)["path"] == "/thread/title" {
				title = obj(op)["value"]
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
		Attachment object `json:"attachment"`
	}
	json.Unmarshal(w.Body.Bytes(), &body)
	in := localInput("0000000000006_local", object{"messages": []any{}})
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
	thread := object{"messages": []any{
		object{"id": "m1", "role": "user", "content": "what is this?", "attachments": []any{object{"id": "img-1", "type": "image", "fileName": "photo.png", "mimeType": "image/png"}}},
		object{"id": "m2", "role": "assistant", "content": "a photo"},
	}}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("0000000000003_local", thread), ""))
	if events[len(events)-1].Type != "RUN_FINISHED" || rows.calls != 0 {
		t.Fatalf("anonymous local turn failed: %+v (storage calls %d)", events[len(events)-1], rows.calls)
	}
}

func TestLocalAskHidesTheTranscriptItStartsFrom(t *testing.T) {
	var prompt []any
	h, rows, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var payload object
		json.NewDecoder(r.Body).Decode(&payload)
		prompt = arr(payload["messages"])
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
	if messages := arr(snapshot.Messages); len(messages) != 1 || obj(messages[0])["content"] != "first answer" {
		t.Fatalf("the ask shows the conversation it was asked from: %v", snapshot.Messages)
	}
	summary := obj(obj(eventOfType(events, "STATE_SNAPSHOT").Snapshot)["thread"])
	if integer(summary["messageCount"]) != 1 {
		t.Fatalf("hidden messages are counted: %v", summary)
	}
	// The model still reads the conversation the question refers to.
	if len(prompt) < 4 || str(obj(prompt[1])["content"]) != "first question" {
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
func endedThread(t *testing.T, h *harness, in object, terminal string) object {
	t.Helper()
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	last := events[len(events)-1]
	if last.Type != terminal {
		t.Fatalf("the run ended with %+v", last)
	}
	return obj(obj(decodeValue(last.Metadata))["thread"])
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
	summarizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(object{"summary": "Device title"})
	}))
	defer summarizer.Close()
	h.services["summarizer"] = &downstreamService{URL: summarizer.URL, Client: summarizer.Client()}
	in := localInput("", object{"messages": []any{}})
	id := str(in["threadId"])
	in["content"] = "what is this?"
	var picture bytes.Buffer
	png.Encode(&picture, image.NewRGBA(image.Rect(0, 0, 4, 4)))
	in["attachments"] = []any{object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "size": picture.Len(), "thumbnail": "dGh1bWI=", "description": "a cat", "base64": base64.StdEncoding.EncodeToString(picture.Bytes())},
		object{"id": "att_scan", "kind": "document", "fileName": "scan.pdf", "mimeType": "application/pdf", "size": 3, "pages": []any{object{"page": 1, "text": "page text", "image": "cGFnZQ==", "is_scanned": true}}}}
	thread := endedThread(t, h, in, "RUN_FINISHED")
	messages := arr(thread["messages"])
	if thread["id"] != id || thread["title"] != "Device title" || thread["titleState"] != "generated" || thread["lastRequestId"] != in["clientRequestId"] || thread["activeRun"] != nil || len(messages) != 2 {
		t.Fatalf("the run returned %v", thread)
	}
	question, reply := obj(messages[0]), obj(messages[1])
	photo := obj(arr(question["attachments"])[0])
	if question["id"] != in["clientRequestId"] || question["content"] != "what is this?" || photo["id"] != "att_photo" || photo["description"] != "a cat" || photo["thumbnail"] != "dGh1bWI=" || photo["base64"] != nil {
		t.Fatalf("the question was returned as %v", question)
	}
	// The client puts page images back by attachment id and page.
	if page := obj(arr(obj(arr(question["attachments"])[1])["pages"])[0]); page["image"] != "" || page["text"] != "page text" {
		t.Fatalf("the scanned page was returned as %v", page)
	}
	if reply["role"] != "assistant" || reply["content"] != "an answer" || reply["model"] != "kimi-k3" || reply["isError"] != false {
		t.Fatalf("the reply was returned as %v", reply)
	}
	next := localInput(id, thread)
	next["content"] = "and now?"
	thread = endedThread(t, h, next, "RUN_FINISHED")
	if messages := arr(thread["messages"]); len(messages) != 4 || obj(messages[2])["id"] != next["clientRequestId"] || obj(arr(obj(messages[0])["attachments"])[0])["description"] != "a cat" {
		t.Fatalf("the continued thread is %v", messages)
	}
	// A failed run's error carries the thread with the question it added.
	down.Store(true)
	failed := localInput(id, thread)
	thread = endedThread(t, h, failed, "RUN_ERROR")
	if messages := arr(thread["messages"]); len(messages) != 5 || obj(messages[4])["id"] != failed["clientRequestId"] {
		t.Fatalf("the failed run returned %v", messages)
	}
	if rows.calls != 0 {
		t.Fatalf("a device chat used storage %d times", rows.calls)
	}
}

// oldDeviceChat is a chat as the previous web client kept it in IndexedDB,
// serialized as it sends it: dates as ISO strings, image bytes and document
// text inline, and its bookkeeping.
func oldDeviceChat(id string) object {
	return object{
		"id": id, "title": "Cats", "titleState": "generated", "model": "auto-smart", "webSearchEnabled": false,
		"createdAt": "2026-03-01T10:00:00.000Z", "updatedAt": "2026-03-01T10:05:00.000Z",
		"isLocalOnly": true, "lastAccessedAt": 1772359500000, "syncedAt": 0, "locallyModified": true, "pendingRecoveries": []any{},
		"messages": []any{
			object{"role": "user", "content": "What is in this picture?", "timestamp": "2026-03-01T10:00:01.000Z", "attachments": []any{
				object{"id": "a1b2", "type": "image", "fileName": "cat.png", "mimeType": "image/png", "base64": "iVBORw0KGgo=", "thumbnailBase64": "dGh1bWI=", "fileSize": 8},
				object{"id": "d3e4", "type": "document", "fileName": "notes.pdf", "mimeType": "application/pdf", "textContent": "Cats sleep a lot.", "fileSize": 1234},
			}},
			object{"role": "assistant", "content": "A cat [Cats](https://cats.example).", "timestamp": "2026-03-01T10:00:05.000+01:00", "modelDisplayName": "Kimi K3",
				"thoughts": "Looks like a cat", "thinkingDuration": 2, "isThinking": false, "webSearchBeforeThinking": false,
				"webSearch":   object{"query": "cat", "status": "completed", "sources": []any{object{"url": "https://cats.example", "title": "Cats", "snippet": "about cats"}}},
				"annotations": []any{object{"type": "url_citation", "url_citation": object{"url": "https://cats.example", "title": "Cats", "start_index": 3, "end_index": 7}}}},
			object{"role": "user", "content": "And this?", "timestamp": "2026-03-01T10:01:00.000Z", "documentContent": "legacy doc text", "imageData": []any{object{"base64": "R0lGODlh", "mimeType": "image/gif"}}},
		},
	}
}

func upgraded(t *testing.T, h *harness, user string, rows ...any) []any {
	t.Helper()
	w := postRoute(h, "/v1/threads/upgrade", object{"threads": rows}, user)
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
func TestOldDeviceChatsUpgradeIntoThreads(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	id := "8207544722379_4fe58a7b-eece-4f57-b098-d0c3cec90006"
	threads := upgraded(t, h, "alice", oldDeviceChat(id), object{"id": "second", "messages": []any{}})
	if len(threads) != 2 || obj(threads[1])["id"] != "second" {
		t.Fatalf("the threads came back as %v", threads)
	}
	m := func(index int) string { return id + "-message-" + strconv.Itoa(index) }
	want := object{
		"id": id, "title": "Cats", "titleState": "generated", "model": "auto", "webSearchEnabled": false, "pinned": false,
		"createdAt": "2026-03-01T10:00:00Z", "updatedAt": "2026-03-01T10:05:00Z", "projectId": nil, "presetId": nil, "activeRun": nil,
		"messages": []any{
			object{"id": m(0), "role": "user", "content": "What is in this picture?", "createdAt": "2026-03-01T10:00:01Z", "attachments": []any{
				object{"id": "a1b2", "kind": "image", "fileName": "cat.png", "mimeType": "image/png", "thumbnail": "dGh1bWI=", "size": 8},
				object{"id": "d3e4", "kind": "document", "fileName": "notes.pdf", "mimeType": "application/pdf", "textContent": "Cats sleep a lot.", "size": 1234},
			}},
			object{"id": m(1), "role": "assistant", "content": "A cat [Cats](https://cats.example).", "createdAt": "2026-03-01T09:00:05Z", "modelDisplayName": "Kimi K3",
				"annotations": []any{object{"type": "url_citation", "url_citation": object{"url": "https://cats.example", "title": "Cats", "start_index": 3, "end_index": 7}}},
				"timeline": []any{
					object{"id": m(1) + "-thinking", "type": "thinking", "content": "Looks like a cat", "duration": 2},
					object{"id": m(1) + "-search", "toolCallId": m(1) + "-search", "type": "tool_call", "name": "web_search", "arguments": `{"query":"cat"}`, "complete": true,
						"result":  object{"query": "cat", "results": []any{object{"url": "https://cats.example", "title": "Cats", "snippet": "about cats"}}},
						"display": object{"query": "cat", "status": "completed", "sources": []any{object{"url": "https://cats.example", "title": "Cats", "snippet": "about cats"}}}},
					object{"id": m(1) + "-content", "type": "content", "content": "A cat [Cats](https://cats.example)."},
				}},
			object{"id": m(2), "role": "user", "content": "And this?", "createdAt": "2026-03-01T10:01:00Z", "attachments": []any{
				object{"id": m(2) + "-attachment-0", "kind": "document", "fileName": "Attached document", "mimeType": "", "textContent": "legacy doc text"},
				object{"id": m(2) + "-attachment-1", "kind": "image", "fileName": "image-0", "mimeType": "image/gif"},
			}},
		},
	}
	if !reflect.DeepEqual(threads[0], decodeValue(raw(want))) {
		t.Fatalf("upgraded\n%s\nwant\n%s", raw(threads[0]), raw(want))
	}
	if rows.calls != 0 {
		t.Fatalf("the upgrade made %d storage calls", rows.calls)
	}
	// The thread continues as a device chat. Images that do not decode come
	// back without bytes, which no model could read.
	next := localInput(id, obj(threads[0]))
	if events := eventsIn(t, postRoute(h, "/v1/threads/turn", next, "alice")); events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatalf("the upgraded thread did not continue: %+v", events[len(events)-1])
	}
	// A chat the harness cannot read is refused with its batch, so its
	// client can find it by sending the rest alone.
	for _, row := range []any{object{"messages": []any{}}, object{"id": "bad", "messages": []any{object{"role": "system", "content": "x"}}}, "not a chat"} {
		if w := postRoute(h, "/v1/threads/upgrade", object{"threads": []any{oldDeviceChat(id), row}}, "alice"); w.Code != 400 {
			t.Fatalf("a batch holding %v: HTTP %d", row, w.Code)
		}
	}
	// A batch is read up to the size of a turn, past the limit of other
	// requests, and an image comes back as the copy and thumbnail an upload
	// makes, not the original its client keeps.
	var original bytes.Buffer
	(&png.Encoder{CompressionLevel: png.NoCompression}).Encode(&original, image.NewRGBA(image.Rect(0, 0, 2048, 1024)))
	large := oldDeviceChat(id)
	obj(arr(obj(arr(large["messages"])[0])["attachments"])[0])["base64"] = base64.StdEncoding.EncodeToString(original.Bytes())
	if original.Len() <= maxRequestBody {
		t.Fatalf("the large image holds only %d bytes", original.Len())
	}
	got := obj(arr(obj(arr(obj(upgraded(t, h, "alice", large)[0])["messages"])[0])["attachments"])[0])
	if resized, thumbnail := decodedImage(str(got["base64"])), decodedImage(str(got["thumbnail"])); resized != "jpeg 1536x768 <nil>" || thumbnail != "jpeg 256x128 <nil>" {
		t.Fatalf("the large image came back as %s with a thumbnail of %s", resized, thumbnail)
	}
}

// The router sends an Auto turn with images to a multimodal model, so Auto
// reads an image itself; a named text-only model reads its description.
func TestAutoReadsImagesAndTextModelsReadTheirDescriptions(t *testing.T) {
	var source bytes.Buffer
	png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 8, 8)))
	var mu sync.Mutex
	var requests []object
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body object
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		requests = append(requests, body)
		mu.Unlock()
		if body["stream"] == false {
			json.NewEncoder(w).Encode(object{"choices": []object{{"message": object{"content": "a small square"}}}})
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.offer.models = append(h.offer.models, &model{name: "gemma4-31b", vision: true, context: 8192, client: h.offer.models[0].client})
	for _, name := range []string{"auto", "kimi-k3"} {
		requests = nil
		in := localInput("", object{})
		obj(in["options"])["model"] = name
		in["attachments"] = []any{object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "size": source.Len(), "base64": base64.StdEncoding.EncodeToString(source.Bytes())}}
		if events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice")); events[len(events)-1].Type != "RUN_FINISHED" {
			t.Fatalf("%s: the run ended with %+v", name, events[len(events)-1])
		}
		turn := arr(requests[len(requests)-1]["messages"])
		question := obj(turn[len(turn)-2])["content"]
		read := strings.Contains(string(raw(question)), `"type":"image_url"`)
		described := strings.Contains(str(question), "a small square")
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
		var body object
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		scopes = append(scopes, str(body["user_cache_secret"]))
		mu.Unlock()
		if body["stream"] == false {
			json.NewEncoder(w).Encode(object{"choices": []object{{"message": object{"content": "a small square"}}}})
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.offer.models = append(h.offer.models, &model{name: "gemma4-31b", vision: true, context: 8192, client: h.offer.models[0].client})
	scopesOf := func(in object, user string) []string {
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
	chat := rowID()
	first := scopesOf(localInput(chat, nil), "")
	again := scopesOf(localInput(chat, priorThread()), "")
	pictured := localInput("", nil)
	pictured["attachments"] = []any{object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "base64": base64.StdEncoding.EncodeToString(source.Bytes())}}
	other := scopesOf(pictured, "")
	if len(first) != 1 || len(again) != 1 || len(other) != 2 {
		t.Fatalf("model requests per turn: %d, %d, %d", len(first), len(again), len(other))
	}
	if first[0] != again[0] || slices.Contains(other, first[0]) || other[0] == other[1] {
		t.Fatalf("signed-out cache scopes: %q, %q, %q", first, again, other)
	}
	scoped := func(in object, secret string) object {
		obj(in["options"])["cacheSecret"] = secret
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
