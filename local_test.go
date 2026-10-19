package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	in["kind"], in["quote"] = "ask", "first answer"
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
}
