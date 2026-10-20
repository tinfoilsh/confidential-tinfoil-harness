package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

// tripThread is a chat with an image, two questions and two replies.
func tripThread() object {
	return object{"title": "Trip", "titleState": "generated", "createdAt": "2026-01-01T00:00:00Z", "model": "kimi-k3", "webSearchEnabled": true, "messages": []any{
		object{"id": "u1", "role": "user", "content": "plan a trip", "attachments": []any{object{"id": "img", "type": "image", "fileName": "map.png", "mimeType": "image/png", "base64": "aW1n", "thumbnailBase64": "dGh1bWI="}}},
		object{"id": "a1", "role": "assistant", "content": "first, pack", "timeline": []any{object{"id": "t1", "type": "thinking", "content": "think"}, object{"id": "c1", "type": "content", "content": "first, pack"}}},
		object{"id": "u2", "role": "user", "content": "go on"},
		object{"id": "a2", "role": "assistant", "content": "then go", "timeline": []any{object{"id": "c2", "type": "content", "content": "then go"}}},
	}}
}

// deviceOperation applies an operation to a device chat and returns the chat
// it answers with.
func deviceOperation(t *testing.T, h *harness, path string, thread object, in object, user string) object {
	t.Helper()
	in["local"], in["thread"] = true, thread
	w := postRoute(h, path, in, user)
	if w.Code != 200 {
		t.Fatalf("%s: HTTP %d %s", path, w.Code, w.Body.String())
	}
	var reply struct {
		Thread object `json:"thread"`
	}
	json.Unmarshal(w.Body.Bytes(), &reply)
	return reply.Thread
}

// comparable is a thread as a device stores it, without its save time.
func comparable(t Thread) any {
	device := deviceThread(&t)
	device.UpdatedAt = ""
	return decodeValue(raw(device))
}

func TestDeviceAndStoredChatsChangeTheSameWay(t *testing.T) {
	h, rows, key, _ := presetHarness(t)
	p := &principal{ID: "alice"}
	rows.rows[rowMapKey(p, "profile", "profile")] = storedRow{ID: "profile", ETag: "1", Data: raw(presetProfile())}
	rows.rows[rowMapKey(p, "chat", "chat-1")] = storedRow{ID: "chat-1", ETag: "1", Data: raw(tripThread())}
	device := tripThread()
	for _, step := range []struct {
		path string
		in   object
	}{
		{"/v1/threads/update", object{"id": "chat-1", "title": "Renamed", "presetId": "user:fast"}},
		{"/v1/threads/messages/update", object{"threadId": "chat-1", "messageId": "a1", "content": "first, pack light"}},
		{"/v1/threads/messages/delete", object{"threadId": "chat-1", "messageId": "u2"}},
	} {
		cloud := object{"key": key.base64()}
		for field, value := range step.in {
			cloud[field] = value
		}
		if w := postRoute(h, step.path, cloud, "alice"); w.Code != 200 {
			t.Fatalf("%s: HTTP %d %s", step.path, w.Code, w.Body.String())
		}
		step.in["profile"] = presetProfile()
		device = deviceOperation(t, h, step.path, device, step.in, "")
		var changed Thread
		json.Unmarshal(raw(device), &changed)
		stored := storedChat(t, rows, key, "chat-1")
		if got, want := comparable(changed), comparable(stored); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: the device chat became\n%s\nthe stored one\n%s", step.path, raw(got), raw(want))
		}
	}
	messages := arr(device["messages"])
	image := obj(arr(obj(messages[0])["attachments"])[0])
	if device["model"] != "gemma4-31b" || device["titleState"] != "manual" || len(messages) != 3 || image["base64"] != nil || image["thumbnail"] != "dGh1bWI=" {
		t.Fatalf("the device chat was answered as %v", device)
	}
	// The harness names a fork, which keeps its attachments' ids.
	source := tripThread()
	source["flagged"], source["lastRequestId"], source["lastRequestKind"] = true, "6f1c4a8e-0d55-4b8f-9a8e-2f0c7b9d1e11", "continue"
	fork := deviceOperation(t, h, "/v1/threads/fork", source, object{"threadId": "chat-1", "messageId": "a1"}, "")
	forked := arr(fork["messages"])
	if id := str(fork["id"]); !rowIDPattern.MatchString(id) || fork["title"] != "Trip (fork)" || len(forked) != 2 || fork["flagged"] != nil || fork["lastRequestKind"] != nil || obj(arr(obj(forked[0])["attachments"])[0])["id"] != "img" || obj(arr(obj(forked[0])["attachments"])[0])["base64"] != nil {
		t.Fatalf("the device fork is %v", fork)
	}
	if w := postRoute(h, "/v1/threads/update", object{"id": "chat-1", "title": "x"}, ""); w.Code != 401 {
		t.Fatalf("a signed-out caller changed a stored chat: HTTP %d", w.Code)
	}
	if w := postRoute(h, "/v1/threads/update", object{"id": "chat-1", "local": true, "thread": tripThread(), "pinned": true}, ""); w.Code != 400 {
		t.Fatalf("a device chat was pinned: HTTP %d", w.Code)
	}
}

func TestADeviceChatsFirstTurnStartsItWithoutAThread(t *testing.T) {
	h, rows, _ := testChatHarness(t, nil)
	in := localInput("", nil)
	thread := endedThread(t, h, in, "RUN_FINISHED")
	if thread["id"] != in["threadId"] || len(arr(thread["messages"])) != 2 || obj(arr(thread["messages"])[0])["id"] != in["clientRequestId"] || rows.calls != 0 {
		t.Fatalf("the first turn left %v", thread)
	}
}
