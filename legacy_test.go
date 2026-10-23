package main

import (
	"reflect"
	"testing"
)

// upgradedReply is the timeline the upgrade route reads an old chat's
// second message as.
func upgradedReply(t *testing.T, reply object) any {
	t.Helper()
	chat := object{"id": "old-chat", "title": "Old", "createdAt": "2026-03-01T10:00:00.000Z", "messages": []any{
		object{"role": "user", "content": "Look it up", "timestamp": "2026-03-01T10:00:01.000Z"},
		reply,
	}}
	h, _, _ := testChatHarness(t, nil)
	return obj(arr(obj(upgraded(t, h, "alice", chat)[0])["messages"])[1])["timeline"]
}

// A search or page read the old apps recorded as failed, blocked or cut off
// reads as a failed call, as the harness records one; a completed one, or
// one the iOS app wrote without a status, reads as completed.
func TestOldFailedSearchesAndPageReadsReadAsFailed(t *testing.T) {
	failed := object{"error": "tool_error"}
	search := func(id, query, status string, result any) object {
		return object{"id": id, "toolCallId": id, "type": "tool_call", "name": "web_search", "arguments": `{"query":"` + query + `"}`, "complete": true, "result": result,
			"display": object{"query": query, "status": status, "sources": []any{}}}
	}
	read := func(id, url, status string, result any) object {
		return object{"id": id, "toolCallId": id, "type": "tool_call", "name": "web_fetch", "arguments": `{"url":"` + url + `"}`, "complete": true, "result": result,
			"display": object{"url": url, "status": status, "sources": []any{}}}
	}
	page := func(url string) object { return object{"url": url, "title": "", "text": ""} }
	for _, tc := range []struct {
		name  string
		reply object
		want  []any
	}{
		{"flat fields", object{"role": "assistant", "content": "Nothing found.", "timestamp": "2026-03-01T10:00:05.000Z",
			"webSearch": object{"query": "cat news", "status": "failed"},
			"urlFetches": []any{
				object{"id": "f1", "url": "https://a.example/", "status": "completed"},
				object{"id": "f2", "url": "https://b.example/", "status": "failed"},
			}}, []any{
			search("old-chat-message-1-search", "cat news", "failed", failed),
			read("f1", "https://a.example/", "completed", page("https://a.example/")),
			read("f2", "https://b.example/", "failed", failed),
			object{"id": "old-chat-message-1-content", "type": "content", "content": "Nothing found."},
		}},
		{"timeline", object{"role": "assistant", "content": "Blocked.", "timestamp": "2026-03-01T10:00:05.000Z", "timeline": []any{
			object{"type": "web_search", "id": "s1", "state": object{"query": "bad", "status": "blocked", "reason": "Blocked by policy"}},
			object{"type": "web_search", "id": "s2", "state": object{"query": "slow", "status": "searching"}},
			object{"type": "url_fetches", "id": "u1", "fetches": []any{
				object{"id": "f3", "url": "https://c.example/", "status": "fetching"},
				object{"id": "f4", "url": "https://d.example/"},
			}},
			object{"type": "content", "id": "c1", "content": "Blocked."},
		}}, []any{
			search("s1", "bad", "failed", failed),
			search("s2", "slow", "failed", failed),
			read("f3", "https://c.example/", "failed", failed),
			read("f4", "https://d.example/", "completed", page("https://d.example/")),
			object{"id": "c1", "type": "content", "content": "Blocked."},
		}},
	} {
		if got := upgradedReply(t, tc.reply); !reflect.DeepEqual(got, decodeValue(raw(tc.want))) {
			t.Errorf("%s: read as\n%s\nwant\n%s", tc.name, raw(got), raw(tc.want))
		}
	}
}
