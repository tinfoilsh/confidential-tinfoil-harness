package main

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// upgradedMessage is what the upgrade route reads an old chat's message as.
func upgradedMessage(t *testing.T, messages []any, index int) api.Object {
	t.Helper()
	chat := api.Object{"id": "old-chat", "title": "Old", "createdAt": "2026-03-01T10:00:00.000Z", "messages": messages}
	h, _, _ := testChatHarness(t, nil)
	return api.Obj(api.Arr(api.Obj(upgraded(t, h, "alice", chat)[0])["messages"])[index])
}

// upgradedReply is the timeline the upgrade route reads an old reply as.
func upgradedReply(t *testing.T, reply api.Object) any {
	t.Helper()
	return upgradedMessage(t, []any{api.Object{"role": "user", "content": "Look it up", "timestamp": "2026-03-01T10:00:01.000Z"}, reply}, 1)["timeline"]
}

// A search or page read the old apps recorded as failed, blocked or cut off
// reads as a failed call, as the harness records one; a completed one, or
// one the iOS app wrote without a status, reads as completed.
func TestOldFailedSearchesAndPageReadsReadAsFailed(t *testing.T) {
	failed := api.Object{"error": "tool_error"}
	search := func(id, query, status string, result any) api.Object {
		return api.Object{"id": id, "toolCallId": id, "type": "tool_call", "name": "web_search", "arguments": `{"query":"` + query + `"}`, "complete": true, "result": result,
			"display": api.Object{"query": query, "status": status, "sources": []any{}}}
	}
	read := func(id, url, status string, result any) api.Object {
		return api.Object{"id": id, "toolCallId": id, "type": "tool_call", "name": "web_fetch", "arguments": `{"url":"` + url + `"}`, "complete": true, "result": result,
			"display": api.Object{"url": url, "status": status, "sources": []any{}}}
	}
	page := func(url string) api.Object { return api.Object{"url": url, "title": "", "text": ""} }
	for _, tc := range []struct {
		name  string
		reply api.Object
		want  []any
	}{
		{"flat fields", api.Object{"role": "assistant", "content": "Nothing found.", "timestamp": "2026-03-01T10:00:05.000Z",
			"webSearch": api.Object{"query": "cat news", "status": "failed"},
			"urlFetches": []any{
				api.Object{"id": "f1", "url": "https://a.example/", "status": "completed"},
				api.Object{"id": "f2", "url": "https://b.example/", "status": "failed"},
			}}, []any{
			search("old-chat-message-1-search", "cat news", "failed", failed),
			read("f1", "https://a.example/", "completed", page("https://a.example/")),
			read("f2", "https://b.example/", "failed", failed),
			api.Object{"id": "old-chat-message-1-content", "type": "content", "content": "Nothing found."},
		}},
		{"timeline", api.Object{"role": "assistant", "content": "Blocked.", "timestamp": "2026-03-01T10:00:05.000Z", "timeline": []any{
			api.Object{"type": "web_search", "id": "s1", "state": api.Object{"query": "bad", "status": "blocked", "reason": "Blocked by policy"}},
			api.Object{"type": "web_search", "id": "s2", "state": api.Object{"query": "slow", "status": "searching"}},
			api.Object{"type": "url_fetches", "id": "u1", "fetches": []any{
				api.Object{"id": "f3", "url": "https://c.example/", "status": "fetching"},
				api.Object{"id": "f4", "url": "https://d.example/"},
			}},
			api.Object{"type": "content", "id": "c1", "content": "Blocked."},
		}}, []any{
			search("s1", "bad", "failed", failed),
			search("s2", "slow", "failed", failed),
			read("f3", "https://c.example/", "failed", failed),
			read("f4", "https://d.example/", "completed", page("https://d.example/")),
			api.Object{"id": "c1", "type": "content", "content": "Blocked."},
		}},
	} {
		if got := upgradedReply(t, tc.reply); !reflect.DeepEqual(got, decodeValue(api.Raw(tc.want))) {
			t.Errorf("%s: read as\n%s\nwant\n%s", tc.name, api.Raw(got), api.Raw(tc.want))
		}
	}
}

// A question the web app stored with its files in flat fields shows each
// file under its name with its own text, in each format the app wrote:
// descriptions apart from the documents' contents, descriptions among them,
// and contents without titles.
func TestOldFlatFilesReadAsOneAttachmentEach(t *testing.T) {
	image := api.Object{"base64": "R0lGODlh", "mimeType": "image/gif"}
	files := func(names ...string) []any {
		named := []any{}
		for _, name := range names {
			named = append(named, api.Object{"name": name})
		}
		return named
	}
	document := func(index int, name, text string) api.Object {
		a := api.Object{"id": "old-chat-message-0-attachment-" + strconv.Itoa(index), "kind": "document", "fileName": name, "mimeType": ""}
		if text != "" {
			a["textContent"] = text
		}
		return a
	}
	for _, tc := range []struct {
		name     string
		question api.Object
		want     []any
	}{
		{"descriptions apart", api.Object{"documents": files("notes.txt", "cat.png", "report.pdf"), "imageData": []any{image},
			"documentContent": "Document title: notes.txt\nDocument contents:\nBuy milk.\n\nDocument title: report.pdf\nDocument contents:\n# Q3\nRevenue rose.\n",
			"multimodalText":  "Image: cat.png\nDescription:\nA cat on a mat."}, []any{
			document(0, "notes.txt", "Buy milk."),
			document(1, "report.pdf", "# Q3\nRevenue rose."),
			api.Object{"id": "old-chat-message-0-attachment-2", "kind": "image", "fileName": "cat.png", "mimeType": "image/gif", "description": "A cat on a mat."},
		}},
		{"descriptions among the contents", api.Object{"documents": files("cat.png", "notes.txt"), "imageData": []any{image},
			"documentContent": "Document title: cat.png\nDocument contents:\nA cat on a mat.\n\nDocument title: notes.txt\nDocument contents:\nBuy milk."}, []any{
			document(0, "notes.txt", "Buy milk."),
			api.Object{"id": "old-chat-message-0-attachment-1", "kind": "image", "fileName": "cat.png", "mimeType": "image/gif", "description": "A cat on a mat."},
		}},
		{"one document without a title", api.Object{"documents": files("notes.txt"), "documentContent": "Buy milk."}, []any{
			document(0, "notes.txt", "Buy milk."),
		}},
		{"documents without titles", api.Object{"documents": files("a.txt", "b.txt"), "documentContent": "Buy milk.\nSell eggs."}, []any{
			document(0, "a.txt", ""), document(1, "b.txt", ""), document(2, "Attached document", "Buy milk.\nSell eggs."),
		}},
	} {
		tc.question["role"], tc.question["content"], tc.question["timestamp"] = "user", "Read these", "2026-03-01T10:00:01.000Z"
		if got := upgradedMessage(t, []any{tc.question}, 0)["attachments"]; !reflect.DeepEqual(got, decodeValue(api.Raw(tc.want))) {
			t.Errorf("%s: read as\n%s\nwant\n%s", tc.name, api.Raw(got), api.Raw(tc.want))
		}
	}
}
