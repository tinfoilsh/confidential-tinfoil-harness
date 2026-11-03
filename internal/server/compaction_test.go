package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
)

// compactionSummary is what the marker of compactedThread says of the
// messages before it.
const compactionSummary = "## Goals and context\nThe user plans a lighthouse tour."

// compactedThread is a chat in version 2 whose first exchange a compaction
// marker summarizes, followed by one more exchange.
func compactedThread(id string) api.Object {
	return api.Object{"id": id, "title": "Long chat", "titleState": "generated", "model": "kimi-k3", "presetId": nil, "pinned": false, "webSearchEnabled": false, "recordVersion": 2,
		"createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "messages": []any{
			api.Object{"id": "m1", "role": "user", "createdAt": "2026-01-01T00:00:00Z", "content": "old question"},
			api.Object{"id": "m2", "role": "assistant", "createdAt": "2026-01-01T00:00:01Z", "content": "old answer", "timeline": []any{api.Object{"id": "b1", "type": "content", "content": "old answer"}}},
			api.Object{"id": "c1", "role": "compaction", "createdAt": "2026-01-02T00:00:00Z", "content": compactionSummary,
				"compaction": api.Object{"messages": 2, "startedAt": 1767312000000.0, "endedAt": 1767312004000.0, "model": "glm-5-3", "version": 1}},
			api.Object{"id": "m3", "role": "user", "createdAt": "2026-01-02T00:00:01Z", "content": "recent question"},
			api.Object{"id": "m4", "role": "assistant", "createdAt": "2026-01-02T00:00:02Z", "content": "recent answer", "timeline": []any{api.Object{"id": "b4", "type": "content", "content": "recent answer"}}},
		}}
}

// storeCompacted stores compactedThread for alice under id.
func storeCompacted(rows *memoryRows, id string) {
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", id)] = domain.StoredRow{ID: id, ETag: "1", Data: api.Raw(compactedThread(id))}
}

// promptOf records the messages each model turn is sent, as role and text.
func promptOf(prompts chan<- []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var sent []string
		for _, m := range lastRequest(r) {
			sent = append(sent, api.Str(api.Obj(m)["role"])+": "+api.Str(api.Obj(m)["content"]))
		}
		prompts <- sent
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	}
}

// The model reads a compacted chat from its latest marker on: the summary
// opens the question after it, and the messages it covers are left out.
func TestTheModelReadsAChatFromItsLatestCompaction(t *testing.T) {
	prompts := make(chan []string, 1)
	h, rows, key := testChatHarness(t, promptOf(prompts))
	storeCompacted(rows, "chat-1")
	finishedRun(t, h, "/v1/threads/turn", continueInput(key, "chat-1"))
	prompt := <-prompts
	if len(prompt) != 5 || !strings.HasPrefix(prompt[1], "user: <conversation_summary>") || !strings.Contains(prompt[1], compactionSummary+"\n</conversation_summary>") ||
		!strings.HasSuffix(prompt[1], "recent question") || prompt[2] != "assistant: recent answer" || prompt[3] != "user: test message" {
		t.Fatalf("the model was sent %q", prompt)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	chat := mustRead(domain.ReadThreadRow(row))
	if ids := messageIDs(api.Parsed(api.Raw(chat.Messages))); !reflect.DeepEqual(ids[:5], []string{"m1", "m2", "c1", "m3", "m4"}) || chat.Messages[2].Compaction == nil || chat.Messages[2].Compaction.Model != "glm-5-3" {
		t.Fatalf("the turn stored %s", row.Data)
	}
}

// The safeguards classify a compacted chat whole, as they did before its
// older messages were summarized, and never take the summary for one of them.
func TestTheSafeguardsClassifyACompactedChatWhole(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	drained := screenedBy(t, h)
	storeCompacted(rows, "chat-1")
	finishedRun(t, h, "/v1/threads/turn", continueInput(key, "chat-1"))
	want := []engine.SafeguardMessage{{Role: "user", Content: "old question"}, {Role: "assistant", Content: "old answer"}, {Role: "user", Content: "recent question"},
		{Role: "assistant", Content: "recent answer"}, {Role: "user", Content: "test message"}, {Role: "assistant", Content: "hello"}}
	if got := drained(); len(got) != 1 || !reflect.DeepEqual(got[0].Messages[1:], want) {
		t.Fatalf("the sidecar received %+v", got)
	}
}

// A device chat keeps its compaction marker through a run, as its client
// sent it, and shows it without counting it as a message.
func TestADeviceChatKeepsItsCompactionMarker(t *testing.T) {
	prompts := make(chan []string, 1)
	h, _, _ := testChatHarness(t, promptOf(prompts))
	in := localInput("", nil)
	sent := api.Obj(api.Parsed(api.Raw(compactedThread(api.Str(in["threadId"])))))
	in["thread"] = sent
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if prompt := <-prompts; len(prompt) != 5 || !strings.Contains(prompt[1], compactionSummary) || strings.Contains(strings.Join(prompt, "\n"), "old question") {
		t.Fatalf("the model was sent %q", prompt)
	}
	last := events[len(events)-1]
	thread := api.Obj(api.Obj(api.Parsed(last.Metadata))["thread"])
	if last.Type != "RUN_FINISHED" || !reflect.DeepEqual(api.Arr(thread["messages"])[2], api.Arr(sent["messages"])[2]) {
		t.Fatalf("the run returned %v", thread)
	}
	summary := api.Obj(api.Obj(eventOfType(events, "STATE_SNAPSHOT").Snapshot)["thread"])
	shown := api.Obj(api.Arr(eventOfType(events, "MESSAGES_SNAPSHOT").Messages)[2])
	if api.Int(summary["messageCount"]) != 5 || shown["role"] != "compaction" || shown["content"] != compactionSummary || api.Obj(shown["compaction"])["messages"] != 2.0 {
		t.Fatalf("the chat was shown as %v with %v", summary, shown)
	}
}

// A summary must not keep what a deleted or edited message said, and a
// marker is not deleted on its own.
func TestChangingACompactedMessageDropsItsSummary(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	storeCompacted(rows, "chat-1")
	change := func(path, message string) *httptest.ResponseRecorder {
		return postRoute(h, path, api.Object{"key": keyBase64(key), "threadId": "chat-1", "messageId": message, "content": "edited"}, "alice")
	}
	if w := change("/v1/threads/messages/delete", "c1"); w.Code != 400 {
		t.Fatalf("deleting the marker: HTTP %d", w.Code)
	}
	if got := windowIDs(t, change("/v1/threads/messages/update", "m4")); !reflect.DeepEqual(got, []string{"m1", "m2", "c1", "m3", "m4"}) {
		t.Fatalf("editing a reply after the marker left %q", got)
	}
	if got := windowIDs(t, change("/v1/threads/messages/update", "m2")); !reflect.DeepEqual(got, []string{"m1", "m2", "m3", "m4"}) {
		t.Fatalf("editing a reply the marker covers left %q", got)
	}
	storeCompacted(rows, "chat-2")
	in := api.Object{"key": keyBase64(key), "threadId": "chat-2", "messageId": "m1"}
	if got := windowIDs(t, postRoute(h, "/v1/threads/messages/delete", in, "alice")); !reflect.DeepEqual(got, []string{"m2", "m3", "m4"}) {
		t.Fatalf("deleting a question the marker covers left %q", got)
	}
}

// A share shows the conversation, not the summary the model read of it.
func TestAShareLeavesOutCompactionMarkers(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	storeCompacted(rows, "chat-1")
	shares := fakeShares(t, h)
	w := postRoute(h, "/v1/threads/share", api.Object{"key": keyBase64(key), "threadId": "chat-1"}, "alice")
	var result api.Object
	json.Unmarshal(w.Body.Bytes(), &result)
	uploaded, _ := shares.uploaded("chat-1")
	shared := api.Obj(api.Parsed(sharedPayload(t, api.Str(result["url"]), uploaded)))
	roles := []string{}
	for _, m := range api.Arr(shared["messages"]) {
		roles = append(roles, api.Str(api.Obj(m)["role"]))
	}
	if !reflect.DeepEqual(roles, []string{"user", "assistant", "user", "assistant"}) {
		t.Fatalf("the share holds %v", shared)
	}
}
