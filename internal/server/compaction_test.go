package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
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

// longSummaryModel writes the tests' compaction summaries, with a window
// that holds their long chats.
const longSummaryModel = "long-summary"

// compactWith has f write the harness's summaries, a compaction's on
// longSummaryModel.
func (h *harness) compactWith(f summarizerFunc) {
	c := h.models.Catalog.Get()
	c.Models = append(c.Models, api.Object{"modelName": longSummaryModel, "name": "Long summary", "budgetWindow": 1 << 20, "chatConfig": api.Object{}})
	h.engine.Summaries = summarize.New(f, &h.models.Catalog, "kimi-k3", longSummaryModel)
}

// compactionOf is a summary in the form a compaction asks for, whose first
// section says goal.
func compactionOf(goal string) string {
	var b strings.Builder
	for i, section := range summarize.Compaction.Sections {
		body := "None"
		if i == 0 {
			body = goal
		}
		fmt.Fprintf(&b, "## %s\n%s\n\n", section.Heading, body)
	}
	return b.String()
}

// filler is text of twenty tokens a repeat, which fills a test model's
// window quickly.
func filler(repeats int) string { return strings.Repeat("0 1 2 3 4 5 6 7 8 9 ", repeats) }

// longThread is a device chat whose first answer alone fills the test
// model's share of its window, followed by a short exchange.
func longThread() api.Object {
	answer := filler(350)
	return api.Object{"title": "Kept title", "createdAt": "2026-01-01T00:00:00Z", "messages": []any{
		api.Object{"id": "m1", "role": "user", "content": "first question"},
		api.Object{"id": "m2", "role": "assistant", "content": answer, "timeline": []any{api.Object{"id": "b1", "type": "content", "content": answer}}},
		api.Object{"id": "m3", "role": "user", "content": "recent question"},
		api.Object{"id": "m4", "role": "assistant", "content": "recent answer", "timeline": []any{api.Object{"id": "b4", "type": "content", "content": "recent answer"}}},
	}}
}

// storeExchanges stores for alice a chat whose messages say texts, a question
// and its reply by turns, with ids m1, m2 and on.
func storeExchanges(rows *memoryRows, id string, texts ...string) {
	messages := []any{}
	for i, text := range texts {
		message := api.Object{"id": fmt.Sprintf("m%d", i+1), "role": "user", "createdAt": "2026-01-01T00:00:00Z", "content": text}
		if i%2 == 1 {
			message["role"], message["timeline"] = "assistant", []any{api.Object{"id": fmt.Sprintf("b%d", i+1), "type": "content", "content": text}}
		}
		messages = append(messages, message)
	}
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", id)] = domain.StoredRow{ID: id, ETag: "1", Data: api.Raw(api.Object{"id": id, "title": "Long chat", "titleState": "generated",
		"model": "kimi-k3", "presetId": nil, "pinned": false, "webSearchEnabled": false, "recordVersion": 2, "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "messages": messages})}
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

// longCompacted stores for alice a compacted chat whose exchange after its
// marker fills the test model's share of its window, and whose reply tried
// to read a page and failed.
func longCompacted(rows *memoryRows, id string) {
	thread := compactedThread(id)
	answer := filler(350)
	recent := api.Obj(api.Arr(thread["messages"])[4])
	recent["content"], recent["timeline"] = answer, []any{
		api.Object{"id": "f4", "type": "tool_call", "toolCallId": "f4", "name": "web_fetch", "arguments": `{"url":"https://tides.test"}`, "complete": true, "result": api.Object{"error": "tool_error"}},
		api.Object{"id": "b4", "type": "content", "content": answer}}
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", id)] = domain.StoredRow{ID: id, ETag: "1", Data: api.Raw(thread)}
}

// A turn whose prompt would outgrow the window first summarizes the messages
// after the chat's latest marker, merged into that marker's summary, into a
// new marker before its question; the model then reads that summary and the
// question alone, and later turns read the chat from the new marker.
func TestALongChatIsCompactedBeforeItsQuestion(t *testing.T) {
	prompts := make(chan []string, 2)
	h, rows, key := testChatHarness(t, promptOf(prompts))
	var asked []string
	h.compactWith(func(task, text string) (string, error) {
		if task == summarize.Compaction.Name {
			asked = append(asked, text)
		}
		return compactionOf("The user plans a lighthouse tour of Maine."), nil
	})
	longCompacted(rows, "chat-1")
	events := finishedRun(t, h, "/v1/threads/turn", continueInput(key, "chat-1"))
	if len(asked) != 1 || !strings.Contains(asked[0], "<previous_summary>\n"+compactionSummary+"\n</previous_summary>") ||
		!strings.Contains(asked[0], `<message from="user" sent=`) || !strings.Contains(asked[0], "recent question") || strings.Contains(asked[0], "old question") || strings.Contains(asked[0], "test message") ||
		!strings.Contains(asked[0], "[Tried to read https://tides.test, but it failed]") {
		t.Fatalf("the summarizer was asked %q", asked)
	}
	prompt := <-prompts
	if len(prompt) != 3 || !strings.Contains(prompt[1], "lighthouse tour of Maine") || !strings.HasSuffix(prompt[1], "test message") || strings.Contains(prompt[1], "recent answer") {
		t.Fatalf("the model was sent %q", prompt)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	chat := mustRead(domain.ReadThreadRow(row))
	marker := chat.Messages[5]
	if len(chat.Messages) != 8 || marker.Role != domain.CompactionRole || chat.Messages[6].Content != "test message" || marker.Compaction == nil ||
		marker.Compaction.Messages != 4 || marker.Compaction.Model != longSummaryModel || chat.ContextUsage == nil || chat.ContextUsage.Percentage >= 100 || rowJSON(row)["recordVersion"] != 2.0 {
		t.Fatalf("the turn stored %s", row.Data)
	}
	types := []string{}
	for _, e := range events {
		types = append(types, e.Type)
	}
	start, end := slices.Index(types, "COMPACTION_START"), slices.Index(types, "COMPACTION_END")
	if start < 0 || end != start+1 || types[end+1] != "STATE_SNAPSHOT" || events[start].MessageID != marker.ID || slices.Index(types, "TEXT_MESSAGE_CHUNK") < end ||
		events[start].Before != chat.Messages[6].ID || events[end].Before != chat.Messages[6].ID {
		t.Fatalf("the run sent %v", types)
	}
	finishedRun(t, h, "/v1/threads/turn", continueInput(key, "chat-1"))
	if prompt := <-prompts; len(asked) != 1 || len(prompt) != 5 || !strings.Contains(prompt[1], "lighthouse tour of Maine") {
		t.Fatalf("the next turn compacted again (%d) or was sent %q", len(asked), prompt)
	}
}

// A compaction keeps the latest exchanges before the question as they were:
// it summarizes only the messages before them, and its marker sits before
// the first question it keeps.
func TestACompactionKeepsTheLatestExchanges(t *testing.T) {
	prompts := make(chan []string, 1)
	h, rows, key := testChatHarness(t, promptOf(prompts))
	var asked []string
	h.compactWith(func(task, text string) (string, error) {
		if task == summarize.Compaction.Name {
			asked = append(asked, text)
		}
		return compactionOf("The user plans a lighthouse tour of Maine."), nil
	})
	storeExchanges(rows, "chat-1", "old question", filler(350), "recent question", "recent answer")
	events := finishedRun(t, h, "/v1/threads/turn", continueInput(key, "chat-1"))
	if len(asked) != 1 || !strings.Contains(asked[0], "old question") || strings.Contains(asked[0], "recent question") || strings.Contains(asked[0], "recent answer") {
		t.Fatalf("the summarizer was asked %q", asked)
	}
	prompt := <-prompts
	if len(prompt) != 5 || !strings.Contains(prompt[1], "lighthouse tour of Maine") || !strings.HasSuffix(prompt[1], "recent question") ||
		prompt[2] != "assistant: recent answer" || prompt[3] != "user: test message" {
		t.Fatalf("the model was sent %q", prompt)
	}
	if start, end := eventOfType(events, "COMPACTION_START"), eventOfType(events, "COMPACTION_END"); start == nil || end == nil || start.Before != "m3" || end.Before != "m3" {
		t.Fatalf("the marker was opened with %+v and closed with %+v", start, end)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	chat := mustRead(domain.ReadThreadRow(row))
	if ids := messageIDs(api.Parsed(api.Raw(chat.Messages))); len(ids) != 7 || !reflect.DeepEqual(ids[:2], []string{"m1", "m2"}) || !reflect.DeepEqual(ids[3:5], []string{"m3", "m4"}) ||
		chat.Messages[2].Compaction == nil || chat.Messages[2].Compaction.Messages != 2 {
		t.Fatalf("the turn stored %s", row.Data)
	}
}

// A tool exchange that outgrows the window keeps no earlier exchange that
// would leave it too little room beside the summary.
func TestAToolExchangeKeepsOnlyTheExchangesItHasRoomFor(t *testing.T) {
	serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
		return nil, api.Object{"results": []api.Object{{"title": "Tides", "url": "https://tides.test", "content": filler(400)}}}
	})
	sent := make(chan []any, 1)
	h, rows, key := testChatHarness(t, toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"tides"}`), sent))
	h.models.Catalog.Get().Models[0]["budgetWindow"] = 16384
	h.compactWith(func(string, string) (string, error) { return compactionOf(filler(150)), nil })
	storeExchanges(rows, "chat-1", "old question", filler(350), "recent question", filler(100))
	in := searchInput(key)
	in["threadId"] = "chat-1"
	events := finishedRun(t, h, "/v1/threads/turn", in)
	if result, start := slices.IndexFunc(events, func(e event) bool { return e.Type == "TOOL_CALL_RESULT" }), slices.IndexFunc(events, func(e event) bool { return e.Type == "COMPACTION_START" }); result < 0 || start < result {
		t.Fatal("the chat was not compacted once the search had outgrown the window")
	}
	if messages := <-sent; strings.Contains(fmt.Sprint(messages), "recent question") || toolMessage(messages, "s") == "" {
		t.Fatalf("after the search the model was sent %v", messages)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	if chat := mustRead(domain.ReadThreadRow(row)); domain.LatestCompaction(chat.Messages) != 4 || chat.Messages[5].Content != "test message" {
		t.Fatalf("the run stored %s", row.Data)
	}
}

// A compaction keeps no message the run's snapshot left out, whether the
// snapshot left it out as one of too many or for the size of the messages
// after it, so a client holds the message its marker sits before.
func TestACompactionKeepsOnlyMessagesTheRunShows(t *testing.T) {
	many := []string{"old question", filler(700)}
	for i := range domain.MessageWindow / 2 {
		many = append(many, fmt.Sprintf("question %d", i), fmt.Sprintf("answer %d", i))
	}
	// Each reply after the first exchange reasoned at length, which a
	// snapshot shows but the model does not read again.
	large := func(messages []any) {
		thought := strings.Repeat("x", 3<<20)
		for _, m := range messages[2:] {
			if message := api.Obj(m); message["role"] == "assistant" {
				message["timeline"] = append([]any{api.Object{"id": "t" + api.Str(message["id"]), "type": "thinking", "content": thought}}, api.Arr(message["timeline"])...)
			}
		}
	}
	// The earliest question a compaction can keep is the first the snapshot
	// shows from its first message on.
	for _, tc := range []struct {
		name        string
		texts       []string
		edit        func([]any)
		first, kept string
	}{
		{"many messages", many, nil, "m4", "m5"},
		{"large messages", []string{"old question", filler(700), "question 0", "answer 0", "question 1", "answer 1", "question 2", "answer 2"}, large, "m5", "m5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, rows, key := testChatHarness(t, nil)
			h.models.Catalog.Get().Models[0]["budgetWindow"] = 16384
			h.compactWith(func(string, string) (string, error) { return compactionOf("The user chatted."), nil })
			storeExchanges(rows, "chat-1", tc.texts...)
			if tc.edit != nil {
				name := rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")
				row := rows.rows[name]
				editRow(&row, func(data api.Object) { tc.edit(api.Arr(data["messages"])) })
				rows.rows[name] = row
			}
			events := finishedRun(t, h, "/v1/threads/turn", continueInput(key, "chat-1"))
			shown := messageIDs(eventOfType(events, "MESSAGES_SNAPSHOT").Messages)
			if start := eventOfType(events, "COMPACTION_START"); start == nil || shown[0] != tc.first || start.Before != tc.kept {
				t.Fatalf("the marker was opened with %+v after a snapshot of %q", start, shown)
			}
		})
	}
}

// A compaction that fails ends the run with an error the client retries,
// and the model never reads a chat cut to fit. Retrying compacts it.
func TestAFailedCompactionEndsTheRunWithoutCuttingTheChat(t *testing.T) {
	prompts := make(chan []string, 1)
	h, rows, key := testChatHarness(t, promptOf(prompts))
	failing := true
	h.compactWith(func(string, string) (string, error) {
		if failing {
			return "", &inference.Refused{Status: http.StatusBadRequest}
		}
		return compactionOf("The user plans a lighthouse tour of Maine."), nil
	})
	longCompacted(rows, "chat-1")
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	last := events[len(events)-1]
	if end := eventOfType(events, "COMPACTION_END"); end == nil || end.Outcome != domain.CompactionFailed || last.Type != "RUN_ERROR" || last.Code != "COMPACTION_FAILED" || len(prompts) != 0 {
		t.Fatalf("the run ended with %s %s after compacting %+v", last.Type, last.Code, end)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	if chat := mustRead(domain.ReadThreadRow(row)); len(chat.Messages) != 6 || chat.Messages[5].Content != "test message" || domain.LatestCompaction(chat.Messages) != 2 {
		t.Fatalf("the failed run stored %s", row.Data)
	}
	failing = false
	retry := continueInput(key, "chat-1")
	retry["kind"] = "retry"
	delete(retry, "content")
	finishedRun(t, h, "/v1/threads/turn", retry)
	if prompt := <-prompts; len(prompt) != 3 || !strings.Contains(prompt[1], "lighthouse tour of Maine") || !strings.HasSuffix(prompt[1], "test message") {
		t.Fatalf("the retry sent %q", prompt)
	}
}

// A tool exchange that outgrows the window compacts the history before the
// question, and the model goes on with the summary, the question and the
// exchange so far.
func TestAToolExchangeThatOutgrowsTheWindowCompactsTheChat(t *testing.T) {
	serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
		return nil, api.Object{"results": []api.Object{{"title": "Tides", "url": "https://tides.test", "content": filler(400)}}}
	})
	sent := make(chan []any, 1)
	h, rows, key := testChatHarness(t, toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"tides"}`), sent))
	h.models.Catalog.Get().Models[0]["budgetWindow"] = 16384
	h.compactWith(func(string, string) (string, error) {
		return compactionOf("The user plans a lighthouse tour of Maine."), nil
	})
	longCompacted(rows, "chat-1")
	in := searchInput(key)
	in["threadId"] = "chat-1"
	events := finishedRun(t, h, "/v1/threads/turn", in)
	messages := <-sent
	roles := []string{}
	for _, m := range messages {
		roles = append(roles, api.Str(api.Obj(m)["role"]))
	}
	if !reflect.DeepEqual(roles, []string{"system", "user", "user", "assistant", "tool"}) || !strings.Contains(api.Str(api.Obj(messages[1])["content"]), "lighthouse tour of Maine") || toolMessage(messages, "s") == "" {
		t.Fatalf("after the search the model was sent %v", roles)
	}
	if result, start := slices.IndexFunc(events, func(e event) bool { return e.Type == "TOOL_CALL_RESULT" }), slices.IndexFunc(events, func(e event) bool { return e.Type == "COMPACTION_START" }); result < 0 || start < result {
		t.Fatal("the chat was not compacted once the search had outgrown the window")
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	if chat := mustRead(domain.ReadThreadRow(row)); domain.LatestCompaction(chat.Messages) != 5 || chat.Messages[6].Content != "test message" || chat.Messages[7].Content != "done" {
		t.Fatalf("the run stored %s", row.Data)
	}
}

// compactInput is a compact turn of alice's stored chat.
func compactInput(key auth.ContentKey, thread string) api.Object {
	in := continueInput(key, thread)
	in["kind"] = "compact"
	delete(in, "content")
	return in
}

// A compact turn summarizes the messages before the chat's latest exchange
// into a marker before it, without asking the model, adding a message or
// changing what a retry repeats, and the chat then fills less of the window.
func TestACompactTurnCompactsTheChatWithoutAskingTheModel(t *testing.T) {
	prompts := make(chan []string, 1)
	h, rows, key := testChatHarness(t, promptOf(prompts))
	var asked []string
	h.compactWith(func(task, text string) (string, error) {
		if task == summarize.Compaction.Name {
			asked = append(asked, text)
		}
		return compactionOf("The user plans a lighthouse tour of Maine."), nil
	})
	storeExchanges(rows, "chat-1", "old question", filler(100), "recent question", "recent answer")
	name := rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")
	row := rows.rows[name]
	editRow(&row, func(data api.Object) { data["lastRequestKind"] = "continue" })
	rows.rows[name] = row
	events := finishedRun(t, h, "/v1/threads/turn", compactInput(key, "chat-1"))
	if len(prompts) != 0 || len(asked) != 1 || !strings.Contains(asked[0], "old question") || strings.Contains(asked[0], "recent question") || strings.Contains(asked[0], "recent answer") {
		t.Fatalf("the model was asked %d times, and the summarizer %q", len(prompts), asked)
	}
	chat := storedChat(t, rows, key, "chat-1")
	if ids := messageIDs(api.Parsed(api.Raw(chat.Messages))); len(ids) != 5 || !reflect.DeepEqual(ids[:2], []string{"m1", "m2"}) || !reflect.DeepEqual(ids[3:], []string{"m3", "m4"}) ||
		chat.Messages[2].Compaction == nil || chat.Messages[2].Compaction.Messages != 2 || chat.ActiveRun != nil || chat.LastRequestKind != "continue" {
		t.Fatalf("the turn stored %+v", chat)
	}
	end := slices.IndexFunc(events, func(e event) bool { return e.Type == "COMPACTION_END" })
	if end < 0 || events[end].Before != "m3" || events[end+1].Type != "STATE_SNAPSHOT" || eventOfType(events, "TEXT_MESSAGE_CHUNK") != nil {
		t.Fatalf("the run sent %+v", events)
	}
	used := func(e event) int {
		return api.Int(api.Obj(api.Obj(api.Obj(e.Snapshot)["thread"])["contextUsage"])["usedTokens"])
	}
	if before, after := used(*eventOfType(events, "STATE_SNAPSHOT")), used(events[end+1]); chat.ContextUsage == nil || chat.ContextUsage.UsedTokens != after || after >= before {
		t.Fatalf("the chat filled %d tokens before and %d after, and stored %+v", before, after, chat.ContextUsage)
	}
}

// A compact turn is refused before it starts when no message after the
// chat's latest marker comes before its last question, and while the chat
// has a running reply, rather than waiting behind it.
func TestACompactTurnIsRefusedWhenItCannotCompact(t *testing.T) {
	model, release := heldModel()
	h, rows, key := testChatHarness(t, model)
	t.Cleanup(release)
	summarized := false
	h.compactWith(func(string, string) (string, error) { summarized = true; return compactionOf("The user chatted."), nil })
	storeExchanges(rows, "short", "only question", "only answer")
	storeCompacted(rows, "compacted")
	for _, id := range []string{"short", "compacted"} {
		if w := postRoute(h, "/v1/threads/turn", compactInput(key, id), "alice"); w.Code != 400 || !strings.Contains(w.Body.String(), "NOTHING_TO_COMPACT") || storedChat(t, rows, key, id).ActiveRun != nil {
			t.Fatalf("compacting %s: HTTP %d %s", id, w.Code, w.Body)
		}
	}
	storeExchanges(rows, "running", "old question", "old answer", "recent question", "recent answer")
	run, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), key, requestOf[engine.TurnRequest](continueInput(key, "running")))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.Log, "TEXT_MESSAGE_CHUNK")
	if w := postRoute(h, "/v1/threads/turn", compactInput(key, "running"), "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "RUN_IN_FLIGHT") {
		t.Fatalf("compacting a chat with a running reply: HTTP %d %s", w.Code, w.Body)
	}
	release()
	<-run.Finished
	if summarized {
		t.Fatal("a refused compaction was summarized")
	}
}

// A retry after a compact turn failed compacts again, and once a compaction
// succeeds a retry no longer does.
func TestARetryCompactsAgainAfterACompactTurnFailed(t *testing.T) {
	prompts := make(chan []string, 1)
	h, rows, key := testChatHarness(t, promptOf(prompts))
	failing := true
	h.compactWith(func(string, string) (string, error) {
		if failing {
			return "", &inference.Refused{Status: http.StatusBadRequest}
		}
		return compactionOf("The user plans a lighthouse tour of Maine."), nil
	})
	storeExchanges(rows, "chat-1", "old question", filler(100), "recent question", "recent answer")
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", compactInput(key, "chat-1"), "alice"))
	if last := events[len(events)-1]; last.Code != "COMPACTION_FAILED" || storedChat(t, rows, key, "chat-1").LastRequestKind != "compact" {
		t.Fatalf("the failed compaction ended with %+v", last)
	}
	failing = false
	retry := compactInput(key, "chat-1")
	retry["kind"] = "retry"
	finishedRun(t, h, "/v1/threads/turn", retry)
	if chat := storedChat(t, rows, key, "chat-1"); len(prompts) != 0 || len(chat.Messages) != 5 || domain.LatestCompaction(chat.Messages) != 2 || chat.LastRequestKind != "" {
		t.Fatalf("the retry asked the model %d times and stored %+v", len(prompts), chat)
	}
}

// A question is refused before anything is summarized only when its chat's
// prompt does not fit and the question would not fit beside a summary of the
// messages before it either.
func TestAQuestionIsRefusedOnlyWhenNoCompactionCouldFitIt(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	summarized := false
	h.compactWith(func(string, string) (string, error) { summarized = true; return "", nil })
	storeCompacted(rows, "fits")
	longCompacted(rows, "outgrown")
	ask := func(id string) *httptest.ResponseRecorder {
		in := continueInput(key, id)
		in["content"] = filler(125)
		return postRoute(h, "/v1/threads/turn", in, "alice")
	}
	if events := eventsIn(t, ask("fits")); events[len(events)-1].Type != "RUN_FINISHED" || eventOfType(events, "COMPACTION_START") != nil {
		t.Fatalf("a question whose prompt fits ended with %+v", events[len(events)-1])
	}
	if w := ask("outgrown"); w.Code != http.StatusRequestEntityTooLarge || !strings.Contains(w.Body.String(), "CONTEXT_TOO_LARGE") || summarized {
		t.Fatalf("HTTP %d %s, summarized %v", w.Code, w.Body, summarized)
	}
}

// A session offers compaction, automatic or on request, only while the
// catalog lists the model that writes its summaries, without which a long
// chat's turn fails.
func TestASessionOffersCompactionOnlyWithItsModel(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	offered := func() map[string]bool {
		var session struct {
			Features map[string]bool `json:"features"`
		}
		json.Unmarshal(postRoute(h, "/v1/session", api.Object{}, "alice").Body.Bytes(), &session)
		return session.Features
	}
	h.engine.Summaries = summarize.New(summarizerFunc(nil), &h.models.Catalog, "kimi-k3", longSummaryModel)
	if features := offered(); features["compaction"] || features["manualCompaction"] {
		t.Fatalf("a session offered compaction on a model the catalog does not list: %v", features)
	}
	h.compactWith(nil)
	if features := offered(); !features["compaction"] || !features["manualCompaction"] {
		t.Fatalf("a session did not offer compaction on a model the catalog lists: %v", features)
	}
}

// A compacted widget retry whose call changed while it ran fails, and its
// chat keeps the compaction the retry made.
func TestAWidgetRetryKeepsItsCompactionWhenItsCallChanged(t *testing.T) {
	var stored atomic.Pointer[memoryRows]
	changed := `{"stats":[{"label":"Count","value":3}]}`
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		rows := stored.Load()
		rows.mu.Lock()
		name := rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")
		row := rows.rows[name]
		editRow(&row, func(data api.Object) {
			for _, m := range api.Arr(data["messages"]) {
				for _, block := range api.Arr(api.Obj(m)["timeline"]) {
					if api.Obj(block)["toolCallId"] == "call" {
						api.Obj(block)["arguments"] = changed
					}
				}
			}
		})
		rows.rows[name] = row
		rows.mu.Unlock()
		json.NewEncoder(w).Encode(api.Object{"choices": []api.Object{{"message": api.Object{"content": `{"stats":"[{\"label\":\"Count\",\"value\":2}]"}`}}}})
	})
	stored.Store(rows)
	h.models.Catalog.Get().EnabledWidgets = []string{"render_stat_cards"}
	h.compactWith(func(string, string) (string, error) { return compactionOf("The user asked for statistics."), nil })
	answer := filler(350)
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")] = domain.StoredRow{ID: "chat-1", ETag: "1", Data: api.Raw(api.Object{"createdAt": "2026-01-01T00:00:00Z", "title": "Stats", "titleState": "manual", "model": "kimi-k3", "messages": []any{
		api.Object{"id": "m1", "role": "user", "content": "first question"},
		api.Object{"id": "m2", "role": "assistant", "content": answer, "timeline": []any{api.Object{"type": "content", "id": "b2", "content": answer}}},
		api.Object{"id": "m3", "role": "user", "content": "show a stat"},
		api.Object{"id": "m4", "role": "assistant", "content": "a stat", "timeline": []any{api.Object{"type": "content", "id": "b4", "content": "a stat"},
			api.Object{"type": "tool_call", "id": "call", "toolCallId": "call", "name": "render_stat_cards", "arguments": `{"stats":[{"label":"Count","value":1}]}`}}},
	}})}
	in := continueInput(key, "chat-1")
	in["kind"], in["toolCallId"], in["widgets"] = "retryToolCall", "call", []any{"render_stat_cards"}
	delete(in, "content")
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if last := events[len(events)-1]; last.Type != "RUN_ERROR" || last.Code != "REVISION_CONFLICT" || eventOfType(events, "COMPACTION_END") == nil {
		t.Fatalf("the retry ended with %s %s", last.Type, last.Code)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat-1")
	chat := mustRead(domain.ReadThreadRow(row))
	if len(chat.Messages) != 5 || chat.Messages[2].Role != domain.CompactionRole || chat.Messages[4].Timeline[1].Arguments != changed {
		t.Fatalf("the retry stored %s", row.Data)
	}
}
