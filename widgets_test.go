package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestCanonicalizeWidgetArgumentsRecursive(t *testing.T) {
	var parsed map[string]any
	json.Unmarshal([]byte(canonicalizeWidgetArguments(`{"outer":"{\"inner\":\"[1,2,3]\"}"}`)), &parsed)
	outer, _ := parsed["outer"].(map[string]any)
	if inner, _ := outer["inner"].([]any); len(inner) != 3 {
		t.Fatalf("expected inner array of length 3, got %v", parsed)
	}
}

func TestCanonicalizeWidgetArgumentsPreservesLargeNumbers(t *testing.T) {
	const large = "9007199254740993"
	got := canonicalizeWidgetArguments(`{"id":` + large + `,"nested":"{\"id\":` + large + `}"}`)
	if !strings.Contains(got, `"id":`+large) || !strings.Contains(got, `"nested":{"id":`+large+`}`) {
		t.Fatalf("numbers were not preserved: %s", got)
	}
}

func catalogWidgets(names ...string) map[string]widget {
	widgets := map[string]widget{}
	for _, w := range widgetCatalog() {
		for _, name := range names {
			if w.Name == name {
				widgets[name] = w
			}
		}
	}
	return widgets
}

// invokeWidget runs one widget call and returns what the model reads and the
// results the caller was sent.
func invokeWidget(t *testing.T, set *toolset, call toolCall) (string, []string) {
	t.Helper()
	out := &stream{run: newRun("00")}
	var results []string
	out.observe = func(e event) {
		if e.Type == "TOOL_CALL_RESULT" {
			results = append(results, string(e.Content))
		}
	}
	var reply struct {
		Content string `json:"content"`
	}
	json.Unmarshal(invoke(context.Background(), out, set, call), &reply)
	return reply.Content, results
}

func widgetToolset() *toolset {
	return &toolset{widgets: catalogWidgets("render_artifact_preview"), local: func(context.Context, toolCall) (any, error) { return "[rendered for the user]", nil }}
}

func TestWidgetSchemaViolationIsReportedToTheModel(t *testing.T) {
	set := widgetToolset()
	call := toolCall{id: "c1", name: "render_artifact_preview", args: `{"title":"Demo","source":"<p>hi</p>"}`}
	for attempt := 1; attempt <= maxWidgetRetries+1; attempt++ {
		model, shown := invokeWidget(t, set, call)
		if len(shown) != 1 || obj(decodeValue([]byte(shown[0])))["status"] != "error" {
			t.Fatalf("attempt %d: the caller was sent %q", attempt, shown)
		}
		if attempt > maxWidgetRetries {
			if model != `{"status":"executed"}` {
				t.Fatalf("retries were not capped at %d: %s", maxWidgetRetries, model)
			}
			continue
		}
		payload := obj(decodeValue([]byte(model)))
		message := str(payload["error"])
		if payload["status"] != "error" || !strings.HasPrefix(message, "The arguments for render_artifact_preview did not match the tool's parameter schema and the component was not displayed: ") ||
			!strings.Contains(message, "source") || strings.Contains(message, "validating root") || !strings.HasSuffix(message, "Call render_artifact_preview again with corrected arguments that match the schema exactly.") {
			t.Fatalf("attempt %d: the model read %s", attempt, model)
		}
	}
	if model, _ := invokeWidget(t, widgetToolset(), toolCall{id: "c2", name: "render_artifact_preview", args: `{"source":`}); !strings.Contains(model, "the component was not displayed: arguments are not valid JSON. Call") {
		t.Fatalf("unparseable arguments read as %s", model)
	}
}

func TestWidgetRetriesAreCountedPerNameWithinRun(t *testing.T) {
	set := widgetToolset()
	set.widgets = catalogWidgets("render_artifact_preview", "render_stat_cards")
	var wg sync.WaitGroup
	var mu sync.Mutex
	told := map[string]int{}
	for range 4 {
		for _, call := range []toolCall{{id: "a", name: "render_artifact_preview", args: `{}`}, {id: "s", name: "render_stat_cards", args: `{"stats":[]}`}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out := &stream{run: newRun("00")}
				if strings.Contains(string(invoke(context.Background(), out, set, call)), "did not match") {
					mu.Lock()
					told[call.name]++
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	if told["render_artifact_preview"] != maxWidgetRetries || told["render_stat_cards"] != maxWidgetRetries {
		t.Fatalf("the model was told %v", told)
	}
}

func TestCallersCopyReadsRepairedArguments(t *testing.T) {
	for _, tc := range []struct{ name, args, content, want string }{
		{"web_search", `{"query":"cats"} Now searching...`, `{"results":[]}`, `{"query":"cats","results":[]}`},
		{"web_fetch", `{"urls":["https://a.test"]} Now fetching...`, `{"results":[{"url":"https://a.test","status":"failed","error":"timeout"}]}`,
			`{"failed":[{"error":"timeout","url":"https://a.test"}],"pages":[],"text":"","title":"Fetched page","url":"https://a.test"}`},
	} {
		if got := string(raw(normalizeToolResult(toolCall{id: "c1", name: tc.name, args: tc.args}, tc.content))); got != tc.want {
			t.Errorf("%s with arguments %q was shown as %s, want %s", tc.name, tc.args, got, tc.want)
		}
	}
}

// statsThread is a chat with a stat card whose arguments miss a required
// field, and one whose arguments are valid. Its stored copy carries marks the
// harness would not set now: on the valid card and on a widget no longer
// declared.
func statsThread() object {
	return object{"title": "Stats", "createdAt": "2026-01-01T00:00:00Z", "model": "kimi-k3", "messages": []any{
		object{"id": "u1", "role": "user", "content": "show the counts", "createdAt": "2026-01-01T00:00:01Z"},
		object{"id": "a1", "role": "assistant", "content": "Here they are.", "createdAt": "2026-01-01T00:00:02Z", "timeline": []any{
			object{"id": "bad", "toolCallId": "bad", "type": "tool_call", "name": "render_stat_cards", "arguments": `{"stats":[{"label":"Count"}]}`, "complete": true},
			object{"id": "good", "toolCallId": "good", "type": "tool_call", "name": "render_stat_cards", "arguments": `{"stats":[{"label":"Count","value":1}]}`, "complete": true, "invalidArguments": true},
			object{"id": "retired", "toolCallId": "retired", "type": "tool_call", "name": "render_retired", "arguments": `{}`, "complete": true, "invalidArguments": true},
			object{"id": "text", "type": "content", "content": "Here they are."},
		}},
	}}
}

// A client offers to regenerate a widget's arguments when the widget's
// current declaration refuses them, wherever it reads the chat from.
func TestEveryViewMarksWidgetArgumentsTheDeclarationRefuses(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	cp := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer cp.Close()
	h.controlplane, h.cpClient = cp.URL, &http.Client{Transport: &callerAuth{inner: cp.Client().Transport}}
	rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", "chat-1")] = storedRow{ID: "chat-1", ETag: "1", Data: raw(statsThread())}
	marks := func(view string, messages any) {
		t.Helper()
		list := arr(messages)
		if len(list) < 2 {
			t.Fatalf("%s holds %v", view, messages)
		}
		marked := map[string]any{}
		for _, b := range arr(obj(list[1])["timeline"]) {
			if flag, ok := obj(b)["invalidArguments"]; ok {
				marked[str(obj(b)["id"])] = flag
			}
		}
		if !reflect.DeepEqual(marked, map[string]any{"bad": true}) {
			t.Fatalf("%s marked %v", view, marked)
		}
	}
	body := func(w *httptest.ResponseRecorder) object {
		t.Helper()
		if w.Code != 200 {
			t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
		}
		return obj(decodeValue(w.Body.Bytes()))
	}
	marks("the thread view", body(postRoute(h, "/v1/threads/get", object{"key": key.base64(), "id": "chat-1"}, "alice"))["messages"])
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	marks("the run's snapshot", eventOfType(events, "MESSAGES_SNAPSHOT").Messages)
	marks("the transcript", obj(body(postRoute(h, "/v1/threads/transcript", object{"key": key.base64(), "threadId": "chat-1"}, "alice"))["thread"])["messages"])
	body(postRoute(h, "/v1/threads/share", object{"key": key.base64(), "threadId": "chat-1"}, "alice"))
	if seals := fake.requests("/v1/share/seal"); len(seals) == 1 {
		plaintext, _ := base64.StdEncoding.DecodeString(str(seals[0]["plaintext"]))
		marks("the share", obj(decodeValue(plaintext))["messages"])
	} else {
		t.Fatalf("%d shares were sealed", len(seals))
	}
	marks("the device run", endedThread(t, h, localInput("", statsThread()), "RUN_FINISHED")["messages"])
	marks("the device operation", deviceOperation(t, h, "/v1/threads/update", statsThread(), object{"id": "chat-2", "title": "Renamed"}, "")["messages"])
	device := statsThread()
	device["id"] = "chat-3"
	marks("the upgrade", obj(upgraded(t, h, "alice", device)[0])["messages"])
}

func TestValidWidgetArgumentsAreAcknowledged(t *testing.T) {
	set := widgetToolset()
	model, shown := invokeWidget(t, set, toolCall{id: "c1", name: "render_artifact_preview", args: `{"title":"Demo","source":{"type":"html","html":"<p>hi</p>"}}`})
	if model != "[rendered for the user]" || len(shown) != 1 || shown[0] != `"[rendered for the user]"` {
		t.Fatalf("a valid widget read as %q and showed %q", model, shown)
	}
	if model, _ := invokeWidget(t, set, toolCall{id: "c2", name: "render_unknown", args: `{"anything":1}`}); model != `{"error":"unknown tool"}` {
		t.Fatalf("an unknown widget read as %q", model)
	}
}
