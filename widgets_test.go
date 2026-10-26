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

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func TestCanonicalizeWidgetArgumentsRecursive(t *testing.T) {
	var parsed map[string]any
	json.Unmarshal([]byte(domain.CanonicalizeWidgetArguments(`{"outer":"{\"inner\":\"[1,2,3]\"}"}`)), &parsed)
	outer, _ := parsed["outer"].(map[string]any)
	if inner, _ := outer["inner"].([]any); len(inner) != 3 {
		t.Fatalf("expected inner array of length 3, got %v", parsed)
	}
}

func TestCanonicalizeWidgetArgumentsPreservesLargeNumbers(t *testing.T) {
	const large = "9007199254740993"
	got := domain.CanonicalizeWidgetArguments(`{"id":` + large + `,"nested":"{\"id\":` + large + `}"}`)
	if !strings.Contains(got, `"id":`+large) || !strings.Contains(got, `"nested":{"id":`+large+`}`) {
		t.Fatalf("numbers were not preserved: %s", got)
	}
}

func catalogWidgets(names ...string) map[string]domain.Widget {
	widgets := map[string]domain.Widget{}
	for _, w := range domain.WidgetCatalog() {
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
	out.observe = func(e domain.Event, _ string) {
		if result, ok := e.(domain.ToolCallResult); ok {
			results = append(results, string(result.Content))
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
	for attempt := 1; attempt <= domain.MaxWidgetRetries+1; attempt++ {
		model, shown := invokeWidget(t, set, call)
		if len(shown) != 1 || api.Obj(api.Parsed([]byte(shown[0])))["status"] != "error" {
			t.Fatalf("attempt %d: the caller was sent %q", attempt, shown)
		}
		if attempt > domain.MaxWidgetRetries {
			if model != `{"status":"executed"}` {
				t.Fatalf("retries were not capped at %d: %s", domain.MaxWidgetRetries, model)
			}
			continue
		}
		payload := api.Obj(api.Parsed([]byte(model)))
		message := api.Str(payload["error"])
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
	if told["render_artifact_preview"] != domain.MaxWidgetRetries || told["render_stat_cards"] != domain.MaxWidgetRetries {
		t.Fatalf("the model was told %v", told)
	}
}

func TestCallersCopyReadsRepairedArguments(t *testing.T) {
	for _, tc := range []struct{ name, args, content, want string }{
		{"web_search", `{"query":"cats"} Now searching...`, `{"results":[]}`, `{"query":"cats","results":[]}`},
		{"web_fetch", `{"urls":["https://a.test"]} Now fetching...`, `{"results":[{"url":"https://a.test","status":"failed","error":"timeout"}]}`,
			`{"failed":[{"error":"timeout","url":"https://a.test"}],"pages":[],"text":"","title":"Fetched page","url":"https://a.test"}`},
	} {
		if got := string(api.Raw(domain.NormalizeToolResult(tc.name, tc.args, tc.content))); got != tc.want {
			t.Errorf("%s with arguments %q was shown as %s, want %s", tc.name, tc.args, got, tc.want)
		}
	}
}

// statsThread is a chat with a stat card whose arguments miss a required
// field, one whose arguments are valid, and one with fields no property
// declares. Its stored copy carries marks the
// harness would not set now: on the valid card and on a widget no longer
// declared.
func statsThread() api.Object {
	return api.Object{"title": "Stats", "createdAt": "2026-01-01T00:00:00Z", "model": "kimi-k3", "messages": []any{
		api.Object{"id": "u1", "role": "user", "content": "show the counts", "createdAt": "2026-01-01T00:00:01Z"},
		api.Object{"id": "a1", "role": "assistant", "content": "Here they are.", "createdAt": "2026-01-01T00:00:02Z", "timeline": []any{
			api.Object{"id": "bad", "toolCallId": "bad", "type": "tool_call", "name": "render_stat_cards", "arguments": `{"stats":[{"label":"Count"}]}`, "complete": true},
			api.Object{"id": "good", "toolCallId": "good", "type": "tool_call", "name": "render_stat_cards", "arguments": `{"stats":[{"label":"Count","value":1}]}`, "complete": true, "invalidArguments": true},
			api.Object{"id": "extra", "toolCallId": "extra", "type": "tool_call", "name": "render_stat_cards", "arguments": `{"stats":[{"label":"Count","value":1,"unit":"%"}],"title":"Counts"}`, "complete": true},
			api.Object{"id": "retired", "toolCallId": "retired", "type": "tool_call", "name": "render_retired", "arguments": `{}`, "complete": true, "invalidArguments": true},
			api.Object{"id": "text", "type": "content", "content": "Here they are."},
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
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")] = domain.StoredRow{ID: "chat-1", ETag: "1", Data: api.Raw(statsThread())}
	marks := func(view string, messages any) {
		t.Helper()
		list := api.Arr(messages)
		if len(list) < 2 {
			t.Fatalf("%s holds %v", view, messages)
		}
		marked := map[string]any{}
		for _, b := range api.Arr(api.Obj(list[1])["timeline"]) {
			if flag, ok := api.Obj(b)["invalidArguments"]; ok {
				marked[api.Str(api.Obj(b)["id"])] = flag
			}
		}
		if !reflect.DeepEqual(marked, map[string]any{"bad": true}) {
			t.Fatalf("%s marked %v", view, marked)
		}
	}
	body := func(w *httptest.ResponseRecorder) api.Object {
		t.Helper()
		if w.Code != 200 {
			t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
		}
		return api.Obj(api.Parsed(w.Body.Bytes()))
	}
	marks("the thread view", body(postRoute(h, "/v1/threads/get", api.Object{"key": keyBase64(key), "id": "chat-1"}, "alice"))["messages"])
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", continueInput(key, "chat-1"), "alice"))
	marks("the run's snapshot", eventOfType(events, "MESSAGES_SNAPSHOT").Messages)
	marks("the transcript", api.Obj(body(postRoute(h, "/v1/threads/transcript", api.Object{"key": keyBase64(key), "threadId": "chat-1"}, "alice"))["thread"])["messages"])
	body(postRoute(h, "/v1/threads/share", api.Object{"key": keyBase64(key), "threadId": "chat-1"}, "alice"))
	if seals := fake.requests("/v1/share/seal"); len(seals) == 1 {
		plaintext, _ := base64.StdEncoding.DecodeString(api.Str(seals[0]["plaintext"]))
		marks("the share", api.Obj(api.Parsed(plaintext))["messages"])
	} else {
		t.Fatalf("%d shares were sealed", len(seals))
	}
	marks("the device run", endedThread(t, h, localInput("", statsThread()), "RUN_FINISHED")["messages"])
	marks("the device operation", deviceOperation(t, h, "/v1/threads/update", statsThread(), api.Object{"id": "chat-2", "title": "Renamed"}, "")["messages"])
	device := statsThread()
	device["id"] = "chat-3"
	marks("the upgrade", api.Obj(upgraded(t, h, "alice", device)[0])["messages"])
}

func TestValidWidgetArgumentsAreAcknowledged(t *testing.T) {
	set := widgetToolset()
	model, shown := invokeWidget(t, set, toolCall{id: "c1", name: "render_artifact_preview", args: `{"title":"Demo","source":{"type":"html","html":"<p>hi</p>"}}`})
	if model != "[rendered for the user]" || len(shown) != 1 || shown[0] != `"[rendered for the user]"` {
		t.Fatalf("a valid widget read as %q and showed %q", model, shown)
	}
	extra := `{"title":"Demo","theme":"dark","source":{"type":"html","html":"<p>hi</p>","language":"html"}}`
	if model, shown := invokeWidget(t, set, toolCall{id: "c3", name: "render_artifact_preview", args: extra}); model != "[rendered for the user]" || len(shown) != 1 {
		t.Fatalf("a widget with undeclared fields read as %q and showed %q", model, shown)
	}
	if model, _ := invokeWidget(t, set, toolCall{id: "c2", name: "render_unknown", args: `{"anything":1}`}); model != `{"error":"unknown tool"}` {
		t.Fatalf("an unknown widget read as %q", model)
	}
}

// A retried link preview reads the page its new arguments name, as a first
// call does, and keeps what it read in place of the refusal.
func TestARetriedLinkPreviewReadsItsPage(t *testing.T) {
	original, fixed := `{"url":"https://example.com/post"}`, `{"url":"https://example.com/post","title":"A post"}`
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(api.Object{"choices": []api.Object{{"message": api.Object{"content": fixed}}}})
	})
	asked := make(chan string, 1)
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		asked <- r.URL.Path + " " + api.Str(in["url"])
		json.NewEncoder(w).Encode(api.Object{"result": api.Object{"title": "The Post", "description": "About it", "site_name": "Example", "image": "https://example.com/i.png"}})
	}))
	defer metadata.Close()
	h.services["metadata"] = &downstreamService{URL: metadata.URL, Client: metadata.Client()}
	h.catalog.get().EnabledWidgets = []string{"render_link_preview"}
	refused := domain.WidgetFailure(catalogWidgets("render_link_preview")["render_link_preview"], original)
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat")] = domain.StoredRow{ID: "chat", ETag: "1", Data: api.Raw(api.Object{"createdAt": api.Timestamp(), "title": "Links", "titleState": "manual", "model": "kimi-k3", "messages": []any{
		api.Object{"id": "user", "role": "user", "content": "link the post", "createdAt": api.Timestamp()},
		api.Object{"id": "assistant", "role": "assistant", "content": "Here it is.", "createdAt": api.Timestamp(), "timeline": []any{
			api.Object{"type": "content", "id": "text", "content": "Here it is."},
			api.Object{"type": "tool_call", "id": "call", "toolCallId": "call", "name": "render_link_preview", "arguments": original, "complete": true, "result": refused},
		}},
	}})}
	in := sendInput(key)
	in["threadId"], in["kind"], in["toolCallId"], in["widgets"] = "chat", "retryToolCall", "call", []any{"render_link_preview"}
	delete(in, "content")
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" {
		t.Fatalf("the retry failed: %+v", last)
	}
	select {
	case got := <-asked:
		if got != "/metadata https://example.com/post" {
			t.Fatalf("the retry read %s", got)
		}
	default:
		t.Fatal("the retry read no page")
	}
	want := api.Parsed(api.Raw(api.Object{"url": "https://example.com/post", "title": "The Post", "description": "About it", "siteName": "Example", "image": "https://example.com/i.png"}))
	if shown := eventOfType(events, "TOOL_CALL_RESULT"); shown == nil || shown.ToolCallID != "call" || !reflect.DeepEqual(api.Parsed(shown.Content), want) {
		t.Fatalf("the retry showed %+v", shown)
	}
	row, _ := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat")
	if call := mustRead(domain.ReadThreadRow(row)).Messages[1].Timeline[1]; call.Arguments != fixed || !reflect.DeepEqual(api.Parsed(call.Result), want) {
		t.Fatalf("the retried widget was stored with arguments %s and result %s", call.Arguments, call.Result)
	}
}

// A link preview in an old chat holds no page details; its client reads
// them with the caller's credential, signed in or not, as the widget's call
// reads them.
func TestALinkPreviewReadsItsPageDetails(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	asked := make(chan string, 2)
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		asked <- r.Header.Get("Authorization") + " " + r.URL.Path + " " + api.Str(in["url"])
		json.NewEncoder(w).Encode(api.Object{"result": api.Object{"title": "The Post", "description": "About it", "site_name": "Example", "image": "https://example.com/i.png", "favicon": "https://example.com/f.ico"}})
	}))
	defer metadata.Close()
	h.services["metadata"] = &downstreamService{URL: metadata.URL, Client: metadata.Client()}
	w := postRoute(h, "/v1/metadata/link", api.Object{"url": "https://example.com/post"}, "")
	if want := `{"description":"About it","image":"https://example.com/i.png","siteName":"Example","title":"The Post","url":"https://example.com/post"}` + "\n"; w.Code != 200 || w.Body.String() != want {
		t.Fatalf("the details: HTTP %d %s", w.Code, w.Body.String())
	}
	select {
	case got := <-asked:
		if got != "Bearer free_anonymous-key /metadata https://example.com/post" {
			t.Fatalf("the metadata service was asked %s", got)
		}
	default:
		t.Fatal("the metadata service was not asked")
	}
	if w := postRoute(h, "/v1/metadata/link", api.Object{"url": " "}, ""); w.Code != 400 || !strings.Contains(w.Body.String(), "BAD_REQUEST") || len(asked) != 0 {
		t.Fatalf("a link without a URL: HTTP %d %s", w.Code, w.Body.String())
	}
}
