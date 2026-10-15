package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestCanonicalizeWidgetArgumentsUnwrapsDeepSeekShape(t *testing.T) {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(canonicalizeWidgetArguments(`{"stats":"[{\"label\":\"Human trials\",\"value\":\"20+\"}]"}`)), &parsed); err != nil {
		t.Fatal(err)
	}
	stats, _ := parsed["stats"].([]any)
	if len(stats) != 1 || !reflect.DeepEqual(stats[0], map[string]any{"label": "Human trials", "value": "20+"}) {
		t.Fatalf("stats were not unwrapped into native rows: %#v", parsed["stats"])
	}
}

func TestCanonicalizeWidgetArgumentsIdempotent(t *testing.T) {
	clean := `{"series":[{"label":"Q1","value":10},{"label":"Q2","value":15}]}`
	if once := canonicalizeWidgetArguments(clean); once != clean || canonicalizeWidgetArguments(once) != once {
		t.Fatalf("canonical arguments changed: %s", once)
	}
}

func TestCanonicalizeWidgetArgumentsLeavesNonJSONAlone(t *testing.T) {
	for _, in := range []string{"", "plain text", "{ unclosed", `"already a string"`, `{"a":1} {"b":2}`} {
		if got := canonicalizeWidgetArguments(in); got != in {
			t.Errorf("canonicalize(%q) = %q; want unchanged", in, got)
		}
	}
}

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
	out := newStream(mustRun(t, "00", "aa"), "thread", "run")
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

func TestWidgetRetriesAreCountedPerRunAndName(t *testing.T) {
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
				out := newStream(mustRun(t, "00", "aa"), "thread", "run")
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
