package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestStripModelControlTokens_LegitimatePipesSurvive(t *testing.T) {
	in := "https://example.com/?x=|y|&z=a"
	if got := stripModelControlTokens(in); got != in {
		t.Errorf("legit pipes got stripped: got=%q", got)
	}
}

func TestStripModelControlTokens_LongSequenceIsNotMatched(t *testing.T) {
	in := "<|" + strings.Repeat("x", 80) + "|>"
	if got := stripModelControlTokens(in); got != in {
		t.Errorf("overlong token was eaten: got=%q", got)
	}
}

func TestDeepStripURLTokens_AllowlistedField(t *testing.T) {
	args := map[string]any{"url": `<|"|https://local.test/a<|"|>`, "max_results": float64(5)}
	if !deepStripURLTokens(args) {
		t.Fatal("deepStripURLTokens reported no change")
	}
	if args["url"] != "https://local.test/a" || args["max_results"] != float64(5) {
		t.Errorf("wrong repair: %v", args)
	}
}

func TestDeepStripURLTokens_URLsArray(t *testing.T) {
	args := map[string]any{"urls": []any{`<|"|https://a<|"|>`, "https://b", `<|tool_call_argument_begin|>https://c<|tool_call_end|>`}}
	if !deepStripURLTokens(args) {
		t.Fatal("array-valued url field not cleaned")
	}
	if want := []any{"https://a", "https://b", "https://c"}; !reflect.DeepEqual(args["urls"], want) {
		t.Errorf("urls array mismatch: got=%v want=%v", args["urls"], want)
	}
}

func TestDeepStripURLTokens_NestedObject(t *testing.T) {
	args := map[string]any{"page": map[string]any{"href": `<|"|https://deep.test/x<|"|>`, "id": float64(3)}}
	if !deepStripURLTokens(args) {
		t.Fatal("nested href not cleaned")
	}
	if got := args["page"].(map[string]any)["href"]; got != "https://deep.test/x" {
		t.Errorf("nested href: got=%q", got)
	}
}

func TestDeepStripURLTokens_NonURLFieldLeftAlone(t *testing.T) {
	args := map[string]any{"query": "find <|maybe|> a restaurant"}
	if deepStripURLTokens(args) || args["query"] != "find <|maybe|> a restaurant" {
		t.Fatalf("non-URL field was stripped: %v", args)
	}
}

func TestDeepStripURLTokens_LeakedURLInUnknownField(t *testing.T) {
	args := map[string]any{"target": `<|"|https://local.test/almanac<|"|>`}
	if !deepStripURLTokens(args) || args["target"] != "https://local.test/almanac" {
		t.Fatalf("unknown field with leaked URL was not cleaned: %v", args)
	}
}

func TestExtractFirstJSONObject(t *testing.T) {
	for in, want := range map[string]string{
		`{"action":"read","path":"/etc/hostname"}Let me read this file...`: `{"action":"read","path":"/etc/hostname"}`,
		`{"a":{"b":2}} Then I'll ...`:                                      `{"a":{"b":2}}`,
		`{"cmd":"echo }"}`:                                                 `{"cmd":"echo }"}`,
		`{"cmd":"she said \"hi\" then }"}`:                                 `{"cmd":"she said \"hi\" then }"}`,
		`{"cmd":"a \\"}trailing`:                                           `{"cmd":"a \\"}`,
		`{"cmd":"unterminated`:                                             "",
		`[1,2,3]`:                                                          "",
		``:                                                                 "",
	} {
		if got, ok := extractFirstJSONObject(in); got != want || ok != (want != "") {
			t.Errorf("extractFirstJSONObject(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
}

// The router's history sanitiser is historyArgs here, applied wherever the
// harness builds history: requested, renderMessage and convert.
func TestHistoryArgs(t *testing.T) {
	for in, want := range map[string]string{
		"":                     "{}",
		`{"query":"cats"}`:     `{"query":"cats"}`,
		`{"cmd":"unterminated`: "{}",
		`{"a"}trailing`:        "{}",
		`not json at all`:      "{}",
		`{"query":"cats","max_results":5} Now searching...`: `{"query":"cats","max_results":5}`,
	} {
		if got := historyArgs(in); got != want {
			t.Errorf("historyArgs(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestContinuationReplaysRepairedArguments(t *testing.T) {
	got := string(requested("", []toolCall{{id: "c1", name: "render_clock"}, {id: "c2", name: "web_search", args: `{"query":"cats"} Now searching...`}}))
	if want := `{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"render_clock","arguments":"{}"}},{"id":"c2","type":"function","function":{"name":"web_search","arguments":"{\"query\":\"cats\"}"}}]}`; got != want {
		t.Fatalf("the continuation carried %s", got)
	}
}

func TestStoredToolCallsReplayWithRepairedArguments(t *testing.T) {
	message := object{"role": "assistant", "timeline": []any{
		object{"type": "tool_call", "toolCallId": "call_1", "name": "web_search", "arguments": `{"query":"cats"}`},
		object{"type": "tool_call", "toolCallId": "call_2", "name": "web_search", "arguments": `{"query":"`},
		object{"type": "tool_call", "toolCallId": "call_3", "name": "web_fetch", "arguments": `{"urls":["https://a.test"]} Now fetching`},
		object{"type": "tool_call", "toolCallId": "call_4", "name": "web_search"},
	}}
	var assistant struct {
		ToolCalls []toolCallJSON `json:"tool_calls"`
	}
	json.Unmarshal(renderMessage(message, false, "none")[0], &assistant)
	var got []string
	for _, call := range assistant.ToolCalls {
		got = append(got, call.Function.Arguments)
	}
	if want := []string{`{"query":"cats"}`, "{}", `{"urls":["https://a.test"]}`, "{}"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed arguments %q, want %q", got, want)
	}
}
