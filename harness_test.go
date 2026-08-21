package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/tinfoilsh/confidential-tinfoil-harness/tools"
)

// gateway streams the given SSE payloads as a model enclave would.
func gateway(t *testing.T, chunks ...string) (*harness, *http.Client) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer caller-key" {
			t.Errorf("turn did not bill the caller: Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, chunk := range chunks {
			w.Write([]byte("data: " + chunk + "\n\n"))
		}
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	t.Cleanup(server.Close)
	return &harness{gateway: server.URL}, server.Client()
}

func stream(t *testing.T) (*sse, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	out, err := newSSE(recorder, "kimi-k3")
	if err != nil {
		t.Fatal(err)
	}
	return out, recorder
}

type query struct {
	Query string `json:"query"`
}

// toolEnclave stands in for the tools enclave: two exposed names plus one that
// is not, with progress and _meta sources.
func toolEnclave(t *testing.T) *tools.Web {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "search", Description: "search the web"},
		func(ctx context.Context, req *mcp.CallToolRequest, in query) (*mcp.CallToolResult, any, error) {
			if in.Query != "tinfoil" {
				t.Errorf("tool got query %q", in.Query)
			}
			req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
				ProgressToken: req.Params.GetProgressToken(),
				Message:       "reading results",
				Progress:      1,
				Total:         2,
			})
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "a page about tinfoil"}},
				Meta:    mcp.Meta{"sources": []any{map[string]any{"url": "https://example.com/a", "title": "A"}}},
			}, nil, nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "fetch", Description: "open a page"},
		func(ctx context.Context, req *mcp.CallToolRequest, in query) (*mcp.CallToolResult, any, error) {
			return nil, nil, errors.New("page is gone")
		})
	mcp.AddTool(server, &mcp.Tool{Name: "shell", Description: "not exposed"},
		func(ctx context.Context, req *mcp.CallToolRequest, in query) (*mcp.CallToolResult, any, error) {
			t.Error("the harness called a tool it does not expose")
			return nil, nil, nil
		})

	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)
	return tools.NewWeb(httpServer.URL, httpServer.Client())
}

// local is an in-process Source: the loop must not tell it apart from the enclave.
type local struct{}

func (local) Open(ctx context.Context, progress tools.Progress) ([]tools.Tool, func(), error) {
	return []tools.Tool{{
		Name:        "echo",
		Description: "echo the argument back",
		Schema:      json.RawMessage(`{"type":"object","properties":{"say":{"type":"string"}}}`),
		Call: func(ctx context.Context, token string, args json.RawMessage) (tools.Result, error) {
			progress(token, json.RawMessage(`{"message":"thinking locally"}`))
			return tools.Result{Output: "echoed " + string(args), Meta: json.RawMessage(`{"local":true}`)}, nil
		},
	}}, func() {}, nil
}

func openSet(t *testing.T, progress tools.Progress, sources ...tools.Source) *tools.Set {
	t.Helper()
	if progress == nil {
		progress = func(string, json.RawMessage) {}
	}
	set, err := tools.Open(context.Background(), progress, sources...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(set.Close)
	return set
}

func TestTurnForwardsContentAndKeepsToolCalls(t *testing.T) {
	h, client := gateway(t,
		`{"id":"upstream-1","choices":[{"delta":{"content":"looking"},"finish_reason":null}],"extra":"kept"}`,
		`{"id":"upstream-1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"web_search","arguments":"{\"query\":"}}]}}]}`,
		`{"id":"upstream-1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"tinfoil\"}"}}]}}]}`,
		`{"id":"upstream-1","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		`{"id":"upstream-1","choices":[],"usage":{"total_tokens":9}}`,
	)
	out, recorder := stream(t)

	calls, err := h.turn(context.Background(), out, client, []byte(`{}`), "caller-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].name != "web_search" || calls[0].args != `{"query":"tinfoil"}` {
		t.Fatalf("tool call not assembled from deltas: %+v", calls)
	}
	body := recorder.Body.String()
	for _, leaked := range []string{"tool_calls", "usage", "upstream-1"} {
		if strings.Contains(body, leaked) {
			t.Errorf("caller saw %q, which belongs to the tool turn:\n%s", leaked, body)
		}
	}
	if !strings.Contains(body, `"content":"looking"`) || !strings.Contains(body, `"extra":"kept"`) {
		t.Errorf("caller did not get the enclave's chunk intact:\n%s", body)
	}
	if !strings.Contains(body, out.id) {
		t.Errorf("chunk was not re-identified as this stream:\n%s", body)
	}
}

func TestTurnPassesTheAnsweringTurnThrough(t *testing.T) {
	h, client := gateway(t,
		`{"id":"upstream-2","choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}`,
		`{"id":"upstream-2","choices":[],"usage":{"total_tokens":4}}`,
	)
	out, recorder := stream(t)

	calls, err := h.turn(context.Background(), out, client, []byte(`{}`), "caller-key")
	if err != nil || len(calls) != 0 {
		t.Fatalf("calls=%v err=%v", calls, err)
	}
	if !strings.Contains(recorder.Body.String(), `"usage"`) {
		t.Errorf("the answering turn's usage should reach the caller:\n%s", recorder.Body.String())
	}
}

func TestUpstreamBodyKeepsCallerFieldsAndScopesTheCache(t *testing.T) {
	req, err := parseRequest([]byte(`{"stream":true,"temperature":0.2,"messages":[{"role":"user","content":"hi"}]}`), "caller-key")
	if err != nil {
		t.Fatal(err)
	}
	body, err := req.upstream("kimi-k3", req.messages, json.RawMessage(`[{"type":"function"}]`))
	if err != nil {
		t.Fatal(err)
	}
	var sent map[string]json.RawMessage
	if err := json.Unmarshal(body, &sent); err != nil {
		t.Fatal(err)
	}
	if string(sent["temperature"]) != "0.2" || string(sent["model"]) != `"kimi-k3"` || string(sent["stream"]) != "true" {
		t.Errorf("upstream body lost or mangled a field: %s", body)
	}
	if strings.Contains(string(sent["user_cache_secret"]), "caller-key") {
		t.Errorf("the cache namespace must not be the caller's key: %s", sent["user_cache_secret"])
	}
	if string(sent["user_cache_secret"]) != string(quote(cacheScope("caller-key"))) {
		t.Errorf("cache namespace is not stable for one caller: %s", sent["user_cache_secret"])
	}
}

func TestRequestsThisEndpointRefuses(t *testing.T) {
	for name, body := range map[string]string{
		"not streaming":   `{"messages":[{"role":"user"}]}`,
		"no messages":     `{"stream":true,"messages":[]}`,
		"caller tools":    `{"stream":true,"messages":[{"role":"user"}],"tools":[]}`,
		"several choices": `{"stream":true,"messages":[{"role":"user"}],"n":2}`,
	} {
		if _, err := parseRequest([]byte(body), "caller-key"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestAutoSelectionReadsTheRequest(t *testing.T) {
	text := &request{messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"hi"}`)}}
	if m, err := pick(text); err != nil || m.name != catalog[0].name {
		t.Errorf("plain text should take the preferred model: %v %v", m, err)
	}
	image := &request{messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":[{"type":"image_url"}]}`)}}
	m, err := pick(image)
	if err != nil || !m.vision {
		t.Errorf("a prompt with an image needs a model that can see: %v %v", m, err)
	}
	if _, err := pick(&request{model: "gpt-4o"}); err == nil {
		t.Error("a model this harness has not pinned must be refused")
	}
}

func TestDefinitionsSpanEverySource(t *testing.T) {
	set := openSet(t, nil, toolEnclave(t), local{})
	var offered []struct {
		Function struct {
			Name       string          `json:"name"`
			Parameters json.RawMessage `json:"parameters"`
		} `json:"function"`
	}
	if err := json.Unmarshal(set.Defs, &offered); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, def := range offered {
		got[def.Function.Name] = true
		if !strings.Contains(string(def.Function.Parameters), "properties") {
			t.Errorf("%s was offered without the schema its source published: %s", def.Function.Name, def.Function.Parameters)
		}
	}
	if len(got) != 3 || !got["web_search"] || !got["web_fetch"] || !got["echo"] {
		t.Errorf("the model should be offered every source's tools and nothing else, got %v", got)
	}
}

func TestTwoSourcesCannotOfferTheSameName(t *testing.T) {
	if _, err := tools.Open(context.Background(), nil, local{}, local{}); err == nil {
		t.Error("a name two sources both offer must fail at assembly, not at call time")
	}
}

func TestExecuteRelaysProgressAndMetaFromAnySource(t *testing.T) {
	out, recorder := stream(t)
	set := openSet(t, func(token string, detail json.RawMessage) {
		out.event(event{ID: token, Status: "progress", Progress: detail})
	}, toolEnclave(t), local{})

	messages := execute(context.Background(), out, set, []toolCall{
		{id: "call_1", name: "web_search", args: `{"query":"tinfoil"}`},
		{id: "call_2", name: "web_fetch", args: `{"query":"x"}`},
		{id: "call_3", name: "invented", args: `{}`},
		{id: "call_4", name: "echo", args: `{"say":"hi"}`},
	})

	if len(messages) != 4 {
		t.Fatalf("every call owes the model an answer: %s", messages)
	}
	if !strings.Contains(string(messages[0]), "a page about tinfoil") {
		t.Errorf("remote tool output did not reach the model: %s", messages[0])
	}
	if !strings.Contains(string(messages[3]), `echoed {`) {
		t.Errorf("local tool output did not reach the model: %s", messages[3])
	}
	for _, refused := range []struct {
		message json.RawMessage
		says    string
	}{{messages[1], "page is gone"}, {messages[2], "no such tool"}} {
		if !strings.Contains(string(refused.message), refused.says) {
			t.Errorf("the model should be told %q: %s", refused.says, refused.message)
		}
	}

	var relayed []event
	for _, frame := range strings.Split(recorder.Body.String(), "data: ") {
		var chunk struct {
			Choices []any  `json:"choices"`
			Event   *event `json:"tinfoil"`
		}
		if json.Unmarshal([]byte(strings.TrimSpace(frame)), &chunk) != nil || chunk.Event == nil {
			continue
		}
		if len(chunk.Choices) != 0 {
			t.Errorf("an event must not occupy a choice: %s", frame)
		}
		relayed = append(relayed, *chunk.Event)
	}

	// Progress correlates the same way whoever ran the tool.
	counts, meta, calls, progress := map[string]int{}, map[string]string{}, map[string]string{}, map[string]string{}
	for _, e := range relayed {
		counts[e.Status]++
		switch e.Status {
		case "in_progress":
			calls[e.ID] = e.Tool
		case "completed":
			meta[e.Tool] = string(e.Meta)
		case "progress":
			progress[calls[e.ID]] = string(e.Progress)
		}
	}
	if counts["in_progress"] != 4 || counts["completed"] != 2 || counts["failed"] != 2 {
		t.Errorf("events do not report what happened: %v", counts)
	}
	if !strings.Contains(meta["web_search"], "example.com") || meta["echo"] != `{"local":true}` {
		t.Errorf("a source's meta was not relayed whole: %v", meta)
	}
	if !strings.Contains(progress["web_search"], "reading results") {
		t.Errorf("remote progress was not relayed against its call: %v", progress)
	}
	if !strings.Contains(progress["echo"], "thinking locally") {
		t.Errorf("local progress was not relayed against its call: %v", progress)
	}
	if strings.Contains(recorder.Body.String(), "tinfoil-event") {
		t.Error("events must not ride inside the content stream")
	}
}
