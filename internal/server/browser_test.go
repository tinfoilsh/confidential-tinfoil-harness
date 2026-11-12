package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
)

const browserTestToken = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

func browserInput() api.Object {
	in := localInput("", priorThread())
	in["browser"] = api.Object{"executorId": uuid.NewString(), "token": browserTestToken}
	return in
}

func awaitBrowser(t *testing.T, log *engine.Log, count int) domain.BrowserToolRequest {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		_, frames, _, wake, closed, done := log.Frames.Read(0)
		var requests []domain.BrowserToolRequest
		for _, raw := range frames {
			var header struct {
				Type string `json:"type"`
			}
			json.Unmarshal(raw, &header)
			if header.Type == "BROWSER_TOOL_REQUEST" {
				var request domain.BrowserToolRequest
				if err := json.Unmarshal(raw, &request); err != nil {
					done()
					t.Fatal(err)
				}
				requests = append(requests, request)
			}
		}
		done()
		if len(requests) >= count {
			return requests[count-1]
		}
		if closed != nil {
			t.Fatalf("run ended before browser action %d: %v", count, closed)
		}
		select {
		case <-wake:
		case <-deadline.C:
			t.Fatalf("no browser action %d", count)
		}
	}
}

func browserReply(run *engine.Run, request domain.BrowserToolRequest, text string) api.Object {
	return api.Object{"threadId": run.Thread.ID, "runId": run.Log.ID, "executorId": request.ExecutorID, "token": browserTestToken, "executionId": request.ExecutionID, "result": api.Object{"text": text}}
}

func TestBrowserToolsRoundTripAndReplay(t *testing.T) {
	sent := make(chan []any, 1)
	chunks := toolCallChunk(0, "read", domain.BrowserTool, `{"action":"read","tabId":1}`) + toolCallChunk(1, "click", domain.BrowserTool, `{"action":"click","tabId":1,"ref":"page-1:3"}`)
	h, rows, _ := testChatHarness(t, toolTurns(chunks, sent))
	in := browserInput()
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	run, err := h.engine.AcceptTurn(context.Background(), p, chatCredential("alice"), auth.ContentKey{}, requestOf[engine.TurnRequest](in))
	if err != nil {
		t.Fatal(err)
	}
	defer run.Log.Frames.Detach()
	first := awaitBrowser(t, run.Log, 1)
	if first.ToolCallID != "read" || first.Action.Action != "read" || first.Action.TabID != 1 || first.ExecutorID != api.Obj(in["browser"])["executorId"] {
		t.Fatalf("wrong browser request: %+v", first)
	}
	result := browserReply(run, first, "[page-1:3] button Save")
	if w := postRoute(h, "/v1/threads/browser/result", result, "bob"); w.Code != 404 {
		t.Fatalf("another account submitted a result: %d", w.Code)
	}
	result["executorId"] = uuid.NewString()
	if w := postRoute(h, "/v1/threads/browser/result", result, "alice"); w.Code != 403 {
		t.Fatalf("another executor used the correct token: %d", w.Code)
	}
	result["executorId"] = first.ExecutorID
	result["token"] = strings.Repeat("0", len(browserTestToken))
	if w := postRoute(h, "/v1/threads/browser/result", result, "alice"); w.Code != 403 {
		t.Fatalf("another executor submitted a result: %d", w.Code)
	}
	result["token"] = browserTestToken
	if w := postRoute(h, "/v1/threads/browser/result", result, "alice"); w.Code != 200 || api.Obj(api.Parsed(w.Body.Bytes()))["accepted"] != true {
		t.Fatalf("result refused: %s", w.Body)
	}
	second := awaitBrowser(t, run.Log, 2)
	if second.ToolCallID != "click" || second.Action.Ref != "page-1:3" || first.ExecutionID == second.ExecutionID {
		t.Fatalf("wrong next operation: %+v", second)
	}
	if w := postRoute(h, "/v1/threads/browser/result", result, "alice"); w.Code != 200 {
		t.Fatalf("identical replay refused: %s", w.Body)
	}
	result["result"] = api.Object{"text": "changed replay"}
	if w := postRoute(h, "/v1/threads/browser/result", result, "alice"); w.Code != 409 {
		t.Fatalf("conflicting replay accepted: %s", w.Body)
	}
	if w := postRoute(h, "/v1/threads/browser/result", browserReply(run, second, "Saved successfully"), "alice"); w.Code != 200 {
		t.Fatalf("second result refused: %s", w.Body)
	}
	awaitFrame(t, run.Log, "RUN_FINISHED")
	<-run.Finished
	messages := <-sent
	if toolMessage(messages, "read") != "[page-1:3] button Save" || toolMessage(messages, "click") != "Saved successfully" {
		t.Fatalf("model did not read the actual observations: %v", messages)
	}
	w := postRoute(h, "/v1/threads/follow", api.Object{"threadId": run.Thread.ID, "runId": run.Log.ID}, "alice")
	events := eventsIn(t, w)
	if strings.Contains(w.Body.String(), browserTestToken) || strings.Contains(string(api.Raw(messages)), browserTestToken) {
		t.Fatal("executor secret escaped into events or the prompt")
	}
	var metadata domain.RunMetadata
	if err := json.Unmarshal(events[len(events)-1].Metadata, &metadata); err != nil || metadata.Thread == nil {
		t.Fatalf("no local thread: %s", w.Body)
	}
	if rows.calls != 0 {
		t.Fatalf("browser run persisted a local chat: %d writes", rows.calls)
	}
	conformance(t, "browser", "A device chat executes two browser tools in order, returning each observation before continuing the model turn.", framesOf(t, w.Body.Bytes()), metadata.Thread)
}

func TestBrowserCancellationRejectsLateResults(t *testing.T) {
	h, _, _ := testChatHarness(t, toolTurns(toolCallChunk(0, "read", domain.BrowserTool, `{"action":"read","tabId":1}`), nil))
	run, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), auth.ContentKey{}, requestOf[engine.TurnRequest](browserInput()))
	if err != nil {
		t.Fatal(err)
	}
	defer run.Log.Frames.Detach()
	request := awaitBrowser(t, run.Log, 1)
	if w := postRoute(h, "/v1/threads/cancel", api.Object{"threadId": run.Thread.ID, "runId": run.Log.ID}, "alice"); w.Code != 200 {
		t.Fatalf("cancel refused: %s", w.Body)
	}
	awaitFrame(t, run.Log, "RUN_FINISHED")
	<-run.Finished
	if w := postRoute(h, "/v1/threads/browser/result", browserReply(run, request, "late result"), "alice"); w.Code != 404 {
		t.Fatalf("late result accepted: %s", w.Body)
	}
}

func TestBrowserAccessOnlyOnExplicitLocalTurns(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	in := sendInput(key)
	in["browser"] = browserInput()["browser"]
	if w := postRoute(h, "/v1/threads/turn", in, "alice"); w.Code != 400 {
		t.Fatalf("cloud browser run accepted: %s", w.Body)
	}
	in = browserInput()
	api.Obj(in["browser"])["token"] = "short"
	if w := postRoute(h, "/v1/threads/turn", in, "alice"); w.Code != 400 {
		t.Fatalf("weak executor token accepted: %s", w.Body)
	}
	features := api.Obj(api.Obj(api.Parsed(postRoute(h, "/v1/session", api.Object{}, "alice").Body.Bytes()))["features"])
	if features["browserTools"] != true {
		t.Fatalf("browser protocol not advertised: %v", features)
	}
}

func TestBrowserScreenshotReachesVisionWithoutEnteringHistory(t *testing.T) {
	sent := make(chan []any, 1)
	h, _, _ := testChatHarness(t, toolTurns(toolCallChunk(0, "screen", domain.BrowserTool, `{"action":"screenshot","tabId":1}`), sent))
	h.models.Catalog.Get().Definition("kimi-k3")["multimodal"] = true
	run, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), auth.ContentKey{}, requestOf[engine.TurnRequest](browserInput()))
	if err != nil {
		t.Fatal(err)
	}
	defer run.Log.Frames.Detach()
	request := awaitBrowser(t, run.Log, 1)
	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, image.NewRGBA(image.Rect(0, 0, 2, 2)), nil); err != nil {
		t.Fatal(err)
	}
	screenshot := base64.StdEncoding.EncodeToString(encoded.Bytes())
	input := browserReply(run, request, "Current viewport")
	api.Obj(input["result"])["image"] = "not-base64"
	if w := postRoute(h, "/v1/threads/browser/result", input, "alice"); w.Code != 400 {
		t.Fatalf("invalid screenshot accepted: %s", w.Body)
	}
	api.Obj(input["result"])["image"] = screenshot
	if w := postRoute(h, "/v1/threads/browser/result", input, "alice"); w.Code != 200 {
		t.Fatalf("screenshot refused: %s", w.Body)
	}
	awaitFrame(t, run.Log, "RUN_FINISHED")
	<-run.Finished
	messages := <-sent
	found := false
	for _, message := range messages {
		for _, part := range api.Arr(api.Obj(message)["content"]) {
			if api.Obj(api.Obj(part)["image_url"])["url"] == "data:image/jpeg;base64,"+screenshot {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("vision model did not receive the screenshot: %s", api.Raw(messages))
	}
	w := postRoute(h, "/v1/threads/follow", api.Object{"threadId": run.Thread.ID, "runId": run.Log.ID}, "alice")
	if w.Code != 200 || strings.Contains(w.Body.String(), screenshot) || !strings.Contains(w.Body.String(), "Current viewport") {
		t.Fatalf("screenshot history was not text-only: %s", w.Body)
	}
}

func TestBrowserActionBudgetStopsAtForty(t *testing.T) {
	const limit = 40
	var chunks strings.Builder
	for index := range limit + 1 {
		chunks.WriteString(toolCallChunk(index, fmt.Sprintf("read-%d", index), domain.BrowserTool, `{"action":"read","tabId":1}`))
	}
	sent := make(chan []any, 1)
	h, _, _ := testChatHarness(t, toolTurns(chunks.String(), sent))
	run, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), auth.ContentKey{}, requestOf[engine.TurnRequest](browserInput()))
	if err != nil {
		t.Fatal(err)
	}
	defer run.Log.Frames.Detach()
	for index := range limit {
		request := awaitBrowser(t, run.Log, index+1)
		if request.ToolCallID != fmt.Sprintf("read-%d", index) {
			t.Fatalf("wrong action at %d: %+v", index, request)
		}
		if w := postRoute(h, "/v1/threads/browser/result", browserReply(run, request, "Page observed"), "alice"); w.Code != 200 {
			t.Fatalf("action %d refused: %s", index, w.Body)
		}
	}
	awaitFrame(t, run.Log, "RUN_FINISHED")
	<-run.Finished
	messages := <-sent
	if toolMessage(messages, "read-39") != "Page observed" || !strings.Contains(toolMessage(messages, "read-40"), "budget is exhausted") {
		t.Fatalf("wrong action boundary: %s", api.Raw(messages))
	}
}

func TestBrowserParameterSchemaRejectsInvalidActionsBeforeDispatch(t *testing.T) {
	sent := make(chan []any, 1)
	chunks := toolCallChunk(0, "bad-key", domain.BrowserTool, `{"action":"press","tabId":1,"ref":"input","key":"Foo"}`) + toolCallChunk(1, "no-ref", domain.BrowserTool, `{"action":"fill","tabId":1,"text":"example"}`)
	h, _, _ := testChatHarness(t, toolTurns(chunks, sent))
	w := postRoute(h, "/v1/threads/turn", browserInput(), "alice")
	events := eventsIn(t, w)
	if eventOfType(events, "BROWSER_TOOL_REQUEST") != nil {
		t.Fatalf("dispatched invalid arguments: %s", w.Body)
	}
	messages := <-sent
	for _, call := range []string{"bad-key", "no-ref"} {
		if !strings.Contains(toolMessage(messages, call), "parameter schema") {
			t.Fatalf("the model was not told to correct %s: %s", call, api.Raw(messages))
		}
	}
}
