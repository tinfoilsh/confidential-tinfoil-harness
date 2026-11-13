package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func TestBrowserTimeoutDoesNotDispatchTheRestOfTheBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		out := &Stream{Run: NewLog("run")}
		set := &Toolset{browser: newBrowserSession(&BrowserAccess{ExecutorID: "executor", Token: "token"})}
		messages := execute(context.Background(), out, set, []ToolCall{
			{ID: "read", Name: domain.BrowserTool, Args: `{"action":"read","tabId":1}`},
			{ID: "send", Name: domain.BrowserTool, Args: `{"action":"click","tabId":1,"ref":"button"}`},
		})
		if failure := api.AsError(set.failure); failure.Code != "BROWSER_TIMEOUT" || failure.Action != api.ActionStop {
			t.Fatalf("timeout did not stop the browser run: %+v", failure)
		}
		_, frames, _, _, _, done := out.Run.Frames.Read(0)
		defer done()
		var requested []string
		for _, frame := range frames {
			var event struct {
				Type string `json:"type"`
				Call string `json:"toolCallId"`
			}
			if err := json.Unmarshal(frame, &event); err != nil {
				t.Fatal(err)
			}
			if event.Type == "BROWSER_TOOL_REQUEST" {
				requested = append(requested, event.Call)
			}
		}
		if len(requested) != 1 || requested[0] != "read" {
			t.Fatalf("dispatched after timeout: %v", requested)
		}
		if len(messages) != 2 || !strings.Contains(string(messages[1]), "not executed") {
			t.Fatalf("later call was not skipped: %s", api.Raw(messages))
		}
	})
}
