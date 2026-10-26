package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A web search or page fetch shows its query or page, its status and its
// sources, kept current as it runs and stored with it.
func TestSearchesShowWhatTheyFoundAsTheyRun(t *testing.T) {
	serveWebsearch(t, func(name string, _ map[string]any) (*mcp.CallToolResult, any) {
		if name == "fetch" {
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "timeout"}}}, nil
		}
		return nil, api.Object{"results": []api.Object{{"title": "A", "url": "https://a.test/1", "content": "alpha", "author": "Ann"}}}
	})
	h, rows, key := testChatHarness(t, toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"cats"}`)+toolCallChunk(1, "f", "web_fetch", `{"urls":["https://b.test"]}`), nil))
	events := finishedRun(t, h, "/v1/threads/turn", searchInput(key))
	displays := map[string][]domain.ToolDisplay{}
	for _, e := range events {
		id := e.ToolCallID
		if e.Type == "ACTIVITY_SNAPSHOT" {
			id = e.MessageID[len("act_"):]
		}
		if e.Display != nil {
			displays[id] = append(displays[id], *e.Display)
		}
	}
	searched := domain.ToolDisplay{Query: "cats", Status: "completed", Sources: []domain.DisplaySource{{URL: "https://a.test/1", Title: "A", Snippet: "alpha"}}}
	want := map[string][]domain.ToolDisplay{
		"s": {{Query: "cats", Status: "running", Sources: []domain.DisplaySource{}}, searched},
		"f": {{URL: "https://b.test", Status: "running", Sources: []domain.DisplaySource{}}, {URL: "https://b.test", Status: "failed", Sources: []domain.DisplaySource{}}},
	}
	if !reflect.DeepEqual(displays, want) {
		t.Fatalf("the run showed %+v", displays)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	stored := mustRead(domain.ReadThreadRow(row))
	if blocks := stored.Messages[1].Timeline; len(blocks) < 2 || !reflect.DeepEqual(blocks[0].Display, &searched) || blocks[1].Display.Status != "failed" {
		t.Fatalf("the reply stored %+v", blocks)
	}
}
