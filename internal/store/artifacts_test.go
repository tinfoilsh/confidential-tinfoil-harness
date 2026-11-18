package store

import (
	"context"
	"fmt"
	"testing"
)

// The artifacts list pages through every chat: each page reads whole pages
// of chats until it holds enough artifacts, and its cursor picks up where
// it stopped, so no chat is skipped or listed twice.
func TestArtifactPagesCoverEveryChatOnce(t *testing.T) {
	h := newRowsHarness(t)
	ctx := context.Background()
	const chats = maxListPage + 20
	for i := range chats {
		id := fmt.Sprintf("c%03d", i)
		h.push(t, "chat", id, fmt.Sprintf(`{"id":%q,"title":"Chat","titleState":"manual","model":"auto","presetId":null,"pinned":false,"webSearchEnabled":false,"recordVersion":2,"messages":[`+
			`{"id":"a","role":"assistant","createdAt":"2026-03-01T00:00:00Z","content":"","timeline":[{"id":"b","type":"tool_call","toolCallId":%q,"name":"render_artifact_preview","arguments":"{\"source\":{\"type\":\"markdown\",\"markdown\":\"x\"}}"}]}]}`, id, "draw-"+id))
	}
	listed, pages := map[string]int{}, 0
	for cursor := ""; ; pages++ {
		artifacts, next, err := h.store.Artifacts(ctx, h.p, h.key, "", cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, artifact := range artifacts {
			listed[artifact.ToolCallID]++
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(listed) != chats || pages != 1 {
		t.Fatalf("listed %d of %d chats' artifacts over %d pages after the first", len(listed), chats, pages)
	}
	for call, times := range listed {
		if times != 1 {
			t.Errorf("%s was listed %d times", call, times)
		}
	}
}
