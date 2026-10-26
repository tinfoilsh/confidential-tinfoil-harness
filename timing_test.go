package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

func TestAReplyRecordsWhenItsStepsRan(t *testing.T) {
	at := func(offset string) string { return "2026-10-01T00:00:" + offset + "Z" }
	const start = 1790812800000.0
	assembler := newMessageAssembler()
	for _, e := range []struct {
		event runEvent
		at    string
	}{
		{reasoningChunk{MessageID: "r-reasoning", Delta: "Plan"}, at("00")},
		{reasoningChunk{MessageID: "r-reasoning", Delta: "ning"}, at("02.5")},
		{textChunk{MessageID: "r", Delta: "Answer"}, at("03")},
		{reasoningChunk{MessageID: "r-reasoning", Delta: " late"}, at("03.2")},
		{textChunk{MessageID: "r", Delta: " done"}, at("03.4")},
		{toolCallStart{ToolCallID: "s", ToolCallName: "web_search", ParentMessageID: "r"}, at("04")},
		{toolCallResult{ToolCallID: "s", Content: api.Raw(api.Object{"results": []any{}})}, at("06")},
		{reasoningChunk{MessageID: "r-reasoning", Delta: "Next"}, at("07")},
		{reasoningChunk{MessageID: "r-reasoning", Delta: " step"}, at("08.25")},
	} {
		assembler.apply(e.event, e.at)
	}
	want := []Block{
		{ID: "r_thinking_0", Type: "thinking", Content: "Planning late", StartedAt: start, EndedAt: start + 2500, Duration: 2.5},
		{ID: "r_content_1", Type: "content", Content: "Answer done"},
		{ID: "s", Type: "tool_call", ToolCallID: "s", Name: "web_search", Progress: []json.RawMessage{}, Result: api.Raw(api.Object{"results": []any{}}), StartedAt: start + 4000, EndedAt: start + 6000},
		{ID: "r_thinking_3", Type: "thinking", Content: "Next step", StartedAt: start + 7000, EndedAt: start + 8250, Duration: 1.25},
	}
	got := assembler.messages()
	if len(got) != 1 || !reflect.DeepEqual(api.Parsed(api.Raw(got[0].Timeline)), api.Parsed(api.Raw(want))) {
		t.Fatalf("timeline %s, want %s", api.Raw(got), api.Raw(want))
	}
}

func TestOldRepliesKeepWhenTheirStepsRan(t *testing.T) {
	thread := mustRead(readThread("chat", api.Raw(api.Object{"title": "Trip", "messages": []any{
		api.Object{"id": "u", "role": "user", "content": "plan"},
		api.Object{"id": "a", "role": "assistant", "content": "Go", "timeline": []any{
			api.Object{"type": "thinking", "id": "t", "content": "Plan", "isThinking": false, "duration": 2.5, "startedAt": 1000, "endedAt": 3500},
			api.Object{"type": "web_search", "id": "w", "state": api.Object{"query": "trip", "sources": []any{}}, "startedAt": 4000, "endedAt": 9000},
			api.Object{"type": "url_fetches", "id": "f", "startedAt": 9000, "endedAt": 15000, "fetches": []any{
				api.Object{"id": "f1", "url": "https://a.test", "startedAt": 9000.5, "endedAt": 12000.25},
				api.Object{"id": "f2", "url": "https://b.test"},
			}},
			api.Object{"type": "content", "id": "c", "content": "Go"},
		}},
		api.Object{"id": "b", "role": "assistant", "content": "Sure", "thoughts": "Think", "thinkingDuration": 1.25},
	}})))
	type step struct {
		ID                           string
		StartedAt, EndedAt, Duration float64
	}
	var got []step
	for _, m := range thread.Messages {
		for _, b := range m.Timeline {
			got = append(got, step{b.ID, b.StartedAt, b.EndedAt, b.Duration})
		}
	}
	want := []step{{"t", 1000, 3500, 2.5}, {"w", 4000, 9000, 0}, {"f1", 9000.5, 12000.25, 0}, {"f2", 9000, 15000, 0}, {"c", 0, 0, 0}, {"b-thinking", 0, 0, 1.25}, {"b-content", 0, 0, 0}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("steps %+v, want %+v", got, want)
	}
}

func TestALateReasoningTailIsNotSummarized(t *testing.T) {
	assembler := newMessageAssembler()
	summaries := &thoughtSummaries{out: &stream{}, assembler: assembler, summarize: func(ctx context.Context, _ string) (string, error) { return "", ctx.Err() }}
	for _, e := range []runEvent{
		reasoningChunk{MessageID: "r-reasoning", Delta: "plan"},
		textChunk{MessageID: "r", Delta: strings.Repeat("answer ", minSummaryWords)},
		reasoningChunk{MessageID: "r-reasoning", Delta: " tail"},
	} {
		assembler.apply(e, "")
		summaries.follow(e)
	}
	if summaries.block != "" || summaries.trace != "" || summaries.inFlight {
		t.Fatalf("followed block %q with trace %q", summaries.block, summaries.trace)
	}
}
