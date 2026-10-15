package main

import (
	"strings"
	"testing"
)

func TestFormatSearchOutput(t *testing.T) {
	got := formatSearchOutput([]any{
		map[string]any{"title": "Climate Article", "url": "https://example.com/climate", "content": "Global temps are rising.", "published_date": "2026-04-17"},
		map[string]any{"title": " ", "url": "https://example.com/tech", "content": "AI is advancing rapidly."},
	})
	want := "Source: Climate Article\nURL: https://example.com/climate\nPublished: 2026-04-17\nGlobal temps are rising.\n\nSource: Search result\nURL: https://example.com/tech\nAI is advancing rapidly."
	if got != want {
		t.Fatalf("formatted search output:\n%s\nwant:\n%s", got, want)
	}
	if got := formatSearchOutput([]any{}); got != "No safe search results were found." {
		t.Errorf("unexpected empty output = %q", got)
	}
}

func TestFormatFetchOutput(t *testing.T) {
	if got := formatFetchOutput([]any{map[string]any{"url": "https://example.com/page", "content": "Page body text."}}); got != "Source: Fetched page\nURL: https://example.com/page\nPage body text." {
		t.Errorf("formatted fetch output %q", got)
	}
	if got := formatFetchOutput(nil); got != "" {
		t.Errorf("nothing fetched formatted as %q", got)
	}
}

func TestFormatFetchFailures(t *testing.T) {
	got := formatFetchFailures([]any{
		map[string]any{"url": "https://ok.test", "status": "completed"},
		map[string]any{"url": "https://a.test", "status": "failed", "error": "timeout"},
		map[string]any{"url": "https://b.test", "status": "failed"},
		map[string]any{"status": "failed", "error": "blocked"},
	})
	if want := "Fetch failed for https://a.test: timeout\nFetch failed for https://b.test.\nFetch failed: blocked"; got != want {
		t.Fatalf("formatted failures %q, want %q", got, want)
	}
}

func TestModelReadsSearchResultsAsCitedText(t *testing.T) {
	search := `{"results":[{"title":"A","url":"https://a.test","content":"alpha"}],"pii_checked":false,"pii_filter_requests":0}`
	for _, tc := range []struct{ name, content, want string }{
		{"web_search", search, "Source: A\nURL: https://a.test\nalpha"},
		{"web_fetch", `{"pages":[{"url":"https://a.test","content":"alpha"}],"results":[{"url":"https://a.test","status":"completed"}]}`, "Source: Fetched page\nURL: https://a.test\nalpha"},
		{"web_fetch", `{"results":[{"url":"https://a.test","status":"failed","error":"timeout"}]}`, "Fetch failed for https://a.test: timeout"},
		{"web_fetch", `{"results":[]}`, `{"results":[]}`},
		{"web_search", "plain text", "plain text"},
		{"render_chart", search, search},
	} {
		if got := modelCopy(tc.name, tc.content); got != tc.want {
			t.Errorf("%s read %q as %q, want %q", tc.name, tc.content, got, tc.want)
		}
	}
}

func TestSearchPromptIsTheRoutersWithHarnessToolNames(t *testing.T) {
	for _, part := range []string{"Use web_search first to discover sources, then web_fetch specific URLs", citationInstructions, toolEconomyInstructions, toolOutputWarning} {
		if !strings.Contains(searchInstructions, part) {
			t.Fatalf("the search prompt lacks %q", part)
		}
	}
	if families[0].prompt != searchInstructions {
		t.Fatal("the webSearch family does not use the search prompt")
	}
}
