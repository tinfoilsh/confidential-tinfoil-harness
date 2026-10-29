package inference

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

// testSummarizer summarizes through a model that answers with reply, and
// records each request it receives.
func testSummarizer(t *testing.T, reply string) (Summarizer, *[]map[string]any) {
	t.Helper()
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		requests = append(requests, body)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "c", "object": "chat.completion", "model": body["model"],
			"choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": reply}}},
		})
	}))
	t.Cleanup(server.Close)
	client := openai.NewClient(option.WithBaseURL(server.URL), option.WithAPIKey("k"), option.WithMaxRetries(0))
	return &modelSummarizer{model: "summary-model", completions: &client.Chat.Completions}, &requests
}

func TestASummaryAsksTheModelInItsStyle(t *testing.T) {
	summarizer, requests := testSummarizer(t, "  Weighing options \n")
	summary, err := summarizer.Summarize(context.Background(), ThoughtsSummary, "the reasoning so far")
	if err != nil || summary != "Weighing options" {
		t.Fatalf("summary %q, error %v", summary, err)
	}
	if _, err := summarizer.Summarize(context.Background(), TitleSummary, "what is a cat?"); err != nil {
		t.Fatal(err)
	}
	thoughts, title := (*requests)[0], (*requests)[1]
	messages, _ := thoughts["messages"].([]any)
	system, _ := messages[0].(map[string]any)
	user, _ := messages[1].(map[string]any)
	if thoughts["model"] != "summary-model" || thoughts["temperature"] != 0.2 || thoughts["reasoning_effort"] != "low" || thoughts["max_completion_tokens"] != float64(1024) {
		t.Fatalf("thoughts request %v", thoughts)
	}
	if prompt, _ := system["content"].(string); !strings.HasPrefix(prompt, "You produce a brief status label") || !strings.HasSuffix(prompt, "Use between 4 and 12 words.") {
		t.Fatalf("thoughts prompt %q", prompt)
	}
	if user["content"] != "the reasoning so far" {
		t.Fatalf("thoughts text %v", user["content"])
	}
	titleMessages, _ := title["messages"].([]any)
	titleSystem, _ := titleMessages[0].(map[string]any)
	if prompt, _ := titleSystem["content"].(string); !strings.HasSuffix(prompt, "Use between 2 and 5 words.") {
		t.Fatalf("title prompt %q", prompt)
	}
	if _, set := title["temperature"]; set {
		t.Fatalf("a title set a temperature: %v", title)
	}
	// Summaries for different callers share the harness's account, so no
	// two share a prompt cache.
	first, _ := thoughts["user_cache_secret"].(string)
	second, _ := title["user_cache_secret"].(string)
	if first == "" || first == second {
		t.Fatalf("cache secrets %q and %q", first, second)
	}
}

func TestAnEmptySummaryFails(t *testing.T) {
	summarizer, _ := testSummarizer(t, "  ")
	if summary, err := summarizer.Summarize(context.Background(), TitleSummary, "hello"); err == nil {
		t.Fatalf("an empty summary passed as %q", summary)
	}
}

func TestAStyleThisHarnessDoesNotKnowFails(t *testing.T) {
	summarizer, requests := testSummarizer(t, "Title")
	if _, err := summarizer.Summarize(context.Background(), SummaryStyle("poem"), "hello"); err == nil || len(*requests) != 0 {
		t.Fatalf("an unknown style asked the model: %v, %d requests", err, len(*requests))
	}
}
