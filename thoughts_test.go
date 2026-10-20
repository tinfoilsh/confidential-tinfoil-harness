package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// reasoningChunk is one streamed chunk of a reply's reasoning.
func reasoningChunk(text string) string {
	return "data: " + string(raw(object{"choices": []object{{"delta": object{"reasoning_content": text}}}})) + "\n\n"
}

// While a reply thinks, the harness summarizes its reasoning and sends the
// summary for the thinking block, which the chat does not store; a
// summarizer that fails leaves the run as it was.
func TestAThinkingReplysReasoningIsSummarized(t *testing.T) {
	for _, failing := range []bool{false, true} {
		trace := strings.Repeat("considering the options carefully ", 7)
		release := make(chan struct{})
		h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, reasoningChunk(trace))
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			// Within the interval, so it asks for no second summary.
			io.WriteString(w, reasoningChunk(" and some more thoughts"))
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\ndata: [DONE]\n\n")
		})
		var mu sync.Mutex
		var asked []string
		summarized := make(chan struct{}, 1)
		summarizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var in struct{ Content, Style string }
			json.NewDecoder(r.Body).Decode(&in)
			if in.Style != thoughtsStyle {
				json.NewEncoder(w).Encode(object{"summary": "Title"})
				return
			}
			mu.Lock()
			asked = append(asked, in.Content)
			mu.Unlock()
			defer func() {
				select {
				case summarized <- struct{}{}:
				default:
				}
			}()
			if failing {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			json.NewEncoder(w).Encode(object{"summary": " Weighing options "})
		}))
		t.Cleanup(summarizer.Close)
		h.services["summarizer"] = &downstreamService{URL: summarizer.URL, Client: summarizer.Client()}
		run, err := h.acceptTurn(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, key, requestOf[turnRequest](sendInput(key)))
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-summarized:
		case <-time.After(5 * time.Second):
			t.Fatalf("failing %v: the reasoning was never summarized", failing)
		}
		if !failing {
			awaitFrame(t, run.log, "REASONING_SUMMARY")
		}
		close(release)
		<-run.finished
		frames, _, _ := run.log.log.read(0)
		var summaries []event
		var last event
		for _, frame := range frames {
			json.Unmarshal(frame, &last)
			if last.Type == "REASONING_SUMMARY" {
				summaries = append(summaries, last)
			}
		}
		chat := storedChat(t, rows, key, run.thread.id)
		reply := chat.Messages[1]
		mu.Lock()
		calls := asked
		mu.Unlock()
		if last.Type != "RUN_FINISHED" || len(calls) != 1 || calls[0] != trace {
			t.Fatalf("failing %v: the run ended %s after summarizing %q", failing, last.Type, calls)
		}
		if failing {
			if len(summaries) != 0 {
				t.Fatalf("a failed summary was sent: %+v", summaries)
			}
			continue
		}
		if len(summaries) != 1 || summaries[0].MessageID != reply.ID || summaries[0].BlockID != reply.ID+"_thinking_0" || string(summaries[0].Content) != `"Weighing options"` {
			t.Fatalf("the run sent summaries %+v for reply %s", summaries, reply.ID)
		}
		if row := rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", run.thread.id)]; strings.Contains(string(row.Data), "Weighing options") {
			t.Fatalf("the summary was stored: %s", row.Data)
		}
	}
}
