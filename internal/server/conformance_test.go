package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Runs recorded as fixtures in protocol/fixtures, which every client replays
// through its own reducer: the frames a run's stream sends, and the chat a
// client that applies them in order shows. The tests that record them check
// the committed fixtures are current; RECORD_FIXTURES=1 rewrites them.

const fixturesDir = "../../protocol/fixtures"

type fixtureFrame struct {
	ID    int             `json:"id"`
	Event json.RawMessage `json:"event"`
}

type runFixture struct {
	Description string         `json:"description"`
	Frames      []fixtureFrame `json:"frames"`
	Expected    fixtureChat    `json:"expected"`
}

// fixtureChat is the chat a client shows after a run: its messages with the
// members a client's reducer builds, its title, and how the run ended.
type fixtureChat struct {
	Messages   []api.Object `json:"messages"`
	Title      string       `json:"title"`
	TitleState string       `json:"titleState"`
	Status     string       `json:"status"`
	// Error is what the run's RUN_ERROR reports of why it failed.
	Error *api.Error `json:"error,omitempty"`
}

// A client that applies a run's frames shows the chat the harness keeps: a
// stored chat as the cloud holds it after the run, a device chat as its run
// returns it. A stream a client receives twice, as following a run without a
// cursor replays it, shows the same chat.
func TestRunsShowTheChatTheHarnessKeeps(t *testing.T) {
	streamed := func(chunks string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, chunks)
		}
	}
	refused := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"message":"Daily request limit reached.","type":"insufficient_quota","code":"insufficient_quota"}}`)
	}
	for _, tc := range []struct {
		name, description      string
		model                  http.HandlerFunc
		search, local, replays bool
	}{
		{"reply", "A reply streamed into a new chat.", streamed("data: {\"choices\":[{\"delta\":{\"content\":\"Hello there.\"}}]}\n\ndata: [DONE]\n\n"), false, false, false},
		{"reasoning", "A reply that reasons before it answers.", streamed("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"Thinking it over.\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"The answer.\"}}]}\n\ndata: [DONE]\n\n"), false, false, false},
		{"search", "A reply that searches the web, shows what it found as the search runs, and answers from it.", toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"cats"}`), nil), true, false, false},
		{"refused", "A run the model refuses because the caller's quota is used up.", refused, false, false, false},
		{"device-chat", "A turn of a chat kept on the device, whose earlier messages the run shows first.", streamed("data: {\"choices\":[{\"delta\":{\"content\":\"Second answer.\"}}]}\n\ndata: [DONE]\n\n"), false, true, false},
		{"replayed", "A reply whose whole stream arrives twice.", streamed("data: {\"choices\":[{\"delta\":{\"content\":\"Said once.\"}}]}\n\ndata: [DONE]\n\n"), false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.search {
				serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
					return nil, api.Object{"results": []api.Object{{"title": "Cats", "url": "https://cats.test", "content": "All about cats."}}}
				})
			}
			h, rows, key := testChatHarness(t, tc.model)
			in := sendInput(key)
			switch {
			case tc.search:
				in = searchInput(key)
			case tc.local:
				in = localInput("", priorThread())
			}
			w := postRoute(h, "/v1/threads/turn", in, "alice")
			frames := framesOf(t, w.Body.Bytes())
			events := eventsIn(t, w)
			var kept domain.Thread
			if tc.local {
				var metadata domain.RunMetadata
				if json.Unmarshal(events[len(events)-1].Metadata, &metadata) != nil || metadata.Thread == nil {
					t.Fatalf("the device run returned no chat: %s", w.Body)
				}
				kept = *metadata.Thread
			} else {
				row, err := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", events[0].ThreadID)
				if err != nil {
					t.Fatal(err)
				}
				kept = mustRead(domain.ReadThreadRow(row)).Thread
			}
			if last := events[len(events)-1].Type; last != map[bool]string{true: "RUN_ERROR", false: "RUN_FINISHED"}[tc.name == "refused"] {
				t.Fatalf("the run ended with %s", last)
			}
			if tc.replays {
				frames = append(frames, frames...)
			}
			conformance(t, tc.name, tc.description, frames, &kept)
		})
	}
}

// framesOf are a stream's whole frames, as sent.
func framesOf(t *testing.T, stream []byte) []fixtureFrame {
	t.Helper()
	var frames []fixtureFrame
	for _, block := range strings.Split(string(stream), "\n\n") {
		var frame fixtureFrame
		var data []string
		for line := range strings.SplitSeq(block, "\n") {
			if value, ok := strings.CutPrefix(line, "id: "); ok {
				frame.ID, _ = strconv.Atoi(value)
			} else if value, ok := strings.CutPrefix(line, "data: "); ok {
				data = append(data, value)
			}
		}
		if len(data) > 0 {
			frame.Event = json.RawMessage(strings.Join(data, "\n"))
			frames = append(frames, frame)
		}
	}
	if len(frames) == 0 {
		t.Fatal("the stream held no frames")
	}
	return frames
}

// conformance records a run's frames, and the chat as the harness keeps it
// after them, as the named fixture.
func conformance(t *testing.T, name, description string, frames []fixtureFrame, thread *domain.Thread) {
	t.Helper()
	recorded := runFixture{Description: description, Expected: fixtureChat{Title: thread.Title, TitleState: thread.TitleState, Status: "idle"}}
	for _, frame := range frames {
		recorded.Frames = append(recorded.Frames, stableFrame(frame))
	}
	for _, message := range domain.Window(&domain.ThreadRow{Thread: *thread}, "", domain.MaxMessageWindow).Messages {
		recorded.Expected.Messages = append(recorded.Expected.Messages, projectedMessage(message))
	}
	var last struct {
		Type string `json:"type"`
		api.Error
	}
	json.Unmarshal(frames[len(frames)-1].Event, &last)
	if last.Type == "RUN_ERROR" {
		recorded.Expected.Status, recorded.Expected.Error = "error", &last.Error
	}
	encoded, err := json.MarshalIndent(recorded, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	stable := append(stableValues(encoded), '\n')
	path := filepath.Join(fixturesDir, name+".json")
	if os.Getenv("RECORD_FIXTURES") != "" {
		if err := os.WriteFile(path, stable, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(committed, stable) {
		t.Fatalf("%s is not what the harness sends now; record it again with RECORD_FIXTURES=1 GOWORK=off go test ./... -run %s", path, t.Name())
	}
}

// projectedMessage is a message with the members a client's reducer must
// build: a question's text, and a reply's timeline steps.
func projectedMessage(m domain.MessageView) api.Object {
	out := api.Object{"id": m.ID, "role": m.Role}
	if m.Role == "user" {
		out["content"] = domain.ValueOr(m.Content, "")
	}
	timeline := []api.Object{}
	for _, block := range m.Timeline {
		step := api.Object{"id": block.ID, "type": block.Type}
		for name, value := range map[string]any{"content": block.Content, "toolCallId": block.ToolCallID, "name": block.Name, "arguments": block.Arguments} {
			if value != "" {
				step[name] = value
			}
		}
		if block.Complete {
			step["complete"] = true
		}
		if len(block.Result) > 0 {
			step["result"] = block.Result
		}
		if block.Progress != nil {
			step["progress"] = block.Progress
		}
		if block.Display != nil {
			step["display"] = block.Display
		}
		timeline = append(timeline, step)
	}
	out["timeline"] = timeline
	return out
}

var (
	volatileIDs    = regexp.MustCompile(`\d{13}_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{32}`)
	moments        = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z`)
	eventTimestamp = regexp.MustCompile(`^(\{"type":"[A-Z_]+","timestamp":")[^"]*"`)
	fixtureEpoch   = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

// stableFrame is a frame with the times a run reads from the clock replaced:
// the event's own a second after the one before it, every other the same.
func stableFrame(frame fixtureFrame) fixtureFrame {
	event := moments.ReplaceAll(frame.Event, []byte(fixtureEpoch.Format(time.RFC3339)))
	emitted := fixtureEpoch.Add(time.Duration(frame.ID) * time.Second).Format(time.RFC3339)
	return fixtureFrame{ID: frame.ID, Event: eventTimestamp.ReplaceAll(event, []byte("${1}"+emitted+`"`))}
}

// stableValues replaces the ids a run draws afresh with ones that keep the
// order they first appear in, so a fixture is the same however often it is
// recorded.
func stableValues(data []byte) []byte {
	ids := map[string]string{}
	data = volatileIDs.ReplaceAllFunc(data, func(id []byte) []byte {
		if stable, ok := ids[string(id)]; ok {
			return []byte(stable)
		}
		n := len(ids) + 1
		stable := fmt.Sprintf("%032x", n)
		switch {
		case len(id) == 50:
			stable = fmt.Sprintf("%013d_00000000-0000-4000-8000-%012d", n, n)
		case len(id) == 36:
			stable = fmt.Sprintf("00000000-0000-4000-8000-%012d", n)
		}
		ids[string(id)] = stable
		return []byte(stable)
	})
	return data
}
