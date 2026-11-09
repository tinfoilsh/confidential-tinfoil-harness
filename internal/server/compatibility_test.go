package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/archives"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
)

func TestImagePreprocessingBoundsAndTransparency(t *testing.T) {
	var source bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 16, 16))
	img.Set(0, 0, color.Black)
	if err := png.Encode(&source, img); err != nil {
		t.Fatal(err)
	}
	resized, thumb, err := attach.ResizeImage(source.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for _, encoded := range [][]byte{resized, thumb} {
		decoded, _, err := image.Decode(bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		r, g, b, _ := decoded.At(15, 15).RGBA()
		if min(r, g, b) < 240*257 {
			t.Fatal("transparent background became dark")
		}
	}
	oversized := bytes.Clone(source.Bytes())
	binary.BigEndian.PutUint32(oversized[16:20], 10000)
	binary.BigEndian.PutUint32(oversized[20:24], 10000)
	binary.BigEndian.PutUint32(oversized[29:33], crc32.ChecksumIEEE(oversized[12:29]))
	if _, _, err := attach.ResizeImage(oversized); err == nil || api.AsError(err).Code != "ATTACHMENT_UNSUPPORTED" || !strings.Contains(api.AsError(err).Message, "pixel limit") {
		t.Fatal("pixel limit was not checked before decoding", err)
	}
	source.Reset()
	if err := png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 2000, 2))); err != nil {
		t.Fatal(err)
	}
	resized, thumb, err = attach.ResizeImage(source.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for index, encoded := range [][]byte{resized, thumb} {
		cfg, _, err := image.DecodeConfig(bytes.NewReader(encoded))
		if err != nil || cfg.Height != 1 || cfg.Width != []int{1536, 256}[index] {
			t.Fatal("unexpected resized dimensions", cfg, err)
		}
	}
}

func TestSharedThreadUsesPublicTimelineAndAttachments(t *testing.T) {
	result := api.Obj(api.Parsed(api.Raw(mustRead(archives.SharedThreadView("share-id", api.Raw(api.Object{"title": "Shared", "createdAt": float64(1700000000000), "messages": []any{
		api.Object{"role": "assistant", "content": "answer", "timestamp": float64(1700000000000)},
		api.Object{"role": "user", "content": "photo", "timestamp": float64(1700000000000), "attachments": []any{api.Object{"id": "attachment", "type": "image", "fileName": "photo.png", "mimeType": "image/png", "encryptionKey": "share-attachment-key", "thumbnailBase64": "thumbnail"}}},
	}}))))))
	if !strings.HasPrefix(api.Str(result["createdAt"]), "2023-") {
		t.Fatalf("invalid timestamp: %v", result)
	}
	messages := api.Arr(result["messages"])
	if len(api.Arr(api.Obj(messages[0])["timeline"])) == 0 || api.Str(api.Obj(messages[0])["id"]) == "" {
		t.Fatal("legacy assistant was not normalized")
	}
	attachment := api.Obj(api.Arr(api.Obj(messages[1])["attachments"])[0])
	if attachment["attKey"] != "share-attachment-key" || attachment["kind"] != "image" || attachment["thumbnail"] != "thumbnail" {
		t.Fatalf("attachment lost fields: %v", attachment)
	}
	if _, ok := attachment["encryptionKey"]; ok {
		t.Fatal("legacy field leaked into public contract")
	}
}

func TestTruncatedModelStreamPreservesPartialAnswer(t *testing.T) {
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
	})
	drained := screenedBy(t, h)
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", sendInput(key), "alice"))
	if events[len(events)-1].Code != "UPSTREAM_REFUSED" {
		t.Fatal("truncated stream reported success")
	}
	if got := drained(); len(got) != 0 {
		t.Fatalf("a failed chat was screened: %+v", got)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	if last := mustRead(domain.ReadThreadRow(row)).Messages[1]; last.Content != "partial" || !domain.ValueOr(last.IsError, false) {
		t.Fatal("partial answer lost")
	}
}
func TestTitleRunsBeforeAnswerFinishes(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		io.WriteString(w, "data: [DONE]\n\n")
	})
	titled := make(chan struct{})
	h.summarizeWith(func(string, string) (string, error) {
		close(titled)
		return "A generated title", nil
	})
	run, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), key, requestOf[engine.TurnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	select {
	case <-titled:
	case <-time.After(10 * time.Second):
		t.Fatal("title waited for the model")
	}
	close(release)
	<-run.Finished
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", run.Thread.ID)
	if mustRead(domain.ReadThreadRow(row)).Title != "A generated title" {
		t.Fatal("title did not commit")
	}
}

// A title the model writes as more than a short line, as an answer in a code
// block, does not name the chat. The chat keeps its placeholder, and a later
// turn names it.
func TestATitleThatIsNotAShortLineDoesNotNameTheChat(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	titles := []string{"```python\nprint(\"hello\")\n```", strings.Repeat("Printing hello in Python ", 5), "Printing hello in Python"}
	var calls atomic.Int32
	h.summarizeWith(func(task, _ string) (string, error) {
		if task != "title" {
			return "", io.EOF
		}
		return titles[min(int(calls.Add(1)), len(titles))-1], nil
	})
	alice := &auth.Principal{ID: "alice"}
	first := sendInput(key)
	turn := func(in api.Object) *domain.ThreadRow {
		t.Helper()
		if events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice")); events[len(events)-1].Type != "RUN_FINISHED" {
			t.Fatalf("the turn ended with %+v", events[len(events)-1])
		}
		row, err := rows.Load(context.Background(), alice, key, "chat", first["threadId"].(string))
		if err != nil {
			t.Fatal(err)
		}
		return mustRead(domain.ReadThreadRow(row))
	}
	if chat := turn(first); chat.Title != "New Chat" || chat.TitleState != "placeholder" || calls.Load() != 2 {
		t.Fatalf("after %d titles, the chat is named %q (%s)", calls.Load(), chat.Title, chat.TitleState)
	}
	next := sendInput(key)
	next["threadId"] = first["threadId"]
	if chat := turn(next); chat.Title != titles[2] || chat.TitleState != "generated" {
		t.Fatalf("a later turn named the chat %q (%s)", chat.Title, chat.TitleState)
	}
}

func TestWidgetSchemaBounds(t *testing.T) {
	clock, ok := catalogWidgets("render_clock")["render_clock"]
	if !ok {
		t.Fatal("render_clock is not in the widget catalog")
	}
	if err := clock.Resolved.Validate(api.Object{"durationSeconds": float64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := clock.Resolved.Validate(api.Object{"durationSeconds": float64(0)}); err == nil {
		t.Fatal("exclusive lower bound ignored")
	}
}

func TestWidgetRetryReplacesOnlyTheArguments(t *testing.T) {
	replacement := `{"stats":[{"label":"Count","value":2}]}`
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body api.Object
		json.NewDecoder(r.Body).Decode(&body)
		if api.Obj(body["response_format"])["type"] != "json_schema" {
			t.Error("retry did not request a schema")
		}
		json.NewEncoder(w).Encode(api.Object{"choices": []api.Object{{"message": api.Object{"content": `{"stats":"[{\"label\":\"Count\",\"value\":2}]"} Here are the stats.`}}}})
	})
	catalog := h.models.Catalog.Get()
	catalog.EnabledWidgets = []string{"render_stat_cards"}
	original := `{"stats":[{"label":"Count","value":1}]}`
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat")] = domain.StoredRow{ID: "chat", ETag: "1", Data: api.Raw(api.Object{"createdAt": api.Timestamp(), "title": "Title", "titleState": "manual", "model": "kimi-k3", "messages": []any{api.Object{"id": "user", "role": "user", "content": "show a stat", "timestamp": api.Timestamp()}, api.Object{"id": "assistant", "role": "assistant", "content": "keep this text", "timestamp": api.Timestamp(), "timeline": []any{api.Object{"type": "content", "id": "text", "content": "keep this text"}, api.Object{"type": "tool_call", "id": "call", "toolCallId": "call", "name": "render_stat_cards", "arguments": original}}}}})}
	in := sendInput(key)
	in["threadId"] = "chat"
	in["kind"] = "retryToolCall"
	in["toolCallId"] = "call"
	in["widgets"] = []any{"render_stat_cards"}
	delete(in, "content")
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	last := events[len(events)-1]
	if last.Type != "RUN_FINISHED" {
		t.Fatalf("retry failed: %+v", last)
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", "chat")
	messages := mustRead(domain.ReadThreadRow(row)).Messages
	if len(messages) != 2 || len(messages[1].Timeline) != 2 || messages[1].Timeline[1].Arguments != replacement {
		t.Fatal("retry changed message structure")
	}
	// The reply keeps the model and state it was committed with, which a
	// client merges from the run's report of its replies.
	var finished struct {
		Messages []domain.CommittedReply `json:"messages"`
	}
	if err := json.Unmarshal(last.Metadata, &finished); err != nil || len(finished.Messages) != 0 {
		t.Fatalf("the retry reported its replies as %s", last.Metadata)
	}
}

func TestManagedToolLoopKeepsReasoningAndOneAssistantMessage(t *testing.T) {
	var calls atomic.Int32
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"reason first\",\"tool_calls\":[{\"index\":0,\"id\":\"clock-call\",\"function\":{\"name\":\"render_clock\",\"arguments\":\"{}\"}}]}}]}\n\ndata: [DONE]\n\n")
			return
		}
		var body api.Object
		json.NewDecoder(r.Body).Decode(&body)
		messages := api.Arr(body["messages"])
		if api.Obj(messages[len(messages)-2])["reasoning_content"] != "reason first" {
			t.Error("reasoning disappeared from the tool exchange")
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"the clock is ready\"}}]}\n\ndata: [DONE]\n\n")
	})
	catalog := h.models.Catalog.Get()
	catalog.EnabledWidgets = []string{"render_clock"}
	api.Obj(catalog.Models[0]["chatConfig"])["reasoningConfig"] = api.Object{"reasoningHistoryPolicy": "tool-call-only"}
	in := sendInput(key)
	in["widgets"] = []any{"render_clock"}
	w := postRoute(h, "/v1/threads/turn", in, "alice")
	events := eventsIn(t, w)
	if events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatal(events[len(events)-1])
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	stored := mustRead(domain.ReadThreadRow(row))
	if len(stored.Messages) != 2 || len(stored.Messages[1].Timeline) != 3 {
		t.Fatal("tool loop did not make one ordered assistant timeline")
	}
	conformance(t, "widget-tool-loop", "A reply that reasons, calls a widget, and answers after its result, all in one assistant message.", framesOf(t, w.Body.Bytes()), &stored.Thread)
}
func TestShutdownCommitsPartialOutput(t *testing.T) {
	started := make(chan struct{})
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
		http.NewResponseController(w).Flush()
		close(started)
		<-r.Context().Done()
	})
	run, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), key, requestOf[engine.TurnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h.engine.Stop()
	h.engine.Wait(ctx)
	select {
	case <-run.Finished:
	default:
		t.Fatal("shutdown did not finish the run")
	}
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", run.Thread.ID)
	if mustRead(domain.ReadThreadRow(row)).ActiveRun != nil {
		t.Fatal("shutdown left an active marker")
	}
	if _, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{ID: "alice"}, chatCredential("alice"), key, requestOf[engine.TurnRequest](sendInput(key))); err == nil {
		t.Fatal("accepted a turn during shutdown")
	}
}
