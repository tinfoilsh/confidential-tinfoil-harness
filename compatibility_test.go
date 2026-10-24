package main

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
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestImagePreprocessingBoundsAndTransparency(t *testing.T) {
	var source bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 16, 16))
	img.Set(0, 0, color.Black)
	if err := png.Encode(&source, img); err != nil {
		t.Fatal(err)
	}
	resized, thumb, err := resizeImage(source.Bytes())
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
	if _, _, err := resizeImage(oversized); err == nil || asAPIError(err).Code != "ATTACHMENT_UNSUPPORTED" || !strings.Contains(asAPIError(err).Message, "pixel limit") {
		t.Fatal("pixel limit was not checked before decoding", err)
	}
	source.Reset()
	if err := png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 2000, 2))); err != nil {
		t.Fatal(err)
	}
	resized, thumb, err = resizeImage(source.Bytes())
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

// A model that refuses a run's chat token is asked once more, with the newest
// token the same caller has presented since, never another caller's.
func TestManagedModelRetriesARejectedChatTokenOnce(t *testing.T) {
	for _, rejectAgain := range []bool{false, true} {
		t.Run(strconv.FormatBool(rejectAgain), func(t *testing.T) {
			soon := mintChatToken("alice", func(_ map[string]any, claims jwt.MapClaims) { claims["exp"] = time.Now().Add(10 * time.Minute).Unix() })
			renewed := mintChatToken("alice", nil)
			others := mintChatToken("bob", func(_ map[string]any, claims jwt.MapClaims) { claims["exp"] = time.Now().Add(2 * time.Hour).Unix() })
			var modelCalls atomic.Int32
			var original []byte
			h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				call := modelCalls.Add(1)
				if call == 1 {
					original = body
				} else if !bytes.Equal(original, body) || r.Header.Get("Authorization") != "Bearer "+renewed {
					t.Error("retry changed the request body or did not take the caller's newest token")
				}
				if call == 1 || rejectAgain {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\ndata: [DONE]\n\n")
			})
			present := func(user, token string) {
				r := httptest.NewRequest(http.MethodPost, "/v1/changes", nil)
				signIn(r, user)
				r.Header.Set(inferenceKeyHeader, token)
				if _, err := h.auth.authenticate(r, true); err != nil {
					t.Fatal(err)
				}
			}
			present("alice", renewed)
			present("bob", others)
			r := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(raw(sendInput(key))))
			signIn(r, "alice")
			r.Header.Set(inferenceKeyHeader, soon)
			w := httptest.NewRecorder()
			h.routes().ServeHTTP(w, r)
			events := eventsIn(t, w)
			if modelCalls.Load() != 2 {
				t.Fatalf("the model was asked %d times", modelCalls.Load())
			}
			last := events[len(events)-1]
			if rejectAgain && last.Code != "UNAUTHENTICATED" || !rejectAgain && last.Type != "RUN_FINISHED" {
				t.Fatal("unexpected run result", last)
			}
			row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
			if !rejectAgain && len(mustRead(readThreadRow(row)).Messages) != 2 {
				t.Fatal("credential retry duplicated the turn")
			}
		})
	}
}

func TestSharedThreadUsesPublicTimelineAndAttachments(t *testing.T) {
	result := obj(decodeValue(raw(mustRead(sharedThreadView("share-id", raw(object{"title": "Shared", "createdAt": float64(1700000000000), "messages": []any{
		object{"role": "assistant", "content": "answer", "timestamp": float64(1700000000000)},
		object{"role": "user", "content": "photo", "timestamp": float64(1700000000000), "attachments": []any{object{"id": "attachment", "type": "image", "fileName": "photo.png", "mimeType": "image/png", "encryptionKey": "share-attachment-key", "thumbnailBase64": "thumbnail"}}},
	}}))))))
	if !strings.HasPrefix(str(result["createdAt"]), "2023-") {
		t.Fatalf("invalid timestamp: %v", result)
	}
	messages := arr(result["messages"])
	if len(arr(obj(messages[0])["timeline"])) == 0 || str(obj(messages[0])["id"]) == "" {
		t.Fatal("legacy assistant was not normalized")
	}
	attachment := obj(arr(obj(messages[1])["attachments"])[0])
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
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	if last := mustRead(readThreadRow(row)).Messages[1]; last.Content != "partial" || !valueOr(last.IsError, false) {
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(object{"summary": "A generated title"})
		close(titled)
	}))
	defer server.Close()
	h.services["summarizer"] = &downstreamService{URL: server.URL, Client: server.Client()}
	run, err := h.acceptTurn(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
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
	<-run.finished
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", run.thread.id)
	if mustRead(readThreadRow(row)).Title != "A generated title" {
		t.Fatal("title did not commit")
	}
}
func TestWidgetSchemaBounds(t *testing.T) {
	clock, ok := catalogWidgets("render_clock")["render_clock"]
	if !ok {
		t.Fatal("render_clock is not in the widget catalog")
	}
	if err := clock.schema.Validate(object{"durationSeconds": float64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := clock.schema.Validate(object{"durationSeconds": float64(0)}); err == nil {
		t.Fatal("exclusive lower bound ignored")
	}
}

func TestWidgetRetryReplacesOnlyTheArguments(t *testing.T) {
	replacement := `{"stats":[{"label":"Count","value":2}]}`
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body object
		json.NewDecoder(r.Body).Decode(&body)
		if obj(body["response_format"])["type"] != "json_schema" {
			t.Error("retry did not request a schema")
		}
		json.NewEncoder(w).Encode(object{"choices": []object{{"message": object{"content": `{"stats":"[{\"label\":\"Count\",\"value\":2}]"} Here are the stats.`}}}})
	})
	catalog := h.catalog.get()
	catalog.EnabledWidgets = []string{"render_stat_cards"}
	original := `{"stats":[{"label":"Count","value":1}]}`
	rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", "chat")] = storedRow{ID: "chat", ETag: "1", Data: raw(object{"createdAt": timestamp(), "title": "Title", "titleState": "manual", "model": "kimi-k3", "messages": []any{object{"id": "user", "role": "user", "content": "show a stat", "timestamp": timestamp()}, object{"id": "assistant", "role": "assistant", "content": "keep this text", "timestamp": timestamp(), "timeline": []any{object{"type": "content", "id": "text", "content": "keep this text"}, object{"type": "tool_call", "id": "call", "toolCallId": "call", "name": "render_stat_cards", "arguments": original}}}}})}
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
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "chat")
	messages := mustRead(readThreadRow(row)).Messages
	if len(messages) != 2 || len(messages[1].Timeline) != 2 || messages[1].Timeline[1].Arguments != replacement {
		t.Fatal("retry changed message structure")
	}
	// The reply keeps the model and state it was committed with, which a
	// client merges from the run's report of its replies.
	var finished struct {
		Messages []committedReply `json:"messages"`
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
		var body object
		json.NewDecoder(r.Body).Decode(&body)
		messages := arr(body["messages"])
		if obj(messages[len(messages)-2])["reasoning_content"] != "reason first" {
			t.Error("reasoning disappeared from the tool exchange")
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"the clock is ready\"}}]}\n\ndata: [DONE]\n\n")
	})
	catalog := h.catalog.get()
	catalog.EnabledWidgets = []string{"render_clock"}
	obj(catalog.Models[0]["chatConfig"])["reasoningConfig"] = object{"reasoningHistoryPolicy": "tool-call-only"}
	in := sendInput(key)
	in["widgets"] = []any{"render_clock"}
	w := postRoute(h, "/v1/threads/turn", in, "alice")
	events := eventsIn(t, w)
	if events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatal(events[len(events)-1])
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	stored := mustRead(readThreadRow(row))
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
	run, err := h.acceptTurn(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h.stopRuns()
	h.waitRuns(ctx)
	select {
	case <-run.finished:
	default:
		t.Fatal("shutdown did not finish the run")
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", run.thread.id)
	if mustRead(readThreadRow(row)).ActiveRun != nil {
		t.Fatal("shutdown left an active marker")
	}
	if _, err := h.acceptTurn(context.Background(), &principal{ID: "alice"}, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key))); err == nil {
		t.Fatal("accepted a turn during shutdown")
	}
}
