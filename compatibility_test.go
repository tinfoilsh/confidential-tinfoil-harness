package main

import (
	"bytes"
	"cmp"
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
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

func TestManagedModelRefreshesRejectedInferenceCredentialOnce(t *testing.T) {
	for _, rejectAgain := range []bool{false, true} {
		t.Run(strconv.FormatBool(rejectAgain), func(t *testing.T) {
			var modelCalls, exchanges atomic.Int32
			var original []byte
			h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				call := modelCalls.Add(1)
				if call == 1 {
					original = body
				} else if !bytes.Equal(original, body) || r.Header.Get("Authorization") != "Bearer fresh-inference" {
					t.Error("retry changed the request body or retained the stale credential")
				}
				if call == 1 || rejectAgain {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\ndata: [DONE]\n\n")
			})
			cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				exchanges.Add(1)
				if r.URL.Path != "/api/chat/token" || r.Header.Get("Authorization") != "Bearer clerk-alice" {
					t.Error("exchange did not use the Clerk session")
				}
				json.NewEncoder(w).Encode(object{"key": "fresh-inference", "expires_at": nowUTC().Add(time.Hour).Format(time.RFC3339)})
			}))
			defer cp.Close()
			h.auth.client, h.auth.controlplane = cp.Client(), cp.URL
			events := eventsIn(t, postRoute(h, "/v1/threads/turn", sendInput(key), "alice"))
			if exchanges.Load() != 1 || modelCalls.Load() != 2 {
				t.Fatal("credential refresh was not bounded to one retry")
			}
			last := events[len(events)-1]
			if rejectAgain && last.Code != "UNAUTHENTICATED" || !rejectAgain && last.Type != "RUN_FINISHED" {
				t.Fatal("unexpected run result", last)
			}
			row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
			if !rejectAgain && len(arr(row.Data["messages"])) != 2 {
				t.Fatal("credential retry duplicated the turn")
			}
		})
	}
}

// referenceCheck replays a run's events through the web client's harness
// reducer, found in TINFOIL_WEBAPP or ../tinfoil-webapp with its node_modules.
func referenceCheck(t *testing.T, events []event, expectedMessages any) {
	t.Helper()
	reference := cmp.Or(os.Getenv("TINFOIL_WEBAPP"), "../tinfoil-webapp")
	if _, err := os.Stat(reference + "/node_modules/typescript"); err != nil {
		t.Skip("reference webapp dependencies are not installed")
	}
	if _, err := os.Stat(reference + "/src/services/harness/reducer.ts"); err != nil {
		t.Skip("reference webapp has no harness reducer")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node is not installed")
	}
	cmd := exec.Command(node, "scripts/check-compatibility.cjs", reference)
	cmd.Stdin = bytes.NewReader(raw(object{"events": events, "expectedMessages": expectedMessages}))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reference validation: %v\n%s", err, output)
	}
}
func TestSharedThreadUsesPublicTimelineAndAttachments(t *testing.T) {
	result := sharedThreadView("share-id", object{"title": "Shared", "createdAt": float64(1700000000000), "messages": []any{
		object{"role": "assistant", "content": "answer", "timestamp": float64(1700000000000)},
		object{"role": "user", "content": "photo", "timestamp": float64(1700000000000), "attachments": []any{object{"id": "attachment", "type": "image", "fileName": "photo.png", "mimeType": "image/png", "encryptionKey": "share-attachment-key", "thumbnailBase64": "thumbnail"}}},
	}})
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
	last := obj(arr(row.Data["messages"])[1])
	if last["content"] != "partial" || last["isError"] != true {
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
	run, err := h.acceptTurn(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, key, sendInput(key))
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
	if row.Data["title"] != "A generated title" {
		t.Fatal("title did not commit")
	}
}
func TestWidgetSchemaBounds(t *testing.T) {
	clock, ok := catalogWidgets("render_clock")["render_clock"]
	if !ok {
		t.Fatal("render_clock is not in the widget catalog")
	}
	if err := validateSchema(clock.Schema, object{"durationSeconds": float64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := validateSchema(clock.Schema, object{"durationSeconds": float64(0)}); err == nil {
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
	rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", "chat")] = storedRow{ID: "chat", ETag: "1", Data: object{"createdAt": timestamp(), "title": "Title", "titleState": "manual", "model": "kimi-k3", "messages": []any{object{"id": "user", "role": "user", "content": "show a stat", "timestamp": timestamp()}, object{"id": "assistant", "role": "assistant", "content": "keep this text", "timestamp": timestamp(), "timeline": []any{object{"type": "content", "id": "text", "content": "keep this text"}, object{"type": "tool_call", "id": "call", "toolCallId": "call", "name": "render_stat_cards", "arguments": original}}}}}}
	in := sendInput(key)
	in["threadId"] = "chat"
	in["kind"] = "retryToolCall"
	in["toolCallId"] = "call"
	in["widgets"] = []any{"render_stat_cards"}
	delete(in, "content")
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" {
		t.Fatalf("retry failed: %+v", last)
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "chat")
	messages := arr(row.Data["messages"])
	blocks := arr(obj(messages[1])["timeline"])
	if len(messages) != 2 || len(blocks) != 2 || obj(blocks[1])["arguments"] != replacement {
		t.Fatal("retry changed message structure")
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
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatal(events[len(events)-1])
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	messages := arr(row.Data["messages"])
	if len(messages) != 2 || len(arr(obj(messages[1])["timeline"])) != 3 {
		t.Fatal("tool loop did not make one ordered assistant timeline")
	}
	referenceCheck(t, events, publicThread(row, "", 50)["messages"])
}
func TestShutdownCommitsPartialOutput(t *testing.T) {
	started := make(chan struct{})
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
		http.NewResponseController(w).Flush()
		close(started)
		<-r.Context().Done()
	})
	run, err := h.acceptTurn(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"}, key, sendInput(key))
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
	if row.Data["activeRun"] != nil {
		t.Fatal("shutdown left an active marker")
	}
	if _, err := h.acceptTurn(context.Background(), &principal{ID: "alice"}, key, sendInput(key)); err == nil {
		t.Fatal("accepted a turn during shutdown")
	}
}
