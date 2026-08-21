package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// This file is the caller's half of the connection, and the only place that
// knows the shape this harness presents: what a request may say, which model
// answers it, and how chunks and errors reach the client.

// chat serves one request. Everything that can still fail with a status does so
// before the first byte of the answer.
func (h *harness) chat(w http.ResponseWriter, r *http.Request) {
	apiKey := bearer(r.Header.Get("Authorization"))
	if apiKey == "" {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read request body")
		return
	}
	req, err := parseRequest(body, apiKey)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	images, size := survey(req.messages)
	m := pick(req.model, images, size)
	if m == nil {
		writeError(w, http.StatusNotFound, unserved(req.model, images, size))
		return
	}
	out := newSSE(w, m.name)
	// Ends the stream, and drops whatever a tool goroutine writes afterwards.
	defer out.done()

	// The key rides the context down to the transport that signs the tool session.
	ctx := context.WithValue(r.Context(), apiKeyKey{}, apiKey)
	start := time.Now()
	if err := h.run(ctx, out, req, m); err != nil {
		slog.Error("run", "model", m.name, "error", err)
		out.fail(err)
		return
	}
	slog.Info("served", "model", m.name, "elapsed", time.Since(start).Round(time.Millisecond))
}

// request is one caller's body, kept as it arrived and forwarded untouched.
// messages is the conversation as it was sent; the loop grows its own copy.
type request struct {
	fields   map[string]json.RawMessage
	messages []json.RawMessage
	model    string
	apiKey   string
}

func parseRequest(body []byte, apiKey string) (*request, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	var stream bool
	json.Unmarshal(fields["stream"], &stream)
	if !stream {
		return nil, errors.New(`this endpoint only streams: set "stream": true`)
	}
	var messages []json.RawMessage
	if json.Unmarshal(fields["messages"], &messages) != nil || len(messages) == 0 {
		return nil, errors.New(`"messages" must be a non-empty array`)
	}
	// The loop executes only the tools it can attest.
	if _, ok := fields["tools"]; ok {
		return nil, errors.New("this endpoint runs its own tools (web_search, web_fetch) and cannot execute caller-supplied ones")
	}
	if n, ok := fields["n"]; ok && strings.TrimSpace(string(n)) != "1" {
		return nil, errors.New(`"n" must be 1: the tool loop follows a single choice`)
	}
	var model string
	json.Unmarshal(fields["model"], &model)
	return &request{fields: fields, messages: messages, model: model, apiKey: apiKey}, nil
}

// pick resolves the model: the named one, or the first pinned one that fits.
func pick(want string, images bool, tokens int) *model {
	named := want != "" && want != "auto"
	for _, m := range live {
		if named {
			if m.name == want {
				return m
			}
			continue
		}
		if (images && !m.vision) || tokens > m.context {
			continue
		}
		return m
	}
	return nil
}

// unserved explains a choice pick could not make.
func unserved(want string, images bool, tokens int) string {
	if want != "" && want != "auto" {
		return fmt.Sprintf("unknown model %q: this harness serves %s and \"auto\"", want, served)
	}
	return fmt.Sprintf("no pinned model fits this request (about %d tokens, images: %t)", tokens, images)
}

// survey sizes the prompt off the wire at four bytes per token; erring high
// only moves a request to a roomier model.
func survey(messages []json.RawMessage) (images bool, tokens int) {
	for _, message := range messages {
		tokens += len(message)
		images = images ||
			bytes.Contains(message, []byte(`"image_url"`)) ||
			bytes.Contains(message, []byte(`"input_image"`))
	}
	return images, tokens / 4
}

// bearer extracts the token, matching the scheme case-insensitively per RFC 7235.
func bearer(header string) string {
	const scheme = "bearer "
	if len(header) < len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return ""
	}
	return strings.TrimSpace(header[len(scheme):])
}

// writeError replies in the OpenAI error shape, before the stream opens.
func writeError(w http.ResponseWriter, status int, message string) {
	kind := "invalid_request_error"
	if status >= 500 {
		kind = "server_error"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"message": message, "type": kind},
	})
}

// event is harness progress, carried in a chunk field an OpenAI client ignores.
type event struct {
	ID       string          `json:"item_id,omitempty"`
	Status   string          `json:"status"`
	Tool     string          `json:"tool,omitempty"`
	Args     json.RawMessage `json:"arguments,omitempty"`
	Meta     json.RawMessage `json:"meta,omitempty"`
	Error    string          `json:"error,omitempty"`
	Progress json.RawMessage `json:"progress,omitempty"`
}

// sse is the answer as the caller receives it. It owns whether anything has
// been written yet, so it also owns how a failure can still be reported.
type sse struct {
	w       http.ResponseWriter
	flush   *http.ResponseController
	id      string
	model   string
	created int64

	// mu serializes writes against the tool goroutines; shut drops late ones.
	mu   sync.Mutex
	open bool
	shut bool
}

func newSSE(w http.ResponseWriter, model string) *sse {
	return &sse{w: w, flush: http.NewResponseController(w), id: "chatcmpl-" + token(), model: model, created: time.Now().Unix()}
}

// pass keeps every field the enclave sent and replaces only the id.
func (s *sse) pass(chunk map[string]json.RawMessage) {
	chunk["id"] = quote(s.id)
	if encoded, err := json.Marshal(chunk); err == nil {
		s.write(string(encoded))
	}
}

func (s *sse) event(e event) {
	chunk, err := json.Marshal(map[string]any{
		"id":      s.id,
		"object":  "chat.completion.chunk",
		"created": s.created,
		"model":   s.model,
		"choices": []any{},
		"tinfoil": e,
	})
	if err != nil {
		return
	}
	s.write(string(chunk))
}

// progress reports that the tool call named by token advanced.
func (s *sse) progress(token string, detail json.RawMessage) {
	s.event(event{ID: token, Status: "progress", Progress: detail})
}

// fail reports the error in the only shape still available: a real status while
// nothing has been written, one more chunk once the answer is underway.
func (s *sse) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shut {
		return
	}
	if s.open {
		chunk, _ := json.Marshal(map[string]any{
			"error": map[string]string{"message": err.Error(), "type": "server_error"},
		})
		s.emit(string(chunk))
		return
	}
	// Nothing streamed yet: the gateway's own answer can still be the reply.
	status, detail := http.StatusBadGateway, err.Error()
	var refused *refusal
	if errors.As(err, &refused) {
		status, detail = refused.status, refused.detail
	}
	s.shut = true
	writeError(s.w, status, detail)
}

// done ends a stream that opened, and closes the writer either way.
func (s *sse) done() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open && !s.shut {
		s.emit("[DONE]")
	}
	s.shut = true
}

func (s *sse) write(payload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shut {
		return
	}
	s.emit(payload)
}

// emit writes one frame, opening the stream if this is the first; mu is held.
func (s *sse) emit(payload string) {
	if !s.open {
		s.open = true
		s.w.Header().Set("Content-Type", "text/event-stream")
		s.w.Header().Set("Cache-Control", "no-store")
		s.w.Header().Set("X-Accel-Buffering", "no")
	}
	fmt.Fprintf(s.w, "data: %s\n\n", payload)
	s.flush.Flush()
}

func quote(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

func token() string {
	var raw [16]byte
	rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}
