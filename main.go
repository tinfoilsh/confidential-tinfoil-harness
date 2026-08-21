// Command confidential-tinfoil-harness serves a streaming, OpenAI-shaped agent endpoint that
// seals every model turn and tool call to enclaves it attests against the repos
// pinned in enclaves.go. It holds no state between requests and no credentials.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/tools"
)

const maxRequestBody = 8 << 20

type harness struct {
	gateway string
	sources []tools.Source

	mu      sync.Mutex
	clients map[string]*http.Client // one verified, sealing client per model
}

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	gateway := flag.String("gateway", env("TINFOIL_GATEWAY_URL", "https://gateway.tinfoil.sh"), "tinfoil-gateway base URL")
	toolsHost := flag.String("tools-enclave", env("TINFOIL_TOOLS_ENCLAVE", defaultToolsEnclave), "host of the enclave serving web_search and web_fetch")
	flag.Parse()

	gw := strings.TrimRight(*gateway, "/")
	// Attested at startup: every request advertises these tools, so a harness
	// that cannot reach them cannot serve.
	web, err := verifiedTools(*toolsHost)
	if err != nil {
		slog.Error("verify tools enclave", "enclave", *toolsHost, "error", err)
		os.Exit(1)
	}
	h := &harness{gateway: gw, sources: []tools.Source{web}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("POST /v1/chat/completions", h.chat)

	// No WriteTimeout: an answer streams for as long as the loop takes.
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	slog.Info("listening", "addr", *addr, "gateway", gw, "tools", *toolsHost, "models", len(catalog))
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
}

func (h *harness) chat(w http.ResponseWriter, r *http.Request) {
	apiKey := bearer(r.Header.Get("Authorization"))
	if apiKey == "" {
		fail(w, http.StatusUnauthorized, "missing API key")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	if err != nil {
		fail(w, http.StatusBadRequest, "could not read request body")
		return
	}
	req, err := parseRequest(body, apiKey)
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	m, err := pick(req)
	if err != nil {
		fail(w, http.StatusNotFound, err.Error())
		return
	}
	// A model's sealing client is verified once and shared; the caller's key
	// and cache namespace stay per request.
	upstream, err := h.client(m)
	if err != nil {
		slog.Error("verify model enclave", "model", m.name, "enclave", m.enclave, "error", err)
		fail(w, http.StatusBadGateway, "no verified enclave for model "+m.name)
		return
	}

	out, err := newSSE(w, m.name)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Closed first, so late tool progress is dropped, not written to a dead writer.
	defer out.close()
	// The key rides the context down to the transport that signs the tool session.
	ctx := context.WithValue(r.Context(), apiKeyKey{}, apiKey)
	start := time.Now()
	if err := h.run(ctx, out, req, m, upstream); err != nil {
		slog.Error("run", "model", m.name, "error", err)
		var refused *refusal
		switch {
		case out.opened():
			// Mid-answer, so the failure can only be one more chunk.
			out.fail(err)
		case errors.As(err, &refused):
			// Nothing streamed yet: the gateway's own answer can still be the reply.
			fail(w, refused.status, refused.detail)
			return
		default:
			fail(w, http.StatusBadGateway, err.Error())
			return
		}
	}
	out.done()
	slog.Info("served", "model", m.name, "elapsed", time.Since(start).Round(time.Millisecond))
}

// request is one caller's body, kept as it arrived and forwarded untouched.
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

// upstream renders one model turn's body: the caller's fields plus the loop's.
func (r *request) upstream(model string, messages []json.RawMessage, tools json.RawMessage) ([]byte, error) {
	conversation, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}
	body := make(map[string]json.RawMessage, len(r.fields)+4)
	maps.Copy(body, r.fields)
	body["model"] = quote(model)
	body["stream"] = json.RawMessage("true")
	body["messages"] = conversation
	body["tools"] = tools
	// Partitions the enclave's prompt cache per caller; a namespace, not a credential.
	body["user_cache_secret"] = quote(cacheScope(r.apiKey))
	return json.Marshal(body)
}

func cacheScope(apiKey string) string {
	sum := sha256.Sum256([]byte("confidential-tinfoil-harness cache scope\x00" + apiKey))
	return hex.EncodeToString(sum[:])
}

// bearer extracts the token, matching the scheme case-insensitively per RFC 7235.
func bearer(header string) string {
	const scheme = "bearer "
	if len(header) < len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return ""
	}
	return strings.TrimSpace(header[len(scheme):])
}

// fail replies in the OpenAI error shape, before the stream opens.
func fail(w http.ResponseWriter, status int, message string) {
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

func quote(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
