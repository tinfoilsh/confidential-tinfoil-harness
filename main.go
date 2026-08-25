// Command confidential-tinfoil-harness serves a streaming AG-UI endpoint that
// seals every model turn and tool call to enclaves it attests before it starts
// serving. It holds no credentials, and no run's content outlives the run: a
// caller that may come back leaves a key, the frames are sealed with it as
// they are produced, and both go when the run ends.
//
// Each file faces one counterparty: main.go the enclaves this harness pins,
// agui.go the caller, agent.go the gateway and the tools enclave, recovery.go
// the store a detached run is read back from.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	tinfoil "github.com/tinfoilsh/tinfoil-go"
)

const (
	toolsRepo           = "tinfoilsh/confidential-websearch"
	defaultToolsEnclave = "websearch.tinfoil.sh"
)

// repo is the trust anchor -- the gateway may reroute, but a host that does not
// measure up is never sealed to. enclave is only where attestation starts.
type model struct {
	name    string
	repo    string
	enclave string
	vision  bool
	context int // usable prompt budget, in tokens

	client *http.Client // the sealing client, once the enclave has measured up
}

var catalog = []*model{
	{name: "kimi-k3", repo: "tinfoilsh/confidential-kimi-k3", enclave: "kimi-k3-inf14.tinfoil.containers.tinfoil.dev", context: 256_000},
	{name: "deepseek-v4-flash", repo: "tinfoilsh/confidential-deepseek-v4-flash", enclave: "deepseek-v4-flash-inf15.tinfoil.containers.tinfoil.dev", context: 128_000},
	{name: "gemma4-31b", repo: "tinfoilsh/confidential-gemma4-31b", enclave: "gemma4-31b-inf6-0.tinfoil.containers.tinfoil.dev", context: 128_000, vision: true},
	{name: "gpt-oss-120b", repo: "tinfoilsh/confidential-gpt-oss-120b", enclave: "gpt-oss-120b-inf6-0.tinfoil.containers.tinfoil.dev", context: 128_000},
	{name: "llama3-3-70b", repo: "tinfoilsh/confidential-llama3-3-70b", enclave: "llama3-3-70b-inf12.tinfoil.containers.tinfoil.dev", context: 128_000},
}

type harness struct {
	gateway       string
	models        []*model
	toolsEndpoint string
	toolsClient   *http.Client
	controlplane  string
	cpClient      *http.Client

	mu   sync.Mutex
	live int // every run in flight, since each one holds its whole log in memory
	runs map[string]*run
}

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	gateway := flag.String("gateway", env("TINFOIL_GATEWAY_URL", "https://gateway.tinfoil.sh"), "tinfoil-gateway base URL")
	toolsHost := flag.String("tools-enclave", env("TINFOIL_TOOLS_ENCLAVE", defaultToolsEnclave), "host of the enclave serving web_search and web_fetch")
	controlplane := flag.String("controlplane", env("TINFOIL_CONTROLPLANE_URL", "https://api.tinfoil.sh"), "base URL of the store a detached run spills its sealed log to")
	flag.Parse()

	gw := strings.TrimRight(*gateway, "/")
	toolsClient, err := attest(*toolsHost, toolsRepo, "")
	if err != nil {
		slog.Error("verify tools enclave", "enclave", *toolsHost, "error", err)
		os.Exit(1)
	}
	models, err := attestCatalog(gw)
	if err != nil {
		slog.Error("verify model enclaves", "gateway", gw, "error", err)
		os.Exit(1)
	}
	h := &harness{gateway: gw, models: models, toolsEndpoint: "https://" + *toolsHost + "/mcp", toolsClient: toolsClient,
		controlplane: strings.TrimRight(*controlplane, "/"),
		cpClient:     &http.Client{Transport: &callerAuth{inner: http.DefaultTransport}},
		runs:         map[string]*run{}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"resume":true}`)
	})
	mux.HandleFunc("POST /agui", h.agui)
	mux.HandleFunc("DELETE /agui", h.drop)

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	slog.Info("listening", "addr", *addr, "gateway", gw, "tools", *toolsHost, "models", served(models))
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func attest(enclave, repo, baseURL string) (*http.Client, error) {
	opts := []tinfoil.ClientOption{tinfoil.WithEnclave(enclave), tinfoil.WithRepo(repo)}
	if baseURL != "" { // the tools enclave is dialled directly, not via the gateway
		opts = append(opts, tinfoil.WithBaseURL(baseURL))
	}
	verified, err := tinfoil.NewClientWithOptions(opts...)
	if err != nil {
		return nil, err
	}
	client := verified.HTTPClient()
	client.Transport = &callerAuth{inner: client.Transport}
	return client, nil
}

func attestCatalog(gateway string) ([]*model, error) {
	var wg sync.WaitGroup
	for _, m := range catalog {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := attest(m.enclave, m.repo, gateway+"/v1/")
			if err != nil {
				slog.Warn("model enclave did not verify", "model", m.name, "enclave", m.enclave, "error", err)
				return
			}
			m.client = client
		}()
	}
	wg.Wait()

	var live []*model
	for _, m := range catalog {
		if m.client != nil {
			live = append(live, m)
		}
	}
	if len(live) == 0 {
		return nil, errors.New("no pinned model enclave verified")
	}
	return live, nil
}

func served(models []*model) string {
	names := make([]string, len(models))
	for i, m := range models {
		names[i] = m.name
	}
	return strings.Join(names, ", ")
}

func (h *harness) pick(req *request) *model {
	named := req.model != "" && req.model != "auto"
	for _, m := range h.models {
		if named {
			if m.name == req.model {
				return m
			}
			continue
		}
		if (req.images && !m.vision) || req.tokens > m.context {
			continue
		}
		return m
	}
	return nil
}

func (h *harness) unserved(req *request) string {
	if req.model != "" && req.model != "auto" {
		return fmt.Sprintf("unknown model %q: this agent serves %s and \"auto\"", req.model, served(h.models))
	}
	return fmt.Sprintf("no pinned model fits this run (about %d tokens, images: %t)", req.tokens, req.images)
}

type apiKeyKey struct{}

// callerAuth signs every attested request -- model turn or tool call -- with
// the caller's key, carried down on the context. Everything is metered against
// the caller: the harness has no key of its own.
type callerAuth struct{ inner http.RoundTripper }

func (t *callerAuth) RoundTrip(r *http.Request) (*http.Response, error) {
	if key, ok := r.Context().Value(apiKeyKey{}).(string); ok && key != "" {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer "+key)
	}
	return t.inner.RoundTrip(r)
}

func token() string {
	var raw [16]byte
	rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}
