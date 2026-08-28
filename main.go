// Command confidential-tinfoil-harness serves an AG-UI endpoint whose model
// turns and tool calls all go to enclaves it attests at startup.
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

// repo is the trust anchor; enclave is only where attestation starts.
type model struct {
	name    string
	repo    string
	enclave string
	vision  bool
	context int // usable prompt budget, in tokens

	client *http.Client // sealing client, set once the enclave verifies
}

var catalog = []*model{
	{name: "kimi-k3", repo: "tinfoilsh/confidential-kimi-k3", enclave: "kimi-k3-inf14.tinfoil.containers.tinfoil.dev", context: 256_000},
	{name: "deepseek-v4-flash", repo: "tinfoilsh/confidential-deepseek-v4-flash", enclave: "deepseek-v4-flash-inf15.tinfoil.containers.tinfoil.dev", context: 128_000},
	{name: "gemma4-31b", repo: "tinfoilsh/confidential-gemma4-31b", enclave: "gemma4-31b-inf6-0.tinfoil.containers.tinfoil.dev", context: 128_000, vision: true},
	{name: "gpt-oss-120b", repo: "tinfoilsh/confidential-gpt-oss-120b", enclave: "gpt-oss-120b-inf6-0.tinfoil.containers.tinfoil.dev", context: 128_000},
	{name: "llama3-3-70b", repo: "tinfoilsh/confidential-llama3-3-70b", enclave: "llama3-3-70b-inf12.tinfoil.containers.tinfoil.dev", context: 128_000},
}

type harness struct {
	gateway      string
	models       []*model
	controlplane string
	cpClient     *http.Client

	mu   sync.Mutex
	live int // runs in flight; each holds its whole log in memory
	runs map[string]*run
}

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	gateway := flag.String("gateway", env("TINFOIL_GATEWAY_URL", "https://inference-gateway.tinfoil.sh"), "tinfoil-gateway base URL")
	controlplane := flag.String("controlplane", env("TINFOIL_CONTROLPLANE_URL", "https://api.tinfoil.sh"), "base URL of the store a detached run spills its sealed log to")
	flag.Parse()

	gw := strings.TrimRight(*gateway, "/")
	attestFamilies()
	models, err := attestCatalog(gw)
	if err != nil {
		slog.Error("verify model enclaves", "gateway", gw, "error", err)
		os.Exit(1)
	}
	h := &harness{gateway: gw, models: models,
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

	slog.Info("listening", "addr", *addr, "gateway", gw, "models", served(models))
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

// A family is opt-in per run, so one that does not verify is skipped, not fatal.
func attestFamilies() {
	for _, f := range families {
		f.enclave = env(f.env, f.enclave)
		client, err := attest(f.enclave, f.repo, "")
		if err != nil {
			slog.Warn("tool enclave did not verify", "family", f.name, "enclave", f.enclave, "error", err)
			continue
		}
		f.client = client
		slog.Info("tool enclave verified", "family", f.name, "enclave", f.enclave)
	}
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

// callerAuth signs every request with the caller's key; the harness has none.
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
