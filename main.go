// Command confidential-tinfoil-harness serves an AG-UI endpoint whose model
// turns and tool calls all go to enclaves it attests against a pinned repo.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	tinfoil "github.com/tinfoilsh/tinfoil-go"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

// repo is the trust anchor; the gateway's catalog supplies the rest.
type model struct {
	name    string
	repo    string
	vision  bool
	context int // usable prompt budget, in tokens

	client *http.Client // sealing client, set once a replica verifies
}

var pinned = []struct{ name, repo string }{
	{"kimi-k3", "tinfoilsh/confidential-kimi-k3"},
	{"deepseek-v4-flash", "tinfoilsh/confidential-deepseek-v4-flash"},
	{"gemma4-31b", "tinfoilsh/confidential-gemma4-31b"},
	{"gpt-oss-120b", "tinfoilsh/confidential-gpt-oss-120b"},
	{"llama3-3-70b", "tinfoilsh/confidential-llama3-3-70b"},
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

	usageSecret := os.Getenv("USAGE_CONTEXT_SECRET")
	if usageSecret == "" {
		slog.Error("USAGE_CONTEXT_SECRET is required: without it every turn of a run bills as its own request")
		os.Exit(1)
	}

	gw := strings.TrimRight(*gateway, "/")
	attestFamilies(usageSecret)
	models, err := attestCatalog(gw, usageSecret)
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

func attest(enclave, repo, baseURL, usageSecret string) (*http.Client, error) {
	opts := []tinfoil.ClientOption{tinfoil.WithEnclave(enclave), tinfoil.WithRepo(repo)}
	if baseURL != "" { // the tools enclave is dialled directly, not via the gateway
		opts = append(opts, tinfoil.WithBaseURL(baseURL))
	}
	verified, err := tinfoil.NewClientWithOptions(opts...)
	if err != nil {
		return nil, err
	}
	client := verified.HTTPClient()
	client.Transport = &callerAuth{inner: client.Transport, usageSecret: usageSecret}
	return client, nil
}

// A family is opt-in per run, so one that does not verify is skipped, not fatal.
func attestFamilies(usageSecret string) {
	for _, f := range families {
		f.enclave = env(f.env, f.enclave)
		client, err := attest(f.enclave, f.repo, "", usageSecret)
		if err != nil {
			slog.Warn("tool enclave did not verify", "family", f.name, "enclave", f.enclave, "error", err)
			continue
		}
		f.client = client
		slog.Info("tool enclave verified", "family", f.name, "enclave", f.enclave)
	}
}

// attestCatalog serves the models the gateway offers and this harness pins,
// attesting one replica of each to start from.
//
// The catalog is plaintext config, not a trust input. It can name any host, but
// a host that does not attest against the pinned repo is never sealed to, and
// the context and vision flags it reports only decide which runs are offered
// which model -- a wrong one costs a refusal or a gateway error, never
// confidentiality. That is what lets the enclave list live in one place, on the
// side that already owns replica placement.
func attestCatalog(gateway, usageSecret string) ([]*model, error) {
	offered, err := fetchCatalog(gateway)
	if err != nil {
		return nil, err
	}

	models := make([]*model, len(pinned))
	var wg sync.WaitGroup
	for i, p := range pinned {
		entry, ok := offered[p.name]
		if !ok {
			slog.Warn("gateway offers no pool for a pinned model", "model", p.name)
			continue
		}
		m := &model{name: p.name, repo: p.repo, vision: entry.Vision, context: entry.Context}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Any replica that verifies will do as the starting point: the
			// gateway answers a 421 naming the one it routed to, and the SDK
			// attests that host against the same repo before re-sealing there.
			for _, host := range entry.Hosts {
				client, err := attest(host, m.repo, gateway+"/v1/", usageSecret)
				if err != nil {
					slog.Warn("replica did not verify", "model", m.name, "enclave", host, "error", err)
					continue
				}
				m.client = client
				models[i] = m
				return
			}
		}()
	}
	wg.Wait()

	live := slices.DeleteFunc(models, func(m *model) bool { return m == nil })
	if len(live) == 0 {
		return nil, errors.New("no replica of any pinned model verified")
	}
	return live, nil
}

// catalogEntry is the gateway's description of one model pool.
type catalogEntry struct {
	Hosts   []string `json:"hosts"`
	Vision  bool     `json:"vision"`
	Context int      `json:"context"`
}

// fetchCatalog reads the pools the gateway routes to. It needs no API key: the
// harness holds none, and every caller's key belongs to a request.
func fetchCatalog(gateway string) (map[string]catalogEntry, error) {
	resp, err := http.Get(gateway + "/catalog")
	if err != nil {
		return nil, fmt.Errorf("read catalog: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read catalog: gateway answered %d", resp.StatusCode)
	}
	var offered map[string]catalogEntry
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&offered); err != nil {
		return nil, fmt.Errorf("decode catalog: %w", err)
	}
	return offered, nil
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

type usageContextKey struct{}

// callerAuth signs every request with the caller's key; the harness has none.
type callerAuth struct {
	inner       http.RoundTripper
	usageSecret string
}

func (t *callerAuth) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if key, ok := r.Context().Value(apiKeyKey{}).(string); ok && key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if usage, ok := r.Context().Value(usageContextKey{}).(usagereporting.Context); ok {
		usage.IssuedAt = time.Now().UTC()
		if err := usagereporting.SetHeaders(r.Header, usage, t.usageSecret); err != nil {
			return nil, err
		}
	}
	return t.inner.RoundTrip(r)
}

func token() string {
	var raw [16]byte
	rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}
