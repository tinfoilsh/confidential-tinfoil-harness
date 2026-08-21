// Command confidential-tinfoil-harness serves a streaming, OpenAI-shaped agent endpoint that
// seals every model turn and tool call to enclaves it attests before it starts
// serving. It holds no state between requests and no credentials.
package main

import (
	"context"
	"errors"
	"flag"
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

// ---- Starting up -----------------------------------------------------

const maxRequestBody = 8 << 20

// harness is immutable once built: every enclave it may reach was attested
// before it began serving, so a request shares nothing but read-only state.
type harness struct {
	gateway       string
	toolsEndpoint string
	toolsClient   *http.Client
}

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	gateway := flag.String("gateway", env("TINFOIL_GATEWAY_URL", "https://gateway.tinfoil.sh"), "tinfoil-gateway base URL")
	toolsHost := flag.String("tools-enclave", env("TINFOIL_TOOLS_ENCLAVE", defaultToolsEnclave), "host of the enclave serving web_search and web_fetch")
	flag.Parse()

	gw := strings.TrimRight(*gateway, "/")
	// Attested at startup: every request advertises these tools, so a harness
	// that cannot reach them cannot serve.
	toolsClient, err := attest(*toolsHost, toolsRepo, "")
	if err != nil {
		slog.Error("verify tools enclave", "enclave", *toolsHost, "error", err)
		os.Exit(1)
	}
	toolsClient.Transport = &callerAuth{inner: toolsClient.Transport}
	if err := attestCatalog(gw); err != nil {
		slog.Error("verify model enclaves", "gateway", gw, "error", err)
		os.Exit(1)
	}
	h := &harness{gateway: gw, toolsEndpoint: "https://" + *toolsHost + "/mcp", toolsClient: toolsClient}

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

	slog.Info("listening", "addr", *addr, "gateway", gw, "tools", *toolsHost, "models", served)
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

// ---- Every host this process may reach, and the proof it measured up ---

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

// catalog is ordered by preference; auto takes the first entry that can serve.
// One row per model: the name it is served under, the repo it must measure up
// to, where attestation starts, and the prompt budget it can be trusted with.
var catalog = []*model{
	{name: "kimi-k3", repo: "tinfoilsh/confidential-kimi-k3", enclave: "kimi-k3.inf6.tinfoil.sh", context: 256_000},
	{name: "deepseek-v4-flash", repo: "tinfoilsh/confidential-deepseek-v4-flash", enclave: "deepseek-v4-flash.inf6.tinfoil.sh", context: 128_000},
	{name: "gemma4-31b", repo: "tinfoilsh/confidential-gemma4-31b", enclave: "gemma4-31b.inf6.tinfoil.sh", context: 128_000, vision: true},
	{name: "gpt-oss-120b", repo: "tinfoilsh/confidential-gpt-oss-120b", enclave: "gpt-oss-120b.inf6.tinfoil.sh", context: 128_000},
	{name: "llama3-3-70b", repo: "tinfoilsh/confidential-llama3-3-70b", enclave: "llama3-3-70b.inf6.tinfoil.sh", context: 128_000},
}

// Tools run in their own enclave, verified exactly like a model is.
const (
	toolsRepo           = "tinfoilsh/confidential-websearch"
	defaultToolsEnclave = "websearch.inf6.tinfoil.sh"
)

// live is the catalog that measured up, in the catalog's preference order, and
// served names it for the caller. A model this process cannot prove is a model
// it will not offer, so trust and availability are one thing.
var (
	live   []*model
	served string
)

// attest is the only place this process obtains an outbound client: every
// request it makes is sealed to a host that measured up to repo.
func attest(enclave, repo, baseURL string) (*http.Client, error) {
	opts := []tinfoil.ClientOption{tinfoil.WithEnclave(enclave), tinfoil.WithRepo(repo)}
	if baseURL != "" { // the tools enclave is dialled directly, not via the gateway
		opts = append(opts, tinfoil.WithBaseURL(baseURL))
	}
	verified, err := tinfoil.NewClientWithOptions(opts...)
	if err != nil {
		return nil, err
	}
	return verified.HTTPClient(), nil
}

// attestCatalog verifies every pinned model concurrently, before the harness
// serves anything, and keeps the ones that answered.
func attestCatalog(gateway string) error {
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

	names := make([]string, 0, len(catalog))
	for _, m := range catalog {
		if m.client != nil {
			live = append(live, m)
			names = append(names, m.name)
		}
	}
	served = strings.Join(names, ", ")
	if len(live) == 0 {
		return errors.New("no pinned model enclave verified")
	}
	return nil
}

// apiKeyKey carries the caller's key to the transport: the sealing client is
// shared, so the key cannot be a header set once at construction.
type apiKeyKey struct{}

type callerAuth struct{ inner http.RoundTripper }

func (t *callerAuth) RoundTrip(r *http.Request) (*http.Response, error) {
	if key, ok := r.Context().Value(apiKeyKey{}).(string); ok && key != "" {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer "+key)
	}
	return t.inner.RoundTrip(r)
}
