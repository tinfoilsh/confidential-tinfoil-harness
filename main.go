// Command confidential-tinfoil-harness runs Tinfoil Chat conversations whose
// model turns and tool calls all go to enclaves it attests against a pinned repo.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	_ "time/tzdata"

	tinfoil "github.com/tinfoilsh/tinfoil-go"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

// The pinned repo is the trust anchor; the gateway's catalog supplies the rest.
type model struct {
	endpoint string
	name     string
	vision   bool
	context  int // usable prompt budget, in tokens

	client *http.Client // seals each request to a verified replica
}

var pinned = []struct{ name, repo string }{
	{"kimi-k3", "tinfoilsh/confidential-kimi-k3"},
	{"deepseek-v4-flash", "tinfoilsh/confidential-deepseek-v4-flash"},
	{"deepseek-v4-1-flash", "tinfoilsh/confidential-deepseek-v4-1-flash"},
	{"glm-5-3", "tinfoilsh/confidential-glm5-3-nvfp4"},
	{"glm-5-3-flash", "tinfoilsh/confidential-glm5-3-flash"},
	{"gemma4-31b", "tinfoilsh/confidential-gemma4-31b"},
	{"gpt-oss-120b", "tinfoilsh/confidential-gpt-oss-120b"},
	{"llama3-3-70b", "tinfoilsh/confidential-llama3-3-70b"},
}

type harness struct {
	gateway      string
	offer        offerCache
	modelClient  *http.Client
	controlplane string
	cpClient     *http.Client
	storeRows    rowStore
	syncAPI      *syncClient
	auth         *authenticator
	catalog      catalogCache
	autoModel    *model
	services     map[string]*downstreamService
	safeguards   *submitter // nil unless SAFEGUARDS_URL names the sidecar
	live         liveRuns
	scratch      *scratchSpace
}

// listenAddr is the shim's upstream port in tinfoil-config.yml, which also
// sets every host this harness reaches.
const listenAddr = ":8081"

func main() {
	usageSecret := os.Getenv("USAGE_CONTEXT_SECRET")
	if usageSecret == "" {
		slog.Error("USAGE_CONTEXT_SECRET is required: without it every turn of a run bills as its own request")
		os.Exit(1)
	}
	gw, controlplane := strings.TrimRight(os.Getenv("TINFOIL_GATEWAY_URL"), "/"), strings.TrimRight(os.Getenv("TINFOIL_CONTROLPLANE_URL"), "/")
	if gw == "" || controlplane == "" {
		slog.Error("TINFOIL_GATEWAY_URL and TINFOIL_CONTROLPLANE_URL are required")
		os.Exit(1)
	}

	ctx, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	attestWebSearch(ctx, usageSecret)
	scratch, err := measuredScratch()
	if err != nil {
		slog.Error("measure the temporary directory", "error", err)
		os.Exit(1)
	}
	h := &harness{gateway: gw,
		controlplane: controlplane,
		cpClient:     &http.Client{Transport: &callerAuth{inner: http.DefaultTransport}},
		safeguards:   newSubmitter(os.Getenv("SAFEGUARDS_URL")),
		scratch:      scratch}

	if err := h.bootChat(ctx, usageSecret); err != nil {
		slog.Error("initialize chat service", "error", err)
		os.Exit(1)
	}

	srv := &http.Server{Addr: listenAddr, Handler: h.routes(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	stopped := make(chan struct{})
	go func() {
		<-stop
		defer close(stopped)
		h.stopRuns()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		h.waitRuns(ctx)
	}()

	slog.Info("listening", "addr", listenAddr, "gateway", gw, "models", served(h.offer.get()), "safeguards", h.safeguards != nil)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
	<-stopped
}

// Requests without a caller's own cache secret fall back to this one, so the
// SDK never tries to persist a secret under the read-only home directory.
var cacheFallback = tinfoil.WithUserCacheSecret(token())

// attest verifies an enclave the harness dials directly.
func attest(ctx context.Context, enclave, repo, usageSecret string) (*http.Client, error) {
	return startupAttestation.run(ctx, func() (*http.Client, error) {
		verified, err := tinfoil.NewClientWithOptions(tinfoil.WithEnclave(enclave), tinfoil.WithRepo(repo), cacheFallback)
		if err != nil {
			return nil, err
		}
		client := verified.HTTPClient()
		client.Transport = &callerAuth{inner: client.Transport, usageSecret: usageSecret}
		return client, nil
	})
}

// newGateway seals each model request to a replica of the model it names,
// verified on first use against the pinned repository.
func newGateway(gateway string, catalog func() tinfoil.Catalog) (*tinfoil.Gateway, error) {
	pins := map[string]tinfoil.ModelPin{}
	for _, p := range pinned {
		pins[p.name] = tinfoil.ModelPin{Repo: p.repo}
	}
	return tinfoil.NewGateway(gateway+"/v1/", catalog, tinfoil.GatewayOptions{ClientOptions: []tinfoil.ClientOption{cacheFallback}, ModelPins: pins, PinnedModelsOnly: true})
}

// Web search is opt-in per run, so an enclave that is not configured or does
// not verify is skipped, not fatal.
func attestWebSearch(ctx context.Context, usageSecret string) {
	webSearch.enclave = os.Getenv("TINFOIL_TOOLS_ENCLAVE")
	if webSearch.enclave == "" {
		slog.Warn("no web search enclave is configured")
		return
	}
	client, err := attest(ctx, webSearch.enclave, webSearch.repo, usageSecret)
	if err != nil {
		slog.Warn("tool enclave did not verify", "enclave", webSearch.enclave, "error", err)
		return
	}
	webSearch.client = client
	slog.Info("tool enclave verified", "enclave", webSearch.enclave)
}

// offerCache holds the pinned models the gateway last offered. The SDK reads
// the catalog on every request, so a stored catalog is never mutated.
type offerCache struct {
	mu      sync.RWMutex
	models  []*model
	catalog tinfoil.Catalog
}

func (c *offerCache) get() []*model        { c.mu.RLock(); defer c.mu.RUnlock(); return c.models }
func (c *offerCache) sdk() tinfoil.Catalog { c.mu.RLock(); defer c.mu.RUnlock(); return c.catalog }
func (c *offerCache) set(models []*model, catalog tinfoil.Catalog) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.models, c.catalog = models, catalog
}

// refreshOffer serves the models the gateway offers and this harness pins.
//
// The catalog is plaintext config, not a trust input. It can name any host, but
// a host that does not attest against the pinned repo is never sealed to, and
// the context and vision flags it reports only decide which runs are offered
// which model -- a wrong one costs a refusal or a gateway error, never
// confidentiality. That is what lets the enclave list live in one place, on the
// side that already owns replica placement. The gateway omits pools without a
// healthy replica, so a catalog that offers no pinned model keeps the last one.
func (h *harness) refreshOffer(ctx context.Context) {
	offered, err := fetchCatalog(ctx, h.gateway)
	if err != nil {
		slog.Warn("gateway catalog unavailable", "error", err)
		return
	}
	models, catalog := offerModels(offered, h.modelClient)
	if len(models) == 0 {
		slog.Warn("gateway offers no pinned model; keeping the last catalog")
		return
	}
	h.offer.set(models, catalog)
}

func offerModels(offered map[string]catalogEntry, client *http.Client) ([]*model, tinfoil.Catalog) {
	models, catalog := []*model{}, tinfoil.Catalog{}
	for _, p := range pinned {
		entry, ok := offered[p.name]
		if !ok || len(entry.Hosts) == 0 {
			continue
		}
		if entry.Repo != p.repo {
			slog.Warn("gateway offers a pinned model from another repository", "model", p.name, "repo", entry.Repo)
			continue
		}
		models = append(models, &model{name: p.name, vision: entry.Vision, context: entry.Context, client: client})
		catalog[p.name] = tinfoil.CatalogEntry{Repo: entry.Repo, Hosts: slices.Clone(entry.Hosts)}
	}
	return models, catalog
}

// catalogEntry is the gateway's description of one model pool.
type catalogEntry struct {
	Repo    string   `json:"repo"`
	Hosts   []string `json:"hosts"`
	Vision  bool     `json:"vision"`
	Context int      `json:"context"`
}

// fetchCatalog reads the pools the gateway routes to. It needs no API key: the
// harness holds none, and every caller's key belongs to a request.
func fetchCatalog(ctx context.Context, gateway string) (map[string]catalogEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway+"/catalog", nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
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

type apiKeyKey struct{}

// callerKey is the key the request was signed with; the harness has none of its own.
func callerKey(ctx context.Context) string {
	key, _ := ctx.Value(apiKeyKey{}).(string)
	return key
}

type usageContextKey struct{}

// callerAuth signs every request with the caller's key; the harness has none.
type callerAuth struct {
	inner       http.RoundTripper
	usageSecret string
}

func (t *callerAuth) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if key := callerKey(r.Context()); key != "" {
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
