package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	tinfoil "github.com/tinfoilsh/tinfoil-go"
)

// The pinned repo is the trust anchor; the gateway's catalog supplies the rest.
type model struct {
	endpoint string
	name     string
	vision   bool
	context  int // usable prompt budget, in tokens

	client *http.Client // seals each request to a verified replica
}

// pin is a model the harness serves: its repository, and the name the
// system prompt gives it.
type pin struct{ name, repo, display string }

var pinned = []pin{
	{"kimi-k3", "tinfoilsh/confidential-kimi-k3", "Kimi K3"},
	{"deepseek-v4-flash", "tinfoilsh/confidential-deepseek-v4-flash", "DeepSeek V4 Flash"},
	{"deepseek-v4-1-flash", "tinfoilsh/confidential-deepseek-v4-1-flash", "DeepSeek V4.1 Flash"},
	{"glm-5-3", "tinfoilsh/confidential-glm5-3-nvfp4", "GLM-5.3"},
	{"glm-5-3-flash", "tinfoilsh/confidential-glm5-3-flash", "GLM-5.3 Flash"},
	{"gemma4-31b", "tinfoilsh/confidential-gemma4-31b", "Gemma 4 31B"},
	{"gpt-oss-120b", "tinfoilsh/confidential-gpt-oss-120b", "GPT-OSS 120B"},
	{"llama3-3-70b", "tinfoilsh/confidential-llama3-3-70b", "Llama 3.3 70B"},
}

// pinnedName is what the system prompt calls a model. The catalog's name is
// not used there, since the catalog comes from outside the enclave.
func pinnedName(model string) string {
	for _, p := range pinned {
		if p.name == model {
			return p.display
		}
	}
	return model
}

// Requests without a caller's own cache secret fall back to this one, so the
// SDK never tries to persist a secret under the read-only home directory.
var cacheFallback = tinfoil.WithUserCacheSecret(api.Token())

// attest verifies an enclave the harness dials directly.
func attest(ctx context.Context, enclave, repo, usageSecret string) (*http.Client, error) {
	return startupAttestation.run(ctx, func() (*http.Client, error) {
		verified, err := tinfoil.NewClientWithOptions(tinfoil.WithEnclave(enclave), tinfoil.WithRepo(repo), cacheFallback)
		if err != nil {
			return nil, err
		}
		client := verified.HTTPClient()
		client.Transport = &auth.CallerTransport{Inner: client.Transport, UsageSecret: usageSecret}
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

type downstreamService struct {
	URL    string
	Client *http.Client
}

// bootModels verifies the gateway and the downstream enclaves, and reads the
// catalog of models the harness serves, which it keeps refreshing.
func (h *harness) bootModels(ctx context.Context, usageSecret string) error {
	gw, err := newGateway(h.gateway, h.offer.sdk)
	if err != nil {
		return fmt.Errorf("configure gateway %s: %w", h.gateway, err)
	}
	// Outermost: the SDK salts each caller's prompt cache with the key set here.
	h.modelClient = &http.Client{Transport: &auth.CallerTransport{Inner: gw.HTTPClient().Transport, UsageSecret: usageSecret}}
	h.services = map[string]*downstreamService{}
	for _, config := range []struct {
		role, repo, variable string
		required             bool
	}{
		{"sync", "tinfoilsh/confidential-sync", "TINFOIL_SYNC_ENCLAVE", true},
		{"summarizer", "tinfoilsh/confidential-summarizer", "TINFOIL_SUMMARIZER_ENCLAVE", false},
		{"metadata", "tinfoilsh/confidential-website-metadata-fetcher", "TINFOIL_METADATA_ENCLAVE", false},
		{"router", "tinfoilsh/confidential-model-router", "TINFOIL_ROUTER_ENCLAVE", true},
	} {
		host := os.Getenv(config.variable)
		if host == "" && config.required {
			return fmt.Errorf("%s is required", config.variable)
		}
		if host == "" {
			slog.Warn("downstream enclave not configured", "role", config.role)
			continue
		}
		client, err := attest(ctx, host, config.repo, usageSecret)
		if err != nil {
			if config.required {
				return fmt.Errorf("verify required %s enclave %s: %w", config.role, host, err)
			}
			slog.Warn("downstream enclave unavailable", "role", config.role, "enclave", host, "error", err)
			continue
		}
		h.services[config.role] = &downstreamService{URL: "https://" + host, Client: client}
	}
	router := h.services["router"]
	h.autoModel = &model{name: "auto", client: router.Client, endpoint: router.URL + "/v1/chat/completions"}
	if err := h.refreshCatalog(ctx); err != nil {
		return err
	}
	go func() {
		defer func() { api.Rescued("catalog maintenance", recover()) }()
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		refresh := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				refresh++
				// Every minute until the gateway first offers a model, then every five.
				if refresh%5 == 0 || len(h.offer.get()) == 0 {
					if err := h.refreshCatalog(ctx); err != nil {
						slog.Warn("catalog refresh refused", "error", err)
					}
				}
			}
		}
	}()
	return nil
}

func (h *harness) serviceReady(role string) bool {
	service := h.services[role]
	return service != nil && service.Client != nil
}
func (h *harness) serviceCall(ctx context.Context, role, path string, in, out any) error {
	s := h.services[role]
	if s == nil {
		return api.Err(503, "TOOL_UNAVAILABLE", role+" is unavailable")
	}
	return api.PostJSON(ctx, s.Client, strings.TrimRight(s.URL, "/")+path, auth.CallerKey(ctx), in, out)
}

type usageSinkKey struct{}

// withUsageSink has the completions made under ctx tell sink of their usage.
func withUsageSink(ctx context.Context, sink usageSink) context.Context {
	return context.WithValue(ctx, usageSinkKey{}, sink)
}

// usageSink is told of a completion the model accepted and the usage it
// reports.
type usageSink interface {
	Accepted()
	Meter(usage json.RawMessage)
}

func (h *harness) completion(ctx context.Context, m *model, payload api.Object) (string, error) {
	endpoint := m.endpoint
	if endpoint == "" {
		endpoint = h.gateway + "/v1/chat/completions"
	}
	payload["stream"] = false
	// Without a scope of its own, a request would share the process-wide
	// cache secret with everyone holding the same credential.
	if _, scoped := payload["user_cache_secret"]; !scoped {
		payload["user_cache_secret"] = api.Token()
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(api.Raw(payload)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return "", api.WithRetryAfter(api.DownstreamError(resp.StatusCode, b), resp.Header)
	}
	if sink, ok := ctx.Value(usageSinkKey{}).(usageSink); ok {
		if usage, ok := auth.Usage(ctx); ok && usage.BillCustomerRequest {
			sink.Accepted()
		}
	}
	var result struct {
		Usage   json.RawMessage `json:"usage"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, domain.MaxShownOutput)).Decode(&result); err != nil {
		return "", err
	}
	if len(result.Choices) == 0 {
		return "", api.Err(502, "UPSTREAM_REFUSED", "The completion is empty")
	}
	if sink, ok := ctx.Value(usageSinkKey{}).(usageSink); ok && len(result.Usage) > 0 {
		sink.Meter(result.Usage)
	}
	return result.Choices[0].Message.Content, nil
}
