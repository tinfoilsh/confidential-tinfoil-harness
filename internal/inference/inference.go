package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

type Models struct {
	Auto         *Model
	Catalog      CatalogCache
	Gateway      string
	Client       *http.Client
	Offer        Offer
	Services     map[string]*Service
	Controlplane string
	// Summarizer names chats and labels a reply's reasoning; nil when the
	// harness has none.
	Summarizer Summarizer
}

// The pinned repo is the trust anchor; the gateway's catalog supplies the rest.
type Model struct {
	Endpoint string
	Name     string
	Vision   bool
	Context  int // usable prompt budget, in tokens

	Client *http.Client // seals each request to a verified replica
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

// PinnedName is what the system prompt calls a model. The catalog's name is
// not used there, since the catalog comes from outside the enclave.
func PinnedName(model string) string {
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

// attestedTLS pins the TLS connection to the key an enclave's attestation
// names, so headers, status codes and bodiless requests, which EHBP leaves
// open, reach no one else. Every enclave the harness dials directly uses it;
// only the gateway, which relays to replicas, needs bodies sealed instead.
var attestedTLS = tinfoil.WithTransport(tinfoil.TransportTLS)

// Attest verifies an enclave the harness dials directly.
func Attest(ctx context.Context, enclave, repo, usageSecret string) (*http.Client, error) {
	return startupAttestation.run(ctx, func() (*http.Client, error) {
		verified, err := tinfoil.NewClientWithOptions(tinfoil.WithEnclave(enclave), tinfoil.WithRepo(repo), attestedTLS, cacheFallback)
		if err != nil {
			return nil, err
		}
		client := verified.HTTPClient()
		client.Transport = &auth.CallerTransport{Inner: client.Transport, UsageSecret: usageSecret}
		return client, nil
	})
}

// NewGateway seals each model request to a replica of the model it names,
// verified on first use against the pinned repository.
func NewGateway(gateway string, catalog func() tinfoil.Catalog) (*tinfoil.Gateway, error) {
	pins := map[string]tinfoil.ModelPin{}
	for _, p := range pinned {
		pins[p.name] = tinfoil.ModelPin{Repo: p.repo}
	}
	return tinfoil.NewGateway(gateway+"/v1/", catalog, tinfoil.GatewayOptions{ClientOptions: []tinfoil.ClientOption{cacheFallback}, ModelPins: pins, PinnedModelsOnly: true})
}

// Offer holds the pinned models the gateway last offered. The SDK reads
// the catalog on every request, so a stored catalog is never mutated.
type Offer struct {
	mu      sync.RWMutex
	models  []*Model
	catalog tinfoil.Catalog
}

func (c *Offer) Get() []*Model        { c.mu.RLock(); defer c.mu.RUnlock(); return c.models }
func (c *Offer) SDK() tinfoil.Catalog { c.mu.RLock(); defer c.mu.RUnlock(); return c.catalog }
func (c *Offer) Set(models []*Model, catalog tinfoil.Catalog) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.models, c.catalog = models, catalog
}

// RefreshOffer serves the models the gateway offers and this harness pins.
//
// The catalog is plaintext config, not a trust input. It can name any host, but
// a host that does not attest against the pinned repo is never sealed to, and
// the context and vision flags it reports only decide which runs are offered
// which model -- a wrong one costs a refusal or a gateway error, never
// confidentiality. That is what lets the enclave list live in one place, on the
// side that already owns replica placement. The gateway omits pools without a
// healthy replica, so a catalog that offers no pinned model keeps the last one.
func (inf *Models) RefreshOffer(ctx context.Context) {
	offered, err := fetchCatalog(ctx, inf.Gateway)
	if err != nil {
		slog.Warn("gateway catalog unavailable", "error", err)
		return
	}
	models, catalog := offerModels(offered, inf.Client)
	if len(models) == 0 {
		slog.Warn("gateway offers no pinned model; keeping the last catalog")
		return
	}
	inf.Offer.Set(models, catalog)
}

func offerModels(offered map[string]catalogEntry, client *http.Client) ([]*Model, tinfoil.Catalog) {
	models, catalog := []*Model{}, tinfoil.Catalog{}
	for _, p := range pinned {
		entry, ok := offered[p.name]
		if !ok || len(entry.Hosts) == 0 {
			continue
		}
		if entry.Repo != p.repo {
			slog.Warn("gateway offers a pinned model from another repository", "model", p.name, "repo", entry.Repo)
			continue
		}
		models = append(models, &Model{Name: p.name, Vision: entry.Vision, Context: entry.Context, Client: client})
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

func Served(models []*Model) string {
	names := make([]string, len(models))
	for i, m := range models {
		names[i] = m.Name
	}
	return strings.Join(names, ", ")
}

type Service struct {
	URL    string
	Client *http.Client
}

// Boot verifies the gateway and the downstream enclaves, and reads the
// catalog of models the harness serves, which it keeps refreshing.
func (inf *Models) Boot(ctx context.Context, usageSecret string) error {
	gw, err := NewGateway(inf.Gateway, inf.Offer.SDK)
	if err != nil {
		return fmt.Errorf("configure gateway %s: %w", inf.Gateway, err)
	}
	// Outermost: the SDK salts each caller's prompt cache with the key set here.
	inf.Client = &http.Client{Transport: &auth.CallerTransport{Inner: gw.HTTPClient().Transport, UsageSecret: usageSecret}}
	host := os.Getenv("TINFOIL_ROUTER_ENCLAVE")
	if host == "" {
		return errors.New("TINFOIL_ROUTER_ENCLAVE is required")
	}
	client, err := Attest(ctx, host, "tinfoilsh/confidential-model-router", usageSecret)
	if err != nil {
		return fmt.Errorf("verify required router enclave %s: %w", host, err)
	}
	router := &Service{URL: "https://" + host, Client: client}
	inf.Services = map[string]*Service{"router": router}
	inf.Auto = &Model{Name: "auto", Client: router.Client, Endpoint: router.URL + "/v1/chat/completions"}
	if err := inf.RefreshCatalog(ctx); err != nil {
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
				if refresh%5 == 0 || len(inf.Offer.Get()) == 0 {
					if err := inf.RefreshCatalog(ctx); err != nil {
						slog.Warn("catalog refresh refused", "error", err)
					}
				}
			}
		}
	}()
	return nil
}

func (inf *Models) ServiceReady(role string) bool {
	service := inf.Services[role]
	return service != nil && service.Client != nil
}

type usageSinkKey struct{}

// WithUsageSink has the completions made under ctx tell sink of their usage.
func WithUsageSink(ctx context.Context, sink UsageSink) context.Context {
	return context.WithValue(ctx, usageSinkKey{}, sink)
}

// UsageSink is told of a completion the model accepted and the usage it
// reports.
type UsageSink interface {
	Accepted()
	Meter(usage json.RawMessage)
}

func (inf *Models) Completion(ctx context.Context, m *Model, payload api.Object) (string, error) {
	endpoint := m.Endpoint
	if endpoint == "" {
		endpoint = inf.Gateway + "/v1/chat/completions"
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
	resp, err := m.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return "", api.WithRetryAfter(api.DownstreamError(resp.StatusCode, b), resp.Header)
	}
	if sink, ok := ctx.Value(usageSinkKey{}).(UsageSink); ok {
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
	if sink, ok := ctx.Value(usageSinkKey{}).(UsageSink); ok && len(result.Usage) > 0 {
		sink.Meter(result.Usage)
	}
	return result.Choices[0].Message.Content, nil
}
