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
	Offer        Offer
	Services     map[string]*Service
	Controlplane string
}

// A model the harness serves through the router, which verifies the replica
// it relays each request to.
type Model struct {
	Endpoint string
	Name     string
	Vision   bool

	Client *http.Client // pinned to the attested router
}

// chatCompletionsPath is where the router takes model turns.
const chatCompletionsPath = "/v1/chat/completions"

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
// open, reach no one else. Every enclave the harness dials uses it.
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

// Offer holds the models the harness serves now.
type Offer struct {
	mu     sync.RWMutex
	models []*Model
}

func (c *Offer) Get() []*Model { c.mu.RLock(); defer c.mu.RUnlock(); return c.models }
func (c *Offer) Set(models []*Model) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.models = models
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

// Boot verifies the router and reads the catalog of models the harness
// serves, which it keeps refreshing.
func (inf *Models) Boot(ctx context.Context, usageSecret string) error {
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
	inf.Auto = &Model{Name: "auto", Client: router.Client, Endpoint: router.URL + chatCompletionsPath}
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
				// Every minute until the controlplane first lists a model, then every five.
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
	payload["stream"] = false
	// Without a scope of its own, a request would share the process-wide
	// cache secret with everyone holding the same credential.
	if _, scoped := payload["user_cache_secret"]; !scoped {
		payload["user_cache_secret"] = api.Token()
	}
	req, err := http.NewRequestWithContext(ctx, "POST", m.Endpoint, bytes.NewReader(api.Raw(payload)))
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
