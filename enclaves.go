package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	tinfoil "github.com/tinfoilsh/tinfoil-go"

	"github.com/tinfoilsh/confidential-tinfoil-harness/tools"
)

// Every host this process will seal to is pinned here at compile time and
// verified here; nothing downstream can widen it.

// repo is the trust anchor -- the gateway may reroute, but a host that does not
// measure up is never sealed to. enclave is only where attestation starts.
type model struct {
	name    string
	repo    string
	enclave string
	vision  bool
	context int // usable prompt budget, in tokens
}

// catalog is ordered by preference; auto takes the first entry that can serve.
var catalog = []model{
	{
		name:    "kimi-k3",
		repo:    "tinfoilsh/confidential-kimi-k3",
		enclave: "kimi-k3.inf6.tinfoil.sh",
		context: 256_000,
	},
	{
		name:    "deepseek-v4-flash",
		repo:    "tinfoilsh/confidential-deepseek-v4-flash",
		enclave: "deepseek-v4-flash.inf6.tinfoil.sh",
		context: 128_000,
	},
	{
		name:    "gemma4-31b",
		repo:    "tinfoilsh/confidential-gemma4-31b",
		enclave: "gemma4-31b.inf6.tinfoil.sh",
		vision:  true,
		context: 128_000,
	},
	{
		name:    "gpt-oss-120b",
		repo:    "tinfoilsh/confidential-gpt-oss-120b",
		enclave: "gpt-oss-120b.inf6.tinfoil.sh",
		context: 128_000,
	},
	{
		name:    "llama3-3-70b",
		repo:    "tinfoilsh/confidential-llama3-3-70b",
		enclave: "llama3-3-70b.inf6.tinfoil.sh",
		context: 128_000,
	},
}

// pick resolves the model: named, or the first pinned one that fits the prompt.
func pick(req *request) (*model, error) {
	if req.model != "" && req.model != "auto" {
		for i := range catalog {
			if catalog[i].name == req.model {
				return &catalog[i], nil
			}
		}
		served := make([]string, len(catalog))
		for i := range catalog {
			served[i] = catalog[i].name
		}
		return nil, fmt.Errorf("unknown model %q: this harness serves %s and \"auto\"", req.model, strings.Join(served, ", "))
	}
	images, size := survey(req.messages)
	for i := range catalog {
		if images && !catalog[i].vision {
			continue
		}
		if size > catalog[i].context {
			continue
		}
		return &catalog[i], nil
	}
	return nil, fmt.Errorf("no pinned model fits this request (about %d tokens, images: %t)", size, images)
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

// client attests a model's enclave the first time it is asked for and shares
// the sealing client from then on. The lock is held across attestation, so a
// second caller for the same model waits rather than attesting it again.
func (h *harness) client(m *model) (*http.Client, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if client, ok := h.clients[m.name]; ok {
		return client, nil
	}
	verified, err := tinfoil.NewClientWithOptions(
		tinfoil.WithEnclave(m.enclave),
		tinfoil.WithRepo(m.repo),
		tinfoil.WithBaseURL(h.gateway+"/v1/"),
	)
	if err != nil {
		return nil, err
	}
	if h.clients == nil {
		h.clients = make(map[string]*http.Client, len(catalog))
	}
	h.clients[m.name] = verified.HTTPClient()
	return h.clients[m.name], nil
}

// Tools run in their own enclave, verified exactly like a model is.
const (
	toolsRepo           = "tinfoilsh/confidential-websearch"
	defaultToolsEnclave = "websearch.inf6.tinfoil.sh"
)

func verifiedTools(enclave string) (*tools.Web, error) {
	verified, err := tinfoil.NewClientWithOptions(
		tinfoil.WithEnclave(enclave),
		tinfoil.WithRepo(toolsRepo),
	)
	if err != nil {
		return nil, err
	}
	client := verified.HTTPClient()
	client.Transport = &callerAuth{inner: client.Transport}
	return tools.NewWeb("https://"+enclave+"/mcp", client), nil
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
