package main

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	tinfoil "github.com/tinfoilsh/tinfoil-go"
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
