package inference

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	tinfoil "github.com/tinfoilsh/tinfoil-go"
)

type discardedAttestationTransport struct{ closed chan struct{} }

func (t *discardedAttestationTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("discarded client must not send requests")
}
func (t *discardedAttestationTransport) CloseIdleConnections() { close(t.closed) }

func TestAttestationTimeoutDiscardsLateClient(t *testing.T) {
	runner := attestationRunner{timeout: 30 * time.Millisecond}
	release := make(chan struct{})
	closed := make(chan struct{})
	late := &http.Client{Transport: &discardedAttestationTransport{closed: closed}}
	if client, err := runner.run(context.Background(), func() (*http.Client, error) {
		<-release
		return late, nil
	}); client != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unresponsive verifier was not bounded: client=%p err=%v", client, err)
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("late verification result was not discarded")
	}
	good := &http.Client{}
	if client, err := runner.run(context.Background(), func() (*http.Client, error) { return good, nil }); client != good || err != nil {
		t.Fatalf("prompt verification lost its client: client=%p err=%v", client, err)
	}
}

func TestGatewayOfferFollowsTheCatalogAndKeepsTheLastGoodOne(t *testing.T) {
	var catalog atomic.Value
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := catalog.Load().(string)
		if r.URL.Path != "/catalog" || body == "" {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		io.WriteString(w, body)
	}))
	defer gateway.Close()
	sealing := &http.Client{}
	h := struct{ models *Models }{&Models{Gateway: gateway.URL, Client: sealing}}
	sdk, err := NewGateway("https://gateway.example", h.models.Offer.SDK)
	if err != nil {
		t.Fatal(err)
	}
	catalog.Store(`{
		"kimi-k3": {"repo": "tinfoilsh/confidential-kimi-k3", "hosts": ["kimi-1.example", "kimi-2.example"], "context": 262144, "vision": true},
		"gemma4-31b": {"repo": "tinfoilsh/confidential-other", "hosts": ["gemma.example"], "context": 131072},
		"gpt-oss-120b": {"repo": "tinfoilsh/confidential-gpt-oss-120b", "hosts": [], "context": 131072},
		"unpinned": {"repo": "tinfoilsh/confidential-unpinned", "hosts": ["unpinned.example"], "context": 8192}
	}`)
	h.models.RefreshOffer(context.Background())
	offered := h.models.Offer.Get()
	if len(offered) != 1 || *offered[0] != (Model{Name: "kimi-k3", Vision: true, Context: 262144, Client: sealing}) {
		t.Fatalf("offered models are not the pinned models the gateway routes: %+v", offered)
	}
	if want := (tinfoil.Catalog{"kimi-k3": {Repo: "tinfoilsh/confidential-kimi-k3", Hosts: []string{"kimi-1.example", "kimi-2.example"}}}); !reflect.DeepEqual(h.models.Offer.SDK(), want) {
		t.Fatalf("SDK catalog = %v, want %v", h.models.Offer.SDK(), want)
	}
	if !sdk.Serves("kimi-k3") || sdk.Serves("gemma4-31b") || sdk.Serves("gpt-oss-120b") || sdk.Serves("unpinned") {
		t.Fatal("the gateway client would seal to a model outside the pinned catalog")
	}
	for _, body := range []string{"", "{}", `{"unpinned": {"repo": "tinfoilsh/confidential-unpinned", "hosts": ["unpinned.example"], "context": 8192}}`} {
		catalog.Store(body)
		h.models.RefreshOffer(context.Background())
		if offered := h.models.Offer.Get(); len(offered) != 1 || offered[0].Name != "kimi-k3" || !sdk.Serves("kimi-k3") {
			t.Fatalf("catalog %q replaced the last one that offered a pinned model", body)
		}
	}
	catalog.Store(`{"gemma4-31b": {"repo": "tinfoilsh/confidential-gemma4-31b", "hosts": ["gemma.example"], "context": 131072}}`)
	h.models.RefreshOffer(context.Background())
	if Served(h.models.Offer.Get()) != "gemma4-31b" || sdk.Serves("kimi-k3") || !sdk.Serves("gemma4-31b") {
		t.Fatal("the gateway client did not follow the refreshed catalog")
	}
}

func TestModelsTheGatewayDoesNotOfferRunThroughTheRouter(t *testing.T) {
	var catalog atomic.Value
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, catalog.Load().(string))
	}))
	defer gateway.Close()
	sealing, routing := &http.Client{}, &http.Client{}
	models := &Models{Gateway: gateway.URL, Client: sealing, Services: map[string]*Service{"router": {URL: "https://router.example", Client: routing}}}
	sdk, err := NewGateway("https://gateway.example", models.Offer.SDK)
	if err != nil {
		t.Fatal(err)
	}
	offered := func() map[string]Model {
		byName := map[string]Model{}
		for _, m := range models.Offer.Get() {
			byName[m.Name] = *m
		}
		return byName
	}
	gemma := `"gemma4-31b": {"repo": "tinfoilsh/confidential-gemma4-31b", "hosts": ["gemma.example"], "context": 131072, "vision": true}`
	catalog.Store(`{` + gemma + `}`)
	models.RefreshOffer(context.Background())
	got := offered()
	if len(got) != len(pinned) {
		t.Fatalf("offered %d models, want every pinned one: %+v", len(got), got)
	}
	if got["gemma4-31b"] != (Model{Name: "gemma4-31b", Vision: true, Context: 131072, Client: sealing}) {
		t.Fatalf("a model the gateway offers left the gateway: %+v", got["gemma4-31b"])
	}
	if got["kimi-k3"] != (Model{Name: "kimi-k3", Endpoint: "https://router.example/v1/chat/completions", Client: routing}) {
		t.Fatalf("a model the gateway does not offer is not served through the router: %+v", got["kimi-k3"])
	}
	if sdk.Serves("kimi-k3") {
		t.Fatal("the gateway client would seal to a model the gateway does not offer")
	}

	catalog.Store(`{` + gemma + `, "kimi-k3": {"repo": "tinfoilsh/confidential-kimi-k3", "hosts": ["kimi.example"], "context": 262144}}`)
	models.RefreshOffer(context.Background())
	if got := offered()["kimi-k3"]; got != (Model{Name: "kimi-k3", Context: 262144, Client: sealing}) || !sdk.Serves("kimi-k3") {
		t.Fatalf("a model the gateway offers again still runs through the router: %+v", got)
	}
}
