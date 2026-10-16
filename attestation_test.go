package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	tinfoil "github.com/tinfoilsh/tinfoil-go"
	"github.com/tinfoilsh/tinfoil-go/verifier/client"
)

type discardedAttestationTransport struct{ closed chan struct{} }

func (t *discardedAttestationTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("discarded client must not send requests")
}
func (t *discardedAttestationTransport) CloseIdleConnections() { close(t.closed) }

func TestAttestationTimeoutDiscardsLateClient(t *testing.T) {
	runner := attestationRunner{slots: make(chan struct{}, 2), timeout: 30 * time.Millisecond}
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

func TestTimedOutAttestationsKeepTheirConcurrencySlot(t *testing.T) {
	runner := attestationRunner{slots: make(chan struct{}, 1), timeout: 30 * time.Millisecond}
	release := make(chan struct{})
	finished := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("verification worker did not finish")
		}
	})
	if client, err := runner.run(context.Background(), func() (*http.Client, error) {
		defer close(finished)
		<-release
		return nil, errors.New("unavailable")
	}); client != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unresponsive verifier was not bounded: client=%p err=%v", client, err)
	}
	var called atomic.Bool
	if _, err := runner.run(context.Background(), func() (*http.Client, error) {
		called.Store(true)
		return &http.Client{}, nil
	}); !errors.Is(err, context.DeadlineExceeded) || called.Load() {
		t.Fatalf("launched extra verification work after timeout: called=%v err=%v", called.Load(), err)
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
	h := &harness{gateway: gateway.URL, modelClient: sealing}
	sdk, err := newGateway("https://gateway.example", h.offer.sdk)
	if err != nil {
		t.Fatal(err)
	}
	catalog.Store(`{
		"kimi-k3": {"repo": "tinfoilsh/confidential-kimi-k3", "hosts": ["kimi-1.example", "kimi-2.example"], "context": 262144, "vision": true},
		"gemma4-31b": {"repo": "tinfoilsh/confidential-other", "hosts": ["gemma.example"], "context": 131072},
		"gpt-oss-120b": {"repo": "tinfoilsh/confidential-gpt-oss-120b", "hosts": [], "context": 131072},
		"unpinned": {"repo": "tinfoilsh/confidential-unpinned", "hosts": ["unpinned.example"], "context": 8192}
	}`)
	h.refreshOffer(context.Background())
	offered := h.offer.get()
	if len(offered) != 1 || *offered[0] != (model{name: "kimi-k3", repo: "tinfoilsh/confidential-kimi-k3", vision: true, context: 262144, client: sealing}) {
		t.Fatalf("offered models are not the pinned models the gateway routes: %+v", offered)
	}
	if want := (tinfoil.Catalog{"kimi-k3": {Repo: "tinfoilsh/confidential-kimi-k3", Hosts: []string{"kimi-1.example", "kimi-2.example"}}}); !reflect.DeepEqual(h.offer.sdk(), want) {
		t.Fatalf("SDK catalog = %v, want %v", h.offer.sdk(), want)
	}
	if !sdk.Serves("kimi-k3") || sdk.Serves("gemma4-31b") || sdk.Serves("gpt-oss-120b") || sdk.Serves("unpinned") {
		t.Fatal("the gateway client would seal to a model outside the pinned catalog")
	}
	for _, body := range []string{"", "{}", `{"unpinned": {"repo": "tinfoilsh/confidential-unpinned", "hosts": ["unpinned.example"], "context": 8192}}`} {
		catalog.Store(body)
		h.refreshOffer(context.Background())
		if offered := h.offer.get(); len(offered) != 1 || offered[0].name != "kimi-k3" || !sdk.Serves("kimi-k3") {
			t.Fatalf("catalog %q replaced the last one that offered a pinned model", body)
		}
	}
	catalog.Store(`{"gemma4-31b": {"repo": "tinfoilsh/confidential-gemma4-31b", "hosts": ["gemma.example"], "context": 131072}}`)
	h.refreshOffer(context.Background())
	if served(h.offer.get()) != "gemma4-31b" || sdk.Serves("kimi-k3") || !sdk.Serves("gemma4-31b") {
		t.Fatal("the gateway client did not follow the refreshed catalog")
	}
}

func TestVerificationReportsUsedReplicasAndDirectEnclaves(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	h.verified = func() []tinfoil.GatewayVerification {
		return []tinfoil.GatewayVerification{{Models: []string{"kimi-k3"}, Reference: "tinfoilsh/confidential-kimi-k3", PinnedModel: "kimi-k3", Verification: &client.VerifiedDocumentV3{EnclaveHost: "kimi-1.example", VerifiedAt: "2026-10-14T00:00:00Z"}}}
	}
	h.services["router"] = &downstreamService{Role: "router", Repo: "tinfoilsh/confidential-model-router", Enclave: "inference.example", At: "2026-10-13T00:00:00Z"}
	response := postRoute(h, "/v1/verify", object{}, "")
	var body struct {
		Verified []object `json:"verified"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != 200 {
		t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
	}
	want := []object{
		{"role": "model", "name": "kimi-k3", "repo": "tinfoilsh/confidential-kimi-k3", "enclave": "kimi-1.example", "at": "2026-10-14T00:00:00Z"},
		{"role": "router", "repo": "tinfoilsh/confidential-model-router", "enclave": "inference.example", "at": "2026-10-13T00:00:00Z"},
	}
	if !reflect.DeepEqual(body.Verified, want) {
		t.Fatalf("verified = %v, want %v", body.Verified, want)
	}
}
