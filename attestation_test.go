package main

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type discardedAttestationTransport struct{ closed chan struct{} }

func (t *discardedAttestationTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("discarded client must not send requests")
}
func (t *discardedAttestationTransport) CloseIdleConnections() { close(t.closed) }

func TestAttestationTimeoutFallsBackAndDiscardsLateClient(t *testing.T) {
	runner := attestationRunner{slots: make(chan struct{}, 2), timeout: 30 * time.Millisecond}
	release := make(chan struct{})
	closed := make(chan struct{})
	late := &http.Client{Transport: &discardedAttestationTransport{closed: closed}}
	good := &http.Client{}
	t.Cleanup(func() {
		close(release)
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Error("late verification result was not discarded")
		}
	})
	verify := func(ctx context.Context, host, repo, baseURL, secret string) (*http.Client, error) {
		if repo != "pinned/repository" || baseURL != "https://gateway.example/v1/" || secret != "usage" {
			t.Error("fallback changed verification identity or transport settings")
		}
		return runner.run(ctx, func() (*http.Client, error) {
			if host == "slow" {
				<-release
				return late, nil
			}
			return good, nil
		})
	}
	host, client, err := attestReplicas(context.Background(), []string{"slow", "healthy"}, "pinned/repository", "https://gateway.example/v1/", "usage", verify)
	if err != nil || host != "healthy" || client != good {
		t.Fatalf("fallback failed: host=%q client=%p err=%v", host, client, err)
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
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runner.run(ctx, func() (*http.Client, error) {
		called.Store(true)
		return &http.Client{}, nil
	}); !errors.Is(err, context.Canceled) || called.Load() {
		t.Fatalf("canceled verification started work: called=%v err=%v", called.Load(), err)
	}
}

func TestReplicaPoolStopsWhenItsContextExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	calls := 0
	verify := func(ctx context.Context, _, _, _, _ string) (*http.Client, error) {
		calls++
		<-ctx.Done()
		return nil, ctx.Err()
	}
	_, client, err := attestReplicas(ctx, []string{"first", "second"}, "pinned/repository", "", "", verify)
	if client != nil || !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("pool ignored cancellation: calls=%d client=%p err=%v", calls, client, err)
	}
}

func TestModelAttestationKeepsHealthyPools(t *testing.T) {
	offered := map[string]catalogEntry{
		"gemma4-31b":   {Hosts: []string{"bad-gemma", "healthy-gemma"}, Vision: true, Context: 262144},
		"gpt-oss-120b": {Hosts: []string{"bad-gpt"}, Context: 131072},
	}
	good := &http.Client{}
	verify := func(_ context.Context, host, _, _, _ string) (*http.Client, error) {
		if host == "healthy-gemma" {
			return good, nil
		}
		return nil, errors.New("replica unavailable")
	}
	models, err := attestModels(context.Background(), offered, "https://gateway.example", "", verify)
	if err != nil || len(models) != 1 {
		t.Fatalf("unavailable pool blocked healthy model: models=%v err=%v", models, err)
	}
	model := models[0]
	if model.name != "gemma4-31b" || model.enclave != "healthy-gemma" || model.client != good || model.context != 262144 || !model.vision {
		t.Fatalf("lost verified replica metadata: %+v", model)
	}
	delete(offered, "gemma4-31b")
	if _, err := attestModels(context.Background(), offered, "https://gateway.example", "", verify); err == nil {
		t.Fatal("startup accepted a catalog without any verified model")
	}
}
