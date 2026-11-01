package inference

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
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
