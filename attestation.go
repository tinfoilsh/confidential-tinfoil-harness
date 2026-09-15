package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

const (
	verificationHTTPTimeout = 15 * time.Second
	attestationTimeout      = 20 * time.Second
	replicaPoolTimeout      = 45 * time.Second
)

type enclaveVerifier func(context.Context, string, string, string, string) (*http.Client, error)

type attestationRunner struct {
	slots   chan struct{}
	timeout time.Duration
}

var startupAttestation = attestationRunner{slots: make(chan struct{}, 8), timeout: attestationTimeout}

// The pinned SDK has no context parameter for verification. Bound how long
// startup waits, discard late results, and retain the slot until the SDK exits
// so an unresponsive dependency cannot accumulate unlimited verification work.
func (r *attestationRunner) run(ctx context.Context, verify func() (*http.Client, error)) (*http.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	type result struct {
		client *http.Client
		err    error
	}
	done := make(chan result)
	go func() {
		defer func() { <-r.slots }()
		client, err := verify()
		select {
		case done <- result{client, err}:
		case <-ctx.Done():
			if client != nil {
				client.CloseIdleConnections()
			}
		}
	}()
	select {
	case value := <-done:
		if err := ctx.Err(); err != nil {
			if value.client != nil {
				value.client.CloseIdleConnections()
			}
			return nil, err
		}
		return value.client, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func attestReplicas(ctx context.Context, hosts []string, repo, baseURL, usageSecret string, verify enclaveVerifier) (string, *http.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, replicaPoolTimeout)
	defer cancel()
	var last error
	for _, host := range hosts {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}
		client, err := verify(ctx, host, repo, baseURL, usageSecret)
		if err == nil {
			return host, client, nil
		}
		last = err
		slog.Warn("replica did not verify", "repo", repo, "enclave", host, "error", err)
	}
	if last == nil {
		last = errors.New("gateway advertised no replicas")
	}
	return "", nil, fmt.Errorf("no replica verified for %s: %w", repo, last)
}
