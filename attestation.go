package main

import (
	"context"
	"net/http"
	"time"
)

const attestationTimeout = 20 * time.Second

type attestationRunner struct {
	timeout time.Duration
}

var startupAttestation = attestationRunner{timeout: attestationTimeout}

// The SDK verifies a direct enclave at construction without a context, and may
// retry a 30-second attestation fetch once. Bound how long startup waits and
// discard late results; startup verifies its few enclaves one at a time.
func (r *attestationRunner) run(ctx context.Context, verify func() (*http.Client, error)) (*http.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		client *http.Client
		err    error
	}
	done := make(chan result)
	go func() {
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
