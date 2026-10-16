package main

import (
	"context"
	"net/http"
	"time"
)

const attestationTimeout = 20 * time.Second

type attestationRunner struct {
	slots   chan struct{}
	timeout time.Duration
}

var startupAttestation = attestationRunner{slots: make(chan struct{}, 8), timeout: attestationTimeout}

// The SDK verifies a direct enclave at construction without a context, and may
// retry a 30-second attestation fetch once. Bound how long startup waits,
// discard late results, and retain the slot until the SDK exits so an
// unresponsive dependency cannot accumulate unlimited verification work.
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
	// With both cases ready, select may take the slot after the deadline.
	if err := ctx.Err(); err != nil {
		<-r.slots
		return nil, err
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
