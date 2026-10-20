package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

type downstreamService struct {
	URL    string
	Client *http.Client
}

func (h *harness) bootChat(ctx context.Context, usageSecret string) error {
	auth, err := newAuthenticator(ctx, os.Getenv("CLERK_ISSUER"), h.controlplane)
	if err != nil {
		return err
	}
	h.auth = auth
	gw, err := newGateway(h.gateway, h.offer.sdk)
	if err != nil {
		return fmt.Errorf("configure gateway %s: %w", h.gateway, err)
	}
	// Outermost: the SDK salts each caller's prompt cache with the key set here.
	h.modelClient = &http.Client{Transport: &callerAuth{inner: gw.HTTPClient().Transport, usageSecret: usageSecret}}
	h.services = map[string]*downstreamService{}
	for _, config := range []struct {
		role, repo, variable string
		required             bool
	}{
		{"sync", "tinfoilsh/confidential-sync", "TINFOIL_SYNC_ENCLAVE", true},
		{"summarizer", "tinfoilsh/confidential-summarizer", "TINFOIL_SUMMARIZER_ENCLAVE", false},
		{"metadata", "tinfoilsh/confidential-website-metadata-fetcher", "TINFOIL_METADATA_ENCLAVE", false},
		{"router", "tinfoilsh/confidential-model-router", "TINFOIL_ROUTER_ENCLAVE", true},
	} {
		host := os.Getenv(config.variable)
		if host == "" && config.required {
			return fmt.Errorf("%s is required", config.variable)
		}
		if host == "" {
			slog.Warn("downstream enclave not configured", "role", config.role)
			continue
		}
		client, err := attest(ctx, host, config.repo, usageSecret)
		if err != nil {
			if config.required {
				return fmt.Errorf("verify required %s enclave %s: %w", config.role, host, err)
			}
			slog.Warn("downstream enclave unavailable", "role", config.role, "enclave", host, "error", err)
			continue
		}
		h.services[config.role] = &downstreamService{URL: "https://" + host, Client: client}
	}
	s := h.services["sync"]
	h.syncAPI = &syncClient{URL: s.URL, Client: s.Client}
	h.storeRows = h.syncAPI
	router := h.services["router"]
	h.autoModel = &model{name: "auto", client: router.Client, endpoint: router.URL + "/v1/chat/completions"}
	if err := h.refreshCatalog(ctx); err != nil {
		return err
	}
	go func() {
		defer func() { rescued("catalog maintenance", recover()) }()
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		refresh := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				refresh++
				// Every minute until the gateway first offers a model, then every five.
				if refresh%5 == 0 || len(h.offer.get()) == 0 {
					if err := h.refreshCatalog(ctx); err != nil {
						slog.Warn("catalog refresh refused", "error", err)
					}
				}
			}
		}
	}()
	return nil
}
func (h *harness) serviceReady(role string) bool {
	service := h.services[role]
	return service != nil && service.Client != nil
}
func decodeValue(b []byte) any { var value any; json.Unmarshal(b, &value); return value }
func (h *harness) serviceCall(ctx context.Context, role, path string, in, out any) error {
	s := h.services[role]
	if s == nil {
		return apiErr(503, "TOOL_UNAVAILABLE", role+" is unavailable")
	}
	return postJSON(ctx, s.Client, strings.TrimRight(s.URL, "/")+path, callerKey(ctx), in, out)
}
