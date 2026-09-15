package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type downstreamService struct {
	Role, Repo, Enclave, At, URL string
	Client                       *http.Client
}

func (h *harness) bootChat(ctx context.Context, usageSecret string) error {
	h.memoryEnabled = env("TINFOIL_MEMORY_ENABLED", "false") == "true"
	auth, err := newAuthenticator(ctx, env("CLERK_ISSUER", "https://clerk.tinfoil.sh"), env("CLERK_AUDIENCE", ""), h.controlplane)
	if err != nil {
		return err
	}
	h.auth = auth
	h.services = map[string]*downstreamService{}
	for _, config := range []struct {
		role, repo, host, variable string
		required                   bool
	}{
		{"sync", "tinfoilsh/confidential-sync", "sync.tinfoil.sh", "TINFOIL_SYNC_ENCLAVE", true},
		{"summarizer", "tinfoilsh/confidential-summarizer", "summarizer.tinfoil.sh", "TINFOIL_SUMMARIZER_ENCLAVE", false},
		{"document", "tinfoilsh/confidential-doc-upload", "docling.tinfoil.sh", "TINFOIL_DOCUMENT_ENCLAVE", false},
		{"metadata", "tinfoilsh/confidential-website-metadata-fetcher", "opengraph-metadata.tinfoil.sh", "TINFOIL_METADATA_ENCLAVE", false},
		{"router", "tinfoilsh/confidential-model-router", "inference.tinfoil.sh", "TINFOIL_ROUTER_ENCLAVE", true},
	} {
		host := env(config.variable, config.host)
		client, err := attest(ctx, host, config.repo, "", usageSecret)
		if err != nil {
			if config.required {
				return fmt.Errorf("verify required %s enclave %s: %w", config.role, host, err)
			}
			slog.Warn("downstream enclave unavailable", "role", config.role, "enclave", host, "error", err)
			continue
		}
		h.services[config.role] = &downstreamService{Role: config.role, Repo: config.repo, Enclave: host, URL: "https://" + host, Client: client, At: timestamp()}
	}
	s := h.services["sync"]
	h.syncAPI = &syncClient{URL: s.URL, Client: s.Client}
	h.storeRows = h.syncAPI
	router := h.services["router"]
	h.autoModel = &model{name: "auto", repo: router.Repo, client: router.Client, endpoint: router.URL + "/v1/chat/completions"}
	if catalog, err := fetchCatalog(ctx, h.gateway); err == nil {
		if audio, ok := catalog["voxtral-small-24b"]; ok {
			host, client, err := attestReplicas(ctx, audio.Hosts, "tinfoilsh/confidential-voxtral-small-24b", h.gateway+"/v1/", usageSecret, attest)
			if err != nil {
				slog.Warn("transcription unavailable", "error", err)
			} else {
				h.services["audio"] = &downstreamService{Role: "audio", Repo: "tinfoilsh/confidential-voxtral-small-24b", Enclave: host, URL: h.gateway, Client: client, At: timestamp()}
			}
		}
	}
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
				h.expireThreads()
				h.expireAttachments()
				refresh++
				if refresh%5 == 0 {
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
func (h *harness) verification(ctx context.Context, _ *principal, _ contentKey, _ object) (any, error) {
	verified := []object{}
	for _, model := range h.models {
		if model.client != nil {
			verified = append(verified, object{"role": "model", "name": model.name, "repo": model.repo, "enclave": model.enclave, "at": model.verifiedAt})
		}
	}
	for _, family := range families {
		if family.client != nil {
			verified = append(verified, object{"role": "tool", "name": family.name, "repo": family.repo, "enclave": family.enclave, "at": family.verifiedAt})
		}
	}
	for _, role := range []string{"sync", "router", "summarizer", "document", "metadata", "audio"} {
		if service := h.services[role]; service != nil {
			verified = append(verified, object{"role": role, "repo": service.Repo, "enclave": service.Enclave, "at": service.At})
		}
	}
	return object{"harness": object{"repo": "tinfoilsh/confidential-tinfoil-harness", "measurement": nil, "verifiedBy": "client"}, "verified": verified}, nil
}
func decodeValue(b []byte) any { var value any; json.Unmarshal(b, &value); return value }
func (h *harness) serviceCall(ctx context.Context, role, path string, in, out any) error {
	s := h.services[role]
	if s == nil {
		return apiErr(503, "TOOL_UNAVAILABLE", role+" is unavailable")
	}
	return postJSON(ctx, s.Client, strings.TrimRight(s.URL, "/")+path, callerKey(ctx), in, out)
}
