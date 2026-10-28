package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/search"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// The control plane answers a row's read or write within controlplaneTimeout;
// an attachment moving into buckets may take up to bucketsTimeout.
const (
	controlplaneTimeout = 30 * time.Second
	bucketsTimeout      = 5 * time.Minute
)

func (h *harness) bootChat(ctx context.Context, usageSecret string) error {
	authenticator, err := auth.New(ctx, os.Getenv("CLERK_ISSUER"), h.controlplane)
	if err != nil {
		return err
	}
	h.auth = authenticator
	if err := h.models.Boot(ctx, usageSecret); err != nil {
		return err
	}
	// This harness seals rows, stores attachments and keeps the search
	// index itself, with the control plane's service secret, the buckets
	// sidecars beside it, and the embedding model of the inference service.
	secret, bucketsURL, bucket := os.Getenv("SYNC_ENCLAVE_SECRET"), os.Getenv("BUCKETS_URL"), os.Getenv("CHAT_ATTACHMENTS_BUCKET")
	searchURL, searchBucket, apiKey, model := os.Getenv("SEARCH_BUCKETS_URL"), os.Getenv("SEARCH_INDEXES_BUCKET"), os.Getenv("TINFOIL_API_KEY"), os.Getenv("EMBEDDING_MODEL")
	for _, required := range []struct{ name, value string }{
		{"SYNC_ENCLAVE_SECRET", secret}, {"BUCKETS_URL", bucketsURL}, {"CHAT_ATTACHMENTS_BUCKET", bucket},
		{"SEARCH_BUCKETS_URL", searchURL}, {"SEARCH_INDEXES_BUCKET", searchBucket}, {"TINFOIL_API_KEY", apiKey}, {"EMBEDDING_MODEL", model},
	} {
		if required.value == "" {
			return fmt.Errorf("%s is required", required.name)
		}
	}
	cp := controlplane.NewClient(h.controlplane, &http.Client{Timeout: controlplaneTimeout}, controlplane.WithServiceSecret(secret))
	bk := buckets.NewClient(bucketsURL, bucket, &http.Client{Timeout: bucketsTimeout})
	if !bk.Configured() {
		return fmt.Errorf("CHAT_ATTACHMENTS_BUCKET %q is not a valid bucket name", bucket)
	}
	sbk := buckets.NewClient(searchURL, searchBucket, &http.Client{Timeout: bucketsTimeout})
	if !sbk.Configured() {
		return fmt.Errorf("SEARCH_INDEXES_BUCKET %q is not a valid bucket name", searchBucket)
	}
	embedder, err := inference.NewEmbedder(apiKey, model)
	if err != nil {
		return err
	}
	rows := &store.Rows{Controlplane: cp, Buckets: bk}
	h.search = search.New(cp, sbk, embedder, rows)
	rows.Index = h.search
	h.store.Controlplane, h.store.Buckets, h.store.Rows, h.store.Search = cp, bk, rows, h.search
	return nil
}
