package server

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
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
	s := h.models.Services["sync"]
	h.store.API = &store.Client{URL: s.URL, Client: s.Client, MinimumGrant: engine.RunTimeout + engine.CommitTimeout}
	h.store.Rows = h.store.API
	// With the control plane's service secret, this harness seals and
	// stores rows itself; without it, the sync enclave does.
	if secret := os.Getenv("SYNC_ENCLAVE_SECRET"); secret != "" {
		h.store.Rows = &store.Rows{
			Controlplane: controlplane.NewClient(h.controlplane, &http.Client{Timeout: controlplaneTimeout}, controlplane.WithServiceSecret(secret)),
			Buckets:      buckets.NewClient(os.Getenv("BUCKETS_URL"), os.Getenv("CHAT_ATTACHMENTS_BUCKET"), &http.Client{Timeout: bucketsTimeout}),
		}
	}
	return nil
}
