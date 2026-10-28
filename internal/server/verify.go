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
	// This harness seals rows and stores attachments itself, with the
	// control plane's service secret and the buckets sidecar beside it.
	secret, bucketsURL, bucket := os.Getenv("SYNC_ENCLAVE_SECRET"), os.Getenv("BUCKETS_URL"), os.Getenv("CHAT_ATTACHMENTS_BUCKET")
	for _, required := range []struct{ name, value string }{{"SYNC_ENCLAVE_SECRET", secret}, {"BUCKETS_URL", bucketsURL}, {"CHAT_ATTACHMENTS_BUCKET", bucket}} {
		if required.value == "" {
			return fmt.Errorf("%s is required", required.name)
		}
	}
	cp := controlplane.NewClient(h.controlplane, &http.Client{Timeout: controlplaneTimeout}, controlplane.WithServiceSecret(secret))
	bk := buckets.NewClient(bucketsURL, bucket, &http.Client{Timeout: bucketsTimeout})
	if !bk.Configured() {
		return fmt.Errorf("CHAT_ATTACHMENTS_BUCKET %q is not a valid bucket name", bucket)
	}
	h.store.Controlplane, h.store.Buckets = cp, bk
	h.store.Rows = &store.Rows{Controlplane: cp, Buckets: bk}
	s := h.models.Services["sync"]
	h.store.API = &store.Client{URL: s.URL, Client: s.Client, MinimumGrant: engine.RunTimeout + engine.CommitTimeout}
	return nil
}
