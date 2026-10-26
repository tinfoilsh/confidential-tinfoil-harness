package main

import (
	"context"
	"os"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
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
	return nil
}
