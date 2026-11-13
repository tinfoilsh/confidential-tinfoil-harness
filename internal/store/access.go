package store

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
)

// ChatAccess is the chat access a run's calls carry. A subscriber's chat
// token is renewed by the control plane for as long as the run lasts, as the
// user its client's session proved; a free key or an API key is carried as
// it is.
func (s *Store) ChatAccess(authenticator *auth.Authenticator, p *auth.Principal, credential auth.Credential) *auth.Access {
	if !credential.FirstParty {
		return auth.NewAccess(credential, nil)
	}
	user := p.ID
	return auth.NewAccess(credential, func(ctx context.Context, held auth.Secret) (auth.Credential, error) {
		token, err := s.Controlplane.RenewChatToken(ctx, user, string(held))
		if err != nil {
			return auth.Credential{}, accessRefused(err)
		}
		fresh, err := authenticator.ChatCredential(user, auth.Secret(token))
		if err != nil {
			return auth.Credential{}, accessRefused(err)
		}
		return fresh, nil
	})
}

// accessRefused is why a run's chat access was not renewed, which the run
// ends saying. Only an account that may no longer chat is told to stop: an
// answer the control plane could not give is worth another try.
func accessRefused(err error) *api.Error {
	slog.Warn("chat access was not renewed", "error", err)
	var refused *controlplane.Error
	if errors.As(err, &refused) {
		switch refused.StatusCode {
		case http.StatusPaymentRequired:
			return api.Err(403, "UPSTREAM_REFUSED", "The Tinfoil Chat subscription has ended").WithAction(api.ActionStop)
		case http.StatusForbidden:
			return api.Err(403, "UPSTREAM_REFUSED", "This account is suspended").WithAction(api.ActionStop)
		}
	}
	return api.Err(503, "UPSTREAM_REFUSED", "Chat access is temporarily unavailable")
}
