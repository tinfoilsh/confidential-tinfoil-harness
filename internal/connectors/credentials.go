package connectors

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// Stored is a connector's sign-in as the account's rows keep it, which
// every session of the connector shares.
type Stored struct {
	Store     *store.Store
	Principal *auth.Principal
	// Key is the account's content key, read as each write is made: a run
	// holds it until it ends, a route until it answers.
	Key func() auth.ContentKey
	ID  string
}

func (s Stored) Claim(ctx context.Context, held *domain.ConnectorAuth) (domain.ConnectorAuth, domain.RefreshClaim, error) {
	key := s.Key()
	defer clear(key[:])
	return s.Store.ClaimConnectorRefresh(ctx, s.Principal, key, s.ID, held)
}

func (s Stored) Save(ctx context.Context, used string, change func(*domain.ConnectorAuth)) error {
	key := s.Key()
	defer clear(key[:])
	return s.Store.KeepConnectorTokens(ctx, s.Principal, key, s.ID, used, change)
}

// Unstored is a sign-in no row keeps yet, as one just completed, which only
// the session that holds it uses.
type Unstored struct{ Auth *domain.ConnectorAuth }

func (u Unstored) Claim(context.Context, *domain.ConnectorAuth) (domain.ConnectorAuth, domain.RefreshClaim, error) {
	return *u.Auth, domain.RefreshClaimed, nil
}

func (u Unstored) Save(_ context.Context, _ string, change func(*domain.ConnectorAuth)) error {
	change(u.Auth)
	return nil
}
