package store

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Profile is a user's settings as a prompt reads them.
func (s *Store) Profile(ctx context.Context, p *auth.Principal, key auth.ContentKey) (domain.Profile, error) {
	stored, err := s.LoadProfile(ctx, p, key)
	if err != nil || stored == nil {
		return domain.Profile{}, err
	}
	return stored.Profile, nil
}
