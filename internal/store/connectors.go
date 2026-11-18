package store

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Connectors

// MaxConnectors bounds the connectors one account keeps.
const MaxConnectors = 50

var errConnectorNotFound = api.Err(404, "CONNECTOR_NOT_FOUND", "The connector was not found")

// Connectors are the user's connectors, the oldest first. One the key
// cannot open, or this harness cannot read, is left out.
func (s *Store) Connectors(ctx context.Context, p *auth.Principal, key auth.ContentKey) ([]domain.Connector, error) {
	found := []domain.Connector{}
	err := s.EachRow(ctx, p, key, "connector", "", func(row *domain.StoredRow) error {
		if c, err := domain.ReadConnector(row); err == nil {
			found = append(found, c)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	created := func(c domain.Connector) time.Time {
		at, _ := time.Parse(time.RFC3339Nano, c.CreatedAt)
		return at
	}
	slices.SortFunc(found, func(a, b domain.Connector) int {
		return cmp.Or(created(a).Compare(created(b)), cmp.Compare(a.ID, b.ID))
	})
	return found, nil
}

func (s *Store) LoadConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string) (domain.Connector, error) {
	if !runIDValid(id) {
		return domain.Connector{}, errConnectorNotFound
	}
	row, err := s.Rows.Load(ctx, p, key, "connector", id)
	if err != nil {
		if api.AsError(err).HTTP == 404 {
			return domain.Connector{}, errConnectorNotFound
		}
		return domain.Connector{}, err
	}
	return domain.ReadConnector(row)
}

// MutateConnector applies change to a connector as stored now, or with
// create to a new one, which change is given empty but for its id.
func (s *Store) MutateConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, create bool, change func(c *domain.Connector, exists bool) error) (domain.Connector, error) {
	if !runIDValid(id) {
		return domain.Connector{}, errConnectorNotFound
	}
	var connector domain.Connector
	err := s.CommitRow(ctx, p, key, "connector", id, create, func(row *domain.StoredRow, exists bool) error {
		connector = domain.Connector{ID: id, CreatedAt: api.Timestamp(), Tools: []domain.ConnectorTool{}}
		if exists {
			var err error
			if connector, err = domain.ReadConnector(row); err != nil {
				return err
			}
		}
		if err := change(&connector, exists); err != nil {
			return err
		}
		connector.UpdatedAt = api.Timestamp()
		row.Data = api.Raw(domain.StoredConnector{Connector: connector, RecordVersion: domain.ConnectorRecordVersion})
		return nil
	})
	if err != nil && api.AsError(err).HTTP == 404 {
		return domain.Connector{}, errConnectorNotFound
	}
	return connector, err
}

// refreshLease is how long a session holds the refresh of a connector's
// tokens: longer than the refresh may take, so the session is done, or has
// given up, before another takes its place.
const refreshLease = 45 * time.Second

var (
	errSignedInAgain = errors.New("the connector was signed in again")
	// errUnchanged leaves a connector as stored when a change finds nothing
	// to store.
	errUnchanged = errors.New("the connector is unchanged")
)

// ClaimConnectorRefresh claims the refresh of a connector's tokens for a
// session that holds them as held, and returns the tokens as stored. The
// claim is a write at the row's revision, so of the sessions that claim at
// once, on this instance or another, one holds it; the others find it busy,
// and once it is done find the tokens changed.
func (s *Store) ClaimConnectorRefresh(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, held *domain.ConnectorAuth) (domain.ConnectorAuth, domain.RefreshClaim, error) {
	var stored domain.ConnectorAuth
	var claim domain.RefreshClaim
	_, err := s.MutateConnector(ctx, p, key, id, false, func(c *domain.Connector, _ bool) error {
		stored = domain.ConnectorAuth{}
		if c.Auth != nil {
			stored = *c.Auth
		}
		switch {
		case c.Auth == nil || stored.RefreshToken != held.RefreshToken || stored.AccessToken != held.AccessToken:
			claim = domain.RefreshChanged
		case stored.RefreshHeld():
			claim = domain.RefreshBusy
		default:
			claim = domain.RefreshClaimed
			c.Auth.Refreshing = api.Now().Add(refreshLease).UTC().Format(time.RFC3339Nano)
			return nil
		}
		return errUnchanged
	})
	if errors.Is(err, errUnchanged) {
		err = nil
	}
	return stored, claim, err
}

// SeeConnectorTools has a connector list what its server offers a reply
// that bears on what its tools may do, no more than limit tools in all.
func (s *Store) SeeConnectorTools(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, live []domain.ConnectorTool, limit int) error {
	_, err := s.MutateConnector(ctx, p, key, id, false, func(c *domain.Connector, _ bool) error {
		if !c.SeeTools(live, limit) {
			return errUnchanged
		}
		return nil
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// KeepConnectorTokens applies change to a connector's sign-in as stored
// now and ends the refresh that made it, unless the user signed in again
// since used, the refresh token the change replaces, was issued: the tokens
// of that sign-in are newer.
func (s *Store) KeepConnectorTokens(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, used string, change func(*domain.ConnectorAuth)) error {
	_, err := s.MutateConnector(ctx, p, key, id, false, func(c *domain.Connector, _ bool) error {
		if c.Auth == nil || c.Auth.RefreshToken != used {
			return errSignedInAgain
		}
		change(c.Auth)
		c.Auth.Refreshing = ""
		return nil
	})
	if errors.Is(err, errSignedInAgain) {
		return nil
	}
	return err
}

func (s *Store) RemoveConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string) error {
	if !runIDValid(id) {
		return errConnectorNotFound
	}
	return s.RemoveRow(ctx, p, key, "connector", id)
}
