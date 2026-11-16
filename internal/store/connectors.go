package store

import (
	"cmp"
	"context"
	"errors"
	"slices"

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
	slices.SortFunc(found, func(a, b domain.Connector) int {
		return cmp.Or(cmp.Compare(a.CreatedAt, b.CreatedAt), cmp.Compare(a.ID, b.ID))
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

var errSignedInAgain = errors.New("the connector was signed in again")

// KeepConnectorTokens applies change to a connector's sign-in as stored
// now, unless the user signed in again since used, the refresh token the
// change replaces, was issued: the tokens of that sign-in are newer.
func (s *Store) KeepConnectorTokens(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, used string, change func(*domain.ConnectorAuth)) error {
	_, err := s.MutateConnector(ctx, p, key, id, false, func(c *domain.Connector, _ bool) error {
		if c.Auth == nil || c.Auth.RefreshToken != used {
			return errSignedInAgain
		}
		change(c.Auth)
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
