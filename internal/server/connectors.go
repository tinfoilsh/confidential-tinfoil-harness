package server

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/connectors"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

const (
	// maxConnectorName bounds the name a user gives a connector.
	maxConnectorName = 64
	// maxSignInValue bounds the code and state a sign-in comes back with.
	maxSignInValue = 4096
)

func (h *harness) connectorRoutes() []routeSpec {
	return []routeSpec{
		{"/v1/connectors/list", false, true, h.listConnectors},
		{"/v1/connectors/add", false, true, decoded(h.addConnector)},
		{"/v1/connectors/authorize", false, true, decoded(h.authorizeConnector)},
		{"/v1/connectors/complete", false, true, decoded(h.completeConnector)},
		{"/v1/connectors/update", false, true, decoded(h.updateConnector)},
		{"/v1/connectors/refresh", false, true, decoded(h.refreshConnector)},
		{"/v1/connectors/delete", false, true, decoded(h.deleteConnector)},
	}
}

type connectorAddRequest struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

func (in *connectorAddRequest) Validate() error {
	if err := validConnectorName(&in.Name); err != nil {
		return err
	}
	server, err := connectors.ServerURL(in.URL)
	if err != nil {
		return api.Err(400, "BAD_REQUEST", "A connector's address must be a public HTTPS address")
	}
	in.URL = server
	return nil
}

func validConnectorName(name *string) error {
	*name = strings.TrimSpace(*name)
	if *name == "" || utf8.RuneCountInString(*name) > maxConnectorName {
		return api.Err(400, "BAD_REQUEST", "A connector's name must be 1 to 64 characters")
	}
	return nil
}

// connectorCompleteRequest is what the user came back from signing in with.
type connectorCompleteRequest struct {
	ID     string `json:"id"`
	Code   string `json:"code"`
	State  string `json:"state"`
	Issuer string `json:"iss"`
}

func (in *connectorCompleteRequest) Validate() error {
	if in.Code == "" || in.State == "" || len(in.Code) > maxSignInValue || len(in.State) > maxSignInValue || len(in.Issuer) > maxSignInValue {
		return api.Err(400, "BAD_REQUEST", "A sign-in needs its code and state")
	}
	return api.RequireID("id", in.ID)
}

type connectorUpdateRequest struct {
	ID      string            `json:"id"`
	Name    *string           `json:"name"`
	Enabled *bool             `json:"enabled"`
	Tools   map[string]string `json:"tools"`
}

func (in *connectorUpdateRequest) Validate() error {
	if in.Name != nil {
		if err := validConnectorName(in.Name); err != nil {
			return err
		}
	}
	for _, permission := range in.Tools {
		if !slices.Contains(domain.ToolPermissions, permission) {
			return api.Err(400, "BAD_REQUEST", "A tool's permission is allow, ask or off")
		}
	}
	return api.RequireID("id", in.ID)
}

// connectorAnswer is a connector as a route returns it, and the page that
// signs the user in to it when it needs that.
type connectorAnswer struct {
	Connector        domain.ConnectorView `json:"connector"`
	AuthorizationURL string               `json:"authorizationUrl,omitempty"`
}

func (h *harness) connectorClient() (*connectors.Client, error) {
	if h.connectors == nil {
		return nil, api.Err(503, "TOOL_UNAVAILABLE", "Connectors are unavailable").WithAction(api.ActionStop)
	}
	return h.connectors, nil
}

// keepTokens is where a session the route opens keeps the tokens it
// refreshes, under the key the route holds while it runs.
func (h *harness) keepTokens(p *auth.Principal, key *auth.ContentKey, id string) connectors.Saver {
	return func(ctx context.Context, used string, change func(*domain.ConnectorAuth)) error {
		return h.store.KeepConnectorTokens(ctx, p, *key, id, used, change)
	}
}

func (h *harness) listConnectors(ctx context.Context, p *auth.Principal, key auth.ContentKey, _ json.RawMessage) (any, error) {
	found, err := h.store.Connectors(ctx, p, key)
	if err != nil {
		return nil, err
	}
	views := make([]domain.ConnectorView, len(found))
	for i := range found {
		views[i] = found[i].View()
	}
	return struct {
		Connectors []domain.ConnectorView `json:"connectors"`
	}{views}, nil
}

// addConnector connects a server: one that needs no sign-in is ready with
// its tools; one that does waits for the user to sign in on the page it
// returns.
func (h *harness) addConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *connectorAddRequest) (any, error) {
	defer clear(key[:])
	client, err := h.connectorClient()
	if err != nil {
		return nil, err
	}
	existing, err := h.store.Connectors(ctx, p, key)
	if err != nil {
		return nil, err
	}
	if len(existing) >= store.MaxConnectors {
		return nil, api.Err(409, "CONNECTOR_LIMIT", "An account keeps at most 50 connectors")
	}
	ins, err := client.Inspect(ctx, in.URL)
	if err != nil {
		return nil, err
	}
	added := domain.Connector{Name: in.Name, URL: in.URL, Enabled: true}
	page := ""
	if ins.Auth == nil {
		if added.Tools, err = client.Tools(ctx, &added, nil); err != nil {
			return nil, err
		}
	} else {
		if err := client.Register(ctx, ins, nil); err != nil {
			return nil, err
		}
		added.Pending, page = client.Begin(ins.Auth)
	}
	saved, err := h.store.MutateConnector(ctx, p, key, domain.RowID(), true, func(c *domain.Connector, _ bool) error {
		c.Name, c.URL, c.Enabled, c.Tools, c.Pending = added.Name, added.URL, added.Enabled, added.Tools, added.Pending
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connectorAnswer{saved.View(), page}, nil
}

// authorizeConnector starts signing the user in to a connector again. Its
// tokens stay until the new sign-in replaces them.
func (h *harness) authorizeConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *api.IDRequest) (any, error) {
	defer clear(key[:])
	client, err := h.connectorClient()
	if err != nil {
		return nil, err
	}
	conn, err := h.store.LoadConnector(ctx, p, key, in.ID)
	if err != nil {
		return nil, err
	}
	ins, err := client.Inspect(ctx, conn.URL)
	if err != nil {
		return nil, err
	}
	if ins.Auth == nil {
		// The server no longer asks anyone to sign in.
		open := domain.Connector{URL: conn.URL}
		tools, err := client.Tools(ctx, &open, nil)
		if err != nil {
			return nil, err
		}
		saved, err := h.store.MutateConnector(ctx, p, key, conn.ID, false, func(c *domain.Connector, _ bool) error {
			c.Auth, c.Pending, c.Tools = nil, nil, domain.MergeTools(c.Tools, tools)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return connectorAnswer{Connector: saved.View()}, nil
	}
	previous := conn.Auth
	if previous == nil && conn.Pending != nil {
		previous = conn.Pending.Auth
	}
	if err := client.Register(ctx, ins, previous); err != nil {
		return nil, err
	}
	pending, page := client.Begin(ins.Auth)
	saved, err := h.store.MutateConnector(ctx, p, key, conn.ID, false, func(c *domain.Connector, _ bool) error {
		c.Pending = pending
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connectorAnswer{saved.View(), page}, nil
}

// completeConnector finishes the sign-in the user came back from, and lists
// the tools the connector offers them.
func (h *harness) completeConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *connectorCompleteRequest) (any, error) {
	defer clear(key[:])
	client, err := h.connectorClient()
	if err != nil {
		return nil, err
	}
	conn, err := h.store.LoadConnector(ctx, p, key, in.ID)
	if err != nil {
		return nil, err
	}
	signed, err := client.Complete(ctx, conn.Pending, in.Code, in.State, in.Issuer)
	if err != nil {
		return nil, err
	}
	state := conn.Pending.State
	// A server that lists no tools now keeps those the connector had; the
	// settings can list them again.
	signedIn := domain.Connector{ID: conn.ID, URL: conn.URL, Auth: signed}
	tools, listErr := client.Tools(ctx, &signedIn, func(_ context.Context, _ string, change func(*domain.ConnectorAuth)) error {
		change(signed)
		return nil
	})
	saved, err := h.store.MutateConnector(ctx, p, key, conn.ID, false, func(c *domain.Connector, _ bool) error {
		if c.Pending == nil || c.Pending.State != state {
			return api.Err(409, "REVISION_CONFLICT", "Another sign-in replaced this one")
		}
		c.Auth, c.Pending = signed, nil
		if listErr == nil {
			c.Tools = domain.MergeTools(c.Tools, tools)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connectorAnswer{Connector: saved.View()}, nil
}

func (h *harness) updateConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *connectorUpdateRequest) (any, error) {
	saved, err := h.store.MutateConnector(ctx, p, key, in.ID, false, func(c *domain.Connector, _ bool) error {
		if in.Name != nil {
			c.Name = *in.Name
		}
		if in.Enabled != nil {
			c.Enabled = *in.Enabled
		}
		for name, permission := range in.Tools {
			i := slices.IndexFunc(c.Tools, func(t domain.ConnectorTool) bool { return t.Name == name })
			if i < 0 {
				return api.Err(409, "REVISION_CONFLICT", "The connector no longer offers that tool")
			}
			c.Tools[i].Permission = permission
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connectorAnswer{Connector: saved.View()}, nil
}

// refreshConnector lists the tools a connector's server offers now.
func (h *harness) refreshConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *api.IDRequest) (any, error) {
	defer clear(key[:])
	client, err := h.connectorClient()
	if err != nil {
		return nil, err
	}
	conn, err := h.store.LoadConnector(ctx, p, key, in.ID)
	if err != nil {
		return nil, err
	}
	if !conn.Signed() {
		return nil, errConnectorSignedOut()
	}
	tools, err := client.Tools(ctx, &conn, h.keepTokens(p, &key, conn.ID))
	if errors.Is(err, connectors.ErrSignedOut) {
		return nil, errConnectorSignedOut()
	}
	if err != nil {
		return nil, err
	}
	saved, err := h.store.MutateConnector(ctx, p, key, conn.ID, false, func(c *domain.Connector, _ bool) error {
		c.Tools = domain.MergeTools(c.Tools, tools)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connectorAnswer{Connector: saved.View()}, nil
}

func errConnectorSignedOut() error {
	return api.Err(409, "CONNECTOR_SIGNED_OUT", "Sign in to the connector again")
}

// deleteConnector removes a connector, and asks its authorization server to
// revoke its tokens.
func (h *harness) deleteConnector(ctx context.Context, p *auth.Principal, key auth.ContentKey, in *api.IDRequest) (any, error) {
	defer clear(key[:])
	conn, err := h.store.LoadConnector(ctx, p, key, in.ID)
	if err != nil {
		return nil, err
	}
	if err := h.store.RemoveConnector(ctx, p, key, conn.ID); err != nil {
		return nil, err
	}
	if h.connectors != nil && conn.Auth != nil {
		h.connectors.Revoke(ctx, conn.Auth)
	}
	return struct{}{}, nil
}
