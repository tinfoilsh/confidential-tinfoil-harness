package domain

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// ConnectorRecordVersion is the version of the connector rows this harness
// writes.
const ConnectorRecordVersion = 1

// What a connector's tool may do in a chat: run when the model calls it,
// run once the user approves the call, or not be offered at all.
const (
	ToolAllow = "allow"
	ToolAsk   = "ask"
	ToolOff   = "off"
)

// ToolPermissions are the permissions a user may give a tool.
var ToolPermissions = []string{ToolAllow, ToolAsk, ToolOff}

// What becomes of a call that needs the user's approval.
const (
	ApprovalPending  = "pending"
	ApprovalApproved = "approved"
	ApprovalDeclined = "declined"
	ApprovalExpired  = "expired"
)

// Connector is a remote MCP server the user connected, as its row stores
// it. Its credentials never leave the enclave: a client is sent its view.
type Connector struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	URL       string            `json:"url"`
	Enabled   bool              `json:"enabled"`
	CreatedAt string            `json:"createdAt"`
	UpdatedAt string            `json:"updatedAt"`
	Auth      *ConnectorAuth    `json:"auth,omitempty"`
	Pending   *ConnectorPending `json:"pending,omitempty"`
	Tools     []ConnectorTool   `json:"tools"`
}

// ConnectorAuth is how the harness signs in to a server that requires it:
// the authorization server, the client it registered there, and the tokens
// the user's consent gave it.
type ConnectorAuth struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorizationEndpoint"`
	TokenEndpoint         string `json:"tokenEndpoint"`
	RevocationEndpoint    string `json:"revocationEndpoint,omitempty"`
	// RegistrationEndpoint is where a client registered itself dynamically,
	// empty for a client named by its metadata document.
	RegistrationEndpoint string `json:"registrationEndpoint,omitempty"`
	// IssuerInResponse is whether the server names itself in the
	// authorization response, which then must name it.
	IssuerInResponse bool     `json:"issuerInResponse,omitempty"`
	ClientID         string   `json:"clientId"`
	ClientSecret     string   `json:"clientSecret,omitempty"`
	TokenAuthMethod  string   `json:"tokenAuthMethod,omitempty"`
	Resource         string   `json:"resource"`
	Scopes           []string `json:"scopes,omitempty"`
	AccessToken      string   `json:"accessToken,omitempty"`
	RefreshToken     string   `json:"refreshToken,omitempty"`
	TokenType        string   `json:"tokenType,omitempty"`
	Expiry           string   `json:"expiry,omitempty"`
	// Refreshing is until when one session holds the refresh of these
	// tokens, so no other presents the same refresh token meanwhile: a
	// server that rotates refresh tokens may revoke them all when one is
	// used twice.
	Refreshing string `json:"refreshing,omitempty"`
}

// RefreshClaim is what became of a session's claim to refresh a connector's
// tokens.
type RefreshClaim int

const (
	// RefreshClaimed: the session holds the refresh until its lease ends.
	RefreshClaimed RefreshClaim = iota
	// RefreshBusy: another session holds it.
	RefreshBusy
	// RefreshChanged: the stored tokens are no longer the ones the session
	// holds, which it takes in their place.
	RefreshChanged
)

// RefreshHeld is whether a session holds the refresh of the tokens now.
func (a *ConnectorAuth) RefreshHeld() bool {
	until, err := time.Parse(time.RFC3339Nano, a.Refreshing)
	return err == nil && api.Now().Before(until)
}

// ConnectorPending is a sign-in the user has started and not finished, and
// the authorization server it signs in through, whose tokens replace the
// connector's once it does.
type ConnectorPending struct {
	State     string         `json:"state"`
	Verifier  string         `json:"verifier"`
	ExpiresAt string         `json:"expiresAt"`
	Auth      *ConnectorAuth `json:"auth"`
}

// ConnectorTool is a tool the server offers, and what the user lets it do.
type ConnectorTool struct {
	Name        string `json:"name"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	ReadOnly    bool   `json:"readOnly,omitempty"`
	Permission  string `json:"permission"`
}

// StoredConnector is the row this harness writes.
type StoredConnector struct {
	Connector
	RecordVersion int `json:"recordVersion"`
}

// ReadConnector reads a connector row.
func ReadConnector(row *StoredRow) (Connector, error) {
	var stored StoredConnector
	if err := json.Unmarshal(row.Data, &stored); err != nil || stored.ID != row.ID {
		return Connector{}, api.Err(422, "RECORD_UNSUPPORTED", "The connector cannot be read")
	}
	if stored.RecordVersion > ConnectorRecordVersion {
		return Connector{}, api.Err(422, "RECORD_UNSUPPORTED", "The connector was written by a newer harness")
	}
	if stored.Tools == nil {
		stored.Tools = []ConnectorTool{}
	}
	return stored.Connector, nil
}

// Signed is whether the connector holds what it signs in with, or needs
// none.
func (c *Connector) Signed() bool {
	if c.Auth == nil {
		return c.Pending == nil
	}
	return c.Auth.AccessToken != "" || c.Auth.RefreshToken != ""
}

// Usable is whether a chat may call the connector's tools.
func (c *Connector) Usable() bool { return c.Enabled && c.Signed() }

// Permission is what the user lets a tool do now, given whether its server
// says it only reads: the permission the connector keeps for it, or to ask
// first for a tool the connector has never listed, which the user has not
// seen.
func (c *Connector) Permission(name string, readOnly bool) string {
	for _, t := range c.Tools {
		if t.Name == name {
			return t.permissionNow(readOnly)
		}
	}
	return ToolAsk
}

// permissionNow is the tool's permission given whether its server says now
// that it only reads. A tool allowed while it only read asks first once it
// no longer does: the user allowed a tool that changed nothing.
func (t *ConnectorTool) permissionNow(readOnly bool) string {
	if t.Permission == ToolAllow && t.ReadOnly && !readOnly {
		return ToolAsk
	}
	return t.Permission
}

// SeeTools has the connector list what its server offers a reply that bears
// on what its tools may do, so its user sees it and can choose, and reports
// whether that changed what it lists. A tool it did not list is listed,
// asking first, while the connector lists fewer than limit. One whose server
// no longer says the same of whether it only reads is listed as it says now.
// What else the connector lists of a tool, and the tools the server no
// longer offers, stay as they are until the settings list its tools again.
func (c *Connector) SeeTools(live []ConnectorTool, limit int) bool {
	changed := false
	for _, offered := range live {
		i := slices.IndexFunc(c.Tools, func(t ConnectorTool) bool { return t.Name == offered.Name })
		switch {
		case i < 0 && len(c.Tools) < limit:
			offered.Permission = ToolAsk
			c.Tools, changed = append(c.Tools, offered), true
		case i >= 0 && c.Tools[i].ReadOnly != offered.ReadOnly:
			c.Tools[i].Permission, c.Tools[i].ReadOnly, changed = c.Tools[i].permissionNow(offered.ReadOnly), offered.ReadOnly, true
		}
	}
	return changed
}

// MergeTools are the tools a server offers now, with the permissions the
// connector keeps for those it offered before.
func MergeTools(stored, live []ConnectorTool) []ConnectorTool {
	for i := range live {
		for _, kept := range stored {
			if kept.Name == live[i].Name {
				live[i].Permission = kept.permissionNow(live[i].ReadOnly)
			}
		}
	}
	return live
}

// DefaultPermission is a tool's permission until the user changes it.
func DefaultPermission(readOnly bool) string {
	if readOnly {
		return ToolAllow
	}
	return ToolAsk
}

// ConnectorView is a connector as a client is sent it, without anything it
// signs in with.
type ConnectorView struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	URL       string          `json:"url"`
	Enabled   bool            `json:"enabled"`
	Status    string          `json:"status"`
	SignIn    bool            `json:"signIn"`
	CreatedAt string          `json:"createdAt"`
	UpdatedAt string          `json:"updatedAt"`
	Tools     []ConnectorTool `json:"tools"`
}

// A connector's status: usable, or waiting for the user to sign in.
const (
	ConnectorConnected = "connected"
	ConnectorSignedOut = "signedOut"
)

// View is what a client is shown of the connector.
func (c *Connector) View() ConnectorView {
	status := ConnectorConnected
	if !c.Signed() {
		status = ConnectorSignedOut
	}
	tools := c.Tools
	if tools == nil {
		tools = []ConnectorTool{}
	}
	return ConnectorView{ID: c.ID, Name: c.Name, URL: c.URL, Enabled: c.Enabled, Status: status, SignIn: c.Auth != nil || c.Pending != nil,
		CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt, Tools: tools}
}

// ConnectorCall is what a client shows of a call to a connector's tool: the
// connector, the tool, and the user's approval when the call needs it.
type ConnectorCall struct {
	ID       string        `json:"id"`
	Name     string        `json:"name"`
	URL      string        `json:"url"`
	Tool     string        `json:"tool"`
	Title    string        `json:"title,omitempty"`
	Approval *ToolApproval `json:"approval,omitempty"`
}

// ToolApproval is the user's answer to a call that needs it: pending until
// they approve or decline it, or it expires. A pending approval names the
// id its answer is sent under, and until when it is accepted.
type ToolApproval struct {
	ID        string `json:"id,omitempty"`
	Status    string `json:"status"`
	ExpiresAt string `json:"expiresAt,omitempty"`
}

// TokenExpiry is when the connector's access token expires, or the zero
// time when it does not say.
func (a *ConnectorAuth) TokenExpiry() time.Time {
	at, err := time.Parse(time.RFC3339Nano, a.Expiry)
	if err != nil {
		return time.Time{}
	}
	return at
}
