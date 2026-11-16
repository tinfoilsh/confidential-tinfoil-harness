package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/oauthex"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

const (
	// mcpProtocolVersion is the MCP version the harness asks a server it
	// inspects for.
	mcpProtocolVersion = "2025-11-25"
	clientName         = "Tinfoil Chat"
	clientVersion      = "1"
	// offlineAccess is the scope that asks an authorization server that
	// offers it for a refresh token.
	offlineAccess = "offline_access"
	pkceMethod    = "S256"
)

func errUnreachable() error {
	return api.Err(502, "CONNECTOR_UNREACHABLE", "The server did not answer as an MCP server")
}

func errUnsupported(reason string) error {
	return api.Err(422, "CONNECTOR_UNSUPPORTED", reason)
}

// Inspection is how a server takes its requests: openly, or once the user
// signs in through an authorization server.
type Inspection struct {
	// Auth is the authorization server the user signs in through, with no
	// client registered yet, or nil when the server needs no sign-in.
	Auth *domain.ConnectorAuth
	// metadataDocument is whether the authorization server reads a client
	// from the metadata document its id names.
	metadataDocument bool
}

// protectedResource is a server's protected resource metadata (RFC 9728).
type protectedResource struct {
	Resource             string   `json:"resource"`
	AuthorizationServers []string `json:"authorization_servers"`
	ScopesSupported      []string `json:"scopes_supported"`
}

// authServer is an authorization server's metadata (RFC 8414).
type authServer struct {
	Issuer                            string   `json:"issuer"`
	AuthorizationEndpoint             string   `json:"authorization_endpoint"`
	TokenEndpoint                     string   `json:"token_endpoint"`
	RegistrationEndpoint              string   `json:"registration_endpoint"`
	RevocationEndpoint                string   `json:"revocation_endpoint"`
	ScopesSupported                   []string `json:"scopes_supported"`
	ResponseTypesSupported            []string `json:"response_types_supported"`
	CodeChallengeMethodsSupported     []string `json:"code_challenge_methods_supported"`
	ClientIDMetadataDocumentSupported bool     `json:"client_id_metadata_document_supported"`
	IssuerInResponse                  bool     `json:"authorization_response_iss_parameter_supported"`
}

// Inspect finds how server takes its requests: it opens a session without
// credentials, and when the server refuses it, reads which authorization
// server the user signs in through, as the MCP authorization spec says.
func (c *Client) Inspect(ctx context.Context, server string) (*Inspection, error) {
	status, challenge, err := c.probe(ctx, server)
	if err != nil {
		return nil, errUnreachable()
	}
	switch {
	case status >= 200 && status < 300:
		return &Inspection{}, nil
	case status != http.StatusUnauthorized && (status != http.StatusForbidden || challenge == nil):
		return nil, errUnreachable()
	}
	resource, issuer, scopes := server, origin(server), []string(nil)
	if challenge != nil && challenge.Params["scope"] != "" {
		scopes = strings.Fields(challenge.Params["scope"])
	}
	prm, err := c.protectedResource(ctx, server, challenge)
	if err != nil {
		return nil, err
	}
	if prm != nil {
		issuer = prm.AuthorizationServers[0]
		if prm.Resource != "" {
			resource = prm.Resource
		}
		if scopes == nil {
			scopes = prm.ScopesSupported
		}
	}
	as, err := c.authServer(ctx, issuer)
	if err != nil {
		return nil, err
	}
	if as.ScopesSupported != nil && slices.Contains(as.ScopesSupported, offlineAccess) && !slices.Contains(scopes, offlineAccess) {
		scopes = append(slices.Clone(scopes), offlineAccess)
	}
	metadataDocument := as.ClientIDMetadataDocumentSupported && c.clientID != ""
	if !metadataDocument && as.RegistrationEndpoint == "" {
		return nil, errUnsupported("The server only signs in clients registered with it in advance")
	}
	auth := &domain.ConnectorAuth{
		Issuer:                as.Issuer,
		AuthorizationEndpoint: as.AuthorizationEndpoint,
		TokenEndpoint:         as.TokenEndpoint,
		RevocationEndpoint:    as.RevocationEndpoint,
		IssuerInResponse:      as.IssuerInResponse,
		Resource:              resource,
		Scopes:                scopes,
	}
	if !metadataDocument {
		auth.RegistrationEndpoint = as.RegistrationEndpoint
	}
	return &Inspection{Auth: auth, metadataDocument: metadataDocument}, nil
}

// probe opens an MCP session at server without credentials, and reports
// how the server answered, with its bearer challenge if it sent one.
func (c *Client) probe(ctx context.Context, server string) (int, *oauthex.Challenge, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{
		"protocolVersion": mcpProtocolVersion, "capabilities": map[string]any{},
		"clientInfo": map[string]any{"name": clientName, "version": clientVersion}}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := c.http.direct.Do(req)
	if err != nil {
		return 0, nil, err
	}
	resp.Body.Close()
	challenges, err := oauthex.ParseWWWAuthenticate(resp.Header.Values("WWW-Authenticate"))
	if err != nil {
		return resp.StatusCode, nil, nil
	}
	for _, ch := range challenges {
		if ch.Scheme == "bearer" {
			return resp.StatusCode, &ch, nil
		}
	}
	return resp.StatusCode, nil, nil
}

// protectedResource reads the server's protected resource metadata: where
// its challenge says, or else at the well-known addresses for the server's
// path and for its host. A server without any is read as older servers
// were, as its own authorization server.
func (c *Client) protectedResource(ctx context.Context, server string, challenge *oauthex.Challenge) (*protectedResource, error) {
	var candidates []string
	if challenge != nil && challenge.Params["resource_metadata"] != "" {
		candidates = append(candidates, challenge.Params["resource_metadata"])
	}
	candidates = append(candidates, wellKnown(server, "oauth-protected-resource")...)
	for _, candidate := range candidates {
		var prm protectedResource
		found, err := c.fetchJSON(ctx, candidate, &prm)
		if err != nil || !found || len(prm.AuthorizationServers) == 0 {
			continue
		}
		if prm.Resource != "" && !covers(prm.Resource, server) {
			return nil, errUnsupported("The server's metadata names another resource")
		}
		if _, err := ServerURL(prm.AuthorizationServers[0]); err != nil {
			return nil, errUnsupported("The server's authorization server is not a public HTTPS address")
		}
		return &prm, nil
	}
	return nil, nil
}

// authServer reads the metadata of the authorization server issuer names,
// at the addresses RFC 8414 and OpenID Connect place it, and refuses one
// that is not who it says, or that cannot sign the user in with PKCE.
func (c *Client) authServer(ctx context.Context, issuer string) (*authServer, error) {
	for _, candidate := range authServerCandidates(issuer) {
		var as authServer
		found, err := c.fetchJSON(ctx, candidate, &as)
		if err != nil || !found || as.AuthorizationEndpoint == "" || as.TokenEndpoint == "" {
			continue
		}
		if strings.TrimSuffix(as.Issuer, "/") != strings.TrimSuffix(issuer, "/") {
			return nil, errUnsupported("The authorization server's metadata names another issuer")
		}
		if !slices.Contains(as.CodeChallengeMethodsSupported, pkceMethod) {
			return nil, errUnsupported("The authorization server does not support PKCE")
		}
		if as.ResponseTypesSupported != nil && !slices.Contains(as.ResponseTypesSupported, "code") {
			return nil, errUnsupported("The authorization server does not issue authorization codes")
		}
		for _, endpoint := range []string{as.AuthorizationEndpoint, as.TokenEndpoint, as.RegistrationEndpoint, as.RevocationEndpoint} {
			if _, err := ServerURL(endpoint); endpoint != "" && err != nil {
				return nil, errUnsupported("The authorization server's endpoints are not public HTTPS addresses")
			}
		}
		return &as, nil
	}
	return nil, errUnsupported("The server's authorization server publishes no metadata")
}

// wellKnown are the addresses of a well-known document for address: under
// its path, then at its host.
func wellKnown(address, name string) []string {
	u, err := url.Parse(address)
	if err != nil {
		return nil
	}
	root := u.Scheme + "://" + u.Host + "/.well-known/" + name
	if path := strings.TrimSuffix(u.EscapedPath(), "/"); path != "" {
		return []string{root + path, root}
	}
	return []string{root}
}

// authServerCandidates are where an issuer's metadata may be: the OAuth
// and OpenID Connect documents, for an issuer with a path both inserted
// before it and, for OpenID Connect, appended to it.
func authServerCandidates(issuer string) []string {
	u, err := url.Parse(issuer)
	if err != nil {
		return nil
	}
	root := u.Scheme + "://" + u.Host
	path := strings.TrimSuffix(u.EscapedPath(), "/")
	if path == "" {
		return []string{root + "/.well-known/oauth-authorization-server", root + "/.well-known/openid-configuration"}
	}
	return []string{
		root + "/.well-known/oauth-authorization-server" + path,
		root + "/.well-known/openid-configuration" + path,
		root + path + "/.well-known/openid-configuration",
	}
}

// origin is address without its path.
func origin(address string) string {
	u, err := url.Parse(address)
	if err != nil {
		return address
	}
	return u.Scheme + "://" + u.Host
}

// covers is whether resource names server: the same origin, at server's
// path or a path above it.
func covers(resource, server string) bool {
	r, err1 := url.Parse(resource)
	s, err2 := url.Parse(server)
	if err1 != nil || err2 != nil || !strings.EqualFold(r.Scheme, s.Scheme) || !strings.EqualFold(r.Host, s.Host) {
		return false
	}
	base := strings.TrimSuffix(r.EscapedPath(), "/")
	path := s.EscapedPath()
	return base == "" || path == base || strings.HasPrefix(path, base+"/")
}
