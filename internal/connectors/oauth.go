package connectors

import (
	"bytes"
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"golang.org/x/oauth2"
)

const (
	// pendingLifetime is how long a user has to finish signing in.
	pendingLifetime = 15 * time.Minute
	// clientURI is the page a server shows users for the harness's client.
	clientURI = "https://tinfoil.sh"
	// The ways a client registered dynamically authenticates to the token
	// endpoint.
	authNone       = "none"
	authSecretPost = "client_secret_post"
	// revokeTimeout bounds the revocation of a removed connector's tokens.
	revokeTimeout = 5 * time.Second
)

// Client signs the harness in to the servers users connect. The harness is
// one client of each authorization server: named by the metadata document at
// clientID where a server reads one, and registered dynamically otherwise.
// Users come back to redirectURL once they sign in.
type Client struct {
	http        clients
	clientID    string
	redirectURL string
}

// New is a client that reaches servers over the public internet.
func New(clientID, redirectURL string) *Client {
	return NewWithTransport(clientID, redirectURL, transport())
}

// NewWithTransport is a client whose requests go through rt, which must keep
// them to public addresses.
func NewWithTransport(clientID, redirectURL string, rt http.RoundTripper) *Client {
	return &Client{http: newClients(rt), clientID: clientID, redirectURL: redirectURL}
}

func errSignIn(reason string) error {
	return api.Err(400, "CONNECTOR_SIGN_IN_FAILED", reason)
}

// Register names the harness's client to a's authorization server: by its
// metadata document where the server reads one, or by registering it, unless
// previous already registered one there.
func (c *Client) Register(ctx context.Context, ins *Inspection, previous *domain.ConnectorAuth) error {
	a := ins.Auth
	if ins.metadataDocument {
		a.ClientID, a.TokenAuthMethod = c.clientID, authNone
		return nil
	}
	if previous != nil && previous.ClientID != "" && previous.Issuer == a.Issuer && previous.RegistrationEndpoint == a.RegistrationEndpoint {
		a.ClientID, a.ClientSecret, a.TokenAuthMethod = previous.ClientID, previous.ClientSecret, previous.TokenAuthMethod
		return nil
	}
	return c.registerDynamically(ctx, a)
}

// registerDynamically registers the harness as a public client of a's
// authorization server (RFC 7591).
func (c *Client) registerDynamically(ctx context.Context, a *domain.ConnectorAuth) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	body, _ := json.Marshal(map[string]any{
		"client_name":                clientName,
		"client_uri":                 clientURI,
		"redirect_uris":              []string{c.redirectURL},
		"grant_types":                []string{"authorization_code", "refresh_token"},
		"response_types":             []string{"code"},
		"token_endpoint_auth_method": authNone,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.RegistrationEndpoint, bytes.NewReader(body))
	if err != nil {
		return errUnsupported("The server's registration endpoint is invalid")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.direct.Do(req)
	if err != nil {
		return errUnreachable()
	}
	defer resp.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, maxMetadataBytes))
	if err != nil || resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errUnsupported("The server refused to register Tinfoil Chat")
	}
	var registered struct {
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		AuthMethod   string `json:"token_endpoint_auth_method"`
	}
	if json.Unmarshal(answer, &registered) != nil || registered.ClientID == "" {
		return errUnsupported("The server's registration answer is invalid")
	}
	a.ClientID, a.ClientSecret, a.TokenAuthMethod = registered.ClientID, registered.ClientSecret, registered.AuthMethod
	return nil
}

// Begin starts the user's sign-in through a's authorization server: what
// the harness keeps until they come back from it, and the page to send them
// to.
func (c *Client) Begin(a *domain.ConnectorAuth) (*domain.ConnectorPending, string) {
	pending := &domain.ConnectorPending{
		State:     oauth2.GenerateVerifier(),
		Verifier:  oauth2.GenerateVerifier(),
		ExpiresAt: api.Now().Add(pendingLifetime).UTC().Format(time.RFC3339Nano),
		Auth:      a,
	}
	page := c.config(a).AuthCodeURL(pending.State, oauth2.S256ChallengeOption(pending.Verifier), oauth2.SetAuthURLParam("resource", a.Resource))
	return pending, page
}

// Complete trades the code the user came back with for tokens, once it has
// checked the sign-in is the one pending and the authorization server the
// one it went through, and returns the sign-in with them.
func (c *Client) Complete(ctx context.Context, pending *domain.ConnectorPending, code, state, issuer string) (*domain.ConnectorAuth, error) {
	if pending == nil || pending.Auth == nil {
		return nil, errSignIn("No sign-in is pending")
	}
	expires, err := time.Parse(time.RFC3339Nano, pending.ExpiresAt)
	if err != nil || !api.Now().Before(expires) {
		return nil, errSignIn("The sign-in expired; start it again")
	}
	if subtle.ConstantTimeCompare([]byte(state), []byte(pending.State)) != 1 {
		return nil, errSignIn("The sign-in does not match the one started")
	}
	a := *pending.Auth
	if issuer != "" && strings.TrimSuffix(issuer, "/") != strings.TrimSuffix(a.Issuer, "/") || issuer == "" && a.IssuerInResponse {
		return nil, errSignIn("The sign-in came back from another authorization server")
	}
	client := &http.Client{Transport: withResource{inner: c.http.direct.Transport, resource: a.Resource}, CheckRedirect: c.http.direct.CheckRedirect}
	ctx, cancel := context.WithTimeout(context.WithValue(ctx, oauth2.HTTPClient, client), requestTimeout)
	defer cancel()
	token, err := c.config(&a).Exchange(ctx, code, oauth2.VerifierOption(pending.Verifier))
	if err != nil {
		return nil, errSignIn("The authorization server refused the sign-in")
	}
	keep(&a, token)
	return &a, nil
}

// config is a's authorization server as x/oauth2 reaches it.
func (c *Client) config(a *domain.ConnectorAuth) *oauth2.Config {
	style := oauth2.AuthStyleInHeader
	if a.ClientSecret == "" || a.TokenAuthMethod == authSecretPost || a.TokenAuthMethod == authNone {
		style = oauth2.AuthStyleInParams
	}
	return &oauth2.Config{
		ClientID:     a.ClientID,
		ClientSecret: a.ClientSecret,
		Endpoint:     oauth2.Endpoint{AuthURL: a.AuthorizationEndpoint, TokenURL: a.TokenEndpoint, AuthStyle: style},
		RedirectURL:  c.redirectURL,
		Scopes:       a.Scopes,
	}
}

// keep stores token in a, holding on to a refresh token the server did not
// replace.
func keep(a *domain.ConnectorAuth, token *oauth2.Token) {
	a.AccessToken, a.TokenType, a.Expiry = token.AccessToken, token.TokenType, ""
	if token.RefreshToken != "" {
		a.RefreshToken = token.RefreshToken
	}
	if !token.Expiry.IsZero() {
		a.Expiry = token.Expiry.UTC().Format(time.RFC3339Nano)
	}
}

// Forget clears what a signs in with, as when its authorization server
// refuses it.
func Forget(a *domain.ConnectorAuth) {
	a.AccessToken, a.RefreshToken, a.TokenType, a.Expiry = "", "", "", ""
}

// Credentials is where a session reads and keeps a connector's sign-in as
// stored, which other sessions of the same connector share: other replies,
// on this instance of the harness or another.
type Credentials interface {
	// Claim claims the refresh of the tokens the session holds, and returns
	// the tokens as stored. Only the session that holds the claim presents
	// the refresh token; one that finds the tokens changed takes them
	// instead.
	Claim(ctx context.Context, held *domain.ConnectorAuth) (domain.ConnectorAuth, domain.RefreshClaim, error)
	// Save applies change to the stored sign-in and ends the session's
	// claim, unless the user signed in again since the refresh token used
	// was issued.
	Save(ctx context.Context, used string, change func(*domain.ConnectorAuth)) error
}

// ErrSignedOut is a connector whose authorization server no longer accepts
// what it signs in with, until the user signs in again.
var ErrSignedOut = errors.New("the connector is signed out")

const (
	// refreshPoll is how often a session looks again at tokens another
	// session is refreshing, and refreshWait how long it does before it
	// gives up: a little longer than a refresh may take.
	refreshPoll = 500 * time.Millisecond
	refreshWait = requestTimeout + 5*time.Second
	// flushTimeout bounds a closing session's last try at keeping tokens it
	// refreshed.
	flushTimeout = 10 * time.Second
)

// errRefreshing is a connector whose tokens another session has been
// refreshing for longer than a session waits.
var errRefreshing = errors.New("the connector's tokens are being refreshed")

// keptSource is a session's access token, refreshed when it expires, by
// one session at a time, and kept once refreshed, so a rotated refresh
// token is not lost.
type keptSource struct {
	ctx    context.Context
	client *http.Client
	// config is the authorization server of the tokens the session holds,
	// which change when it takes those of a later sign-in.
	config func(*domain.ConnectorAuth) *oauth2.Config
	creds  Credentials

	mu   sync.Mutex
	auth domain.ConnectorAuth
	// unsaved is what a refresh changed that the session could not keep,
	// with the refresh token it used, which it tries to keep again until it
	// closes.
	unsaved     func(*domain.ConnectorAuth)
	unsavedUsed string
}

func (k *keptSource) Token() (*oauth2.Token, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.flush(k.ctx)
	return k.token("")
}

// renew is a token in place of one the server refused though it had not
// expired, as one revoked.
func (k *keptSource) renew() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	_, err := k.token(k.auth.AccessToken)
	return err
}

// held is the access token the session holds, if it is good: neither
// expired nor the one refused.
func (k *keptSource) held(refused string) *oauth2.Token {
	t := &oauth2.Token{AccessToken: k.auth.AccessToken, TokenType: k.auth.TokenType, Expiry: k.auth.TokenExpiry()}
	if !t.Valid() || t.AccessToken == refused {
		return nil
	}
	return t
}

// token is the session's access token, refreshed unless it is good. The
// session refreshes it only while it holds the claim to; while another
// session does, it waits for that session's tokens.
func (k *keptSource) token(refused string) (*oauth2.Token, error) {
	waiting, cancel := context.WithTimeout(k.ctx, refreshWait)
	defer cancel()
	for {
		if t := k.held(refused); t != nil {
			return t, nil
		}
		if k.auth.RefreshToken == "" {
			return nil, ErrSignedOut
		}
		stored, claim, err := k.creds.Claim(k.ctx, &k.auth)
		if err != nil {
			return nil, err
		}
		if claim == domain.RefreshClaimed {
			return k.refresh()
		}
		if claim == domain.RefreshChanged {
			k.auth = stored
			continue
		}
		select {
		case <-waiting.Done():
			return nil, cmp.Or(k.ctx.Err(), errRefreshing)
		case <-time.After(refreshPoll):
		}
	}
}

// refresh trades the session's refresh token for new tokens, which it
// keeps. It holds the claim to.
func (k *keptSource) refresh() (*oauth2.Token, error) {
	used := k.auth.RefreshToken
	client := &http.Client{Transport: withResource{inner: k.client.Transport, resource: k.auth.Resource}, CheckRedirect: k.client.CheckRedirect}
	ctx, cancel := context.WithTimeout(context.WithValue(k.ctx, oauth2.HTTPClient, client), requestTimeout)
	defer cancel()
	token, err := k.config(&k.auth).TokenSource(ctx, &oauth2.Token{RefreshToken: used}).Token()
	var refusal *oauth2.RetrieveError
	if errors.As(err, &refusal) && refusal.ErrorCode == "invalid_grant" {
		Forget(&k.auth)
		k.keep(used, Forget)
		return nil, ErrSignedOut
	}
	if err != nil {
		// The tokens are as they were; another session may try.
		if err := k.creds.Save(k.ctx, used, func(*domain.ConnectorAuth) {}); err != nil {
			slog.Warn("a connector's refresh stays claimed until its lease ends", "error", err)
		}
		return nil, err
	}
	keep(&k.auth, token)
	refreshed := k.auth
	k.keep(used, func(a *domain.ConnectorAuth) {
		a.AccessToken, a.RefreshToken, a.TokenType, a.Expiry = refreshed.AccessToken, refreshed.RefreshToken, refreshed.TokenType, refreshed.Expiry
	})
	return token, nil
}

// keep stores what a refresh changed. The session goes on with its tokens
// when it cannot, and tries again with its next request and as it closes:
// the refresh token it used is spent, so what it holds is all that signs
// the connector in.
func (k *keptSource) keep(used string, change func(*domain.ConnectorAuth)) {
	if err := k.creds.Save(k.ctx, used, change); err != nil {
		k.unsaved, k.unsavedUsed = change, used
		slog.Warn("a connector's refreshed tokens are not stored yet", "error", err)
	}
}

// flush tries again to keep what a refresh changed and the session could
// not.
func (k *keptSource) flush(ctx context.Context) {
	if k.unsaved != nil && k.creds.Save(ctx, k.unsavedUsed, k.unsaved) == nil {
		k.unsaved = nil
	}
}

// signOut forgets tokens the session cannot use or renew, as stored too, so
// the connector shows that its user must sign in again. Tokens stored since
// are those of a later sign-in, and stay.
func (k *keptSource) signOut() {
	k.mu.Lock()
	defer k.mu.Unlock()
	held := k.auth
	if held.AccessToken == "" && held.RefreshToken == "" {
		return
	}
	Forget(&k.auth)
	k.keep(held.RefreshToken, func(a *domain.ConnectorAuth) {
		if a.AccessToken == held.AccessToken {
			Forget(a)
		}
	})
}

// close makes the session's last try at keeping tokens it refreshed, which
// the request that ends it does not cut short.
func (k *keptSource) close() {
	if k == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.unsaved == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(k.ctx), flushTimeout)
	defer cancel()
	if k.flush(ctx); k.unsaved != nil {
		slog.Error("a connector's refreshed tokens were lost; its user signs in again")
	}
}

// withResource names the MCP server a token is for in each token request,
// as the MCP authorization spec requires of refreshes too, which x/oauth2
// has no parameter for.
type withResource struct {
	inner    http.RoundTripper
	resource string
}

func (w withResource) RoundTrip(r *http.Request) (*http.Response, error) {
	if w.resource == "" || r.Method != http.MethodPost || r.Body == nil || r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
		return w.inner.RoundTrip(r)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxMetadataBytes))
	r.Body.Close()
	if err != nil {
		return nil, err
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	if form.Get("resource") == "" {
		form.Set("resource", w.resource)
	}
	encoded := form.Encode()
	r = r.Clone(r.Context())
	r.Body, r.ContentLength = io.NopCloser(strings.NewReader(encoded)), int64(len(encoded))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(encoded)), nil }
	return w.inner.RoundTrip(r)
}

// Revoke asks a's authorization server to revoke what it signs in with
// (RFC 7009), as the connector is removed. A server that does not answer
// is left to expire the tokens itself.
func (c *Client) Revoke(ctx context.Context, a *domain.ConnectorAuth) {
	token, hint := a.RefreshToken, "refresh_token"
	if token == "" {
		token, hint = a.AccessToken, "access_token"
	}
	if a.RevocationEndpoint == "" || token == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, revokeTimeout)
	defer cancel()
	form := url.Values{"token": {token}, "token_type_hint": {hint}}
	if a.ClientSecret == "" || a.TokenAuthMethod == authSecretPost {
		form.Set("client_id", a.ClientID)
		if a.ClientSecret != "" {
			form.Set("client_secret", a.ClientSecret)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.RevocationEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if a.ClientSecret != "" && a.TokenAuthMethod != authSecretPost {
		req.SetBasicAuth(url.QueryEscape(a.ClientID), url.QueryEscape(a.ClientSecret))
	}
	if resp, err := c.http.direct.Do(req); err == nil {
		resp.Body.Close()
	}
}
