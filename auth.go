package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

type secret string

func (secret) LogValue() slog.Value { return slog.StringValue("REDACTED") }

func bearer(header string) string {
	const scheme = "bearer "
	if len(header) < len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return ""
	}
	return strings.TrimSpace(header[len(scheme):])
}

type principal struct {
	ID           string
	JWT          secret
	SyncToken    secret
	SyncExpires  time.Time
	Anonymous    bool
	AnonymousID  string
	InferenceKey secret
	// The validation already performed while recognizing this request.
	validatedFree *inferenceCredential
}

func (p *principal) scope() string {
	if !p.Anonymous {
		return "user:" + p.ID
	}
	return "anonymous:" + p.AnonymousID
}

type tokenBudget struct {
	Max       int `json:"max"`
	Used      int `json:"used"`
	Remaining int `json:"remaining"`
}

// Only free keys count requests; a subscriber's hourly cap is in tokens.
type rateLimit struct {
	Kind         any          `json:"kind"`
	MaxRequests  *int         `json:"maxRequests,omitempty"`
	Remaining    *int         `json:"remaining,omitempty"`
	InputTokens  *tokenBudget `json:"inputTokens,omitempty"`
	OutputTokens *tokenBudget `json:"outputTokens,omitempty"`
	ResetsAt     string       `json:"resetsAt,omitempty"`
}

func (l rateLimit) exhausted() bool { return l.Remaining != nil && *l.Remaining <= 0 }

func exhaustedLimit(kind string) rateLimit {
	return rateLimit{Kind: kind, MaxRequests: new(0), Remaining: new(0)}
}

// quota is the rate_limit object both credential exchanges answer with. A
// missing or zero maximum leaves that dimension unlimited.
type quota struct {
	MaxRequests     *int   `json:"max_requests"`
	Remaining       *int   `json:"remaining"`
	MaxInput        *int   `json:"max_input_tokens"`
	InputUsed       *int   `json:"input_tokens_used"`
	InputRemaining  *int   `json:"input_tokens_remaining"`
	MaxOutput       *int   `json:"max_output_tokens"`
	OutputUsed      *int   `json:"output_tokens_used"`
	OutputRemaining *int   `json:"output_tokens_remaining"`
	ResetsAt        string `json:"resets_at"`
}

func (q quota) limit(kind string) rateLimit {
	l := rateLimit{Kind: kind, InputTokens: budget(q.MaxInput, q.InputUsed, q.InputRemaining), OutputTokens: budget(q.MaxOutput, q.OutputUsed, q.OutputRemaining), ResetsAt: q.ResetsAt}
	if q.MaxRequests != nil && *q.MaxRequests > 0 && q.Remaining != nil {
		l.MaxRequests, l.Remaining = q.MaxRequests, q.Remaining
	}
	return l
}

func budget(limit, used, remaining *int) *tokenBudget {
	if limit == nil || used == nil || remaining == nil || *limit <= 0 {
		return nil
	}
	return &tokenBudget{*limit, *used, *remaining}
}

type inferenceCredential struct {
	Key     secret
	Expires time.Time
	Limit   rateLimit
	// firstParty marks a Tinfoil Chat access token, which only a subscriber
	// holds and whose conversations are screened.
	firstParty bool
}

// inferenceKeyHeader carries the credential a signed-in client minted for
// inference: its chat access token, or its free key without a subscription.
// It is read before the body, like the Clerk session beside it, which can
// mint one.
const inferenceKeyHeader = "X-Tinfoil-Inference-Key"

// Chat access tokens are minted by the control plane for the Tinfoil Chat
// client and verified here as the enclave shims verify them.
const (
	chatTokenType     = "at+jwt"
	chatTokenAudience = "https://inference.tinfoil.sh"
	chatTokenScope    = "inference:api"
	chatTokenClient   = "tinfoil-chat"
	chatTokenProduct  = "chat"
)

type chatTokenClaims struct {
	ClientID string `json:"client_id"`
	Scope    string `json:"scope"`
	Product  string `json:"product"`
	jwt.RegisteredClaims
}

// Callers remembered by the caches below, each evicting its least recently
// used entry when full. A free key validated within freeKeyTTL admits its
// caller without asking the control plane again.
const (
	maxCachedCallers = 10000
	freeKeyTTL       = time.Minute
	credentialTTL    = time.Hour
)

var (
	errCredentialRequired   = apiErr(401, "CREDENTIAL_REQUIRED", "Chat access is missing or expired")
	errSubscriptionRequired = apiErr(403, "SUBSCRIPTION_REQUIRED", "This requires a Tinfoil Chat subscription")
)

type authenticator struct {
	verify       func(context.Context, string) (string, error)
	chatKeys     jwt.Keyfunc // the control plane's chat token signing keys
	chatIssuer   string
	client       *http.Client
	controlplane string
	cachesOnce   sync.Once
	newest       *expirable.LRU[string, inferenceCredential]
	valid        *expirable.LRU[string, struct{}]
}

func (a *authenticator) caches() {
	a.cachesOnce.Do(func() {
		a.newest = expirable.NewLRU[string, inferenceCredential](maxCachedCallers, nil, credentialTTL)
		a.valid = expirable.NewLRU[string, struct{}](maxCachedCallers, nil, freeKeyTTL)
	})
}

func (a *authenticator) validKeys() *expirable.LRU[string, struct{}] { a.caches(); return a.valid }

func newAuthenticator(ctx context.Context, issuer, cp string) (*authenticator, error) {
	if !strings.HasPrefix(issuer, "https://") {
		return nil, errors.New("CLERK_ISSUER must be an HTTPS issuer URL")
	}
	issuer = strings.TrimRight(issuer, "/")
	kf, err := keyfunc.NewDefaultCtx(ctx, []string{issuer + "/.well-known/jwks.json"})
	if err != nil {
		return nil, err
	}
	chatKeys, err := keyfunc.NewDefaultCtx(ctx, []string{cp + "/.well-known/jwks.json"})
	if err != nil {
		return nil, err
	}
	options := []jwt.ParserOption{jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}), jwt.WithIssuer(issuer), jwt.WithExpirationRequired(), jwt.WithLeeway(30 * time.Second)}
	a := &authenticator{client: &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, controlplane: cp, chatKeys: chatKeys.Keyfunc, chatIssuer: cp}
	a.verify = func(ctx context.Context, token string) (string, error) {
		parsed, err := jwt.NewParser(options...).Parse(token, kf.KeyfuncCtx(ctx))
		if err != nil || !parsed.Valid {
			return "", errors.New("invalid session")
		}
		sub, err := parsed.Claims.GetSubject()
		if err != nil || sub == "" {
			return "", errors.New("missing subject")
		}
		return sub, nil
	}
	return a, nil
}

// authenticate reads who a request is from: a signed-in caller's Clerk
// session, or, where anonymous is allowed, a signed-out caller's free key.
func (a *authenticator) authenticate(r *http.Request, anonymous bool) (*principal, error) {
	if a == nil {
		return nil, apiErr(503, "UNAUTHENTICATED", "Authentication is not configured")
	}
	token := bearer(r.Header.Get("Authorization"))
	if token == "" {
		return nil, apiErr(401, "UNAUTHENTICATED", "A Clerk session is required")
	}
	if anonymous && strings.HasPrefix(token, "free_") && len(token) > len("free_") && len(token) <= 256 {
		// Possession scopes ephemeral state. The prefix is not validation:
		// recognize checks the key with the control plane before any work.
		return &principal{Anonymous: true, AnonymousID: hashHex([]byte(token)), InferenceKey: secret(token)}, nil
	}
	id, err := a.verify(r.Context(), token)
	if err != nil {
		return nil, apiErr(401, "UNAUTHENTICATED", "The Clerk session is invalid or expired")
	}
	p := &principal{ID: id, JWT: secret(token), InferenceKey: secret(strings.TrimSpace(r.Header.Get(inferenceKeyHeader)))}
	// Every request carrying a chat token keeps the newest one at hand for
	// runs that outlive theirs.
	if p.InferenceKey != "" && !strings.HasPrefix(string(p.InferenceKey), "free_") {
		a.chatToken(p)
	}
	return p, nil
}

// recognize refuses a caller before its body is read: a signed-out caller
// is served only with a free key the control plane accepted, recently or now.
func (a *authenticator) recognize(ctx context.Context, p *principal) error {
	if !p.Anonymous {
		return nil
	}
	if _, ok := a.validKeys().Get(hashHex([]byte(p.InferenceKey))); ok {
		return nil
	}
	credential, err := a.freeKey(ctx, p)
	if err == nil {
		p.validatedFree = &credential
	}
	return err
}

// keyValidation asks the control plane whether a free key is valid, and
// for the quota it has left.
type keyValidation struct {
	APIKey           secret `json:"api_key"`
	IncludeRateLimit bool   `json:"include_rate_limit"`
}

// validatedKey is a valid key's validation, with a free chat key's quota.
type validatedKey struct {
	OK        bool   `json:"ok"`
	RateLimit *quota `json:"rate_limit"`
}

// freeKey validates a free key, a signed-out caller's or a signed-in
// caller's without a subscription, and reads its quota. A key validated
// recently admits its caller without asking again.
func (a *authenticator) freeKey(ctx context.Context, p *principal) (inferenceCredential, error) {
	if p.validatedFree != nil {
		credential := *p.validatedFree
		p.validatedFree = nil
		return credential, nil
	}
	credential, err := a.validateFreeKey(ctx, p)
	if err == nil {
		a.validKeys().Add(hashHex([]byte(p.InferenceKey)), struct{}{})
	}
	return credential, err
}

func (a *authenticator) validateFreeKey(ctx context.Context, p *principal) (inferenceCredential, error) {
	// Reuse the validation endpoint already used by shims on Tinfoil hosts,
	// asking it for the key's quota. No key issuance, client IP, or expiry
	// metadata comes from this call.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.controlplane+"/api/shim/validate-key", bytes.NewReader(raw(keyValidation{APIKey: p.InferenceKey, IncludeRateLimit: true})))
	if err != nil {
		return inferenceCredential{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return inferenceCredential{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<10))
	if err != nil {
		return inferenceCredential{}, err
	}
	// Downstream shims still check validity, model access, and quota on each
	// call. A control plane that does not report the quota answers "OK".
	if resp.StatusCode == http.StatusOK && string(body) == "OK" {
		return inferenceCredential{Key: p.InferenceKey}, nil
	}
	var validated validatedKey
	if resp.StatusCode == http.StatusOK && json.Unmarshal(body, &validated) == nil && validated.OK {
		credential := inferenceCredential{Key: p.InferenceKey}
		if validated.RateLimit != nil {
			credential.Limit = validated.RateLimit.limit("free_daily")
		}
		return credential, nil
	}
	var detail object
	json.Unmarshal(body, &detail)
	if resp.StatusCode == http.StatusTooManyRequests && str(obj(detail["error"])["code"]) == "insufficient_quota" {
		// An exhausted key may still open the session and show the quota
		// banner. acceptTurn refuses to start work with this zero balance.
		return inferenceCredential{Key: p.InferenceKey, Limit: exhaustedLimit("free_daily")}, nil
	}
	switch {
	case resp.StatusCode/100 == 2:
		return inferenceCredential{}, errors.New("invalid key validation response")
	case resp.StatusCode == http.StatusPaymentRequired:
		return inferenceCredential{}, apiErr(403, "UPSTREAM_REFUSED", "This chat key is disabled")
	case resp.StatusCode == http.StatusUnauthorized && str(detail["error"]) != "":
		// A refused caller address, not a refused key: the controlplane
		// answers only Tinfoil hosts registered for key validation.
		slog.Error("controlplane refused key validation from this host", "status", resp.StatusCode)
		return inferenceCredential{}, apiErr(503, "UPSTREAM_REFUSED", "Anonymous chat is unavailable on this deployment")
	case resp.StatusCode == http.StatusServiceUnavailable:
		e := apiErr(503, "UPSTREAM_REFUSED", "Key validation is temporarily unavailable")
		e.RetryAfter = retryAfter(resp.Header)
		return inferenceCredential{}, e
	}
	return inferenceCredential{}, withRetryAfter(downstreamError(resp.StatusCode, body), resp.Header)
}

// inference is the credential a caller's model calls carry: a signed-out
// caller's free key, or the chat token or free key a signed-in client minted.
func (a *authenticator) inference(ctx context.Context, p *principal) (inferenceCredential, error) {
	switch {
	case p.InferenceKey == "" && p.Anonymous:
		return inferenceCredential{}, apiErr(401, "UNAUTHENTICATED", "An anonymous chat key is required")
	case p.InferenceKey == "":
		return inferenceCredential{}, errCredentialRequired
	case strings.HasPrefix(string(p.InferenceKey), "free_"):
		return a.freeKey(ctx, p)
	}
	return a.chatToken(p)
}

// chatToken verifies a signed-in caller's chat access token: the control
// plane signed it for the Tinfoil Chat client, for this caller, and it has
// not expired.
func (a *authenticator) chatToken(p *principal) (inferenceCredential, error) {
	var claims chatTokenClaims
	parsed, err := jwt.NewParser(jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(a.chatIssuer), jwt.WithAudience(chatTokenAudience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithLeeway(30*time.Second)).
		ParseWithClaims(string(p.InferenceKey), &claims, a.chatKeys)
	if err != nil || !parsed.Valid {
		return inferenceCredential{}, errCredentialRequired
	}
	kind, _ := parsed.Header["typ"].(string)
	if !strings.EqualFold(kind, chatTokenType) || p.Anonymous || claims.Subject != p.ID || claims.ID == "" || claims.ClientID != chatTokenClient || claims.Product != chatTokenProduct || !slices.Contains(strings.Fields(claims.Scope), chatTokenScope) {
		return inferenceCredential{}, errCredentialRequired
	}
	credential := inferenceCredential{Key: p.InferenceKey, Expires: claims.ExpiresAt.Time, firstParty: true}
	a.caches()
	if held, ok := a.newest.Get(p.scope()); !ok || credential.Expires.After(held.Expires) {
		a.newest.Add(p.scope(), credential)
	}
	return credential, nil
}

// renewed is the newest chat token the caller presented, for a run whose
// own was refused, unless it is the refused one or has expired.
func (a *authenticator) renewed(p *principal, refused secret) (inferenceCredential, error) {
	a.caches()
	credential, ok := a.newest.Get(p.scope())
	if !ok || credential.Key == refused || !credential.Expires.After(nowUTC()) {
		return inferenceCredential{}, errCredentialRequired
	}
	return credential, nil
}

// subscriber is a signed-in caller holding a chat token, which only a Tinfoil
// Chat subscription grants.
func (a *authenticator) subscriber(r *http.Request) (*principal, error) {
	p, err := a.authenticate(r, false)
	if err != nil {
		return nil, err
	}
	credential, err := a.inference(r.Context(), p)
	if err != nil {
		return nil, err
	}
	if !credential.firstParty {
		return nil, errSubscriptionRequired
	}
	return p, nil
}

// debited is the quota after a run's request and the tokens it spent.
func (l rateLimit) debited(spent usage) rateLimit {
	if l.Remaining != nil {
		l.Remaining = new(max(0, *l.Remaining-1))
	}
	l.InputTokens = l.InputTokens.spend(spent.Prompt)
	l.OutputTokens = l.OutputTokens.spend(spent.Completion)
	return l
}

// none is the quota with no request left.
func (l rateLimit) none() rateLimit {
	if l.MaxRequests == nil {
		l.MaxRequests = new(0)
	}
	l.Remaining = new(0)
	return l
}

// spend is a token budget after tokens are spent from it.
func (b *tokenBudget) spend(tokens int64) *tokenBudget {
	if b == nil {
		return nil
	}
	return &tokenBudget{Max: b.Max, Used: b.Used + int(tokens), Remaining: max(0, b.Remaining-int(tokens))}
}

// exhausted is the quota a refusal for a spent one leaves: no request left,
// and a free key's quota is its daily one.
func exhausted(p *principal, held rateLimit) rateLimit {
	if strings.HasPrefix(string(p.InferenceKey), "free_") {
		held.Kind = "free_daily"
	}
	return held.none()
}
