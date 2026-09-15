package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type principal struct {
	ID           string
	JWT          secret
	Anonymous    bool
	AnonymousID  string
	InferenceKey secret
}

func (p *principal) scope() string {
	if !p.Anonymous {
		return "user:" + p.ID
	}
	return "anonymous:" + p.AnonymousID
}

type rateLimit struct {
	Kind        any    `json:"kind"`
	MaxRequests int    `json:"maxRequests"`
	Remaining   int    `json:"remaining"`
	ResetsAt    string `json:"resetsAt,omitempty"`
}
type inferenceCredential struct {
	Key     secret
	Expires time.Time
	Limit   rateLimit
}
type authenticator struct {
	verify       func(context.Context, string) (string, error)
	client       *http.Client
	controlplane string
	mu           sync.Mutex
	credentials  map[string]inferenceCredential
	refreshing   map[string]chan struct{}
}

func newAuthenticator(ctx context.Context, issuer, audience, cp string) (*authenticator, error) {
	if !strings.HasPrefix(issuer, "https://") {
		return nil, errors.New("CLERK_ISSUER must be an HTTPS issuer URL")
	}
	issuer = strings.TrimRight(issuer, "/")
	kf, err := keyfunc.NewDefaultCtx(ctx, []string{issuer + "/.well-known/jwks.json"})
	if err != nil {
		return nil, err
	}
	options := []jwt.ParserOption{jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}), jwt.WithIssuer(issuer), jwt.WithExpirationRequired(), jwt.WithLeeway(30 * time.Second)}
	if audience != "" {
		options = append(options, jwt.WithAudience(audience))
	}
	a := &authenticator{client: &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, controlplane: cp, credentials: map[string]inferenceCredential{}}
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

func (a *authenticator) authenticate(r *http.Request, anonymous bool) (*principal, error) {
	if a == nil {
		return nil, apiErr(503, "UNAUTHENTICATED", "Authentication is not configured")
	}
	header := r.Header.Get("Authorization")
	if header == "" && anonymous {
		return &principal{Anonymous: true}, nil
	}
	token := bearer(header)
	if token == "" {
		return nil, apiErr(401, "UNAUTHENTICATED", "A Clerk session is required")
	}
	if anonymous && strings.HasPrefix(token, "free_") && len(token) > len("free_") && len(token) <= 256 {
		// Possession scopes ephemeral state. The prefix is not validation:
		// inference checks the key with controlplane before accepting work.
		return &principal{Anonymous: true, AnonymousID: hashHex([]byte(token)), InferenceKey: secret(token)}, nil
	}
	id, err := a.verify(r.Context(), token)
	if err != nil {
		return nil, apiErr(401, "UNAUTHENTICATED", "The Clerk session is invalid or expired")
	}
	return &principal{ID: id, JWT: secret(token)}, nil
}

func (a *authenticator) anonymousInference(ctx context.Context, p *principal) (inferenceCredential, error) {
	if p.InferenceKey == "" {
		return inferenceCredential{}, apiErr(401, "UNAUTHENTICATED", "An anonymous chat key is required")
	}
	// Reuse the validation endpoint already used by shims on Tinfoil hosts.
	// No key issuance, client IP, expiry, or quota metadata comes from this call.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.controlplane+"/api/shim/validate-key", bytes.NewReader(raw(object{"api_key": p.InferenceKey})))
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
	if resp.StatusCode == http.StatusOK && string(body) == "OK" {
		// Downstream shims still check validity, model access, and quota on
		// each call. The browser's issuance metadata is only for its display.
		return inferenceCredential{Key: p.InferenceKey}, nil
	}
	var detail object
	if resp.StatusCode == http.StatusTooManyRequests && json.Unmarshal(body, &detail) == nil && str(obj(detail["error"])["code"]) == "insufficient_quota" {
		// An exhausted key may still open the session and show the quota
		// banner. acceptTurn refuses to start work with this zero balance.
		return inferenceCredential{Key: p.InferenceKey, Limit: rateLimit{Kind: "free_daily"}}, nil
	}
	if resp.StatusCode/100 == 2 {
		return inferenceCredential{}, errors.New("invalid key validation response")
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return inferenceCredential{}, apiErr(403, "UPSTREAM_REFUSED", "This chat key is disabled")
	}
	return inferenceCredential{}, downstreamError(resp.StatusCode, body)
}

func (a *authenticator) inference(ctx context.Context, p *principal) (inferenceCredential, error) {
	if p.Anonymous {
		return a.anonymousInference(ctx, p)
	}
	a.mu.Lock()
	if cached, ok := a.credentials[p.scope()]; ok && nowUTC().Add(time.Minute).Before(cached.Expires) {
		a.mu.Unlock()
		return cached, nil
	}
	if a.refreshing == nil {
		a.refreshing = map[string]chan struct{}{}
	}
	if pending := a.refreshing[p.scope()]; pending != nil {
		a.mu.Unlock()
		select {
		case <-pending:
			return a.inference(ctx, p)
		case <-ctx.Done():
			return inferenceCredential{}, ctx.Err()
		}
	}
	a.refreshing[p.scope()] = make(chan struct{})
	a.mu.Unlock()
	defer func() { a.mu.Lock(); close(a.refreshing[p.scope()]); delete(a.refreshing, p.scope()); a.mu.Unlock() }()
	paths := []string{"/api/chat/token", "/api/keys/chat"}
	for i, path := range paths {
		req, _ := http.NewRequestWithContext(ctx, "GET", a.controlplane+path, nil)
		if p.JWT != "" {
			req.Header.Set("Authorization", "Bearer "+string(p.JWT))
		}
		resp, err := a.client.Do(req)
		if err != nil {
			return inferenceCredential{}, err
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<10))
		resp.Body.Close()
		if err != nil {
			return inferenceCredential{}, err
		}
		if resp.StatusCode/100 != 2 {
			if i+1 < len(paths) && (resp.StatusCode == 402 || resp.StatusCode == 403) {
				continue
			}
			return inferenceCredential{}, downstreamError(resp.StatusCode, b)
		}
		var in struct {
			Key     secret `json:"key"`
			Expires string `json:"expires_at"`
			Free    bool   `json:"is_free_tier"`
			Rate    struct {
				Max       int    `json:"max_requests"`
				Remaining int    `json:"remaining"`
				Resets    string `json:"resets_at"`
			} `json:"rate_limit"`
		}
		if json.Unmarshal(b, &in) != nil || in.Key == "" {
			return inferenceCredential{}, errors.New("invalid inference credential response")
		}
		exp, err := time.Parse(time.RFC3339, in.Expires)
		if err != nil || !exp.After(nowUTC()) {
			return inferenceCredential{}, errors.New("invalid inference credential expiry")
		}
		cred := inferenceCredential{Key: in.Key, Expires: exp, Limit: rateLimit{MaxRequests: in.Rate.Max, Remaining: in.Rate.Remaining, ResetsAt: in.Rate.Resets}}
		if in.Free {
			cred.Limit.Kind = "free_daily"
		}
		a.mu.Lock()
		if a.credentials == nil {
			a.credentials = map[string]inferenceCredential{}
		}
		for id, entry := range a.credentials {
			if !entry.Expires.After(nowUTC()) {
				delete(a.credentials, id)
			}
		}
		if len(a.credentials) >= 10000 {
			a.mu.Unlock()
			return inferenceCredential{}, apiErr(503, "UPSTREAM_REFUSED", "The session cache is at capacity")
		}
		a.credentials[p.scope()] = cred
		a.mu.Unlock()
		return cred, nil
	}
	return inferenceCredential{}, errors.New("no inference credential")
}
func (a *authenticator) consumed(p *principal) rateLimit {
	a.mu.Lock()
	defer a.mu.Unlock()
	cred := a.credentials[p.scope()]
	if cred.Limit.Kind != nil && cred.Limit.Remaining > 0 {
		cred.Limit.Remaining--
		a.credentials[p.scope()] = cred
	}
	return cred.Limit
}

func (a *authenticator) currentLimit(p *principal) rateLimit {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.credentials[p.scope()].Limit
}

func (a *authenticator) reject(p *principal, rejected secret) {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Another run may already have replaced the rejected credential.
	if current := a.credentials[p.scope()]; current.Key == rejected {
		delete(a.credentials, p.scope())
	}
}
