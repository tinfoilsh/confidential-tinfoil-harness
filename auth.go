package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
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
}

// hourlyCap is a subscriber's refused exchange, remembered until the cap resets.
type hourlyCap struct {
	refusal apiError
	limit   rateLimit
	until   time.Time
}
type authenticator struct {
	verify       func(context.Context, string) (string, error)
	client       *http.Client
	controlplane string
	mu           sync.Mutex
	credentials  map[string]inferenceCredential
	capped       map[string]hourlyCap
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

func (a *authenticator) inference(ctx context.Context, p *principal) (inferenceCredential, error) {
	if p.Anonymous {
		return a.anonymousInference(ctx, p)
	}
	a.mu.Lock()
	if cached, ok := a.credentials[p.scope()]; ok && nowUTC().Add(time.Minute).Before(cached.Expires) {
		a.mu.Unlock()
		return cached, nil
	}
	if capped, ok := a.capped[p.scope()]; ok && nowUTC().Before(capped.until) {
		a.mu.Unlock()
		refusal := capped.refusal
		refusal.RetryAfter = max(1, int(math.Ceil(time.Until(capped.until).Seconds())))
		return inferenceCredential{}, &refusal
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
			// Only a missing subscription falls back to a free key: a suspended
			// account or a failed exchange must never downgrade a subscriber.
			if i == 0 && resp.StatusCode == http.StatusPaymentRequired {
				continue
			}
			return inferenceCredential{}, a.refused(p, resp.StatusCode, resp.Header, b)
		}
		var in struct {
			Key     secret `json:"key"`
			Expires string `json:"expires_at"`
			Free    bool   `json:"is_free_tier"`
			Rate    *quota `json:"rate_limit"`
		}
		if json.Unmarshal(b, &in) != nil || in.Key == "" {
			return inferenceCredential{}, errors.New("invalid inference credential response")
		}
		exp, err := time.Parse(time.RFC3339, in.Expires)
		if err != nil || !exp.After(nowUTC()) {
			return inferenceCredential{}, errors.New("invalid inference credential expiry")
		}
		cred := inferenceCredential{Key: in.Key, Expires: exp}
		switch {
		case in.Rate != nil && i == 0:
			cred.Limit = in.Rate.limit("hourly")
		case in.Rate != nil && in.Free:
			cred.Limit = in.Rate.limit("free_daily")
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
		delete(a.capped, p.scope())
		a.mu.Unlock()
		return cred, nil
	}
	return inferenceCredential{}, errors.New("no inference credential")
}

// refused maps a declined credential exchange. Every harness user shares one
// egress address, which the controlplane rate-limits, so an hourly cap is
// remembered until it resets instead of being asked about again.
func (a *authenticator) refused(p *principal, status int, header http.Header, body []byte) error {
	var detail object
	json.Unmarshal(body, &detail)
	switch {
	case status == http.StatusTooManyRequests && str(detail["code"]) == "HOURLY_LIMIT_REACHED":
		var budgets quota
		json.Unmarshal(raw(detail["rate_limit"]), &budgets)
		limit := budgets.limit("hourly")
		limit.MaxRequests, limit.Remaining = new(0), new(0)
		limit.ResetsAt = firstString(detail["resets_at"], limit.ResetsAt)
		e := apiErr(429, "QUOTA_EXHAUSTED", firstString(detail["error"], "The hourly usage limit is reached"))
		e.Kind, e.ResetsAt, e.RetryAfter = "hourly", limit.ResetsAt, retryAfter(header)
		until := nowUTC().Add(time.Minute)
		if at, err := time.Parse(time.RFC3339, limit.ResetsAt); err == nil && at.After(nowUTC()) {
			until = at
		} else if e.RetryAfter > 0 {
			until = nowUTC().Add(time.Duration(e.RetryAfter) * time.Second)
		}
		if latest := nowUTC().Add(time.Hour); until.After(latest) {
			until = latest
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.capped == nil {
			a.capped = map[string]hourlyCap{}
		}
		for id, entry := range a.capped {
			if !entry.until.After(nowUTC()) {
				delete(a.capped, id)
			}
		}
		if len(a.capped) < 10000 {
			a.capped[p.scope()] = hourlyCap{refusal: *e, limit: limit, until: until}
		}
		return e
	case status == http.StatusTooManyRequests:
		e := apiErr(429, "RATE_LIMITED", "Chat access is busy; retry shortly")
		e.RetryAfter = retryAfter(header)
		return e
	case status == http.StatusForbidden:
		return apiErr(403, "UPSTREAM_REFUSED", "This account is suspended")
	case status == http.StatusServiceUnavailable:
		e := apiErr(503, "UPSTREAM_REFUSED", "Chat access is temporarily unavailable")
		if str(detail["code"]) == "ACCOUNT_PROVISIONING" {
			e.Message = "The account is still being set up"
		}
		e.RetryAfter = retryAfter(header)
		return e
	}
	return withRetryAfter(downstreamError(status, body), header)
}

func (a *authenticator) consumed(p *principal) rateLimit {
	a.mu.Lock()
	defer a.mu.Unlock()
	cred := a.credentials[p.scope()]
	if cred.Limit.Remaining != nil && *cred.Limit.Remaining > 0 {
		cred.Limit.Remaining = new(*cred.Limit.Remaining - 1)
		a.credentials[p.scope()] = cred
	}
	return cred.Limit
}

func (a *authenticator) currentLimit(p *principal) rateLimit {
	a.mu.Lock()
	defer a.mu.Unlock()
	if capped, ok := a.capped[p.scope()]; ok && nowUTC().Before(capped.until) {
		return capped.limit
	}
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
