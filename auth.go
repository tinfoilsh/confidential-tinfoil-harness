package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type principal struct {
	ID        string
	JWT       secret
	IP        string
	Anonymous bool
}

func (p *principal) scope() string {
	if !p.Anonymous {
		return "user:" + p.ID
	}
	digest := sha256.Sum256([]byte(p.IP))
	return "anonymous:" + hex.EncodeToString(digest[:])
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
	verify         func(context.Context, string) (string, error)
	client         *http.Client
	controlplane   string
	trustedProxies []netip.Prefix
	mu             sync.Mutex
	credentials    map[string]inferenceCredential
	identitySecret secret
	refreshing     map[string]chan struct{}
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
	a := &authenticator{client: &http.Client{Timeout: 15 * time.Second}, controlplane: cp, identitySecret: secret(env("HARNESS_IDENTITY_SECRET", "")), credentials: map[string]inferenceCredential{}}
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
	for _, value := range strings.Split(env("TINFOIL_TRUSTED_PROXIES", "127.0.0.0/8,::1/128,172.31.255.1/32"), ",") {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(value))
		if err != nil {
			return nil, errors.New("invalid TINFOIL_TRUSTED_PROXIES")
		}
		a.trustedProxies = append(a.trustedProxies, prefix)
	}
	return a, nil
}

func (a *authenticator) authenticate(r *http.Request, anonymous bool) (*principal, error) {
	if a == nil {
		return nil, apiErr(503, "UNAUTHENTICATED", "Authentication is not configured")
	}
	header := r.Header.Get("Authorization")
	if header == "" && anonymous {
		return &principal{Anonymous: true, IP: a.clientIP(r)}, nil
	}
	token := bearer(header)
	if token == "" {
		return nil, apiErr(401, "UNAUTHENTICATED", "A Clerk session is required")
	}
	id, err := a.verify(r.Context(), token)
	if err != nil {
		return nil, apiErr(401, "UNAUTHENTICATED", "The Clerk session is invalid or expired")
	}
	return &principal{ID: id, JWT: secret(token), IP: a.clientIP(r)}, nil
}

func (a *authenticator) trusted(ip netip.Addr) bool {
	for _, p := range a.trustedProxies {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}
func (a *authenticator) clientIP(r *http.Request) string {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	ip, _ := netip.ParseAddr(host)
	if a.trusted(ip) {
		chain := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
		for i := len(chain) - 1; i >= 0; i-- {
			candidate, err := netip.ParseAddr(strings.TrimSpace(chain[i]))
			if err != nil {
				break
			}
			ip = candidate
			if !a.trusted(ip) {
				break
			}
		}
	}
	if !ip.IsValid() {
		return ""
	}
	return ip.Unmap().String()
}

func (a *authenticator) inference(ctx context.Context, p *principal) (inferenceCredential, error) {
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
	if p.Anonymous && a.identitySecret == "" {
		return inferenceCredential{}, apiErr(503, "UPSTREAM_REFUSED", "Anonymous identity forwarding is not configured")
	}
	paths := []string{"/api/keys/chat"}
	if !p.Anonymous {
		paths = append([]string{"/api/chat/token"}, paths...)
	}
	for i, path := range paths {
		req, _ := http.NewRequestWithContext(ctx, "GET", a.controlplane+path, nil)
		if p.JWT != "" {
			req.Header.Set("Authorization", "Bearer "+string(p.JWT))
		}
		if p.IP != "" {
			if a.identitySecret != "" && path == "/api/keys/chat" {
				signClientIP(req, p.IP, a.identitySecret, nowUTC())
			}
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
		if p.Anonymous && resp.Header.Get(clientIPAcceptedHeader) != "true" {
			return inferenceCredential{}, apiErr(503, "UPSTREAM_REFUSED", "Controlplane did not acknowledge the forwarded identity")
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
