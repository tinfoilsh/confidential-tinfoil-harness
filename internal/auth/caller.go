package auth

import (
	"context"
	"net/http"
	"time"

	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

type callerKeyKey struct{}

// WithCallerKey has ctx's requests signed with key, the caller's; an empty
// key leaves them unsigned, since the harness has no key of its own.
func WithCallerKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, callerKeyKey{}, key)
}

// WithCaller has ctx's requests signed with the key access holds when each
// is sent, as a run's are, which may outlast the key it started with.
func WithCaller(ctx context.Context, access *Access) context.Context {
	return context.WithValue(ctx, callerKeyKey{}, access)
}

// CallerKey is the key ctx's requests are signed with now.
func CallerKey(ctx context.Context) string {
	switch caller := ctx.Value(callerKeyKey{}).(type) {
	case string:
		return caller
	case *Access:
		return string(caller.Current())
	}
	return ""
}

type usageKey struct{}

// WithUsage has ctx's requests report usage under the run it names.
func WithUsage(ctx context.Context, usage usagereporting.Context) context.Context {
	return context.WithValue(ctx, usageKey{}, usage)
}

// Usage is the usage ctx's requests report, if any.
func Usage(ctx context.Context) (usagereporting.Context, bool) {
	usage, ok := ctx.Value(usageKey{}).(usagereporting.Context)
	return usage, ok
}

// CallerTransport signs every request with the caller's key and usage. A
// run's request carries the key its access holds as the request goes out,
// and one refused as unauthenticated is sent once more with the next key: a
// 401 rejects a request before it is processed, so the same body is sent
// again under the same usage.
type CallerTransport struct {
	Inner       http.RoundTripper
	UsageSecret string
}

func (t *CallerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	access, _ := r.Context().Value(callerKeyKey{}).(*Access)
	if access == nil {
		return t.send(r, CallerKey(r.Context()))
	}
	key, err := access.Key(r.Context())
	if err != nil {
		if r.Body != nil {
			r.Body.Close()
		}
		return nil, err
	}
	resp, err := t.send(r, string(key))
	if err != nil || resp.StatusCode != http.StatusUnauthorized || r.Body != nil && r.GetBody == nil {
		return resp, err
	}
	next, err := access.Refused(r.Context(), key)
	if err == nil && next == key {
		return resp, nil
	}
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	again := r.Clone(r.Context())
	if r.GetBody != nil {
		if again.Body, err = r.GetBody(); err != nil {
			return nil, err
		}
	}
	resp, err = t.send(again, string(next))
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}
	// A key refused as it was issued is not the caller's to replace by
	// signing in again.
	if _, err := access.Refused(r.Context(), next); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// send signs r with key, and with the usage of its context as key's.
func (t *CallerTransport) send(r *http.Request, key string) (*http.Response, error) {
	r = r.Clone(r.Context())
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if usage, ok := Usage(r.Context()); ok {
		usage.IssuedAt = time.Now().UTC()
		if key != "" {
			usage.APIKeyHash = usagereporting.HashAPIKey(key)
		}
		if err := usagereporting.SetHeaders(r.Header, usage, t.UsageSecret); err != nil {
			return nil, err
		}
	}
	return t.Inner.RoundTrip(r)
}
