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

// CallerKey is the key ctx's requests are signed with.
func CallerKey(ctx context.Context) string {
	key, _ := ctx.Value(callerKeyKey{}).(string)
	return key
}

type usageKey struct{}

// WithUsage has ctx's requests report usage under the run it names.
func WithUsage(ctx context.Context, usage usagereporting.Context) context.Context {
	return context.WithValue(ctx, usageKey{}, usage)
}

// WithoutUsage has ctx's requests report no usage.
func WithoutUsage(ctx context.Context) context.Context {
	return context.WithValue(ctx, usageKey{}, false)
}

// Usage is the usage ctx's requests report, if any.
func Usage(ctx context.Context) (usagereporting.Context, bool) {
	usage, ok := ctx.Value(usageKey{}).(usagereporting.Context)
	return usage, ok
}

// CallerTransport signs every request with the caller's key and usage.
type CallerTransport struct {
	Inner       http.RoundTripper
	UsageSecret string
}

func (t *CallerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if key := CallerKey(r.Context()); key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if usage, ok := Usage(r.Context()); ok {
		usage.IssuedAt = time.Now().UTC()
		if err := usagereporting.SetHeaders(r.Header, usage, t.UsageSecret); err != nil {
			return nil, err
		}
	}
	return t.Inner.RoundTrip(r)
}
