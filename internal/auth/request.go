package auth

import (
	"errors"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Caller is who a request is from, recognized before its body is read.
func (a *Authenticator) Caller(r *http.Request, anonymous bool) (*Principal, error) {
	p, err := a.Authenticate(r, anonymous)
	if err != nil {
		return nil, err
	}
	return p, a.Recognize(r.Context(), p)
}

// WithQuota gives a refusal for an exhausted quota the caller's quota as the
// refusal leaves it. A signed-out caller's is the one its request held.
func WithQuota(p *Principal, held api.RateLimit, err error) error {
	var e *api.Error
	if p == nil || !errors.As(err, &e) || e.Code != "QUOTA_EXHAUSTED" || e.RateLimit != nil {
		return err
	}
	refused, limit := *e, Exhausted(p, held)
	refused.RateLimit = &limit
	return &refused
}

// RequestKey is the content key a request about a run names, which a run
// of a keyless thread leaves out. A missing key is checked against the run: keyless threads accept it, and
// every other thread compares the key's fingerprint.
func RequestKey(key []byte) (ContentKey, error) {
	if len(key) == 0 {
		return ContentKey{}, nil
	}
	return DecodeContentKey(key)
}

// ErrAnonymousCloud refuses a signed-out caller a stored chat.
var ErrAnonymousCloud = api.Err(401, "UNAUTHENTICATED", "Anonymous chats are kept on the device").WithAction(api.ActionStop)
