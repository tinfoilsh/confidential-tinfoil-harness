package auth

import (
	"context"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

const (
	// renewAhead is how long before a chat token expires a run asks for the
	// next one, so a control plane that is slow to answer costs it nothing.
	renewAhead = 5 * time.Minute
	// renewPause is how long a run waits before it asks for a chat token
	// again, whether the last answer was a token or a refusal.
	renewPause = 30 * time.Second
	// renewTimeout bounds one request for a chat token.
	renewTimeout = 15 * time.Second
)

// errAccessRefused ends a run whose chat token a model or tool refused
// moments after the control plane issued it.
var errAccessRefused = api.Err(503, "UPSTREAM_REFUSED", "Chat access was refused; try again shortly")

// Access is the chat access a run's calls carry for as long as it runs. A
// subscriber's chat token lasts minutes and a run may last longer, so the
// harness asks the control plane for the next one itself, before the one it
// holds expires: the run does not depend on its caller's client still being
// there to mint one. A free key or an API key is not renewed.
type Access struct {
	// renew trades the token held for a fresh one; nil for a key that is
	// not renewed.
	renew func(ctx context.Context, held Secret) (Credential, error)

	mu         sync.Mutex
	credential Credential
	// asking is closed when the request for a token now in flight ends.
	asking chan struct{}
	// refusal is why the last request for a token failed, and askAgain when
	// the next one may be made.
	refusal  error
	askAgain time.Time
}

// NewAccess is the access a run starts with, which renew keeps current.
func NewAccess(credential Credential, renew func(ctx context.Context, held Secret) (Credential, error)) *Access {
	return &Access{credential: credential, renew: renew}
}

// Current is the key held now, without asking for another.
func (a *Access) Current() Secret {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.credential.Key
}

// Key is the key a request sent now carries. A token within renewAhead of
// its expiry is renewed, and is still the one sent while it lasts. Once it
// has expired the request waits for the next one, and fails with the control
// plane's refusal instead of going out with a token that would be refused.
func (a *Access) Key(ctx context.Context) (Secret, error) {
	for {
		a.mu.Lock()
		held, now := a.credential, api.Now()
		if a.renew == nil || held.Expires.Sub(now) > renewAhead {
			a.mu.Unlock()
			return held.Key, nil
		}
		if a.asking == nil && !now.Before(a.askAgain) {
			a.asking = make(chan struct{})
			go a.ask(ctx, held, a.asking)
		}
		asking, refusal := a.asking, a.refusal
		a.mu.Unlock()
		switch {
		case held.Expires.After(now):
			return held.Key, nil
		case asking != nil:
			select {
			case <-asking:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		case refusal != nil:
			return "", refusal
		default:
			return "", errAccessRefused
		}
	}
}

// ask requests the token after held, for every caller waiting on asking.
// It outlives the request that happened to need it first.
func (a *Access) ask(ctx context.Context, held Credential, asking chan struct{}) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), renewTimeout)
	defer cancel()
	fresh, err := a.renew(ctx, held.Key)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.asking, a.refusal, a.askAgain = nil, err, api.Now().Add(renewPause)
	if err == nil {
		// The quota the run was admitted with stays its own.
		fresh.Limit = held.Limit
		a.credential = fresh
	}
	close(asking)
}

// Refused is the key to send again after a model or tool refused key: the
// one held now if it is another, or else the next one, asked for at once. A
// key that is not renewed is returned as it is.
func (a *Access) Refused(ctx context.Context, key Secret) (Secret, error) {
	a.mu.Lock()
	if a.renew != nil && a.credential.Key == key {
		a.credential.Expires = time.Time{}
	}
	a.mu.Unlock()
	return a.Key(ctx)
}
