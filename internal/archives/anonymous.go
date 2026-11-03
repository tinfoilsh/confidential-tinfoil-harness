package archives

import (
	"sync"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// A signed-out caller's archives hold only the chats its browser tab keeps,
// which last as long as the tab: each is smaller than a signed-in caller's,
// a caller makes one at a time, and only a few run at once, so together they
// hold a small share of the scratch space signed-in callers need.

const (
	maxAnonymousExportBody = 32 << 20
	maxAnonymousImportBody = 32<<20 + 1<<20
	maxAnonymousArchives   = 4
)

var errAnonymousArchiveRunning = api.Err(429, "RATE_LIMITED", "Another archive is running for this browser; retry once it ends")

// anonymousArchives are the signed-out callers with an archive running.
type anonymousArchives struct {
	mu      sync.Mutex
	running map[string]bool
}

// start takes a place for a signed-out caller's archive, which end gives
// back.
func (a *anonymousArchives) start(p *auth.Principal) (end func(), err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	caller := p.Scope()
	switch {
	case a.running[caller]:
		return nil, errAnonymousArchiveRunning
	case len(a.running) >= maxAnonymousArchives:
		return nil, errScratchBusy
	}
	if a.running == nil {
		a.running = map[string]bool{}
	}
	a.running[caller] = true
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		delete(a.running, caller)
	}, nil
}
