package connectors

import (
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A session opened before its user signed in again holds tokens the server
// no longer takes. Signing it out forgets those, not the ones stored since.
func TestSigningASessionOutLeavesALaterSignIn(t *testing.T) {
	for name, tc := range map[string]struct {
		stored domain.ConnectorAuth
		kept   string
	}{
		"the tokens it holds":      {domain.ConnectorAuth{AccessToken: "earlier"}, ""},
		"those of a later sign-in": {domain.ConnectorAuth{AccessToken: "later"}, "later"},
	} {
		t.Run(name, func(t *testing.T) {
			k := &keptSource{ctx: t.Context(), auth: domain.ConnectorAuth{AccessToken: "earlier"}, creds: Unstored{Auth: &tc.stored}}
			k.signOut()
			if _, err := k.Token(); err != ErrSignedOut {
				t.Fatalf("the session still signs in: %v", err)
			}
			if tc.stored.AccessToken != tc.kept {
				t.Fatalf("stored %q, want %q", tc.stored.AccessToken, tc.kept)
			}
		})
	}
}
