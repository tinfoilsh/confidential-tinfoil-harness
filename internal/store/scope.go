package store

import (
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// keyID is the hex id a content key is registered and sealed under.
func keyID(key auth.ContentKey) string {
	id, _ := seal.DeriveKeyID(key[:])
	return seal.KeyIDHex(id)
}
