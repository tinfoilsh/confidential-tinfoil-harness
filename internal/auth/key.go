package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"log/slog"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// ContentKey is request-scoped. It is never included in response models.
type ContentKey [32]byte

func (ContentKey) LogValue() slog.Value { return slog.StringValue("REDACTED") }

func DecodeContentKey(value []byte) (ContentKey, error) {
	var key ContentKey
	if len(value) == 0 {
		return key, api.Err(401, "KEY_REQUIRED", "A content key is required")
	}
	if len(value) != len(key) {
		return key, api.Err(400, "BAD_REQUEST", "key must be base64 encoding of 32 bytes")
	}
	copy(key[:], value)
	return key, nil
}

func DecodeBase64Key(value []byte) (ContentKey, error) {
	b := make([]byte, base64.StdEncoding.DecodedLen(len(value)))
	defer clear(b)
	n, err := base64.StdEncoding.Strict().Decode(b, value)
	if err != nil {
		return ContentKey{}, api.Err(400, "BAD_REQUEST", "key must be base64 encoding of 32 bytes")
	}
	return DecodeContentKey(b[:n])
}

func (k ContentKey) Fingerprint() string { b := sha256.Sum256(k[:]); return hex.EncodeToString(b[:]) }
