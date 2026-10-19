package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// contentKey is request-scoped. It is never included in response models.
type contentKey [32]byte

func (contentKey) LogValue() slog.Value { return slog.StringValue("REDACTED") }

func decodeContentKey(value string) (contentKey, error) {
	var key contentKey
	if value == "" {
		return key, apiErr(401, "KEY_REQUIRED", "A content key is required")
	}
	b, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(b) != len(key) {
		return key, apiErr(400, "BAD_REQUEST", "key must be base64 encoding of 32 bytes")
	}
	copy(key[:], b)
	clear(b)
	return key, nil
}

func (k contentKey) base64() string      { return base64.StdEncoding.EncodeToString(k[:]) }
func (k contentKey) fingerprint() string { b := sha256.Sum256(k[:]); return hex.EncodeToString(b[:]) }

func rowID() string {
	return fmt.Sprintf("%013d_%s", int64(9999999999999)-nowUTC().UnixMilli(), uuid.NewString())
}
