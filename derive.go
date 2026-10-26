package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// contentKey is request-scoped. It is never included in response models.
type contentKey [32]byte

func (contentKey) LogValue() slog.Value { return slog.StringValue("REDACTED") }

func decodeContentKey(value []byte) (contentKey, error) {
	var key contentKey
	if len(value) == 0 {
		return key, api.Err(401, "KEY_REQUIRED", "A content key is required")
	}
	if len(value) != len(key) {
		return key, api.Err(400, "BAD_REQUEST", "key must be base64 encoding of 32 bytes")
	}
	copy(key[:], value)
	return key, nil
}

func decodeBase64Key(value []byte) (contentKey, error) {
	b := make([]byte, base64.StdEncoding.DecodedLen(len(value)))
	defer clear(b)
	n, err := base64.StdEncoding.Strict().Decode(b, value)
	if err != nil {
		return contentKey{}, api.Err(400, "BAD_REQUEST", "key must be base64 encoding of 32 bytes")
	}
	return decodeContentKey(b[:n])
}

func (k contentKey) fingerprint() string { b := sha256.Sum256(k[:]); return hex.EncodeToString(b[:]) }

// maxReverseMillis is the instant a reverse-time chat id counts down from.
const maxReverseMillis = 9999999999999

// rowIDPattern is the id a client gives a chat it creates, as rowID makes it.
var rowIDPattern = regexp.MustCompile(`^\d{13}_[0-9a-f-]{36}$`)

func rowID() string {
	return fmt.Sprintf("%013d_%s", maxReverseMillis-api.Now().UnixMilli(), uuid.NewString())
}
