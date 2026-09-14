package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
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

func (k contentKey) base64() string { return base64.StdEncoding.EncodeToString(k[:]) }
func (k contentKey) derive(info string) []byte {
	b, err := hkdf.Key(sha256.New, k[:], nil, info, 32)
	if err != nil {
		panic("invalid HKDF length")
	}
	return b
}
func (k contentKey) fingerprint() string { b := sha256.Sum256(k[:]); return hex.EncodeToString(b[:]) }
func (k contentKey) codeKey() string {
	return hex.EncodeToString(k.derive("tinfoil-code-execution-encryption-key-v1"))
}
func (k contentKey) containerToken(id string) string {
	return hex.EncodeToString(k.derive("tinfoil-code-execution-v1:" + id))
}
func (k contentKey) sandboxSecret() string {
	return hex.EncodeToString(k.derive("tinfoil-agent-sandbox-cache-secret-v1"))
}

func randomKey() contentKey { var k contentKey; rand.Read(k[:]); return k }
func rowID() string {
	return fmt.Sprintf("%013d_%s", int64(9999999999999)-nowUTC().UnixMilli(), uuid.NewString())
}

func newContentRun(id string, key contentKey) (*run, error) {
	material := key.derive("confidential-tinfoil-harness run log v2:" + id)
	defer clear(material)
	block, err := aes.NewCipher(material)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return nil, err
	}
	idle := func() {}
	return &run{id: id, aead: aead, log: newRunlog(), begin: idle, stop: idle, abandon: idle}, nil
}
