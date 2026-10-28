package seal

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"errors"
	"io"
)

// A shared chat is sealed under a key its link carries, with no account
// involved: gzip, then AES-GCM under a fresh key with no AAD, the nonce
// before the ciphertext.

// MaxSharePlaintextBytes bounds what a share holds: before compression
// when sealed, after decompression when opened. MaxShareBytes bounds the
// sealed share, which is what is stored and fetched; compression can
// leave a share larger than its plaintext.
const (
	MaxSharePlaintextBytes = 32 << 20
	MaxShareBytes          = MaxSharePlaintextBytes + 1<<20
)

var (
	ErrShareTooLarge = errors.New("seal: the share exceeds the size limit")
	// ErrShareInvalid is a share the key does not open, whatever the
	// reason: to its reader a wrong key and a damaged share are the same.
	ErrShareInvalid = errors.New("seal: the share cannot be opened")
)

// SealShare seals plaintext under a fresh key, returned as hex for the
// link's fragment.
func SealShare(plaintext []byte) ([]byte, string, error) {
	if len(plaintext) > MaxSharePlaintextBytes {
		return nil, "", ErrShareTooLarge
	}
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write(plaintext); err != nil {
		return nil, "", err
	}
	if err := zw.Close(); err != nil {
		return nil, "", err
	}
	defer Zero(compressed.Bytes())
	key, err := RandomKey()
	if err != nil {
		return nil, "", err
	}
	defer Zero(key)
	nonce, ciphertext, err := GCMSeal(key, compressed.Bytes(), nil)
	if err != nil {
		return nil, "", err
	}
	sealed := append(nonce, ciphertext...)
	if len(sealed) > MaxShareBytes {
		return nil, "", ErrShareTooLarge
	}
	return sealed, hex.EncodeToString(key), nil
}

// OpenShare is the plaintext of a share sealed under key.
func OpenShare(key string, ciphertext []byte) ([]byte, error) {
	if len(key) != KeySize*2 || len(ciphertext) < NonceSize+TagSize || len(ciphertext) > MaxShareBytes {
		return nil, ErrShareInvalid
	}
	raw, err := hex.DecodeString(key)
	if err != nil {
		return nil, ErrShareInvalid
	}
	defer Zero(raw)
	compressed, err := GCMOpen(raw, ciphertext[:NonceSize], ciphertext[NonceSize:], nil)
	if err != nil {
		return nil, ErrShareInvalid
	}
	defer Zero(compressed)
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, ErrShareInvalid
	}
	defer zr.Close()
	plaintext, err := io.ReadAll(io.LimitReader(zr, MaxSharePlaintextBytes+1))
	if err != nil || len(plaintext) > MaxSharePlaintextBytes {
		Zero(plaintext)
		return nil, ErrShareInvalid
	}
	return plaintext, nil
}
