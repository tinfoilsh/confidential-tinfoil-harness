package api

import (
	"crypto/rand"
	"encoding/hex"
)

// Token is a random 128-bit hex string.
func Token() string {
	var raw [16]byte
	rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}
