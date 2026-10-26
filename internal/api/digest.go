package api

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashHex is the hex SHA-256 of data.
func HashHex(data []byte) string { d := sha256.Sum256(data); return hex.EncodeToString(d[:]) }
