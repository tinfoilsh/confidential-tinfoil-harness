package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const clientIPHeader = "X-Tinfoil-Client-IP"
const clientIPTimeHeader = "X-Tinfoil-Client-IP-Time"
const clientIPSignatureHeader = "X-Tinfoil-Client-IP-Signature"
const clientIPAcceptedHeader = "X-Tinfoil-Client-IP-Accepted"

// This assertion is checked by controlplane's pkg/harnessidentity. It binds the
// source IP to this endpoint and Authorization header, with a 60-second window.
func signClientIP(req *http.Request, ip string, key secret, now time.Time) {
	at := strconv.FormatInt(now.Unix(), 10)
	auth := sha256.Sum256([]byte(req.Header.Get("Authorization")))
	message := strings.Join([]string{"tinfoil-harness-client-ip-v1", req.Method, req.URL.Path, at, ip, hex.EncodeToString(auth[:])}, "\n")
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(message))
	req.Header.Set(clientIPHeader, ip)
	req.Header.Set(clientIPTimeHeader, at)
	req.Header.Set(clientIPSignatureHeader, hex.EncodeToString(mac.Sum(nil)))
}
