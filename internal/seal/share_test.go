package seal

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"
)

func TestShareRoundTripsUnderItsLinkKeyAlone(t *testing.T) {
	plaintext := bytes.Repeat([]byte("a shared chat "), 256)
	ciphertext, key, err := SealShare(plaintext)
	if err != nil || len(key) != KeySize*2 {
		t.Fatalf("seal: key=%q err=%v", key, err)
	}
	if bytes.Contains(ciphertext, []byte("shared chat")) || len(ciphertext) > len(plaintext)/4 {
		t.Fatalf("the share is readable, or was not compressed: %d bytes", len(ciphertext))
	}
	opened, err := OpenShare(key, ciphertext)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatalf("open = %q (%v)", opened, err)
	}
	other, _, _ := SealShare(plaintext)
	if bytes.Equal(other, ciphertext) {
		t.Fatal("two shares of the same chat are sealed alike")
	}
}

func TestShareRefusesTheWrongKeyAndADamagedShare(t *testing.T) {
	ciphertext, key, err := SealShare([]byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	wrong := []byte(key)
	wrong[0] ^= 1
	if _, err := OpenShare(string(wrong), ciphertext); !errors.Is(err, ErrShareInvalid) {
		t.Fatalf("open with the wrong key: %v", err)
	}
	if _, err := OpenShare(key[:len(key)-2], ciphertext); !errors.Is(err, ErrShareInvalid) {
		t.Fatalf("open with a short key: %v", err)
	}
	if _, err := OpenShare(key, ciphertext[:len(ciphertext)-1]); !errors.Is(err, ErrShareInvalid) {
		t.Fatalf("open of a truncated share: %v", err)
	}
	if _, err := OpenShare(key, ciphertext[:NonceSize+TagSize-1]); !errors.Is(err, ErrShareInvalid) {
		t.Fatalf("open of a share too short to hold a tag: %v", err)
	}
	tampered := append([]byte(nil), ciphertext...)
	tampered[len(tampered)/2] ^= 1
	if _, err := OpenShare(key, tampered); !errors.Is(err, ErrShareInvalid) {
		t.Fatalf("open of a tampered share: %v", err)
	}
}

func TestShareRefusesMoreThanAShareHolds(t *testing.T) {
	if _, _, err := SealShare(make([]byte, MaxSharePlaintextBytes+1)); !errors.Is(err, ErrShareTooLarge) {
		t.Fatalf("seal past the limit: %v", err)
	}
	// The largest plaintext a share holds still fits a sealed share when
	// nothing in it compresses.
	largest := make([]byte, MaxSharePlaintextBytes)
	rand.Read(largest)
	sealed, linkKey, err := SealShare(largest)
	if err != nil || len(sealed) > MaxShareBytes {
		t.Fatalf("seal of the largest plaintext: %d bytes, %v", len(sealed), err)
	}
	if opened, err := OpenShare(linkKey, sealed); err != nil || !bytes.Equal(opened, largest) {
		t.Fatalf("open of the largest share: %v", err)
	}
	// A small share can decompress past the limit; the reader stops there.
	key := mustRandKey(t)
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	chunk := bytes.Repeat([]byte("A"), 64<<10)
	for written := 0; written <= MaxSharePlaintextBytes; written += len(chunk) {
		if _, err := zw.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	nonce, ciphertext, err := GCMSeal(key, compressed.Bytes(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenShare(hex.EncodeToString(key), append(nonce, ciphertext...)); !errors.Is(err, ErrShareInvalid) {
		t.Fatalf("open of a share that decompresses past the limit: %v", err)
	}
}
