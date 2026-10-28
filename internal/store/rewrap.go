package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// Legacy rows and their attachments: rows older clients sealed in a format
// without a key id, which a read reseals under the current key, moving the
// attachments they embed keys for into buckets.

const (
	// legacyAttachmentKeySize is the per-attachment key older clients
	// embedded in the chat.
	legacyAttachmentKeySize = 32
	// bucketsCleanupTimeout bounds the removal of a deleted chat's
	// attachments, which outlives the request.
	bucketsCleanupTimeout = 30 * time.Second
)

// rewrapRow reseals a legacy row under target and writes it back at the
// revision it was read at. A chat's legacy attachments move into buckets
// first, under the attachment id and the key the chat embeds, so the
// embedded key keeps addressing them. Returns the new etag.
func (r *Rows) rewrapRow(ctx context.Context, p *auth.Principal, scope, id string, plaintext []byte, priorETag string, target seal.Key) (string, error) {
	key, kidHex := target.Bytes, target.KeyIDHex
	sealed := plaintext
	if scope == "chat" {
		normalized, err := r.rewrapChatAttachments(ctx, p, id, plaintext)
		if err != nil {
			return "", err
		}
		if normalized != nil {
			sealed = normalized
			defer seal.Zero(normalized)
		}
	}
	aad := seal.AAD{KeyIDHex: kidHex, Scope: seal.Scope(scope), ID: aadID(scope, id), ClerkUserID: p.ID}
	payloadAAD, err := seal.CanonicalPayloadAAD(aad)
	if err != nil {
		return "", err
	}
	envelope, err := seal.Encrypt(key, sealed, aad)
	if err != nil {
		return "", err
	}
	nonce := "rewrap:" + kidHex + ":" + id + ":" + priorETag
	opKey, err := seal.DeriveOpHashKey(key)
	if err != nil {
		return "", err
	}
	defer seal.Zero(opKey)
	resp, err := r.Controlplane.PutBlob(ctx, controlplane.PutBlobRequest{
		Scope:          scope,
		ID:             id,
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kidHex,
		IfMatch:        priorETag,
		IdempotencyKey: nonce,
		Rewrap:         true,
		Ciphertext:     envelope,
		OperationHash: seal.ComputeOperationHash(opKey, seal.CanonicalInput{
			Method:         http.MethodPost,
			Path:           controlplane.RewrapPath,
			KeyIDHex:       kidHex,
			IfMatch:        priorETag,
			IdempotencyKey: nonce,
			Body:           sealed,
			AAD:            payloadAAD,
		}),
	})
	if err != nil {
		return "", err
	}
	return resp.ETag, nil
}

// rewrapChatAttachments moves every image attachment a chat embeds a key
// for into buckets, under the same id and key. A key older clients stored
// as `key` is copied to `encryptionKey`, so later reads address the
// bucket directly; `key` stays for clients that only know it. Returns the
// chat with that change, or nil when the plaintext is not a chat or no
// key needed copying, so the row reseals as it is.
func (r *Rows) rewrapChatAttachments(ctx context.Context, p *auth.Principal, chatID string, plaintext []byte) ([]byte, error) {
	// Numbers stay as written so re-marshaling the chat cannot round a
	// large size or timestamp, or turn 1.0 into 1.
	dec := json.NewDecoder(bytes.NewReader(plaintext))
	dec.UseNumber()
	var parsed map[string]any
	if err := dec.Decode(&parsed); err != nil {
		return nil, nil
	}
	rawMessages, ok := parsed["messages"].([]any)
	if !ok {
		return nil, nil
	}
	// Every attachment is attempted even after one fails, so the next
	// read starts from a smaller backlog; the reseal still fails as soon
	// as any one did, since a resealed chat that still embeds a legacy
	// attachment could never be read whole again.
	var failures []error
	normalized := false
	for _, m := range rawMessages {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}
		rawAtts, ok := msg["attachments"].([]any)
		if !ok {
			continue
		}
		for _, a := range rawAtts {
			att, ok := a.(map[string]any)
			if !ok || att["type"] != "image" {
				continue
			}
			rawID, _ := att["id"].(string)
			rawKey, _ := att["encryptionKey"].(string)
			keyFromLegacyField := false
			if rawKey == "" {
				rawKey, _ = att["key"].(string)
				keyFromLegacyField = rawKey != ""
			}
			if rawID == "" || rawKey == "" {
				continue
			}
			if err := r.promoteAttachment(ctx, p, chatID, rawID, rawKey); err != nil {
				failures = append(failures, err)
				continue
			}
			if keyFromLegacyField {
				att["encryptionKey"] = rawKey
				normalized = true
			}
		}
	}
	if len(failures) > 0 {
		return nil, errors.Join(failures...)
	}
	if normalized {
		return json.Marshal(parsed)
	}
	return nil, nil
}

// promoteAttachment reads one legacy attachment from the control plane,
// opens it with the key the chat embeds, stores the bytes in buckets
// under the same id and key, and registers the attachment as held there.
func (r *Rows) promoteAttachment(ctx context.Context, p *auth.Principal, chatID, attID, legacyKeyB64 string) error {
	if !r.Buckets.Configured() {
		return errors.New("rewrap: buckets backend not configured")
	}
	ciphertext, err := r.Controlplane.GetLegacyAttachment(ctx, string(p.JWT), p.ID, attID)
	if err != nil {
		// A missing attachment has nothing to move; one already moved is
		// addressed by the embedded key as it is.
		if errors.Is(err, controlplane.ErrLegacyAttachmentNotFound) || errors.Is(err, controlplane.ErrLegacyAttachmentGone) {
			return nil
		}
		return fmt.Errorf("rewrap: fetch legacy attachment %s: %w", attID, err)
	}
	legacyKey, err := base64.StdEncoding.DecodeString(legacyKeyB64)
	if err != nil || len(legacyKey) != legacyAttachmentKeySize {
		return fmt.Errorf("rewrap: invalid legacy attachment key for %s", attID)
	}
	defer seal.Zero(legacyKey)
	plaintext, err := openLegacyAttachment(ciphertext, legacyKey)
	if err != nil {
		return fmt.Errorf("rewrap: decrypt legacy attachment %s: %w", attID, err)
	}
	defer seal.Zero(plaintext)
	if err := r.Buckets.Put(ctx, p.ID, attID, plaintext, legacyKey); err != nil {
		return fmt.Errorf("rewrap: promote attachment %s to buckets: %w", attID, err)
	}
	// With the bytes in buckets but the index not yet told, the next read
	// registers them again; storing the same id and key is idempotent.
	if err := r.Controlplane.RegisterAttachmentIndex(ctx, string(p.JWT), p.ID, attID, chatID); err != nil {
		return fmt.Errorf("rewrap: register attachment index %s: %w", attID, err)
	}
	return nil
}

// deleteBucketAttachments removes a deleted chat's attachments from
// buckets, best effort. The ids come from the control plane's answer to
// the delete, never from the chat's plaintext, which could name another
// user's attachment. An orphan left in buckets is unaddressable without
// the per-attachment key, which went with the chat.
func (r *Rows) deleteBucketAttachments(ctx context.Context, owner string, ids []string) {
	if !r.Buckets.Configured() || len(ids) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bucketsCleanupTimeout)
	defer cancel()
	for _, attID := range ids {
		_ = r.Buckets.Delete(cleanupCtx, owner, attID)
	}
}

// openLegacyAttachment opens an attachment as older clients sealed it:
// a nonce, then AES-GCM ciphertext and tag, with no AAD.
func openLegacyAttachment(blob, key []byte) ([]byte, error) {
	if len(blob) < seal.NonceSize {
		return nil, errors.New("rewrap: attachment ciphertext too short")
	}
	return seal.GCMOpen(key, blob[:seal.NonceSize], blob[seal.NonceSize:], nil)
}
