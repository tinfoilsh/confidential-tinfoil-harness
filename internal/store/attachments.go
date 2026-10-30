package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"golang.org/x/crypto/hkdf"
)

// Attachments are the bytes a chat's attachments hold: stored in buckets
// under a key of their own, which the chat embeds, and indexed by the
// control plane under the chat, which keeps them while the chat names
// them.

const (
	// attachmentIDBytes is the length of an attachment id before hex.
	attachmentIDBytes = 18
	// attachmentKeySize is the per-attachment key buckets seals under.
	attachmentKeySize = seal.KeySize
	// An attachment's id and key are derived from its upload, so the same
	// upload stored again under the same content key lands on the same
	// object. Nothing derives a stored attachment's again: reads use the
	// id and key its chat embeds.
	attachmentIDInfo   = "tinfoil-attachment-id-v1"
	attachmentKeyInfo  = "tinfoil-attachment-key-v1"
	attachmentSaltInfo = "tinfoil-attachment-salt-v1"
	// reserveAttempts bounds the reservations of an id whose earlier
	// copy the control plane is still purging; a purge finishes within
	// seconds, and each wait is a step longer than the last.
	reserveAttempts = 3
	reserveWaitStep = time.Second
)

var (
	errAttachmentMissing     = api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")
	errAttachmentKeyInvalid  = api.Err(422, "ATTACHMENT_UNSUPPORTED", "The attachment key is invalid")
	errAttachmentKeyMismatch = api.Err(422, "ATTACHMENT_UNSUPPORTED", "The attachment key does not match")
	errAttachmentNotStored   = api.Err(502, "UPSTREAM_REFUSED", "The attachment could not be stored")
	errAttachmentNotRead     = api.Err(502, "UPSTREAM_REFUSED", "The attachment could not be read")
	errAttachmentKeyless     = errors.New("an attachment is derived under a content key")
)

// PutAttachment stores data as an attachment of chatID and returns its id
// and the key that opens it, base64, as a chat embeds it. The same upload
// stored again under the same content key is the same id and key over the
// same object; the same client id with other bytes, or under another chat,
// is another object.
func (s *Store) PutAttachment(ctx context.Context, p *auth.Principal, contentKey auth.ContentKey, chatID, clientID string, data []byte) (string, string, error) {
	defer clear(contentKey[:])
	if chatID == "" || clientID == "" {
		return "", "", api.Err(400, "BAD_REQUEST", "a chat id and an attachment id are required")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsPutAttachment(chatID) {
		return "", "", errRunScope
	}
	id, key, err := attachmentMaterials(contentKey, clientID, chatID, p.ID, data)
	if err != nil {
		return "", "", err
	}
	defer seal.Zero(key)
	// The reservation lets the control plane's sweep find bytes whose
	// chat is never saved, so it comes before the bytes.
	if err := s.reserveAttachment(ctx, p, id, chatID); err != nil {
		return "", "", err
	}
	if err := s.Buckets.Put(ctx, p.ID, id, data, key); err != nil {
		return "", "", errAttachmentNotStored
	}
	if err := s.Controlplane.RegisterAttachmentIndex(ctx, string(p.JWT), p.ID, id, chatID); err != nil {
		// Only a refusal proves no index row names the bytes, so only
		// then are they removed here. After any other failure the
		// control plane may hold the row, and its sweep, which checks
		// the index first, decides.
		if refusedByControlplane(err) {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bucketsCleanupTimeout)
			_ = s.Buckets.Delete(cleanupCtx, p.ID, id)
			cancel()
		}
		return "", "", FromControlplane(err)
	}
	return id, base64.StdEncoding.EncodeToString(key), nil
}

// RetireAttachments removes attachments stored for a chat that never came
// to name them: each leaves the index, then buckets. One whose index entry
// the control plane could not drop keeps its bytes, since a row may still
// name it; the control plane's sweeps decide. The removal outlives ctx.
func (s *Store) RetireAttachments(ctx context.Context, p *auth.Principal, ids []string) {
	if len(ids) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bucketsCleanupTimeout)
	defer cancel()
	for _, id := range ids {
		if err := s.Controlplane.DeleteAttachmentIndex(cleanupCtx, string(p.JWT), p.ID, id); err != nil {
			continue
		}
		_ = s.Buckets.Delete(cleanupCtx, p.ID, id)
	}
}

// reserveAttachment has the control plane expect the bytes. A reservation
// refused while the id's earlier copy is still being purged is tried
// again shortly.
func (s *Store) reserveAttachment(ctx context.Context, p *auth.Principal, id, chatID string) error {
	for attempt := 1; ; attempt++ {
		err := s.Controlplane.ReservePendingAttachmentWrite(ctx, string(p.JWT), p.ID, id, chatID)
		if err == nil {
			return nil
		}
		if !controlplane.IsCode(err, controlplane.StatusAttachmentPurgeInProgress) || attempt == reserveAttempts {
			return FromControlplane(err)
		}
		select {
		case <-time.After(time.Duration(attempt) * reserveWaitStep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// attachmentMaterials is an attachment's id and key, derived from who
// stores it, the chat it is under, the client's id for the upload and a
// digest of the bytes, salted with a subkey of the content key. The
// control plane knows all but the bytes, so without the salt it could
// check any guess at an attachment's bytes against its id. Each input is
// length-prefixed, so none can run into the next.
func attachmentMaterials(contentKey auth.ContentKey, clientID, chatID, owner string, data []byte) (string, []byte, error) {
	defer clear(contentKey[:])
	if contentKey == (auth.ContentKey{}) {
		return "", nil, errAttachmentKeyless
	}
	salt := make([]byte, seal.KeySize)
	defer seal.Zero(salt)
	if _, err := io.ReadFull(hkdf.New(sha256.New, contentKey[:], nil, []byte(attachmentSaltInfo)), salt); err != nil {
		return "", nil, err
	}
	digest := sha256.Sum256(data)
	ikm := encodeIKM(clientID, chatID, owner, hex.EncodeToString(digest[:]))
	id := make([]byte, attachmentIDBytes)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ikm, salt, []byte(attachmentIDInfo)), id); err != nil {
		return "", nil, err
	}
	key := make([]byte, attachmentKeySize)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ikm, salt, []byte(attachmentKeyInfo)), key); err != nil {
		return "", nil, err
	}
	return hex.EncodeToString(id), key, nil
}

// encodeIKM is parts as one byte string: each as its length, big-endian
// in eight bytes, then its bytes.
func encodeIKM(parts ...string) []byte {
	total := 0
	for _, part := range parts {
		total += 8 + len(part)
	}
	out := make([]byte, 0, total)
	for _, part := range parts {
		out = binary.BigEndian.AppendUint64(out, uint64(len(part)))
		out = append(out, part...)
	}
	return out
}

// GetAttachment reads an attachment's bytes with the key the chat embeds,
// base64. Knowing the id and the key is the whole proof of access, so a
// share's reader reads one as its owner does. An attachment the index
// does not know may be one an older client stored in the control plane
// itself, which only its owner reads from there.
func (s *Store) GetAttachment(ctx context.Context, p *auth.Principal, id, key string) ([]byte, error) {
	attKey, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(attKey) != attachmentKeySize {
		return nil, errAttachmentKeyInvalid
	}
	defer seal.Zero(attKey)
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opGetAttachment) {
		return nil, errRunScope
	}
	owner, err := s.Controlplane.ResolveAttachmentOwner(ctx, id)
	if errors.Is(err, controlplane.ErrAttachmentNotFound) {
		if p.Anonymous {
			return nil, errAttachmentMissing
		}
		return s.legacyAttachment(ctx, p, id, attKey)
	}
	if err != nil {
		return nil, FromControlplane(err)
	}
	data, err := s.Buckets.Get(ctx, owner, id, attKey)
	switch {
	case errors.Is(err, buckets.ErrNotFound):
		return nil, errAttachmentMissing
	case errors.Is(err, buckets.ErrForbidden):
		return nil, errAttachmentKeyMismatch
	case err != nil:
		return nil, errAttachmentNotRead
	}
	return data, nil
}

// legacyAttachment reads an attachment as an older client stored it, in
// the control plane itself, and opens it with the key the chat embeds.
func (s *Store) legacyAttachment(ctx context.Context, p *auth.Principal, id string, key []byte) ([]byte, error) {
	ciphertext, err := s.Controlplane.GetLegacyAttachment(ctx, string(p.JWT), p.ID, id)
	if errors.Is(err, controlplane.ErrLegacyAttachmentNotFound) || errors.Is(err, controlplane.ErrLegacyAttachmentGone) {
		return nil, errAttachmentMissing
	}
	if err != nil {
		return nil, FromControlplane(err)
	}
	data, err := openLegacyAttachment(ciphertext, key)
	if err != nil {
		return nil, errAttachmentKeyMismatch
	}
	return data, nil
}
