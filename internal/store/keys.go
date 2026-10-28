package store

import (
	"context"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
)

// The key registry: the caller's current content key as the control plane
// holds it, by id, and the passkey bundles that wrap it. The key itself
// never leaves the request; the control plane checks each change against
// a proof of possession.

// KeyState is the caller's current content key as the control plane holds
// it: its id and etag, the passkey bundles that wrap it, how it was made,
// and whether rows exist that it did not seal.
type KeyState struct {
	KeyID      *string              `json:"key_id"`
	ETag       string               `json:"etag,omitempty"`
	Bundles    map[string]KeyBundle `json:"bundles"`
	CreatedVia string               `json:"created_via,omitempty"`
	CreatedAt  string               `json:"created_at,omitempty"`
	HasData    bool                 `json:"has_data"`
}

// KeyBundle is the content key wrapped under one passkey: the key AES-GCM
// sealed under the passkey's output, hex, with its IV.
type KeyBundle struct {
	CredentialID  string `json:"credential_id"`
	KEKIV         string `json:"kek_iv"`
	EncryptedKeys string `json:"encrypted_keys"`
	CreatedAt     string `json:"created_at,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

// KeyBundleInput is a bundle as a client registers it.
type KeyBundleInput struct {
	CredentialID  string `json:"credential_id"`
	KEKIV         string `json:"kek_iv"`
	EncryptedKeys string `json:"encrypted_keys"`
}

const (
	// A bundle is AES-256-GCM over the 32-byte content key under a
	// 12-byte IV: the IV is 24 hex characters, and the wrapped key with
	// its tag 96.
	bundleIVHexLen         = 24
	bundleWrappedKeyHexLen = 96
	// keyIDHexLen is a key id's length as the registry names it.
	keyIDHexLen = 32
)

var errKeyNotItsID = api.Err(400, "BAD_REQUEST", "The key does not match its id")

// CurrentKey is the caller's registered key with its bundles, or the
// no-key state: no id, no bundles, and whether rows exist that predate
// the registry.
func (s *Store) CurrentKey(ctx context.Context, p *auth.Principal) (*KeyState, error) {
	current, err := s.Controlplane.GetCurrentKey(ctx, string(p.JWT), p.ID)
	if err != nil {
		return nil, FromControlplane(err)
	}
	state := &KeyState{Bundles: map[string]KeyBundle{}}
	if current == nil || current.KeyID == "" {
		if current != nil {
			state.HasData = current.HasData
		}
		return state, nil
	}
	id := current.KeyID
	state.KeyID, state.ETag, state.CreatedVia, state.CreatedAt, state.HasData = &id, current.ETag, current.CreatedVia, current.CreatedAt, current.HasData
	for cid, b := range current.Bundles {
		state.Bundles[cid] = KeyBundle{CredentialID: b.CredentialID, KEKIV: b.KEKIV, EncryptedKeys: b.EncryptedKeys, CreatedAt: b.RegisteredAt, UpdatedAt: b.RegisteredAt}
	}
	return state, nil
}

// CheckRegisteredKey refuses a key that is not the caller's registered
// current key, naming the registered one when there is one: the control
// plane takes new rows only under it, so a job sealing many would be
// refused row by row.
func (s *Store) CheckRegisteredKey(ctx context.Context, p *auth.Principal, key auth.ContentKey) error {
	defer clear(key[:])
	current, err := s.Controlplane.GetCurrentKey(ctx, string(p.JWT), p.ID)
	if err != nil {
		return FromControlplane(err)
	}
	if current == nil || current.KeyID != keyID(key) {
		e := api.Err(409, "KEY_MISMATCH", "The key is not the registered content key")
		if current != nil {
			e.KeyID = current.KeyID
		}
		return e
	}
	return nil
}

// RegisterKey registers key as the caller's current key, with a bundle
// when given, and returns its id. ifMatch is the current key's etag, or
// "*" for an account that must have none. The control plane refuses a
// key that would strand rows another key sealed, unless the key starts
// fresh, which wipes them; the attachments it wiped leave buckets here.
func (s *Store) RegisterKey(ctx context.Context, p *auth.Principal, key auth.ContentKey, ifMatch, createdVia, nonce string, bundle *KeyBundleInput) (string, error) {
	defer clear(key[:])
	switch createdVia {
	case "passkey", "manual", "recovery", "start_fresh":
	default:
		return "", api.Err(400, "BAD_REQUEST", "created_via must be passkey, manual, recovery or start_fresh")
	}
	if ifMatch == "" {
		return "", api.Err(400, "BAD_REQUEST", "if_match is required: the current key's etag, or \"*\" without one")
	}
	if nonce == "" {
		return "", api.Err(400, "BAD_REQUEST", "idempotency_key is required")
	}
	var initial *controlplane.RegisterKeyBundle
	if bundle != nil {
		if bundle.CredentialID == "" {
			return "", api.Err(400, "BAD_REQUEST", "initial_bundle.credential_id is required")
		}
		if err := validateBundleShape(bundle.KEKIV, bundle.EncryptedKeys); err != nil {
			return "", err
		}
		initial = &controlplane.RegisterKeyBundle{CredentialID: bundle.CredentialID, KEKIV: bundle.KEKIV, EncryptedKeys: bundle.EncryptedKeys}
	}
	kidHex := keyID(key)
	req := controlplane.RegisterKeyRequest{
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kidHex,
		IfMatch:        ifMatch,
		CreatedVia:     createdVia,
		IdempotencyKey: nonce,
		InitialBundle:  initial,
	}
	body, err := controlplane.RegisterKeyBody(req)
	if err != nil {
		return "", FromControlplane(err)
	}
	req.OperationHash = operationHash(key, http.MethodPost, controlplane.RegisterKeyPath, kidHex, ifMatch, nonce, 0, nil, body)
	// A key that replaces another, or starts fresh, fences off the search
	// index the control plane names now; which object that is must be
	// known before the fence clears the name.
	fencedObject := ""
	if s.Search != nil {
		if state, err := s.Controlplane.GetSearchIndexState(ctx, string(p.JWT), p.ID); err == nil {
			fencedObject = state.ObjectKey
		}
	}
	resp, err := s.Controlplane.RegisterKey(ctx, req)
	if err != nil {
		return "", FromControlplane(err)
	}
	deleteBucketAttachments(ctx, s.Buckets, p.ID, resp.WipedV2Attachments)
	if resp.SearchIndexFenced && s.Search != nil {
		s.Search.Forget(ctx, p.ID, fencedObject)
	}
	return kidHex, nil
}

// AddKeyBundle wraps the current key, named by keyID, under one more
// passkey.
func (s *Store) AddKeyBundle(ctx context.Context, p *auth.Principal, key auth.ContentKey, kid string, bundle KeyBundleInput, nonce string) error {
	defer clear(key[:])
	if err := validateKeyID(kid); err != nil {
		return err
	}
	if bundle.CredentialID == "" || bundle.KEKIV == "" || bundle.EncryptedKeys == "" {
		return api.Err(400, "BAD_REQUEST", "credential_id, kek_iv and encrypted_keys are required")
	}
	if err := validateBundleShape(bundle.KEKIV, bundle.EncryptedKeys); err != nil {
		return err
	}
	if nonce == "" {
		return api.Err(400, "BAD_REQUEST", "idempotency_key is required")
	}
	if keyID(key) != kid {
		return errKeyNotItsID
	}
	req := controlplane.AddBundleRequest{
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kid,
		CredentialID:   bundle.CredentialID,
		KEKIV:          bundle.KEKIV,
		EncryptedKeys:  bundle.EncryptedKeys,
		IdempotencyKey: nonce,
	}
	body, err := controlplane.AddBundleBody(req)
	if err != nil {
		return FromControlplane(err)
	}
	req.OperationHash = operationHash(key, http.MethodPost, controlplane.AddBundlePath(kid), kid, "", nonce, 0, nil, body)
	if err := s.Controlplane.AddBundle(ctx, req); err != nil {
		return FromControlplane(err)
	}
	return nil
}

// RemoveKeyBundle drops one passkey's bundle from the current key, named
// by keyID.
func (s *Store) RemoveKeyBundle(ctx context.Context, p *auth.Principal, key auth.ContentKey, kid, credentialID, nonce string) error {
	defer clear(key[:])
	if err := validateKeyID(kid); err != nil {
		return err
	}
	if credentialID == "" {
		return api.Err(400, "BAD_REQUEST", "credential_id is required")
	}
	if nonce == "" {
		return api.Err(400, "BAD_REQUEST", "idempotency_key is required")
	}
	if keyID(key) != kid {
		return errKeyNotItsID
	}
	req := controlplane.RemoveBundleRequest{
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kid,
		CredentialID:   credentialID,
		IdempotencyKey: nonce,
		OperationHash:  operationHash(key, http.MethodDelete, controlplane.RemoveBundlePath(kid, credentialID), kid, "", nonce, 0, nil, nil),
	}
	if err := s.Controlplane.RemoveBundle(ctx, req); err != nil {
		return FromControlplane(err)
	}
	return nil
}

func validateKeyID(id string) error {
	if len(id) != keyIDHexLen || !isLowerHex(id) {
		return api.Err(400, "BAD_REQUEST", "key_id must be 32 lowercase hex characters")
	}
	return nil
}

// validateBundleShape holds a bundle to the one shape the registry
// stores, so no client can register a bundle another cannot open.
func validateBundleShape(kekIV, encryptedKeys string) error {
	if len(kekIV) != bundleIVHexLen || !isLowerHex(kekIV) {
		return api.Err(400, "BAD_REQUEST", "kek_iv must be 24 lowercase hex characters")
	}
	if len(encryptedKeys) != bundleWrappedKeyHexLen || !isLowerHex(encryptedKeys) {
		return api.Err(400, "BAD_REQUEST", "encrypted_keys must be 96 lowercase hex characters")
	}
	return nil
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
