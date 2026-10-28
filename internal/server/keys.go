package server

import (
	"cmp"
	"context"
	"encoding/json"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// The key routes carry the content key under its own name in the body,
// as the key registry names it, rather than as the key a stored row is
// read with.

func (h *harness) keyRoutes() []routeSpec {
	return []routeSpec{
		{"/v1/keys/current", false, false, h.currentKey},
		{"/v1/keys/register", false, false, decoded(h.registerKey)},
		{"/v1/keys/add-bundle", false, false, decoded(h.addKeyBundle)},
		{"/v1/keys/remove-bundle", false, false, decoded(h.removeKeyBundle)},
		{"/v1/keys/migrate-all", false, false, decoded(h.resealRows)},
	}
}

type keyRegisterRequest struct {
	api.KeyRequest
	IfMatch        string                `json:"if_match"`
	CreatedVia     string                `json:"created_via"`
	IdempotencyKey string                `json:"idempotency_key"`
	InitialBundle  *store.KeyBundleInput `json:"initial_bundle"`
}

func (in *keyRegisterRequest) Validate() error {
	return cmp.Or(required("if_match", in.IfMatch), required("created_via", in.CreatedVia), required("idempotency_key", in.IdempotencyKey))
}

type addKeyBundleRequest struct {
	api.KeyRequest
	store.KeyBundleInput
	KeyID          string `json:"key_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (in *addKeyBundleRequest) Validate() error {
	return cmp.Or(required("credential_id", in.CredentialID), required("kek_iv", in.KEKIV), required("encrypted_keys", in.EncryptedKeys), required("key_id", in.KeyID), required("idempotency_key", in.IdempotencyKey))
}

type removeKeyBundleRequest struct {
	api.KeyRequest
	KeyID          string `json:"key_id"`
	CredentialID   string `json:"credential_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (in *removeKeyBundleRequest) Validate() error {
	return cmp.Or(required("key_id", in.KeyID), required("credential_id", in.CredentialID), required("idempotency_key", in.IdempotencyKey))
}

// resealRequest names the keys that may open the stored rows and the one
// to seal them under.
type resealRequest struct {
	Keys   []api.KeyRequest `json:"keys"`
	Target api.KeyRequest   `json:"target"`
}

func (in *resealRequest) Validate() error {
	if len(in.Keys) == 0 {
		return api.Err(400, "BAD_REQUEST", "keys must name at least one key")
	}
	return nil
}

func (in *resealRequest) ClearKey() {
	for i := range in.Keys {
		in.Keys[i].ClearKey()
	}
	in.Target.ClearKey()
}

// required refuses a request without a member the route needs.
func required(name, value string) error {
	if value == "" {
		return api.Err(400, "BAD_REQUEST", name+" is required")
	}
	return nil
}

type keyRegistered struct {
	OK    bool   `json:"ok"`
	KeyID string `json:"key_id"`
}

type okResponse struct {
	OK bool `json:"ok"`
}

func (h *harness) currentKey(ctx context.Context, p *auth.Principal, _ auth.ContentKey, _ json.RawMessage) (any, error) {
	return h.store.CurrentKey(ctx, p)
}

func (h *harness) registerKey(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *keyRegisterRequest) (any, error) {
	key, err := auth.DecodeContentKey(in.Key)
	in.ClearKey()
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	id, err := h.store.RegisterKey(ctx, p, key, in.IfMatch, in.CreatedVia, in.IdempotencyKey, in.InitialBundle)
	if err != nil {
		return nil, err
	}
	return keyRegistered{OK: true, KeyID: id}, nil
}

func (h *harness) addKeyBundle(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *addKeyBundleRequest) (any, error) {
	key, err := auth.DecodeContentKey(in.Key)
	in.ClearKey()
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	if err := h.store.AddKeyBundle(ctx, p, key, in.KeyID, in.KeyBundleInput, in.IdempotencyKey); err != nil {
		return nil, err
	}
	return okResponse{OK: true}, nil
}

func (h *harness) removeKeyBundle(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *removeKeyBundleRequest) (any, error) {
	key, err := auth.DecodeContentKey(in.Key)
	in.ClearKey()
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	if err := h.store.RemoveKeyBundle(ctx, p, key, in.KeyID, in.CredentialID, in.IdempotencyKey); err != nil {
		return nil, err
	}
	return okResponse{OK: true}, nil
}

// resealRows starts, or reports on, the reseal of every stored row an
// older key sealed under the target.
func (h *harness) resealRows(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *resealRequest) (any, error) {
	keys := make([]auth.ContentKey, 0, len(in.Keys))
	defer func() {
		for i := range keys {
			clear(keys[i][:])
		}
	}()
	for _, ref := range in.Keys {
		key, err := auth.DecodeContentKey(ref.Key)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	target, err := auth.DecodeContentKey(in.Target.Key)
	if err != nil {
		return nil, err
	}
	defer clear(target[:])
	in.ClearKey()
	return h.store.Reseal(ctx, p, keys, target)
}
