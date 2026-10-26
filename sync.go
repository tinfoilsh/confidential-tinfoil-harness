package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

type rowPage struct {
	Rows []domain.StoredRow
	Next string
	// Rows sync holds but cannot open, such as one sealed to a retired key,
	// and why. A page lists them rather than failing, so one row cannot hide
	// the rest.
	Unreadable map[string]error
}
type rowStore interface {
	authorizeRun(context.Context, *auth.Principal, auth.ContentKey, string, *string) (storageGrant, error)
	authorizeStorage(context.Context, *auth.Principal, auth.ContentKey, string, string) (auth.Secret, error)
	load(context.Context, *auth.Principal, auth.ContentKey, string, string) (*domain.StoredRow, error)
	pull(context.Context, *auth.Principal, auth.ContentKey, string, []string) ([]domain.StoredRow, map[string]error, error)
	list(context.Context, *auth.Principal, auth.ContentKey, string, string, string, int) (rowPage, error)
	push(context.Context, *auth.Principal, auth.ContentKey, string, *domain.StoredRow, string) (string, error)
	// remove deletes a row whatever its revision; a missing row is gone.
	remove(context.Context, *auth.Principal, auth.ContentKey, string, string) error
	removeRevision(context.Context, *auth.Principal, auth.ContentKey, string, string) error
	// ids lists every row of a scope, or a project's chats, unopened.
	ids(context.Context, *auth.Principal, string, string) ([]string, error)
}
type syncClient struct {
	URL    string
	Client *http.Client
}

const (
	// Sync refuses clients that do not store documents as attachment blobs.
	syncProtocol = "3"
	// Sync answers within its own route budgets; this bounds a call to the
	// longest of them, an attachment's.
	syncTimeout = 5 * time.Minute
	// pullBatch is the most rows sync opens in one pull (MaxPullIDs).
	pullBatch = 100
	// listStatusPage is the largest page of row ids sync lists.
	listStatusPage = 500
	// maxCommitAttempts bounds the pushes of one change to a row.
	maxCommitAttempts = 3
)

func (s *syncClient) call(ctx context.Context, p *auth.Principal, path string, in, out any) error {
	if body, ok := in.(json.RawMessage); ok {
		defer clear(body)
	}
	if s == nil || s.Client == nil {
		return api.Err(503, "UPSTREAM_REFUSED", "The sync enclave is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, apiKeyKey{}, "")
	ctx = context.WithValue(ctx, usageContextKey{}, false)
	return api.PostJSONWithHeaders(ctx, s.Client, s.URL+path, string(cmp.Or(p.SyncToken, p.JWT)), map[string]string{"X-Sync-Protocol": syncProtocol}, in, out)
}

// keyedBody appends the key directly to an owned buffer, keeping its base64
// form out of strings and encoding/json's pooled encoder buffers.
func keyedBody(key auth.ContentKey, in api.Object, multiple bool) json.RawMessage {
	defer clear(key[:])
	body := api.Raw(in)
	defer clear(body)
	prefix, suffix := `"key":"`, `"}`
	if multiple {
		prefix, suffix = `"keys":[{"key":"`, `"}]}`
	}
	if len(in) != 0 {
		prefix = "," + prefix
	}
	out := make([]byte, 0, len(body)-1+len(prefix)+base64.StdEncoding.EncodedLen(len(key))+len(suffix))
	out = append(out, body[:len(body)-1]...)
	out = append(out, prefix...)
	out = base64.StdEncoding.AppendEncode(out, key[:])
	return append(out, suffix...)
}

func (s *syncClient) callKey(ctx context.Context, p *auth.Principal, path string, key auth.ContentKey, in api.Object, out any) error {
	defer clear(key[:])
	return s.call(ctx, p, path, keyedBody(key, in, false), out)
}

func (s *syncClient) authorizeRun(ctx context.Context, p *auth.Principal, key auth.ContentKey, threadID string, projectID *string) (storageGrant, error) {
	defer clear(key[:])
	in := api.Object{"thread_id": threadID}
	if projectID != nil {
		in["project_id"] = projectID
	}
	return s.storageCredential(ctx, p, "/v1/runs/authorize", key, in)
}

func (s *syncClient) authorizeStorage(ctx context.Context, p *auth.Principal, key auth.ContentKey, operation, projectID string) (auth.Secret, error) {
	defer clear(key[:])
	in := api.Object{"operation": operation}
	if projectID != "" {
		in["project_id"] = projectID
	}
	grant, err := s.storageCredential(ctx, p, "/v1/operations/authorize", key, in)
	return grant.Token, err
}

type storageGrant struct {
	Token     auth.Secret `json:"token"`
	ExpiresAt time.Time   `json:"expires_at"`
}

func (s *syncClient) storageCredential(ctx context.Context, p *auth.Principal, path string, key auth.ContentKey, input api.Object) (storageGrant, error) {
	defer clear(key[:])
	var out storageGrant
	err := s.callKey(ctx, p, path, key, input, &out)
	if err != nil {
		return storageGrant{}, err
	}
	if out.Token == "" || !out.ExpiresAt.After(api.Now().Add(runTimeout+commitTimeout)) {
		return storageGrant{}, api.Err(502, "UPSTREAM_REFUSED", "The storage service returned an invalid run credential")
	}
	return out, nil
}

type pullItem struct {
	ID         string  `json:"id"`
	OK         bool    `json:"ok"`
	Plaintext  string  `json:"plaintext"`
	ETag       string  `json:"etag"`
	Code       string  `json:"code"`
	KeyID      string  `json:"key_id"`
	ProjectSet bool    `json:"project_id_set"`
	ProjectID  *string `json:"project_id"`
}

// pull returns the rows it could open and the error for each it could not.
// errRowNotFetched is a row sync could not fetch within its deadline twice
// running. A later request may fetch it.
var errRowNotFetched = api.Err(502, "UPSTREAM_REFUSED", "The sync service could not fetch the data in time")

func (s *syncClient) pull(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, ids []string) ([]domain.StoredRow, map[string]error, error) {
	rows, failed := []domain.StoredRow{}, map[string]error{}
	for start := 0; start < len(ids); start += pullBatch {
		if err := s.pullBatch(ctx, p, key, scope, ids[start:min(len(ids), start+pullBatch)], true, &rows, failed); err != nil {
			return nil, nil, err
		}
	}
	return rows, failed, nil
}

func (s *syncClient) pullBatch(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, ids []string, retry bool, rows *[]domain.StoredRow, failed map[string]error) error {
	var result struct {
		Items []pullItem `json:"items"`
	}
	err := s.call(ctx, p, "/v1/sync/pull", keyedBody(key, api.Object{"scope": scope, "ids": ids}, true), &result)
	if err != nil {
		return err
	}
	again := []string{}
	for _, item := range result.Items {
		if !item.OK {
			switch {
			case item.Code == "NOT_FOUND":
			case item.Code == "NETWORK" && retry: // sync could not fetch it within its deadline
				again = append(again, item.ID)
			case item.Code == "NETWORK":
				failed[item.ID] = errRowNotFetched
			default:
				// The item has no status of its own; its code says what failed.
				failed[item.ID] = api.DownstreamError(0, api.Raw(api.Object{"code": item.Code, "key_id": item.KeyID}))
			}
			continue
		}
		plain, err := base64.StdEncoding.DecodeString(item.Plaintext)
		if err != nil {
			failed[item.ID] = errors.New("invalid sync plaintext encoding")
			continue
		}
		if trimmed := bytes.TrimSpace(plain); len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
			clear(plain)
			failed[item.ID] = errors.New("invalid sync row")
			continue
		}
		*rows = append(*rows, domain.StoredRow{ID: item.ID, ETag: item.ETag, Data: plain, Project: item.ProjectID, ProjectSet: item.ProjectSet})
	}
	if len(again) > 0 {
		return s.pullBatch(ctx, p, key, scope, again, false, rows, failed)
	}
	return nil
}
func (s *syncClient) load(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) (*domain.StoredRow, error) {
	rows, failed, err := s.pull(ctx, p, key, scope, []string{id})
	if err != nil {
		return nil, err
	}
	if err := failed[id]; err != nil {
		return nil, err
	}
	for _, r := range rows {
		if r.ID == id {
			return &r, nil
		}
	}
	return nil, api.Err(404, "THREAD_NOT_FOUND", "The requested data was not found")
}

// listStatus is one page of row ids, newest first.
func (s *syncClient) listStatus(ctx context.Context, p *auth.Principal, scope, cursor, project string, limit int) ([]string, string, error) {
	var status struct {
		Updates []struct {
			ID string `json:"id"`
		} `json:"updates"`
		Next string `json:"next_cursor"`
	}
	in := api.Object{"scope": scope, "cursor": cursor, "limit": limit, "direction": "desc"}
	if scope == "chat" && project != "" {
		in["project_id"] = project
	}
	if err := s.call(ctx, p, "/v1/sync/list-status", in, &status); err != nil {
		return nil, "", err
	}
	ids := []string{}
	for _, item := range status.Updates {
		// Sync only filters chats by project. Document IDs encode their
		// parent, so select them before pulling any document contents.
		if scope == "project_document" && project != "" && !strings.HasPrefix(item.ID, project+"/") {
			continue
		}
		ids = append(ids, item.ID)
	}
	return ids, status.Next, nil
}

func (s *syncClient) ids(ctx context.Context, p *auth.Principal, scope, project string) ([]string, error) {
	all, cursor := []string{}, ""
	for {
		ids, next, err := s.listStatus(ctx, p, scope, cursor, project, listStatusPage)
		if err != nil {
			return nil, err
		}
		all = append(all, ids...)
		if next == "" {
			return all, nil
		}
		if next == cursor {
			return nil, api.Err(502, "UPSTREAM_REFUSED", "The storage cursor did not advance")
		}
		cursor = next
	}
}

func (s *syncClient) list(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, cursor, project string, limit int) (rowPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	ids, next, err := s.listStatus(ctx, p, scope, cursor, project, limit)
	if err != nil {
		return rowPage{}, err
	}
	page := rowPage{Rows: []domain.StoredRow{}, Next: next}
	if len(ids) != 0 {
		page.Rows, page.Unreadable, err = s.pull(ctx, p, key, scope, ids)
	}
	return page, err
}
func (s *syncClient) push(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, row *domain.StoredRow, nonce string) (string, error) {
	var match any
	if row.ETag != "" {
		match = row.ETag
	}
	metadata := api.Object{}
	if scope == "chat" || scope == "project_document" {
		metadata["projectId"] = row.Project
	}
	if scope == "chat" && row.Restore {
		metadata["restoreDeleted"] = true
	}
	if scope == "profile" {
		metadata["profile_sync_protocol"] = 2
	}
	var result struct {
		ETag string `json:"etag"`
	}
	err := s.callKey(ctx, p, "/v1/sync/push", key, api.Object{"scope": scope, "id": row.ID, "plaintext": base64.StdEncoding.EncodeToString(row.Data), "if_match": match, "idempotency_key": nonce, "metadata": metadata}, &result)
	return result.ETag, err
}

// An empty if_match has sync read the row's revision, delete at it, retry a
// race, and treat a missing row as deleted.
func (s *syncClient) remove(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) error {
	return s.callKey(ctx, p, "/v1/sync/delete", key, api.Object{"scope": scope, "id": id, "if_match": "", "idempotency_key": uuid.NewString()}, nil)
}

func (s *syncClient) removeRevision(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, revision string) error {
	return s.callKey(ctx, p, "/v1/sync/delete", key, api.Object{"scope": "chat", "id": id, "if_match": revision, "idempotency_key": uuid.NewString()}, nil)
}
