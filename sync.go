package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// storedRow is a row as sync holds it: sealed JSON, opened only by the
// store's readers.
type storedRow struct {
	ID, ETag string
	Data     json.RawMessage
	// project is the project sync files a chat or document under, apart
	// from its sealed data; projectSet says sync reported one, even none.
	project    *string
	projectSet bool
	// restore lets a create reuse the id of a chat deleted from sync, as a
	// chat moved to the device and back does.
	restore bool
}
type rowPage struct {
	Rows []storedRow
	Next string
	// Rows sync holds but cannot open, such as one sealed to a retired key.
	// A page lists them rather than failing, so one row cannot hide the rest.
	Unreadable []string
}
type rowStore interface {
	authorizeRun(context.Context, *principal, contentKey, string, *string) (secret, error)
	load(context.Context, *principal, contentKey, string, string) (*storedRow, error)
	list(context.Context, *principal, contentKey, string, string, string, int) (rowPage, error)
	push(context.Context, *principal, contentKey, string, *storedRow, string) (string, error)
	// remove deletes a row whatever its revision; a missing row is gone.
	remove(context.Context, *principal, contentKey, string, string) error
	// ids lists every row of a scope, or a project's chats, unopened.
	ids(context.Context, *principal, string, string) ([]string, error)
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

func (s *syncClient) call(ctx context.Context, p *principal, path string, in, out any) error {
	if s == nil || s.Client == nil {
		return apiErr(503, "UPSTREAM_REFUSED", "The sync enclave is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, apiKeyKey{}, "")
	ctx = context.WithValue(ctx, usageContextKey{}, false)
	return postJSONWithHeaders(ctx, s.Client, s.URL+path, string(cmp.Or(p.SyncToken, p.JWT)), map[string]string{"X-Sync-Protocol": syncProtocol}, in, out)
}

func (s *syncClient) authorizeRun(ctx context.Context, p *principal, key contentKey, threadID string, projectID *string) (secret, error) {
	var out struct {
		Token     secret    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	err := s.call(ctx, p, "/v1/runs/authorize", struct {
		ThreadID  string  `json:"thread_id"`
		ProjectID *string `json:"project_id,omitempty"`
		Key       string  `json:"key"`
	}{threadID, projectID, key.base64()}, &out)
	if err != nil {
		return "", err
	}
	if out.Token == "" || !out.ExpiresAt.After(nowUTC().Add(runTimeout)) {
		return "", apiErr(502, "UPSTREAM_REFUSED", "The storage service returned an invalid run credential")
	}
	return out.Token, nil
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
func (s *syncClient) pull(ctx context.Context, p *principal, key contentKey, scope string, ids []string) ([]storedRow, map[string]error, error) {
	rows, failed := []storedRow{}, map[string]error{}
	for start := 0; start < len(ids); start += pullBatch {
		if err := s.pullBatch(ctx, p, key, scope, ids[start:min(len(ids), start+pullBatch)], true, &rows, failed); err != nil {
			return nil, nil, err
		}
	}
	return rows, failed, nil
}

func (s *syncClient) pullBatch(ctx context.Context, p *principal, key contentKey, scope string, ids []string, retry bool, rows *[]storedRow, failed map[string]error) error {
	var result struct {
		Items []pullItem `json:"items"`
	}
	err := s.call(ctx, p, "/v1/sync/pull", object{"scope": scope, "ids": ids, "keys": []object{{"key": key.base64()}}}, &result)
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
			default:
				failed[item.ID] = downstreamError(409, raw(object{"code": item.Code, "key_id": item.KeyID}))
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
		*rows = append(*rows, storedRow{ID: item.ID, ETag: item.ETag, Data: plain, project: item.ProjectID, projectSet: item.ProjectSet})
	}
	if len(again) > 0 {
		return s.pullBatch(ctx, p, key, scope, again, false, rows, failed)
	}
	return nil
}
func (s *syncClient) load(ctx context.Context, p *principal, key contentKey, scope, id string) (*storedRow, error) {
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
	return nil, apiErr(404, "THREAD_NOT_FOUND", "The requested data was not found")
}

// listStatus is one page of row ids, newest first.
func (s *syncClient) listStatus(ctx context.Context, p *principal, scope, cursor, project string, limit int) ([]string, string, error) {
	var status struct {
		Updates []struct {
			ID string `json:"id"`
		} `json:"updates"`
		Next string `json:"next_cursor"`
	}
	in := object{"scope": scope, "cursor": cursor, "limit": limit, "direction": "desc"}
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

func (s *syncClient) ids(ctx context.Context, p *principal, scope, project string) ([]string, error) {
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
			return nil, apiErr(502, "UPSTREAM_REFUSED", "The storage cursor did not advance")
		}
		cursor = next
	}
}

func (s *syncClient) list(ctx context.Context, p *principal, key contentKey, scope, cursor, project string, limit int) (rowPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	ids, next, err := s.listStatus(ctx, p, scope, cursor, project, limit)
	if err != nil {
		return rowPage{}, err
	}
	page := rowPage{Rows: []storedRow{}, Next: next}
	if len(ids) != 0 {
		var failed map[string]error
		page.Rows, failed, err = s.pull(ctx, p, key, scope, ids)
		for id := range failed {
			page.Unreadable = append(page.Unreadable, id)
		}
		slices.Sort(page.Unreadable)
	}
	return page, err
}
func (s *syncClient) push(ctx context.Context, p *principal, key contentKey, scope string, row *storedRow, nonce string) (string, error) {
	var match any
	if row.ETag != "" {
		match = row.ETag
	}
	metadata := object{}
	if scope == "chat" || scope == "project_document" {
		metadata["projectId"] = row.project
	}
	if scope == "chat" && row.restore {
		metadata["restoreDeleted"] = true
	}
	if scope == "profile" {
		metadata["profile_sync_protocol"] = 2
	}
	var result struct {
		ETag string `json:"etag"`
	}
	err := s.call(ctx, p, "/v1/sync/push", object{"scope": scope, "id": row.ID, "key": key.base64(), "plaintext": base64.StdEncoding.EncodeToString(row.Data), "if_match": match, "idempotency_key": nonce, "metadata": metadata}, &result)
	return result.ETag, err
}

// An empty if_match has sync read the row's revision, delete at it, retry a
// race, and treat a missing row as deleted.
func (s *syncClient) remove(ctx context.Context, p *principal, key contentKey, scope, id string) error {
	return s.call(ctx, p, "/v1/sync/delete", object{"scope": scope, "id": id, "key": key.base64(), "if_match": "", "idempotency_key": uuid.NewString()}, nil)
}
