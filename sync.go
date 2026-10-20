package main

import (
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

type storedRow struct {
	ID, ETag string
	Data     object
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
	return postJSONWithHeaders(ctx, s.Client, s.URL+path, string(p.JWT), map[string]string{"X-Sync-Protocol": syncProtocol}, in, out)
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
		var data object
		err = json.Unmarshal(plain, &data)
		clear(plain)
		if err != nil || data == nil {
			failed[item.ID] = errors.New("invalid sync row")
			continue
		}
		if item.ProjectSet {
			data["projectId"] = item.ProjectID
			if item.ProjectID != nil {
				data["projectId"] = *item.ProjectID
			}
		}
		*rows = append(*rows, storedRow{ID: item.ID, ETag: item.ETag, Data: data})
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
	if scope == "chat" {
		metadata["projectId"] = row.Data["projectId"]
		if row.restore {
			metadata["restoreDeleted"] = true
		}
	}
	if scope == "profile" {
		metadata["profile_sync_protocol"] = 2
	}
	if scope == "project_document" {
		metadata["projectId"] = row.Data["projectId"]
	}
	plaintext := raw(row.Data)
	var result struct {
		ETag string `json:"etag"`
	}
	err := s.call(ctx, p, "/v1/sync/push", object{"scope": scope, "id": row.ID, "key": key.base64(), "plaintext": base64.StdEncoding.EncodeToString(plaintext), "if_match": match, "idempotency_key": nonce, "metadata": metadata}, &result)
	return result.ETag, err
}

// An empty if_match has sync read the row's revision, delete at it, retry a
// race, and treat a missing row as deleted.
func (s *syncClient) remove(ctx context.Context, p *principal, key contentKey, scope, id string) error {
	return s.call(ctx, p, "/v1/sync/delete", object{"scope": scope, "id": id, "key": key.base64(), "if_match": "", "idempotency_key": uuid.NewString()}, nil)
}

// mutate always re-applies the operation to the freshly pulled row. A conflict
// never causes an old whole-row image to overwrite another writer's fields.
func (h *harness) eachRow(ctx context.Context, p *principal, key contentKey, scope, project string, visit func(*storedRow) error) error {
	return h.eachRowOrUnreadable(ctx, p, key, scope, project, visit, func(string) {})
}

func (h *harness) eachRowOrUnreadable(ctx context.Context, p *principal, key contentKey, scope, project string, visit func(*storedRow) error, unreadable func(string)) error {
	cursor := ""
	for {
		page, err := h.storeRows.list(ctx, p, key, scope, cursor, project, 100)
		if err != nil {
			return err
		}
		for _, id := range page.Unreadable {
			unreadable(id)
		}
		for _, row := range page.Rows {
			if err := visit(&row); err != nil {
				return err
			}
		}
		if page.Next == "" {
			return nil
		}
		if cursor == page.Next {
			return apiErr(502, "UPSTREAM_REFUSED", "The storage cursor did not advance")
		}
		cursor = page.Next
	}
}

func (h *harness) mutate(ctx context.Context, p *principal, key contentKey, scope, id string, create bool, change func(object) error) (*storedRow, error) {
	for attempt := 0; attempt < maxCommitAttempts; attempt++ {
		row, err := h.storeRows.load(ctx, p, key, scope, id)
		if err != nil {
			if !create || asAPIError(err).HTTP != 404 {
				return nil, err
			}
			row = &storedRow{ID: id, Data: object{"createdAt": timestamp()}}
		}
		if scope == "chat" {
			normalizeThread(row)
		}
		if err := change(row.Data); err != nil {
			return nil, err
		}
		err = h.store(ctx, p, key, scope, row)
		if err == nil {
			return row, nil
		}
		if asAPIError(err).Code != "REVISION_CONFLICT" {
			return nil, err
		}
	}
	return nil, apiErr(409, "REVISION_CONFLICT", "The row changed during every commit attempt")
}

// store pushes a row at the revision it was read at. A push whose outcome is
// unknown is sent again under the same idempotency key, which sync answers
// with the first push's outcome. A chat naming attachments sync does not hold
// under it is repaired once and pushed again.
func (h *harness) store(ctx context.Context, p *principal, key contentKey, scope string, row *storedRow) error {
	row.Data["updatedAt"] = timestamp()
	nonce, repaired := uuid.NewString(), false
	for attempt := 1; ; attempt++ {
		etag, err := h.storeRows.push(ctx, p, key, scope, row, nonce)
		e := asAPIError(err)
		switch {
		case err == nil:
			row.ETag = etag
			return nil
		case e.Code == "SYNC_OUTCOME_UNKNOWN" && attempt < maxCommitAttempts:
			continue
		case e.Code == "MISSING_ATTACHMENT" && scope == "chat" && !repaired:
			if err := h.repairAttachments(ctx, p, row, e.missing); err != nil {
				return err
			}
			nonce, repaired = uuid.NewString(), true
			continue
		}
		return err
	}
}
