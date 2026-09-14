package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/google/uuid"
)

type storedRow struct {
	ID, ETag string
	Data     object
}
type rowPage struct {
	Rows []storedRow
	Next string
}
type rowStore interface {
	load(context.Context, *principal, contentKey, string, string) (*storedRow, error)
	list(context.Context, *principal, contentKey, string, string, string, int) (rowPage, error)
	push(context.Context, *principal, contentKey, string, *storedRow, string) (string, error)
	remove(context.Context, *principal, contentKey, string, *storedRow) error
}
type syncClient struct {
	URL    string
	Client *http.Client
}

func (s *syncClient) call(ctx context.Context, p *principal, path string, in, out any) error {
	if s == nil || s.Client == nil {
		return apiErr(503, "UPSTREAM_REFUSED", "The sync enclave is unavailable")
	}
	ctx = context.WithValue(ctx, apiKeyKey{}, "")
	ctx = context.WithValue(ctx, usageContextKey{}, false)
	return postJSON(ctx, s.Client, s.URL+path, string(p.JWT), in, out)
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

func (s *syncClient) pull(ctx context.Context, p *principal, key contentKey, scope string, ids []string) ([]storedRow, error) {
	var result struct {
		Items []pullItem `json:"items"`
	}
	err := s.call(ctx, p, "/v1/sync/pull", object{"scope": scope, "ids": ids, "keys": []object{{"key": key.base64()}}}, &result)
	if err != nil {
		return nil, err
	}
	rows := []storedRow{}
	for _, item := range result.Items {
		if !item.OK {
			if item.Code == "NOT_FOUND" {
				continue
			}
			return nil, downstreamError(409, raw(object{"code": item.Code, "key_id": item.KeyID}))
		}
		plain, err := base64.StdEncoding.DecodeString(item.Plaintext)
		if err != nil {
			return nil, errors.New("invalid sync plaintext encoding")
		}
		var data object
		err = json.Unmarshal(plain, &data)
		clear(plain)
		if err != nil || data == nil {
			return nil, errors.New("invalid sync row")
		}
		if item.ProjectSet {
			data["projectId"] = item.ProjectID
			if item.ProjectID != nil {
				data["projectId"] = *item.ProjectID
			}
		}
		rows = append(rows, storedRow{ID: item.ID, ETag: item.ETag, Data: data})
	}
	return rows, nil
}
func (s *syncClient) load(ctx context.Context, p *principal, key contentKey, scope, id string) (*storedRow, error) {
	rows, err := s.pull(ctx, p, key, scope, []string{id})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if r.ID == id {
			return &r, nil
		}
	}
	return nil, apiErr(404, "THREAD_NOT_FOUND", "The requested data was not found")
}
func (s *syncClient) list(ctx context.Context, p *principal, key contentKey, scope, cursor, project string, limit int) (rowPage, error) {
	var status struct {
		Updates []struct {
			ID string `json:"id"`
		} `json:"updates"`
		Next string `json:"next_cursor"`
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	err := s.call(ctx, p, "/v1/sync/list-status", object{"scope": scope, "cursor": cursor, "limit": limit, "project_id": project, "direction": "desc"}, &status)
	if err != nil {
		return rowPage{}, err
	}
	page := rowPage{Rows: []storedRow{}, Next: status.Next}
	ids := []string{}
	for _, item := range status.Updates {
		ids = append(ids, item.ID)
	}
	if len(ids) != 0 {
		page.Rows, err = s.pull(ctx, p, key, scope, ids)
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
		metadata["messageCount"] = len(arr(row.Data["messages"]))
		metadata["projectId"] = row.Data["projectId"]
	}
	if scope == "profile" {
		metadata["profile_sync_protocol"] = 2
	}
	if scope == "project_document" {
		metadata["projectId"] = row.Data["projectId"]
	}
	var result struct {
		ETag string `json:"etag"`
	}
	err := s.call(ctx, p, "/v1/sync/push", object{"scope": scope, "id": row.ID, "key": key.base64(), "plaintext": base64.StdEncoding.EncodeToString(raw(row.Data)), "if_match": match, "idempotency_key": nonce, "metadata": metadata}, &result)
	return result.ETag, err
}
func (s *syncClient) remove(ctx context.Context, p *principal, key contentKey, scope string, row *storedRow) error {
	return s.call(ctx, p, "/v1/sync/delete", object{"scope": scope, "id": row.ID, "key": key.base64(), "if_match": row.ETag, "idempotency_key": uuid.NewString()}, nil)
}

func stampRow(row *storedRow, scope string, fields []string) {
	version, _ := strconv.Atoi(row.ETag)
	row.Data["updatedAt"] = timestamp()
	row.Data["clockVersion"] = version + 1
	if scope == "profile" {
		row.Data["version"] = version + 1
		clocks := obj(row.Data["fieldClocks"])
		for _, field := range fields {
			clock := clone(obj(clocks[field]))
			clock["v"] = max(integer(clock["v"]), version) + 1
			clock["w"] = "harness"
			clocks[field] = clock
		}
		row.Data["fieldClocks"] = clocks
	} else {
		row.Data["clock"] = max(integer(row.Data["clock"]), version) + 1
		row.Data["writer"] = "harness"
	}
}

// mutate always re-applies the operation to the freshly pulled row. A conflict
// never causes an old whole-row image to overwrite another writer's fields.
func (h *harness) eachRow(ctx context.Context, p *principal, key contentKey, scope, project string, visit func(*storedRow) error) error {
	cursor := ""
	for {
		page, err := h.storeRows.list(ctx, p, key, scope, cursor, project, 100)
		if err != nil {
			return err
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

// Collect identifiers before deleting so changing the list cannot skip a page.
func (h *harness) rowIDs(ctx context.Context, p *principal, key contentKey, scope, project string) ([]string, error) {
	ids := []string{}
	err := h.eachRow(ctx, p, key, scope, project, func(row *storedRow) error { ids = append(ids, row.ID); return nil })
	return ids, err
}

func (h *harness) mutate(ctx context.Context, p *principal, key contentKey, scope, id string, create bool, fields []string, change func(object) error) (*storedRow, error) {
	for attempt := 0; attempt < 3; attempt++ {
		row, err := h.storeRows.load(ctx, p, key, scope, id)
		if err != nil {
			if !create || asAPIError(err).HTTP != 404 {
				return nil, err
			}
			row = &storedRow{ID: id, Data: object{"createdAt": timestamp()}}
		}
		if err := change(row.Data); err != nil {
			return nil, err
		}
		stampRow(row, scope, fields)
		etag, err := h.storeRows.push(ctx, p, key, scope, row, uuid.NewString())
		if err == nil {
			row.ETag = etag
			return row, nil
		}
		if asAPIError(err).Code != "REVISION_CONFLICT" {
			return nil, err
		}
	}
	return nil, apiErr(409, "REVISION_CONFLICT", "The row changed during three commit attempts")
}
