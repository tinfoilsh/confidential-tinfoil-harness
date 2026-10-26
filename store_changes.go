package main

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Change tokens describe storage metadata, without loading chat contents.
func (h *harness) storageChanges(ctx context.Context, p *principal) (any, error) {
	var chats struct {
		Current string `json:"current_revision"`
	}
	if err := h.syncAPI.call(ctx, p, "/v1/sync/revision-summary", struct{}{}, &chats); err != nil {
		return nil, err
	}
	tokens := map[string]string{"chats": chats.Current}
	for _, scope := range []string{"profile", "project", "project_document"} {
		var status struct {
			Updates []struct {
				ID        string `json:"id"`
				ETag      string `json:"etag"`
				UpdatedAt string `json:"updated_at"`
			} `json:"updates"`
			Deletes []struct {
				ID        string `json:"id"`
				DeletedAt string `json:"deleted_at"`
			} `json:"deletes"`
		}
		direction := "desc"
		if scope == "profile" {
			direction = "asc"
		}
		if err := h.syncAPI.call(ctx, p, "/v1/sync/list-status", api.Object{"scope": scope, "direction": direction, "limit": 1}, &status); err != nil {
			return nil, err
		}
		tokens[scope] = api.HashHex(api.Raw(status))
	}
	return struct {
		Chats    string `json:"chats"`
		Profile  string `json:"profile"`
		Projects string `json:"projects"`
	}{tokens["chats"], tokens["profile"], api.HashHex([]byte(tokens["project"] + tokens["project_document"]))}, nil
}
