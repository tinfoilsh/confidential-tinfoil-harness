package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// The store is the harness's typed view of sync: chats, the profile,
// projects and their documents, attachment blobs, shares, search and keys.
// Nothing else calls sync. Rows are read through the legacy reader, so a
// row any client wrote reads in the current format.

var errEveryCommitConflicted = apiErr(409, "REVISION_CONFLICT", "The row changed during every commit attempt")

// push writes a row at the revision it was read at; an empty one creates
// it. A push whose outcome is unknown is sent again under the same
// idempotency key, which sync answers with the first push's outcome.
func (h *harness) push(ctx context.Context, p *principal, key contentKey, scope string, row *storedRow) error {
	nonce := uuid.NewString()
	for attempt := 1; ; attempt++ {
		etag, err := h.storeRows.push(ctx, p, key, scope, row, nonce)
		if err == nil {
			row.ETag = etag
			return nil
		}
		if asAPIError(err).Code != "SYNC_OUTCOME_UNKNOWN" || attempt == maxCommitAttempts {
			return err
		}
	}
}

// commitRow applies edit to a row as read now and pushes it at that
// revision, again after a conflict, so a conflict never overwrites another
// writer's change with an old copy. With create, a missing row is made.
func (h *harness) commitRow(ctx context.Context, p *principal, key contentKey, scope, id string, create bool, edit func(row *storedRow, exists bool) error) error {
	for attempt := 0; attempt < maxCommitAttempts; attempt++ {
		row, err := h.storeRows.load(ctx, p, key, scope, id)
		exists := err == nil
		if !exists {
			if !create || asAPIError(err).HTTP != 404 {
				return err
			}
			row = &storedRow{ID: id}
		}
		if err := edit(row, exists); err != nil {
			return err
		}
		err = h.push(ctx, p, key, scope, row)
		if err == nil || asAPIError(err).Code != "REVISION_CONFLICT" {
			return err
		}
	}
	return errEveryCommitConflicted
}

// eachRowOrUnreadable visits every row of a scope, or of a project's chats,
// and names each row sync holds but cannot open.
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

func (h *harness) eachRow(ctx context.Context, p *principal, key contentKey, scope, project string, visit func(*storedRow) error) error {
	return h.eachRowOrUnreadable(ctx, p, key, scope, project, visit, func(string) {})
}

// Chats

func (h *harness) loadThread(ctx context.Context, p *principal, key contentKey, id string) (*threadRow, error) {
	row, err := h.storeRows.load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	return readThreadRow(row), nil
}

// threadPage is one page of a user's chats, newest first, or of a project's.
func (h *harness) threadPage(ctx context.Context, p *principal, key contentKey, cursor, project string, limit int) ([]*threadRow, string, error) {
	page, err := h.storeRows.list(ctx, p, key, "chat", cursor, project, limit)
	if err != nil {
		return nil, "", err
	}
	threads := make([]*threadRow, len(page.Rows))
	for i := range page.Rows {
		threads[i] = readThreadRow(&page.Rows[i])
	}
	return threads, page.Next, nil
}

// saveThread pushes a chat at the revision it was read at. A chat naming
// attachments sync does not hold under it is repaired once and pushed again.
func (h *harness) saveThread(ctx context.Context, p *principal, key contentKey, row *threadRow) error {
	row.UpdatedAt = timestamp()
	for repaired := false; ; repaired = true {
		stored := storedRow{ID: row.ID, ETag: row.etag, Data: raw(row.Thread), project: row.ProjectID, projectSet: true, restore: row.restore}
		err := h.push(ctx, p, key, "chat", &stored)
		if err == nil {
			row.etag = stored.ETag
			return nil
		}
		if e := asAPIError(err); e.Code != "MISSING_ATTACHMENT" || repaired {
			return err
		} else if err := h.repairAttachments(ctx, p, &row.Thread, e.missing); err != nil {
			return err
		}
	}
}

// mutateThread applies change to a stored chat as read now, again after a
// conflict, so a conflict never overwrites another writer's change.
func (h *harness) mutateThread(ctx context.Context, p *principal, key contentKey, id string, change func(*Thread) error) (*threadRow, error) {
	for attempt := 0; attempt < maxCommitAttempts; attempt++ {
		row, err := h.loadThread(ctx, p, key, id)
		if err != nil {
			return nil, err
		}
		if err := change(&row.Thread); err != nil {
			return nil, err
		}
		err = h.saveThread(ctx, p, key, row)
		if err == nil {
			return row, nil
		}
		if asAPIError(err).Code != "REVISION_CONFLICT" {
			return nil, err
		}
	}
	return nil, errEveryCommitConflicted
}

// A deletion needs no revision: sync reads the row's, retries a race, and
// treats a missing row as deleted.
func (h *harness) removeRow(ctx context.Context, p *principal, key contentKey, scope, id string) error {
	return h.storeRows.remove(ctx, p, key, scope, id)
}

// chatIDs lists every chat id of a user, or of a project, unopened.
func (h *harness) chatIDs(ctx context.Context, p *principal, project string) ([]string, error) {
	return h.storeRows.ids(ctx, p, "chat", project)
}

// forkStored has sync copy a chat's first count messages, with their
// attachments, into a new chat.
func (h *harness) forkStored(ctx context.Context, p *principal, key contentKey, source, target, title string, count int) error {
	return h.syncAPI.call(ctx, p, "/v1/sync/fork", object{"source_id": source, "target_id": target, "key": key.base64(), "message_count": count, "title": title, "created_at": timestamp(), "idempotency_key": uuid.NewString()}, nil)
}

// searchChats finds the ids of a user's chats that match a query, and
// whether sync must index them first.
func (h *harness) searchChats(ctx context.Context, p *principal, key contentKey, query string, limit int) ([]string, bool, error) {
	var found struct {
		NeedsReindex bool `json:"needs_reindex"`
		Results      []struct {
			ID string `json:"id"`
		} `json:"results"`
	}
	if err := h.syncAPI.call(ctx, p, "/v1/search/query", object{"key": key.base64(), "query": query, "limit": limit}, &found); err != nil {
		return nil, false, err
	}
	ids := make([]string, len(found.Results))
	for i, result := range found.Results {
		ids[i] = result.ID
	}
	return ids, found.NeedsReindex, nil
}

// reindexChats has sync index a user's chats, reporting whether it is still running.
func (h *harness) reindexChats(ctx context.Context, p *principal, key contentKey) (bool, error) {
	var job struct {
		Status string `json:"status"`
	}
	err := h.syncAPI.call(ctx, p, "/v1/search/reindex", object{"keys": []object{{"key": key.base64()}}}, &job)
	return job.Status == "running", err
}

// Profile

// loadProfile is the user's profile, or nil without one.
func (h *harness) loadProfile(ctx context.Context, p *principal, key contentKey) (*storedProfile, error) {
	row, err := h.storeRows.load(ctx, p, key, "profile", "profile")
	if err != nil {
		if asAPIError(err).HTTP == 404 {
			return nil, nil
		}
		return nil, err
	}
	profile := readProfile(row.Data)
	return &profile, nil
}

func (h *harness) mutateProfile(ctx context.Context, p *principal, key contentKey, change func(*storedProfile)) (storedProfile, error) {
	var profile storedProfile
	err := h.commitRow(ctx, p, key, "profile", "profile", true, func(row *storedRow, exists bool) error {
		profile = storedProfile{}
		if exists {
			profile = readProfile(row.Data)
		}
		change(&profile)
		profile.UpdatedAt = timestamp()
		row.Data = raw(profile)
		return nil
	})
	return profile, err
}

// Projects

func (h *harness) loadProject(ctx context.Context, p *principal, key contentKey, id string) (Project, error) {
	row, err := h.storeRows.load(ctx, p, key, "project", id)
	if err != nil {
		return Project{}, err
	}
	return readProject(row), nil
}

func (h *harness) projectPage(ctx context.Context, p *principal, key contentKey, cursor string, limit int) ([]Project, string, error) {
	page, err := h.storeRows.list(ctx, p, key, "project", cursor, "", limit)
	if err != nil {
		return nil, "", err
	}
	projects := make([]Project, len(page.Rows))
	for i := range page.Rows {
		projects[i] = readProject(&page.Rows[i])
	}
	return projects, page.Next, nil
}

// mutateProject applies change to a project, or with create to a new one.
func (h *harness) mutateProject(ctx context.Context, p *principal, key contentKey, id string, create bool, change func(*Project)) (Project, error) {
	var project Project
	err := h.commitRow(ctx, p, key, "project", id, create, func(row *storedRow, exists bool) error {
		project = Project{ID: id, CreatedAt: timestamp()}
		if exists {
			project = readProject(row)
		}
		change(&project)
		project.UpdatedAt = timestamp()
		row.Data = raw(project)
		return nil
	})
	return project, err
}

// Sync deletes every project and project document in one step.
func (h *harness) removeAllProjects(ctx context.Context, p *principal, key contentKey) (int, error) {
	var result struct {
		Deleted int `json:"deleted"`
	}
	err := h.syncAPI.call(ctx, p, "/v1/sync/delete-all-projects", object{"key": key.base64(), "idempotency_key": uuid.NewString()}, &result)
	return result.Deleted, err
}

// projectDocuments are every document of a project.
func (h *harness) projectDocuments(ctx context.Context, p *principal, key contentKey, id string) ([]ProjectDocument, error) {
	list, cursor, index := []ProjectDocument{}, "", map[string]int{}
	for {
		page, err := h.storeRows.list(ctx, p, key, "project_document", cursor, id, 100)
		if err != nil {
			return nil, err
		}
		for _, row := range page.Rows {
			doc := readProjectDocument(&row, id)
			// Unfiltered pages can repeat a row that changed while listing;
			// the later copy is the newer one.
			if i, ok := index[row.ID]; ok {
				list[i] = doc
				continue
			}
			index[row.ID] = len(list)
			list = append(list, doc)
		}
		if page.Next == "" {
			return list, nil
		}
		if page.Next == cursor {
			return nil, apiErr(502, "UPSTREAM_REFUSED", "The document cursor did not advance")
		}
		cursor = page.Next
	}
}

// createProjectDocument stores a new document under its project.
func (h *harness) createProjectDocument(ctx context.Context, p *principal, key contentKey, doc ProjectDocument) (ProjectDocument, error) {
	id := doc.ProjectID + "/" + doc.ID
	err := h.commitRow(ctx, p, key, "project_document", id, true, func(row *storedRow, exists bool) error {
		doc.CreatedAt = timestamp()
		if exists {
			doc.CreatedAt = readProjectDocument(row, doc.ProjectID).CreatedAt
		}
		doc.UpdatedAt = timestamp()
		row.Data, row.project, row.projectSet = raw(doc), &doc.ProjectID, true
		return nil
	})
	return doc, err
}

// Attachments

// putAttachment stores a blob under a chat. Sync derives its id from the
// idempotency key, chat and bytes, so storing the same attachment again
// reuses its blob.
func (h *harness) putAttachment(ctx context.Context, p *principal, chatID, clientID string, data []byte) (string, string, error) {
	in := object{"chat_id": chatID, "plaintext": base64.StdEncoding.EncodeToString(data), "idempotency_key": clientID}
	var stored struct {
		ID  string `json:"id"`
		Key string `json:"att_key"`
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-ctx.Done():
				return "", "", ctx.Err()
			}
		}
		stored.ID, stored.Key = "", ""
		// A purge of the same chat or a lost index write fails with a 502.
		// The idempotency key makes the retry store the same blob.
		if err = h.syncAPI.call(ctx, p, "/v1/attachment/put", in, &stored); err == nil || asAPIError(err).Status != 502 {
			break
		}
	}
	if err != nil {
		return "", "", err
	}
	if !enclaveAttachmentID.MatchString(stored.ID) || stored.Key == "" {
		return "", "", apiErr(502, "UPSTREAM_REFUSED", "The sync service returned an invalid attachment")
	}
	return stored.ID, stored.Key, nil
}

// getAttachment reads a blob, or with public a blob a share holds.
func (h *harness) getAttachment(ctx context.Context, p *principal, id, attKey string, public bool) ([]byte, error) {
	path := "/v1/attachment/get"
	if public {
		path += "-public"
	}
	var result struct {
		Plaintext string `json:"plaintext"`
	}
	if err := h.syncAPI.call(ctx, p, path, object{"id": id, "att_key": attKey}, &result); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(result.Plaintext)
}

// Shares

// sealShare seals a share's payload under a key the link carries.
func (h *harness) sealShare(ctx context.Context, p *principal, plaintext []byte) ([]byte, string, error) {
	var sealed struct {
		Ciphertext string `json:"ciphertext"`
		ShareKey   string `json:"share_key"`
	}
	if err := h.syncAPI.call(ctx, p, "/v1/share/seal", object{"plaintext": base64.StdEncoding.EncodeToString(plaintext)}, &sealed); err != nil {
		return nil, "", err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(sealed.Ciphertext)
	return ciphertext, sealed.ShareKey, err
}

func (h *harness) openSealedShare(ctx context.Context, shareKey string, ciphertext []byte) ([]byte, error) {
	var opened struct {
		Plaintext string `json:"plaintext"`
	}
	if err := h.syncAPI.call(ctx, &principal{Anonymous: true}, "/v1/share/open", object{"share_key": shareKey, "ciphertext": base64.StdEncoding.EncodeToString(ciphertext)}, &opened); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(opened.Plaintext)
}

// Keys and imports

// keyCall passes a key management request to sync as the client sent it.
func (h *harness) keyCall(ctx context.Context, p *principal, path string, in json.RawMessage) (json.RawMessage, error) {
	var out json.RawMessage
	err := h.syncAPI.call(ctx, p, path, in, &out)
	return out, err
}

// syncImport makes one call of an archive import, whose job sync runs.
func (h *harness) syncImport(ctx context.Context, p *principal, step string, in, out any) error {
	return h.syncAPI.call(ctx, p, "/v1/import/"+step, in, out)
}
