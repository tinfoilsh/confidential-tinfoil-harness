package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// The store is the harness's typed view of sync: chats, the profile,
// projects and their documents, attachment blobs, shares, search and keys.
// Nothing else calls sync. Rows are read through the legacy reader, so a
// row any client wrote reads in the current format.

var errEveryCommitConflicted = api.Err(409, "REVISION_CONFLICT", "The row changed during every commit attempt")

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
		if api.AsError(err).Code != "SYNC_OUTCOME_UNKNOWN" || attempt == maxCommitAttempts {
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
			if !create || api.AsError(err).HTTP != 404 {
				return err
			}
			row = &storedRow{ID: id}
		}
		if err := edit(row, exists); err != nil {
			return err
		}
		err = h.push(ctx, p, key, scope, row)
		if err == nil || api.AsError(err).Code != "REVISION_CONFLICT" {
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
		for _, id := range slices.Sorted(maps.Keys(page.Unreadable)) {
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
			return api.Err(502, "UPSTREAM_REFUSED", "The storage cursor did not advance")
		}
		cursor = page.Next
	}
}

func (h *harness) eachRow(ctx context.Context, p *principal, key contentKey, scope, project string, visit func(*storedRow) error) error {
	return h.eachRowOrUnreadable(ctx, p, key, scope, project, visit, func(string) {})
}

// refuseUnreadable fails a read that would leave out a row this harness
// cannot read, as a turn of a project does: it reads every document, and one
// left out would change what the model is told.
func refuseUnreadable(_ string, err error) error { return err }

// Chats

func (h *harness) loadThread(ctx context.Context, p *principal, key contentKey, id string) (*threadRow, error) {
	row, err := h.storeRows.load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	return readThreadRow(row)
}

func (h *harness) threadBatch(ctx context.Context, p *principal, key contentKey, ids []string) ([]*threadRow, error) {
	rows, failed, err := h.storeRows.pull(ctx, p, key, "chat", ids)
	if err != nil {
		return nil, err
	}
	for _, err := range failed {
		if api.AsError(err).HTTP != 409 {
			return nil, err
		}
	}
	threads := make([]*threadRow, 0, len(rows))
	for i := range rows {
		// A chat this harness cannot read is left out, as one sync cannot
		// open is.
		if t, err := readThreadRow(&rows[i]); err == nil {
			threads = append(threads, t)
		}
	}
	return threads, nil
}

func (h *harness) removeThreadRevision(ctx context.Context, p *principal, key contentKey, id, revision string) error {
	return h.storeRows.removeRevision(ctx, p, key, id, revision)
}

// threadPage is one page of a user's chats, newest first, or of a project's.
func (h *harness) threadPage(ctx context.Context, p *principal, key contentKey, cursor, project string, limit int) ([]*threadRow, string, error) {
	page, err := h.storeRows.list(ctx, p, key, "chat", cursor, project, limit)
	if err != nil {
		return nil, "", err
	}
	threads := make([]*threadRow, 0, len(page.Rows))
	for i := range page.Rows {
		// A chat this harness cannot read is left out, as one sync cannot
		// open is.
		if t, err := readThreadRow(&page.Rows[i]); err == nil {
			threads = append(threads, t)
		}
	}
	return threads, page.Next, nil
}

// saveThread pushes a chat at the revision it was read at. A chat naming
// attachments sync does not hold under it is repaired once and pushed again.
func (h *harness) saveThread(ctx context.Context, p *principal, key contentKey, row *threadRow) error {
	row.RecordVersion = recordVersion
	row.UpdatedAt = api.Timestamp()
	for repaired := false; ; repaired = true {
		stored := storedRow{ID: row.ID, ETag: row.etag, Data: api.Raw(row.Thread), project: row.ProjectID, projectSet: true, restore: row.restore}
		err := h.push(ctx, p, key, "chat", &stored)
		if err == nil {
			row.etag = stored.ETag
			return nil
		}
		if e := api.AsError(err); e.Code != "MISSING_ATTACHMENT" || repaired {
			return err
		} else if err := h.repairAttachments(ctx, p, &row.Thread, e.Missing); err != nil {
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
		if api.AsError(err).Code != "REVISION_CONFLICT" {
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
	return h.syncAPI.callKey(ctx, p, "/v1/sync/fork", key, api.Object{"source_id": source, "target_id": target, "message_count": count, "title": title, "created_at": api.Timestamp(), "idempotency_key": uuid.NewString()}, nil)
}

// searchResults are the chats a search found, in its order, and how many of
// them sync or this harness cannot open, which are left out. A chat deleted
// since it was indexed is not a result. One sync could not fetch in time
// fails the search, which may find it when run again.
func (h *harness) searchResults(ctx context.Context, p *principal, key contentKey, ids []string) ([]*threadRow, int, error) {
	rows, failed, err := h.storeRows.pull(ctx, p, key, "chat", ids)
	if err != nil {
		return nil, 0, err
	}
	pulled := map[string]*storedRow{}
	for i := range rows {
		pulled[rows[i].ID] = &rows[i]
	}
	found, unreadable := []*threadRow{}, 0
	for _, id := range ids {
		row, pulledRow := pulled[id]
		switch err := failed[id]; {
		case errors.Is(err, errRowNotFetched):
			return nil, 0, err
		case err != nil:
			unreadable++
		case pulledRow:
			if thread, err := readThreadRow(row); err != nil {
				unreadable++
			} else {
				found = append(found, thread)
			}
		}
	}
	return found, unreadable, nil
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
	if err := h.syncAPI.callKey(ctx, p, "/v1/search/query", key, api.Object{"query": query, "limit": limit}, &found); err != nil {
		return nil, false, err
	}
	ids := make([]string, len(found.Results))
	for i, result := range found.Results {
		ids[i] = result.ID
	}
	return ids, found.NeedsReindex, nil
}

const (
	reindexRunning   = "running"
	reindexCompleted = "completed"
)

// reindexChats has sync index a user's chats, reporting whether more indexing is needed.
func (h *harness) reindexChats(ctx context.Context, p *principal, key contentKey) (bool, error) {
	var job struct {
		Status  string `json:"status"`
		Partial bool   `json:"partial"`
	}
	if err := h.syncAPI.call(ctx, p, "/v1/search/reindex", keyedBody(key, api.Object{}, true), &job); err != nil {
		return false, err
	}
	switch job.Status {
	case reindexRunning:
		return true, nil
	case reindexCompleted:
		// A clean partial checkpoint resumes on the next request.
		return job.Partial, nil
	default:
		return false, api.Err(http.StatusBadGateway, "SEARCH_INDEX_FAILED", "The search index could not be rebuilt")
	}
}

// Profile

// loadProfile is the user's profile, or nil without one.
func (h *harness) loadProfile(ctx context.Context, p *principal, key contentKey) (*storedProfile, error) {
	row, err := h.storeRows.load(ctx, p, key, "profile", "profile")
	if err != nil {
		if api.AsError(err).HTTP == 404 {
			return nil, nil
		}
		return nil, err
	}
	profile, err := readProfile(row.Data)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

func (h *harness) mutateProfile(ctx context.Context, p *principal, key contentKey, change func(*storedProfile) error) (storedProfile, error) {
	var profile storedProfile
	err := h.commitRow(ctx, p, key, "profile", "profile", true, func(row *storedRow, exists bool) error {
		profile = storedProfile{}
		if exists {
			var err error
			if profile, err = readProfile(row.Data); err != nil {
				return err
			}
		}
		before := api.Raw(profile)
		if err := change(&profile); err != nil {
			return err
		}
		if bytes.Equal(before, api.Raw(profile)) {
			return errProfileUnchanged
		}
		profile.UpdatedAt = api.Timestamp()
		profile.RecordVersion = recordVersion
		row.Data = api.Raw(profile)
		return nil
	})
	if errors.Is(err, errProfileUnchanged) {
		err = nil
	}
	return profile, err
}

var errProfileUnchanged = errors.New("profile unchanged")

// Projects

func (h *harness) loadProject(ctx context.Context, p *principal, key contentKey, id string) (Project, error) {
	row, err := h.storeRows.load(ctx, p, key, "project", id)
	if err != nil {
		return Project{}, err
	}
	return readProject(row)
}

func (h *harness) projectPage(ctx context.Context, p *principal, key contentKey, cursor string, limit int) ([]Project, string, error) {
	page, err := h.storeRows.list(ctx, p, key, "project", cursor, "", limit)
	if err != nil {
		return nil, "", err
	}
	projects := make([]Project, 0, len(page.Rows))
	for i := range page.Rows {
		// A project this harness cannot read is left out, as one sync cannot
		// open is.
		if project, err := readProject(&page.Rows[i]); err == nil {
			projects = append(projects, project)
		}
	}
	return projects, page.Next, nil
}

// mutateProject applies change to a project, or with create to a new one.
func (h *harness) mutateProject(ctx context.Context, p *principal, key contentKey, id string, create bool, change func(*Project)) (Project, error) {
	var project Project
	err := h.commitRow(ctx, p, key, "project", id, create, func(row *storedRow, exists bool) error {
		project = Project{ID: id, CreatedAt: api.Timestamp()}
		if exists {
			var err error
			if project, err = readProject(row); err != nil {
				return err
			}
		}
		change(&project)
		project.UpdatedAt = api.Timestamp()
		row.Data = api.Raw(storedProject{project, recordVersion})
		return nil
	})
	return project, err
}

// Sync deletes every project and project document in one step.
func (h *harness) removeAllProjects(ctx context.Context, p *principal, key contentKey) (int, error) {
	var result struct {
		Deleted int `json:"deleted"`
	}
	err := h.syncAPI.callKey(ctx, p, "/v1/sync/delete-all-projects", key, api.Object{"idempotency_key": uuid.NewString()}, &result)
	return result.Deleted, err
}

// projectDocuments are every document of a project. A document sync cannot
// open, or this harness cannot read, is passed to unreadable with why, which
// fails the read by returning an error or leaves the document out by
// returning nil.
func (h *harness) projectDocuments(ctx context.Context, p *principal, key contentKey, id string, unreadable func(row string, err error) error) ([]ProjectDocument, error) {
	// Unfiltered pages can repeat a row that changed while listing; the
	// later copy is the newer one.
	var order []string
	latest := map[string]documentCopy{}
	keep := func(rowID string, listed documentCopy) {
		if _, ok := latest[rowID]; !ok {
			order = append(order, rowID)
		}
		latest[rowID] = listed
	}
	for cursor := ""; ; {
		page, err := h.storeRows.list(ctx, p, key, "project_document", cursor, id, 100)
		if err != nil {
			return nil, err
		}
		for i := range page.Rows {
			keep(page.Rows[i].ID, documentCopy{row: &page.Rows[i]})
		}
		for _, rowID := range slices.Sorted(maps.Keys(page.Unreadable)) {
			keep(rowID, documentCopy{err: page.Unreadable[rowID]})
		}
		if page.Next == "" {
			break
		}
		if page.Next == cursor {
			return nil, api.Err(502, "UPSTREAM_REFUSED", "The document cursor did not advance")
		}
		cursor = page.Next
	}
	list := []ProjectDocument{}
	for _, rowID := range order {
		listed := latest[rowID]
		doc, err := ProjectDocument{}, listed.err
		if err == nil {
			doc, err = readProjectDocument(listed.row, id)
		}
		if err != nil {
			if err := unreadable(rowID, err); err != nil {
				return nil, err
			}
			continue
		}
		list = append(list, doc)
	}
	return list, nil
}

// documentCopy is a document row as a page listed it, or why sync could not
// open it.
type documentCopy struct {
	row *storedRow
	err error
}

// createProjectDocument stores a new document under its project.
func (h *harness) createProjectDocument(ctx context.Context, p *principal, key contentKey, doc ProjectDocument) (ProjectDocument, error) {
	id := doc.ProjectID + "/" + doc.ID
	err := h.commitRow(ctx, p, key, "project_document", id, true, func(row *storedRow, exists bool) error {
		row.project, row.projectSet = &doc.ProjectID, true
		doc.CreatedAt = api.Timestamp()
		if exists {
			stored, err := readProjectDocument(row, doc.ProjectID)
			if err != nil {
				return err
			}
			doc.CreatedAt = stored.CreatedAt
		}
		doc.UpdatedAt = api.Timestamp()
		row.Data, row.project, row.projectSet = api.Raw(storedDocument{doc, recordVersion}), &doc.ProjectID, true
		return nil
	})
	return doc, err
}

// Attachments

// putAttachment stores a blob under a chat. Sync derives its id from the
// idempotency key, chat and bytes, so storing the same attachment again
// reuses its blob.
func (h *harness) putAttachment(ctx context.Context, p *principal, chatID, clientID string, data []byte) (string, string, error) {
	in := api.Object{"chat_id": chatID, "plaintext": base64.StdEncoding.EncodeToString(data), "idempotency_key": clientID}
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
		if err = h.syncAPI.call(ctx, p, "/v1/attachment/put", in, &stored); err == nil || api.AsError(err).Status != 502 {
			break
		}
	}
	if err != nil {
		return "", "", err
	}
	if !enclaveAttachmentID.MatchString(stored.ID) || stored.Key == "" {
		return "", "", api.Err(502, "UPSTREAM_REFUSED", "The sync service returned an invalid attachment")
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
	if err := h.syncAPI.call(ctx, p, path, api.Object{"id": id, "att_key": attKey}, &result); err != nil {
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
	if err := h.syncAPI.call(ctx, p, "/v1/share/seal", api.Object{"plaintext": base64.StdEncoding.EncodeToString(plaintext)}, &sealed); err != nil {
		return nil, "", err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(sealed.Ciphertext)
	return ciphertext, sealed.ShareKey, err
}

func (h *harness) openSealedShare(ctx context.Context, shareKey string, ciphertext []byte) ([]byte, error) {
	var opened struct {
		Plaintext string `json:"plaintext"`
	}
	if err := h.syncAPI.call(ctx, &principal{Anonymous: true}, "/v1/share/open", api.Object{"share_key": shareKey, "ciphertext": base64.StdEncoding.EncodeToString(ciphertext)}, &opened); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(opened.Plaintext)
}

// Keys and imports

func (h *harness) authorizeArchive(ctx context.Context, p *principal, keyBytes []byte, operation string) error {
	if len(keyBytes) == 0 {
		return nil
	}
	key, err := decodeContentKey(keyBytes)
	if err != nil {
		return err
	}
	defer clear(key[:])
	token, err := h.storeRows.authorizeStorage(ctx, p, key, operation, "")
	if err != nil {
		return err
	}
	p.SyncToken = token
	return nil
}

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
