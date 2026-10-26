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
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

type store struct {
	repair attachmentRepairer
	rows   rowStore
	api    *syncClient
}

// The store is the harness's typed view of sync: chats, the profile,
// projects and their documents, attachment blobs, shares, search and keys.
// Nothing else calls sync. Rows are read through the legacy reader, so a
// row any client wrote reads in the current format.

var errEveryCommitConflicted = api.Err(409, "REVISION_CONFLICT", "The row changed during every commit attempt")

// push writes a row at the revision it was read at; an empty one creates
// it. A push whose outcome is unknown is sent again under the same
// idempotency key, which sync answers with the first push's outcome.
func (s *store) push(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, row *domain.StoredRow) error {
	nonce := uuid.NewString()
	for attempt := 1; ; attempt++ {
		etag, err := s.rows.push(ctx, p, key, scope, row, nonce)
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
func (s *store) commitRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string, create bool, edit func(row *domain.StoredRow, exists bool) error) error {
	for attempt := 0; attempt < maxCommitAttempts; attempt++ {
		row, err := s.rows.load(ctx, p, key, scope, id)
		exists := err == nil
		if !exists {
			if !create || api.AsError(err).HTTP != 404 {
				return err
			}
			row = &domain.StoredRow{ID: id}
		}
		if err := edit(row, exists); err != nil {
			return err
		}
		err = s.push(ctx, p, key, scope, row)
		if err == nil || api.AsError(err).Code != "REVISION_CONFLICT" {
			return err
		}
	}
	return errEveryCommitConflicted
}

// eachRowOrUnreadable visits every row of a scope, or of a project's chats,
// and names each row sync holds but cannot open.
func (s *store) eachRowOrUnreadable(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, project string, visit func(*domain.StoredRow) error, unreadable func(string)) error {
	cursor := ""
	for {
		page, err := s.rows.list(ctx, p, key, scope, cursor, project, 100)
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

func (s *store) eachRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, project string, visit func(*domain.StoredRow) error) error {
	return s.eachRowOrUnreadable(ctx, p, key, scope, project, visit, func(string) {})
}

// refuseUnreadable fails a read that would leave out a row this harness
// cannot read, as a turn of a project does: it reads every document, and one
// left out would change what the model is told.
func refuseUnreadable(_ string, err error) error { return err }

// Chats

func (s *store) loadThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string) (*domain.ThreadRow, error) {
	row, err := s.rows.load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	return domain.ReadThreadRow(row)
}

func (s *store) threadBatch(ctx context.Context, p *auth.Principal, key auth.ContentKey, ids []string) ([]*domain.ThreadRow, error) {
	rows, failed, err := s.rows.pull(ctx, p, key, "chat", ids)
	if err != nil {
		return nil, err
	}
	for _, err := range failed {
		if api.AsError(err).HTTP != 409 {
			return nil, err
		}
	}
	threads := make([]*domain.ThreadRow, 0, len(rows))
	for i := range rows {
		// A chat this harness cannot read is left out, as one sync cannot
		// open is.
		if t, err := domain.ReadThreadRow(&rows[i]); err == nil {
			threads = append(threads, t)
		}
	}
	return threads, nil
}

func (s *store) removeThreadRevision(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, revision string) error {
	return s.rows.removeRevision(ctx, p, key, id, revision)
}

// threadPage is one page of a user's chats, newest first, or of a project's.
func (s *store) threadPage(ctx context.Context, p *auth.Principal, key auth.ContentKey, cursor, project string, limit int) ([]*domain.ThreadRow, string, error) {
	page, err := s.rows.list(ctx, p, key, "chat", cursor, project, limit)
	if err != nil {
		return nil, "", err
	}
	threads := make([]*domain.ThreadRow, 0, len(page.Rows))
	for i := range page.Rows {
		// A chat this harness cannot read is left out, as one sync cannot
		// open is.
		if t, err := domain.ReadThreadRow(&page.Rows[i]); err == nil {
			threads = append(threads, t)
		}
	}
	return threads, page.Next, nil
}

// attachmentRepairer places the attachments a pushed chat names that sync
// does not hold under it.
type attachmentRepairer interface {
	repairAttachments(context.Context, *auth.Principal, *domain.Thread, []string) error
}

// saveThread pushes a chat at the revision it was read at. A chat naming
// attachments sync does not hold under it is repaired once and pushed again.
func (s *store) saveThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, row *domain.ThreadRow) error {
	row.RecordVersion = domain.RecordVersion
	row.UpdatedAt = api.Timestamp()
	for repaired := false; ; repaired = true {
		stored := domain.StoredRow{ID: row.ID, ETag: row.ETag, Data: api.Raw(row.Thread), Project: row.ProjectID, ProjectSet: true, Restore: row.Restore}
		err := s.push(ctx, p, key, "chat", &stored)
		if err == nil {
			row.ETag = stored.ETag
			return nil
		}
		if e := api.AsError(err); e.Code != "MISSING_ATTACHMENT" || repaired {
			return err
		} else if err := s.repair.repairAttachments(ctx, p, &row.Thread, e.Missing); err != nil {
			return err
		}
	}
}

// mutateThread applies change to a stored chat as read now, again after a
// conflict, so a conflict never overwrites another writer's change.
func (s *store) mutateThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, change func(*domain.Thread) error) (*domain.ThreadRow, error) {
	for attempt := 0; attempt < maxCommitAttempts; attempt++ {
		row, err := s.loadThread(ctx, p, key, id)
		if err != nil {
			return nil, err
		}
		if err := change(&row.Thread); err != nil {
			return nil, err
		}
		err = s.saveThread(ctx, p, key, row)
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
func (s *store) removeRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) error {
	return s.rows.remove(ctx, p, key, scope, id)
}

// chatIDs lists every chat id of a user, or of a project, unopened.
func (s *store) chatIDs(ctx context.Context, p *auth.Principal, project string) ([]string, error) {
	return s.rows.ids(ctx, p, "chat", project)
}

// forkStored has sync copy a chat's first count messages, with their
// attachments, into a new chat.
func (s *store) forkStored(ctx context.Context, p *auth.Principal, key auth.ContentKey, source, target, title string, count int) error {
	return s.api.callKey(ctx, p, "/v1/sync/fork", key, api.Object{"source_id": source, "target_id": target, "message_count": count, "title": title, "created_at": api.Timestamp(), "idempotency_key": uuid.NewString()}, nil)
}

// searchResults are the chats a search found, in its order, and how many of
// them sync or this harness cannot open, which are left out. A chat deleted
// since it was indexed is not a result. One sync could not fetch in time
// fails the search, which may find it when run again.
func (s *store) searchResults(ctx context.Context, p *auth.Principal, key auth.ContentKey, ids []string) ([]*domain.ThreadRow, int, error) {
	rows, failed, err := s.rows.pull(ctx, p, key, "chat", ids)
	if err != nil {
		return nil, 0, err
	}
	pulled := map[string]*domain.StoredRow{}
	for i := range rows {
		pulled[rows[i].ID] = &rows[i]
	}
	found, unreadable := []*domain.ThreadRow{}, 0
	for _, id := range ids {
		row, pulledRow := pulled[id]
		switch err := failed[id]; {
		case errors.Is(err, errRowNotFetched):
			return nil, 0, err
		case err != nil:
			unreadable++
		case pulledRow:
			if thread, err := domain.ReadThreadRow(row); err != nil {
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
func (s *store) searchChats(ctx context.Context, p *auth.Principal, key auth.ContentKey, query string, limit int) ([]string, bool, error) {
	var found struct {
		NeedsReindex bool `json:"needs_reindex"`
		Results      []struct {
			ID string `json:"id"`
		} `json:"results"`
	}
	if err := s.api.callKey(ctx, p, "/v1/search/query", key, api.Object{"query": query, "limit": limit}, &found); err != nil {
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
func (s *store) reindexChats(ctx context.Context, p *auth.Principal, key auth.ContentKey) (bool, error) {
	var job struct {
		Status  string `json:"status"`
		Partial bool   `json:"partial"`
	}
	if err := s.api.call(ctx, p, "/v1/search/reindex", keyedBody(key, api.Object{}, true), &job); err != nil {
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
func (s *store) loadProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey) (*domain.StoredProfile, error) {
	row, err := s.rows.load(ctx, p, key, "profile", "profile")
	if err != nil {
		if api.AsError(err).HTTP == 404 {
			return nil, nil
		}
		return nil, err
	}
	profile, err := domain.ReadProfile(row.Data)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

func (s *store) mutateProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey, change func(*domain.StoredProfile) error) (domain.StoredProfile, error) {
	var profile domain.StoredProfile
	err := s.commitRow(ctx, p, key, "profile", "profile", true, func(row *domain.StoredRow, exists bool) error {
		profile = domain.StoredProfile{}
		if exists {
			var err error
			if profile, err = domain.ReadProfile(row.Data); err != nil {
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
		profile.RecordVersion = domain.RecordVersion
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

func (s *store) loadProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string) (domain.Project, error) {
	row, err := s.rows.load(ctx, p, key, "project", id)
	if err != nil {
		return domain.Project{}, err
	}
	return domain.ReadProject(row)
}

func (s *store) projectPage(ctx context.Context, p *auth.Principal, key auth.ContentKey, cursor string, limit int) ([]domain.Project, string, error) {
	page, err := s.rows.list(ctx, p, key, "project", cursor, "", limit)
	if err != nil {
		return nil, "", err
	}
	projects := make([]domain.Project, 0, len(page.Rows))
	for i := range page.Rows {
		// A project this harness cannot read is left out, as one sync cannot
		// open is.
		if project, err := domain.ReadProject(&page.Rows[i]); err == nil {
			projects = append(projects, project)
		}
	}
	return projects, page.Next, nil
}

// mutateProject applies change to a project, or with create to a new one.
func (s *store) mutateProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, create bool, change func(*domain.Project)) (domain.Project, error) {
	var project domain.Project
	err := s.commitRow(ctx, p, key, "project", id, create, func(row *domain.StoredRow, exists bool) error {
		project = domain.Project{ID: id, CreatedAt: api.Timestamp()}
		if exists {
			var err error
			if project, err = domain.ReadProject(row); err != nil {
				return err
			}
		}
		change(&project)
		project.UpdatedAt = api.Timestamp()
		row.Data = api.Raw(domain.StoredProject{Project: project, RecordVersion: domain.RecordVersion})
		return nil
	})
	return project, err
}

// Sync deletes every project and project document in one step.
func (s *store) removeAllProjects(ctx context.Context, p *auth.Principal, key auth.ContentKey) (int, error) {
	var result struct {
		Deleted int `json:"deleted"`
	}
	err := s.api.callKey(ctx, p, "/v1/sync/delete-all-projects", key, api.Object{"idempotency_key": uuid.NewString()}, &result)
	return result.Deleted, err
}

// projectDocuments are every document of a project. A document sync cannot
// open, or this harness cannot read, is passed to unreadable with why, which
// fails the read by returning an error or leaves the document out by
// returning nil.
func (s *store) projectDocuments(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, unreadable func(row string, err error) error) ([]domain.ProjectDocument, error) {
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
		page, err := s.rows.list(ctx, p, key, "project_document", cursor, id, 100)
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
	list := []domain.ProjectDocument{}
	for _, rowID := range order {
		listed := latest[rowID]
		doc, err := domain.ProjectDocument{}, listed.err
		if err == nil {
			doc, err = domain.ReadProjectDocument(listed.row, id)
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
	row *domain.StoredRow
	err error
}

// createProjectDocument stores a new document under its project.
func (s *store) createProjectDocument(ctx context.Context, p *auth.Principal, key auth.ContentKey, doc domain.ProjectDocument) (domain.ProjectDocument, error) {
	id := doc.ProjectID + "/" + doc.ID
	err := s.commitRow(ctx, p, key, "project_document", id, true, func(row *domain.StoredRow, exists bool) error {
		row.Project, row.ProjectSet = &doc.ProjectID, true
		doc.CreatedAt = api.Timestamp()
		if exists {
			stored, err := domain.ReadProjectDocument(row, doc.ProjectID)
			if err != nil {
				return err
			}
			doc.CreatedAt = stored.CreatedAt
		}
		doc.UpdatedAt = api.Timestamp()
		row.Data, row.Project, row.ProjectSet = api.Raw(domain.StoredDocument{ProjectDocument: doc, RecordVersion: domain.RecordVersion}), &doc.ProjectID, true
		return nil
	})
	return doc, err
}

// Attachments

// putAttachment stores a blob under a chat. Sync derives its id from the
// idempotency key, chat and bytes, so storing the same attachment again
// reuses its blob.
func (s *store) putAttachment(ctx context.Context, p *auth.Principal, chatID, clientID string, data []byte) (string, string, error) {
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
		if err = s.api.call(ctx, p, "/v1/attachment/put", in, &stored); err == nil || api.AsError(err).Status != 502 {
			break
		}
	}
	if err != nil {
		return "", "", err
	}
	if !domain.EnclaveAttachmentID.MatchString(stored.ID) || stored.Key == "" {
		return "", "", api.Err(502, "UPSTREAM_REFUSED", "The sync service returned an invalid attachment")
	}
	return stored.ID, stored.Key, nil
}

// getAttachment reads a blob, or with public a blob a share holds.
func (s *store) getAttachment(ctx context.Context, p *auth.Principal, id, attKey string, public bool) ([]byte, error) {
	path := "/v1/attachment/get"
	if public {
		path += "-public"
	}
	var result struct {
		Plaintext string `json:"plaintext"`
	}
	if err := s.api.call(ctx, p, path, api.Object{"id": id, "att_key": attKey}, &result); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(result.Plaintext)
}

// Shares

// sealShare seals a share's payload under a key the link carries.
func (s *store) sealShare(ctx context.Context, p *auth.Principal, plaintext []byte) ([]byte, string, error) {
	var sealed struct {
		Ciphertext string `json:"ciphertext"`
		ShareKey   string `json:"share_key"`
	}
	if err := s.api.call(ctx, p, "/v1/share/seal", api.Object{"plaintext": base64.StdEncoding.EncodeToString(plaintext)}, &sealed); err != nil {
		return nil, "", err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(sealed.Ciphertext)
	return ciphertext, sealed.ShareKey, err
}

func (s *store) openSealedShare(ctx context.Context, shareKey string, ciphertext []byte) ([]byte, error) {
	var opened struct {
		Plaintext string `json:"plaintext"`
	}
	if err := s.api.call(ctx, &auth.Principal{Anonymous: true}, "/v1/share/open", api.Object{"share_key": shareKey, "ciphertext": base64.StdEncoding.EncodeToString(ciphertext)}, &opened); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(opened.Plaintext)
}

// Keys and imports

func (s *store) authorizeArchive(ctx context.Context, p *auth.Principal, keyBytes []byte, operation string) error {
	if len(keyBytes) == 0 {
		return nil
	}
	key, err := auth.DecodeContentKey(keyBytes)
	if err != nil {
		return err
	}
	defer clear(key[:])
	token, err := s.rows.authorizeStorage(ctx, p, key, operation, "")
	if err != nil {
		return err
	}
	p.SyncToken = token
	return nil
}

// keyCall passes a key management request to sync as the client sent it.
func (s *store) keyCall(ctx context.Context, p *auth.Principal, path string, in json.RawMessage) (json.RawMessage, error) {
	var out json.RawMessage
	err := s.api.call(ctx, p, path, in, &out)
	return out, err
}

// syncImport makes one call of an archive import, whose job sync runs.
func (s *store) syncImport(ctx context.Context, p *auth.Principal, step string, in, out any) error {
	return s.api.call(ctx, p, "/v1/import/"+step, in, out)
}
