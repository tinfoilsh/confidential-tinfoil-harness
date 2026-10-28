package store

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"net/http"
	"slices"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
)

type Store struct {
	Repair Repairer
	Rows   RowStore
	// Controlplane keeps the attachment index, the key registry and the
	// change ledger; Buckets holds attachment bytes.
	Controlplane *controlplane.Client
	Buckets      *buckets.Client
	// Reseals runs each caller's reseal of the rows an older key sealed.
	Reseals *jobs.Runner[ResealReport]
	// Search forgets a caller's search index when a new key fences it off.
	Search SearchResetter
	// API is the sync enclave's HTTP API, for what has not moved here yet.
	API *Client
}

// SearchResetter drops what is held for a caller's search index once the
// control plane has fenced it off, and the object it named.
type SearchResetter interface {
	Forget(ctx context.Context, owner, objectKey string)
}

// The store is the harness's typed view of what a caller keeps: chats, the
// profile, projects and their documents, attachments and keys. Nothing
// else calls sync. Rows are read through the legacy reader, so a row any
// client wrote reads in the current format.

var errEveryCommitConflicted = api.Err(409, "REVISION_CONFLICT", "The row changed during every commit attempt")

// Push writes a row at the revision it was read at; an empty one creates
// it. A push whose outcome is unknown is sent again under the same
// idempotency key, which sync answers with the first push's outcome.
func (s *Store) Push(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, row *domain.StoredRow) error {
	nonce := uuid.NewString()
	for attempt := 1; ; attempt++ {
		etag, err := s.Rows.Push(ctx, p, key, scope, row, nonce)
		if err == nil {
			row.ETag = etag
			return nil
		}
		if api.AsError(err).Code != "SYNC_OUTCOME_UNKNOWN" || attempt == MaxCommitAttempts {
			return err
		}
	}
}

// CommitRow applies edit to a row as read now and pushes it at that
// revision, again after a conflict, so a conflict never overwrites another
// writer's change with an old copy. With create, a missing row is made.
func (s *Store) CommitRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string, create bool, edit func(row *domain.StoredRow, exists bool) error) error {
	for attempt := 0; attempt < MaxCommitAttempts; attempt++ {
		row, err := s.Rows.Load(ctx, p, key, scope, id)
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
		err = s.Push(ctx, p, key, scope, row)
		if err == nil || api.AsError(err).Code != "REVISION_CONFLICT" {
			return err
		}
	}
	return errEveryCommitConflicted
}

// EachRowOrUnreadable visits every row of a scope, or of a project's chats,
// and names each row sync holds but cannot open.
func (s *Store) EachRowOrUnreadable(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, project string, visit func(*domain.StoredRow) error, unreadable func(string)) error {
	cursor := ""
	for {
		page, err := s.Rows.List(ctx, p, key, scope, cursor, project, 100)
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

func (s *Store) EachRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, project string, visit func(*domain.StoredRow) error) error {
	return s.EachRowOrUnreadable(ctx, p, key, scope, project, visit, func(string) {})
}

// RefuseUnreadable fails a read that would leave out a row this harness
// cannot read, as a turn of a project does: it reads every document, and one
// left out would change what the model is told.
func RefuseUnreadable(_ string, err error) error { return err }

// Chats

func (s *Store) LoadThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string) (*domain.ThreadRow, error) {
	row, err := s.Rows.Load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	return domain.ReadThreadRow(row)
}

func (s *Store) ThreadBatch(ctx context.Context, p *auth.Principal, key auth.ContentKey, ids []string) ([]*domain.ThreadRow, error) {
	rows, failed, err := s.Rows.Pull(ctx, p, key, "chat", ids)
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

func (s *Store) RemoveThreadRevision(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, revision string) error {
	return s.Rows.RemoveRevision(ctx, p, key, id, revision)
}

// ThreadPage is one page of a user's chats, newest first, or of a project's.
func (s *Store) ThreadPage(ctx context.Context, p *auth.Principal, key auth.ContentKey, cursor, project string, limit int) ([]*domain.ThreadRow, string, error) {
	page, err := s.Rows.List(ctx, p, key, "chat", cursor, project, limit)
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

// Repairer places the attachments a pushed chat names that sync
// does not hold under it.
type Repairer interface {
	RepairAttachments(context.Context, *auth.Principal, *domain.Thread, []string) error
}

// SaveThread pushes a chat at the revision it was read at. A chat naming
// attachments sync does not hold under it is repaired once and pushed again.
func (s *Store) SaveThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, row *domain.ThreadRow) error {
	row.RecordVersion = domain.RecordVersion
	row.UpdatedAt = api.Timestamp()
	for repaired := false; ; repaired = true {
		stored := domain.StoredRow{ID: row.ID, ETag: row.ETag, Data: api.Raw(row.Thread), Project: row.ProjectID, ProjectSet: true}
		err := s.Push(ctx, p, key, "chat", &stored)
		if err == nil {
			row.ETag = stored.ETag
			return nil
		}
		if e := api.AsError(err); e.Code != "MISSING_ATTACHMENT" || repaired {
			return err
		} else if err := s.Repair.RepairAttachments(ctx, p, &row.Thread, e.Missing); err != nil {
			return err
		}
	}
}

// MutateThread applies change to a stored chat as read now, again after a
// conflict, so a conflict never overwrites another writer's change.
func (s *Store) MutateThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, change func(*domain.Thread) error) (*domain.ThreadRow, error) {
	for attempt := 0; attempt < MaxCommitAttempts; attempt++ {
		row, err := s.LoadThread(ctx, p, key, id)
		if err != nil {
			return nil, err
		}
		if err := change(&row.Thread); err != nil {
			return nil, err
		}
		err = s.SaveThread(ctx, p, key, row)
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
func (s *Store) RemoveRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) error {
	return s.Rows.Remove(ctx, p, key, scope, id)
}

// ChatIDs lists every chat id of a user, or of a project, unopened.
func (s *Store) ChatIDs(ctx context.Context, p *auth.Principal, project string) ([]string, error) {
	return s.Rows.IDs(ctx, p, "chat", project)
}

// SearchResults are the chats a search found, in its order, and how many of
// them this harness cannot open, which are left out. A chat deleted since
// it was indexed is not a result. One the control plane could not serve in
// time fails the search, which may find it when run again.
func (s *Store) SearchResults(ctx context.Context, p *auth.Principal, key auth.ContentKey, ids []string) ([]*domain.ThreadRow, int, error) {
	rows, failed, err := s.Rows.Pull(ctx, p, key, "chat", ids)
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
		case errors.Is(err, ErrRowNotFetched):
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

// Profile

// LoadProfile is the user's profile, or nil without one.
func (s *Store) LoadProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey) (*domain.StoredProfile, error) {
	row, err := s.Rows.Load(ctx, p, key, "profile", "profile")
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

func (s *Store) MutateProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey, change func(*domain.StoredProfile) error) (domain.StoredProfile, error) {
	var profile domain.StoredProfile
	err := s.CommitRow(ctx, p, key, "profile", "profile", true, func(row *domain.StoredRow, exists bool) error {
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

func (s *Store) LoadProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string) (domain.Project, error) {
	row, err := s.Rows.Load(ctx, p, key, "project", id)
	if err != nil {
		return domain.Project{}, err
	}
	return domain.ReadProject(row)
}

func (s *Store) ProjectPage(ctx context.Context, p *auth.Principal, key auth.ContentKey, cursor string, limit int) ([]domain.Project, string, error) {
	page, err := s.Rows.List(ctx, p, key, "project", cursor, "", limit)
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

// MutateProject applies change to a project, or with create to a new one.
func (s *Store) MutateProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, create bool, change func(*domain.Project)) (domain.Project, error) {
	var project domain.Project
	err := s.CommitRow(ctx, p, key, "project", id, create, func(row *domain.StoredRow, exists bool) error {
		project = domain.Project{ID: id, CreatedAt: api.Timestamp()}
		var origin *domain.ImportOrigin
		if exists {
			var err error
			if project, err = domain.ReadProject(row); err != nil {
				return err
			}
			origin = domain.ReadImportOrigin(row)
		}
		change(&project)
		project.UpdatedAt = api.Timestamp()
		row.Data = api.Raw(domain.StoredProject{Project: project, RecordVersion: domain.RecordVersion, ImportedFrom: origin})
		return nil
	})
	return project, err
}

// RemoveAllProjects deletes every project and project document in one
// step, and is how many projects went.
func (s *Store) RemoveAllProjects(ctx context.Context, p *auth.Principal, key auth.ContentKey) (int, error) {
	defer clear(key[:])
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opRemove) {
		return 0, errRunScope
	}
	kidHex, nonce := keyID(key), uuid.NewString()
	resp, err := s.Controlplane.DeleteAllProjects(ctx, controlplane.DeleteAllProjectsRequest{
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kidHex,
		IdempotencyKey: nonce,
		OperationHash:  operationHash(key, http.MethodDelete, controlplane.DeleteAllProjectsPath, kidHex, "", nonce, 0, nil, nil),
	})
	if err != nil {
		return 0, FromControlplane(err)
	}
	if !resp.OK {
		return 0, api.Err(502, "UPSTREAM_REFUSED", "The projects were not deleted")
	}
	return resp.Deleted, nil
}

// ProjectDocuments are every document of a project. A document sync cannot
// open, or this harness cannot read, is passed to unreadable with why, which
// fails the read by returning an error or leaves the document out by
// returning nil.
func (s *Store) ProjectDocuments(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, unreadable func(row string, err error) error) ([]domain.ProjectDocument, error) {
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
		page, err := s.Rows.List(ctx, p, key, "project_document", cursor, id, 100)
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

// CreateProjectDocument stores a new document under its project.
func (s *Store) CreateProjectDocument(ctx context.Context, p *auth.Principal, key auth.ContentKey, doc domain.ProjectDocument) (domain.ProjectDocument, error) {
	id := doc.ProjectID + "/" + doc.ID
	err := s.CommitRow(ctx, p, key, "project_document", id, true, func(row *domain.StoredRow, exists bool) error {
		row.Project, row.ProjectSet = &doc.ProjectID, true
		doc.CreatedAt = api.Timestamp()
		var origin *domain.ImportOrigin
		if exists {
			stored, err := domain.ReadProjectDocument(row, doc.ProjectID)
			if err != nil {
				return err
			}
			doc.CreatedAt = stored.CreatedAt
			origin = domain.ReadImportOrigin(row)
		}
		doc.UpdatedAt = api.Timestamp()
		row.Data, row.Project, row.ProjectSet = api.Raw(domain.StoredDocument{ProjectDocument: doc, RecordVersion: domain.RecordVersion, ImportedFrom: origin}), &doc.ProjectID, true
		return nil
	})
	return doc, err
}

// Keys and imports

func (s *Store) AuthorizeArchive(ctx context.Context, p *auth.Principal, keyBytes []byte, operation string) error {
	if len(keyBytes) == 0 {
		return nil
	}
	key, err := auth.DecodeContentKey(keyBytes)
	if err != nil {
		return err
	}
	defer clear(key[:])
	grant, err := s.Rows.AuthorizeStorage(ctx, p, key, operation, "")
	if err != nil {
		return err
	}
	p.Grant = grant
	return nil
}

// SyncImport makes one call of an archive import, whose job sync runs.
func (s *Store) SyncImport(ctx context.Context, p *auth.Principal, step string, in, out any) error {
	return s.API.call(ctx, p, "/v1/import/"+step, in, out)
}
