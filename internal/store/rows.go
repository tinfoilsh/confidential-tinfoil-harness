package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"golang.org/x/sync/errgroup"
)

// Rows is what the control plane stores for a caller, sealed under the
// caller's content key: read, written and deleted here, in the enclave,
// with the key the request carried.
type Rows struct {
	Controlplane *controlplane.Client
	// Buckets holds attachments; a legacy row's attachments move into it
	// as the row is resealed, and a deleted chat's leave it.
	Buckets *buckets.Client
	// Index keeps the search index in step with chats, when search is on.
	Index ChatIndexer // nil until search moves in
}

// ChatIndexer keeps a caller's search index in step with the chats stored:
// each chat as it is stored, and each as it is removed.
type ChatIndexer interface {
	ChatStored(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, plaintext []byte, etag string, at time.Time, revision int64) error
	ChatRemoved(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, revision int64)
}

const (
	// grantLifetime is how long a run or an operation may use the rows.
	grantLifetime = time.Hour
	// pullConcurrency bounds the rows fetched from the control plane at
	// once, so a full batch finishes well within a request without
	// saturating the connection pool.
	pullConcurrency = 8
	// removeAttempts bounds the deletes of a row another writer keeps
	// changing under them.
	removeAttempts = 3
	// profileSyncProtocol is the profile merge protocol this harness
	// writes, which the control plane refuses older writers for.
	profileSyncProtocol = controlplane.ProfileSyncProtocolV2
	// A page of rows is at most maxListPage; a page size outside that is
	// defaultListPage.
	maxListPage     = 100
	defaultListPage = 50
)

// AuthorizeRun grants a run the chat it is for and the project its context
// reads: an existing chat's own project, including one a move changed only
// in storage metadata, or the project a new chat names.
func (r *Rows) AuthorizeRun(ctx context.Context, p *auth.Principal, key auth.ContentKey, threadID string, projectID *string) (*auth.StorageGrant, error) {
	defer clear(key[:])
	project := domain.ValueOr(projectID, "")
	if !runIDValid(threadID) || project != "" && !runIDValid(project) {
		return nil, api.Err(400, "BAD_REQUEST", "invalid chat or project id")
	}
	rows, failed, err := r.Pull(ctx, p, key, "chat", []string{threadID})
	if err != nil {
		return nil, err
	}
	if err := failed[threadID]; err != nil {
		return nil, err
	}
	for i := range rows {
		row := &rows[i]
		var chat struct {
			ProjectID string `json:"projectId"`
		}
		err := json.Unmarshal(row.Data, &chat)
		seal.Zero(row.Data)
		switch {
		case row.ProjectSet:
			project = domain.ValueOr(row.Project, "")
		case err != nil:
			return nil, api.Err(400, "BAD_REQUEST", "invalid chat")
		default:
			project = chat.ProjectID
		}
	}
	if project != "" && !runIDValid(project) {
		return nil, api.Err(400, "BAD_REQUEST", "invalid project id")
	}
	return &auth.StorageGrant{ChatID: threadID, ProjectID: project, KeyID: keyID(key), ExpiresAt: api.Now().Add(grantLifetime)}, nil
}

// AuthorizeStorage grants an operation its rows: an import's writes, an
// export's reads, or a project upload's documents.
func (r *Rows) AuthorizeStorage(ctx context.Context, p *auth.Principal, key auth.ContentKey, operation, projectID string) (*auth.StorageGrant, error) {
	defer clear(key[:])
	switch operation {
	case "import", "export":
	case "project-upload":
		if !runIDValid(projectID) {
			return nil, api.Err(400, "BAD_REQUEST", "invalid project id")
		}
	default:
		return nil, api.Err(400, "BAD_REQUEST", "invalid storage operation")
	}
	return &auth.StorageGrant{Operation: operation, ProjectID: projectID, KeyID: keyID(key), ExpiresAt: api.Now().Add(grantLifetime)}, nil
}

// Push seals a row under key and writes it at the revision it was read at,
// or creates it when it has none. The proof of possession the control
// plane checks covers the plaintext, not the sealed bytes, so a retry
// under the same nonce after a lost answer replays the first write.
func (r *Rows) Push(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, row *domain.StoredRow, nonce string) (string, error) {
	defer clear(key[:])
	if !seal.Scope(scope).Valid() {
		return "", api.Err(400, "BAD_REQUEST", "invalid scope")
	}
	if row.ID == "" || nonce == "" {
		return "", api.Err(400, "BAD_REQUEST", "a row id and an idempotency key are required")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsPush(key, scope, row) {
		return "", errRunScope
	}
	path, err := controlplane.PathFor(scope, row.ID)
	if err != nil {
		return "", api.Err(400, "BAD_REQUEST", "invalid row id")
	}
	kidHex := keyID(key)
	aad := seal.AAD{KeyIDHex: kidHex, Scope: seal.Scope(scope), ID: row.ID, ClerkUserID: p.ID}
	payloadAAD, err := seal.CanonicalPayloadAAD(aad)
	if err != nil {
		return "", api.Err(400, "BAD_REQUEST", "invalid envelope inputs")
	}
	envelope, err := seal.Encrypt(key[:], row.Data, aad)
	if err != nil {
		return "", fromControlplane(err)
	}
	ifMatch := controlplane.IfMatchCreateOnly
	if row.ETag != "" {
		ifMatch = row.ETag
	}
	protocol := 0
	if scope == "profile" {
		protocol = profileSyncProtocol
	}
	req := controlplane.PutBlobRequest{
		Scope:               scope,
		ID:                  row.ID,
		JWT:                 string(p.JWT),
		ClerkUserID:         p.ID,
		KeyIDHex:            kidHex,
		IfMatch:             ifMatch,
		IdempotencyKey:      nonce,
		OperationHash:       operationHash(key, http.MethodPut, path, kidHex, ifMatch, nonce, protocol, payloadAAD, row.Data),
		ProfileSyncProtocol: protocol,
		Ciphertext:          envelope,
	}
	if scope == "chat" {
		// The project is metadata beside the sealed row, so a move shows
		// in a listing without opening it; an empty one clears it. Which
		// attachments the chat still names tells the control plane what
		// to keep under it, and the activity time orders the chat list.
		// Both come from the plaintext the proof of possession covers.
		req.ProjectIDSet = true
		if row.Project != nil && *row.Project != "" {
			project := *row.Project
			req.ProjectID = &project
		}
		req.AttachmentRefs = chatAttachmentRefs(row.Data)
		if len(req.AttachmentRefs) > maxAttachmentRefsPerChat {
			return "", api.Err(400, "BAD_REQUEST", fmt.Sprintf("The chat references more than %d attachments", maxAttachmentRefsPerChat))
		}
		if at, ok := chatActivityAt(row.Data); ok {
			req.ActivityAt = &at
		}
	}
	resp, err := r.Controlplane.PutBlob(ctx, req)
	if err != nil {
		return "", fromControlplane(err)
	}
	if scope == "chat" && r.Index != nil {
		// The plaintext and key exist only for this request, so the index
		// is updated now; a failure degrades search, which a rebuild
		// repairs, rather than failing a stored write.
		_ = r.Index.ChatStored(ctx, p, key, row.ID, row.Data, resp.ETag, api.Now(), resp.SourceRevision)
	}
	return resp.ETag, nil
}

// operationHash is the proof of possession a change to a row carries: a
// MAC under a key derived from the content key, over the request as the
// control plane sees it and the plaintext, so the control plane can match
// a retry without reading either.
func operationHash(key auth.ContentKey, method, path, kidHex, ifMatch, nonce string, protocol int, aad, plaintext []byte) string {
	opKey, _ := seal.DeriveOpHashKey(key[:])
	defer seal.Zero(opKey)
	protocolValue := ""
	if protocol > 0 {
		protocolValue = strconv.Itoa(protocol)
	}
	return seal.ComputeOperationHash(opKey, seal.CanonicalInput{
		Method:              method,
		Path:                path,
		KeyIDHex:            kidHex,
		IfMatch:             ifMatch,
		IdempotencyKey:      nonce,
		Body:                plaintext,
		AAD:                 aad,
		ProfileSyncProtocol: protocolValue,
	})
}

// Pull returns the rows it could open, one per distinct id in request
// order, and the error for each it could not. An absent id is neither.
func (r *Rows) Pull(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, ids []string) ([]domain.StoredRow, map[string]error, error) {
	defer clear(key[:])
	rows, failed := []domain.StoredRow{}, map[string]error{}
	if len(ids) == 0 {
		return rows, failed, nil
	}
	if !seal.Scope(scope).Valid() {
		return nil, nil, api.Err(400, "BAD_REQUEST", "invalid scope")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsPull(key, scope, ids) {
		return nil, nil, errRunScope
	}
	target := seal.Key{Bytes: key[:], KeyIDHex: keyID(key)}
	// A legacy row is resealed under the key as it is read, but the control
	// plane accepts that only under the registered current key. One probe
	// per pull spares every row a refused reseal when the key is another.
	canRewrap := r.currentPrimaryKeyIs(ctx, p, target.KeyIDHex)
	for start := 0; start < len(ids); start += PullBatch {
		r.pullBatch(ctx, p, target, scope, ids[start:min(len(ids), start+PullBatch)], canRewrap, true, &rows, failed)
	}
	return rows, failed, nil
}

// pulled is one row as a fetch found it: opened, absent (neither row nor
// error), or failed; transient marks a failure a second fetch may not meet.
type pulled struct {
	row       *domain.StoredRow
	err       error
	transient bool
}

// pullBatch fetches each distinct id once, concurrently, and appends the
// rows opened in request order. Ids the control plane could not serve are
// fetched once more together; those it still cannot are reported as such.
func (r *Rows) pullBatch(ctx context.Context, p *auth.Principal, target seal.Key, scope string, ids []string, canRewrap, retry bool, rows *[]domain.StoredRow, failed map[string]error) {
	// Concurrent reads of one legacy id would race their reseal, leaving
	// one copy at a stale revision.
	results := make(map[string]*pulled, len(ids))
	var group errgroup.Group
	group.SetLimit(pullConcurrency)
	for _, id := range ids {
		if _, seen := results[id]; seen {
			continue
		}
		item := &pulled{}
		results[id] = item
		group.Go(func() error {
			*item = r.pullOne(ctx, p, target, scope, id, canRewrap)
			return nil
		})
	}
	_ = group.Wait()
	again, seen := []string{}, map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		item := results[id]
		switch {
		case item.transient && retry:
			again = append(again, id)
		case item.transient:
			failed[id] = errRowNotFetched
		case item.err != nil:
			failed[id] = item.err
		case item.row != nil:
			*rows = append(*rows, *item.row)
		}
	}
	if len(again) > 0 {
		r.pullBatch(ctx, p, target, scope, again, canRewrap, false, rows, failed)
	}
}

func (r *Rows) pullOne(ctx context.Context, p *auth.Principal, target seal.Key, scope, id string, canRewrap bool) pulled {
	blob, err := r.Controlplane.GetBlob(ctx, scope, id, string(p.JWT), p.ID)
	if err != nil {
		var cpe *controlplane.Error
		switch {
		case errors.As(err, &cpe) && cpe.StatusCode == http.StatusNotFound:
			return pulled{}
		case errors.As(err, &cpe) && cpe.StatusCode/100 == 4:
			return pulled{err: fromControlplane(err)}
		}
		return pulled{err: fromControlplane(err), transient: true}
	}
	keys := []seal.Key{target}
	row := &domain.StoredRow{ID: id, ETag: blob.ETag, Project: blob.ProjectID, ProjectSet: blob.ProjectIDSet}
	legacy := false
	switch seal.Detect(blob.Ciphertext) {
	case seal.VersionV2:
		dec, err := seal.DecryptV2(blob.Ciphertext, keys, seal.AAD{Scope: seal.Scope(scope), ID: aadID(scope, id), ClerkUserID: p.ID})
		if errors.Is(err, seal.ErrNoMatchingKey) {
			e := api.Err(409, "KEY_MISMATCH", "The stored data uses another content key")
			e.KeyID = dec.KeyIDHex
			return pulled{err: e}
		}
		if err != nil {
			return pulled{err: errRowUnreadable}
		}
		row.Data = dec.Plaintext
	case seal.VersionV0, seal.VersionV1:
		dec, err := seal.DecryptLegacy(blob.Ciphertext, keys)
		if err != nil {
			return pulled{err: api.Err(409, "KEY_MISMATCH", "The stored data uses another content key")}
		}
		row.Data, legacy = dec.Plaintext, true
	default:
		return pulled{err: errRowUnreadable}
	}
	if trimmed := bytes.TrimSpace(row.Data); len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		seal.Zero(row.Data)
		return pulled{err: errRowUnreadable}
	}
	if legacy && canRewrap {
		// Best effort: a row that stays legacy is still read, and the
		// next read reseals it.
		if etag, err := r.rewrapRow(ctx, p, scope, id, row.Data, blob.ETag, target); err == nil {
			row.ETag = etag
		}
	}
	r.backfillChatActivity(ctx, p, scope, id, blob, row.ETag, row.Data)
	return pulled{row: row}
}

// aadID is the id a row is sealed under. The control plane files a user's
// profile under the user id, and lists it so, while its envelope names the
// profile singleton.
func aadID(scope, id string) string {
	if scope == "profile" {
		return seal.ProfileSingletonID
	}
	return id
}

// currentPrimaryKeyIs is whether the caller's registered current key is
// kidHex. A probe that fails answers no, which leaves rows as they are.
func (r *Rows) currentPrimaryKeyIs(ctx context.Context, p *auth.Principal, kidHex string) bool {
	current, err := r.Controlplane.GetCurrentKey(ctx, string(p.JWT), p.ID)
	return err == nil && current != nil && current.KeyID == kidHex
}

func (r *Rows) Load(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) (*domain.StoredRow, error) {
	rows, failed, err := r.Pull(ctx, p, key, scope, []string{id})
	if err != nil {
		return nil, err
	}
	if err := failed[id]; err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].ID == id {
			return &rows[i], nil
		}
	}
	return nil, api.Err(404, "THREAD_NOT_FOUND", "The requested data was not found")
}

// listStatus is one page of row ids, newest first: a project's chats, or
// a project's documents, or every row of a scope.
func (r *Rows) listStatus(ctx context.Context, p *auth.Principal, scope, cursor, project string, limit int) ([]string, string, error) {
	if !seal.Scope(scope).Valid() {
		return nil, "", api.Err(400, "BAD_REQUEST", "invalid scope")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsList(scope, project) {
		return nil, "", errRunScope
	}
	chatsOf := ""
	if scope == "chat" {
		chatsOf = project
	}
	list, err := r.Controlplane.ListStatus(ctx, scope, cursor, limit, string(p.JWT), p.ID, chatsOf, "desc")
	if err != nil {
		return nil, "", fromControlplane(err)
	}
	ids := []string{}
	for _, item := range list.Updates {
		// The control plane files only chats by project. Document ids name
		// their project, so select them before any document is opened.
		if scope == "project_document" && project != "" && !strings.HasPrefix(item.ID, project+"/") {
			continue
		}
		ids = append(ids, item.ID)
	}
	return ids, list.NextCursor, nil
}

func (r *Rows) IDs(ctx context.Context, p *auth.Principal, scope, project string) ([]string, error) {
	all, cursor := []string{}, ""
	for {
		ids, next, err := r.listStatus(ctx, p, scope, cursor, project, ListStatusPage)
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

func (r *Rows) List(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, cursor, project string, limit int) (Page, error) {
	defer clear(key[:])
	if limit <= 0 || limit > maxListPage {
		limit = defaultListPage
	}
	ids, next, err := r.listStatus(ctx, p, scope, cursor, project, limit)
	if err != nil {
		return Page{}, err
	}
	page := Page{Rows: []domain.StoredRow{}, Next: next}
	if len(ids) != 0 {
		page.Rows, page.Unreadable, err = r.Pull(ctx, p, key, scope, ids)
	}
	return page, err
}

// Remove deletes a row whatever its revision: it reads the revision,
// deletes at it, and tries again when another writer got in between. A
// missing row is gone. A chat's attachments go with it.
func (r *Rows) Remove(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) error {
	defer clear(key[:])
	if !seal.Scope(scope).Valid() {
		return api.Err(400, "BAD_REQUEST", "invalid scope")
	}
	if id == "" {
		return api.Err(400, "BAD_REQUEST", "a row id is required")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opRemove) {
		return errRunScope
	}
	path, err := controlplane.PathFor(scope, id)
	if err != nil {
		return api.Err(400, "BAD_REQUEST", "invalid row id")
	}
	// Each attempt is its own operation to the control plane: a replay of
	// the first could otherwise answer for a later one that deleted a
	// newer revision.
	nonce := uuid.NewString()
	for attempt := 0; attempt < removeAttempts; attempt++ {
		blob, err := r.Controlplane.GetBlob(ctx, scope, id, string(p.JWT), p.ID)
		if err != nil {
			var cpe *controlplane.Error
			if errors.As(err, &cpe) && cpe.StatusCode == http.StatusNotFound {
				// The row may be gone while the search index still names
				// it, after an earlier delete that got no further.
				if scope == "chat" && r.Index != nil {
					r.Index.ChatRemoved(ctx, p, key, id, 0)
				}
				return nil
			}
			return fromControlplane(err)
		}
		attemptNonce := nonce
		if attempt > 0 {
			attemptNonce = fmt.Sprintf("%s:retry:%d", nonce, attempt)
		}
		resp, err := r.removeAt(ctx, p, key, scope, path, id, blob.ETag, attemptNonce)
		if err == nil {
			r.finishRemove(ctx, p, key, scope, id, resp)
			return nil
		}
		if controlplane.IsCode(err, controlplane.StatusStaleBlob) {
			continue
		}
		return fromControlplane(err)
	}
	return api.Err(409, "REVISION_CONFLICT", "The row changed during every delete attempt")
}

// RemoveRevision deletes a chat at a revision; one that changed since is a
// conflict.
func (r *Rows) RemoveRevision(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, revision string) error {
	defer clear(key[:])
	if id == "" || revision == "" {
		return api.Err(400, "BAD_REQUEST", "a row id and a revision are required")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opRemove) {
		return errRunScope
	}
	path, err := controlplane.PathFor("chat", id)
	if err != nil {
		return api.Err(400, "BAD_REQUEST", "invalid row id")
	}
	resp, err := r.removeAt(ctx, p, key, "chat", path, id, revision, uuid.NewString())
	if err != nil {
		return fromControlplane(err)
	}
	r.finishRemove(ctx, p, key, "chat", id, resp)
	return nil
}

func (r *Rows) removeAt(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, path, id, revision, nonce string) (*controlplane.DeleteBlobResponse, error) {
	return r.Controlplane.DeleteBlob(ctx, controlplane.DeleteBlobRequest{
		Scope:          scope,
		ID:             id,
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		IfMatch:        revision,
		IdempotencyKey: nonce,
		OperationHash:  operationHash(key, http.MethodDelete, path, keyID(key), revision, nonce, 0, nil, nil),
	})
}

// finishRemove sees a deleted chat out: its attachments, as the control
// plane names them, leave buckets, and the search index forgets it.
func (r *Rows) finishRemove(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string, resp *controlplane.DeleteBlobResponse) {
	if scope != "chat" {
		return
	}
	r.deleteBucketAttachments(ctx, p.ID, resp.WipedV2Attachments)
	if r.Index != nil {
		r.Index.ChatRemoved(ctx, p, key, id, resp.SourceRevision)
	}
}
