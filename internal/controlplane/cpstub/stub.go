// Package cpstub is an in-memory stand-in for the control plane's storage
// API as the harness's rows use it: sealed blobs with their revisions and
// metadata, the registered current key, the legacy attachments and the
// attachment index, chat activity, and the migration ledger. Tests seed its
// state, read it back after a call, and inject the failures the control
// plane can answer with.
package cpstub

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
)

// Blob is one stored row: its revision, the key id it was sealed under,
// the sealed bytes, and the metadata the control plane keeps beside it.
type Blob struct {
	ETag         int64
	KeyID        string
	Body         []byte
	ProjectIDSet bool
	ProjectID    *string
	UpdatedAt    time.Time
	ActivityAt   *time.Time
	// AttachmentRefs is the attachments a chat's last write said it
	// still names; nil when the write did not say.
	AttachmentRefs []string
}

// ActivityBackfill is one request to record a chat's activity time.
type ActivityBackfill struct {
	ID         string `json:"id"`
	ETag       string `json:"etag"`
	ActivityAt string `json:"activity_at"`
}

// Store is the stub's state. Handlers run under Mu, and a test holds it
// while reading or changing the state once the server is up.
type Store struct {
	Mu sync.Mutex
	// Blobs by "scope/id"; a profile's id is "profile".
	Blobs        map[string]*Blob
	CurrentKeyID string
	// NoKeyHasData answers the no-key case as a newer control plane does:
	// a 200 with has_data rather than a 404.
	NoKeyHasData      bool
	Bundles           map[string]map[string]controlplane.CurrentKeyBundle
	LegacyAttachments map[string][]byte // attachment id to ciphertext
	GoneAttachments   map[string]bool   // attachment ids already moved to buckets
	AttachmentIndex   map[string]string // attachment id to chat id
	MigrationFailures map[string]int
	NeedsMigration    []string
	SourceRevision    int64
	// MinimumProfileSyncProtocol refuses profile overwrites by older
	// writers when set.
	MinimumProfileSyncProtocol int
	ActivityBackfills          []ActivityBackfill
	// Secret signs the claim on a legacy attachment, as the control plane
	// does when it shares one with the enclave. Empty omits the claim.
	Secret string

	// Failure injection. A count is how many calls fail before the next
	// succeeds; GetBlobFailures lists the statuses to answer in turn.
	BeforePutBlob                 func(scope, id string)
	BeforeDeleteBlob              func(scope, id string)
	PutBlobFailures               map[string]int
	GetBlobFailures               map[string][]int
	DeleteAttachmentIndexFailures map[string]int
	// CaptureHeaders sees every request before it is served, outside Mu.
	CaptureHeaders func(r *http.Request)

	mux *http.ServeMux
}

func New() *Store {
	s := &Store{
		Blobs:                         map[string]*Blob{},
		Bundles:                       map[string]map[string]controlplane.CurrentKeyBundle{},
		MigrationFailures:             map[string]int{},
		PutBlobFailures:               map[string]int{},
		GetBlobFailures:               map[string][]int{},
		DeleteAttachmentIndexFailures: map[string]int{},
		mux:                           http.NewServeMux(),
	}
	s.mux.HandleFunc("PUT /api/sync/blob/chat/{id}", s.handlePutBlob("chat"))
	s.mux.HandleFunc("PUT /api/sync/blob/profile", s.handlePutBlob("profile"))
	s.mux.HandleFunc("PUT /api/sync/blob/project/{id}", s.handlePutBlob("project"))
	s.mux.HandleFunc("PUT /api/sync/blob/project_document/{pid}/{did}", s.handlePutBlob("project_document"))
	s.mux.HandleFunc("GET /api/sync/blob/chat/{id}", s.handleGetBlob("chat"))
	s.mux.HandleFunc("GET /api/sync/blob/profile", s.handleGetBlob("profile"))
	s.mux.HandleFunc("GET /api/sync/blob/project/{id}", s.handleGetBlob("project"))
	s.mux.HandleFunc("GET /api/sync/blob/project_document/{pid}/{did}", s.handleGetBlob("project_document"))
	s.mux.HandleFunc("DELETE /api/sync/blob/chat/{id}", s.handleDeleteBlob("chat"))
	s.mux.HandleFunc("DELETE /api/sync/blob/profile", s.handleDeleteBlob("profile"))
	s.mux.HandleFunc("DELETE /api/sync/blob/project/{id}", s.handleDeleteBlob("project"))
	s.mux.HandleFunc("DELETE /api/sync/blob/project_document/{pid}/{did}", s.handleDeleteBlob("project_document"))
	s.mux.HandleFunc("POST "+controlplane.RewrapPath, s.handleRewrap)
	s.mux.HandleFunc("POST "+controlplane.ChatActivityPath, s.handleBackfillChatActivity)
	s.mux.HandleFunc("GET /api/sync/list-status", s.handleListStatus)
	s.mux.HandleFunc("GET /api/sync/needs-migration", s.handleNeedsMigration)
	s.mux.HandleFunc("POST /api/sync/migration-failure", s.handleMigrationFailure)
	s.mux.HandleFunc("GET /api/sync/keys/current", s.handleCurrentKey)
	s.mux.HandleFunc("GET /api/storage/attachment/{aid}", s.handleLegacyAttachment)
	s.mux.HandleFunc("POST /api/sync/attachment-index/{aid}", s.handleRegisterAttachmentIndex)
	s.mux.HandleFunc("DELETE /api/sync/attachment-index/{aid}", s.handleDeleteAttachmentIndex)
	s.mux.HandleFunc("POST /api/sync/pending-attachments/{aid}", s.handleReservePendingAttachment)
	s.mux.HandleFunc("POST /api/sync/pending-attachments/sweep", s.handleSweepPendingAttachments)
	return s
}

func (s *Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.Mu.Lock()
	capture := s.CaptureHeaders
	s.Mu.Unlock()
	if capture != nil {
		capture(r)
	}
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.mux.ServeHTTP(w, r)
}

// Key is the Blobs key of a row.
func Key(scope, id string) string { return scope + "/" + id }

// SetBlob seeds a row as written now, under Mu.
func (s *Store) SetBlob(scope, id string, blob Blob) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if blob.UpdatedAt.IsZero() {
		blob.UpdatedAt = time.Now().UTC()
	}
	s.Blobs[Key(scope, id)] = &blob
}

// FormatETag is a revision as the control plane sends it.
func FormatETag(n int64) string { return strconv.FormatInt(n, 10) }

func extractID(scope string, r *http.Request) string {
	switch scope {
	case "profile":
		return "profile"
	case "project_document":
		return r.PathValue("pid") + "/" + r.PathValue("did")
	}
	return r.PathValue("id")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func staleBlob(w http.ResponseWriter, currentETag string) {
	writeJSON(w, http.StatusPreconditionFailed, map[string]string{"code": controlplane.StatusStaleBlob, "current_etag": currentETag})
}

// Every write and delete names the revision it expects, as the control
// plane requires.
func preconditionRequired(w http.ResponseWriter) {
	writeJSON(w, http.StatusPreconditionRequired, map[string]string{"code": controlplane.StatusPreconditionRequired, "message": "If-Match is required"})
}

func (s *Store) handlePutBlob(scope string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := extractID(scope, r)
		key := Key(scope, id)
		if s.BeforePutBlob != nil {
			s.BeforePutBlob(scope, id)
		}
		if s.PutBlobFailures[key] > 0 {
			s.PutBlobFailures[key]--
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ifMatch := r.Header.Get(controlplane.HeaderIfMatch)
		if ifMatch == "" {
			preconditionRequired(w)
			return
		}
		blob := s.Blobs[key]
		if scope == "profile" && blob != nil && s.MinimumProfileSyncProtocol > 0 {
			protocol, _ := strconv.Atoi(r.Header.Get(controlplane.HeaderProfileSyncProtocol))
			if protocol < s.MinimumProfileSyncProtocol {
				writeJSON(w, http.StatusUpgradeRequired, map[string]any{"code": controlplane.StatusProfileSyncUpgradeRequired, "minimum_protocol": s.MinimumProfileSyncProtocol})
				return
			}
		}
		if blob != nil && ifMatch != FormatETag(blob.ETag) {
			staleBlob(w, FormatETag(blob.ETag))
			return
		}
		if blob == nil && ifMatch != controlplane.IfMatchCreateOnly {
			staleBlob(w, "0")
			return
		}
		body, _ := io.ReadAll(r.Body)
		var nextETag int64 = 1
		if blob != nil {
			nextETag = blob.ETag + 1
		}
		stored := &Blob{ETag: nextETag, KeyID: r.Header.Get(controlplane.HeaderKeyID), Body: body, UpdatedAt: time.Now().UTC()}
		if blob != nil {
			// A write that says nothing about the project leaves it as it is.
			stored.ProjectIDSet, stored.ProjectID = blob.ProjectIDSet, blob.ProjectID
		}
		if scope == "chat" && r.Header.Get(controlplane.HeaderProjectIDSet) == "1" {
			stored.ProjectIDSet, stored.ProjectID = true, nil
			if projectID := r.Header.Get(controlplane.HeaderProjectID); projectID != "" {
				stored.ProjectID = &projectID
			}
		}
		if activityAt, err := time.Parse(time.RFC3339Nano, r.Header.Get(controlplane.HeaderActivityAt)); err == nil {
			stored.ActivityAt = &activityAt
		}
		if refs, sent := r.Header[controlplane.HeaderAttachmentRefs]; sent {
			stored.AttachmentRefs = []string{}
			if refs[0] != "" {
				stored.AttachmentRefs = strings.Split(refs[0], ",")
			}
		}
		s.Blobs[key] = stored
		if scope == "chat" {
			s.SourceRevision++
		}
		w.Header().Set(controlplane.HeaderETag, FormatETag(nextETag))
		w.Header().Set(controlplane.HeaderKeyID, stored.KeyID)
		writeJSON(w, http.StatusOK, map[string]any{"etag": FormatETag(nextETag), "source_revision": s.SourceRevision})
	}
}

func (s *Store) handleGetBlob(scope string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := Key(scope, extractID(scope, r))
		if pending := s.GetBlobFailures[key]; len(pending) > 0 {
			s.GetBlobFailures[key] = pending[1:]
			w.WriteHeader(pending[0])
			return
		}
		blob, ok := s.Blobs[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set(controlplane.HeaderETag, FormatETag(blob.ETag))
		w.Header().Set(controlplane.HeaderKeyID, blob.KeyID)
		if scope == "chat" && blob.ProjectIDSet {
			w.Header().Set(controlplane.HeaderProjectIDSet, "1")
			if blob.ProjectID != nil {
				w.Header().Set(controlplane.HeaderProjectID, *blob.ProjectID)
			}
		}
		if scope == "chat" && blob.ActivityAt != nil {
			w.Header().Set(controlplane.HeaderActivityAt, blob.ActivityAt.Format(time.RFC3339Nano))
		}
		_, _ = w.Write(blob.Body)
	}
}

func (s *Store) handleDeleteBlob(scope string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := extractID(scope, r)
		key := Key(scope, id)
		if s.BeforeDeleteBlob != nil {
			s.BeforeDeleteBlob(scope, id)
		}
		ifMatch := r.Header.Get(controlplane.HeaderIfMatch)
		if ifMatch == "" {
			preconditionRequired(w)
			return
		}
		blob, ok := s.Blobs[key]
		if !ok {
			staleBlob(w, "0")
			return
		}
		if ifMatch != FormatETag(blob.ETag) {
			staleBlob(w, FormatETag(blob.ETag))
			return
		}
		delete(s.Blobs, key)
		// A chat's delete drops its attachment index rows and names them,
		// so the enclave can wipe the bytes in buckets.
		wiped := []string{}
		if scope == "chat" {
			s.SourceRevision++
			for aid, cid := range s.AttachmentIndex {
				if cid == id {
					wiped = append(wiped, aid)
					delete(s.AttachmentIndex, aid)
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "wiped_v2_attachments": wiped, "source_revision": s.SourceRevision})
	}
}

// handleRewrap replaces a row's sealed bytes and key id at a revision
// without the write counting as a change to the row.
func (s *Store) handleRewrap(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Scope         string `json:"scope"`
		ID            string `json:"id"`
		KeyID         string `json:"key_id"`
		IfMatch       string `json:"if_match"`
		CiphertextB64 string `json:"ciphertext_b64"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ciphertext, err := base64.StdEncoding.DecodeString(req.CiphertextB64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	id := req.ID
	if req.Scope == "profile" {
		id = "profile"
	}
	key := Key(req.Scope, id)
	blob, ok := s.Blobs[key]
	if !ok {
		staleBlob(w, "0")
		return
	}
	if req.IfMatch != FormatETag(blob.ETag) {
		staleBlob(w, FormatETag(blob.ETag))
		return
	}
	resealed := *blob
	resealed.ETag, resealed.KeyID, resealed.Body = blob.ETag+1, req.KeyID, ciphertext
	s.Blobs[key] = &resealed
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "etag": FormatETag(resealed.ETag), "key_id": req.KeyID})
}

func (s *Store) handleBackfillChatActivity(w http.ResponseWriter, r *http.Request) {
	var req ActivityBackfill
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.ActivityBackfills = append(s.ActivityBackfills, req)
	updated := false
	activityAt, err := time.Parse(time.RFC3339Nano, req.ActivityAt)
	if blob := s.Blobs[Key("chat", req.ID)]; err == nil && blob != nil && blob.ActivityAt == nil && FormatETag(blob.ETag) == req.ETag {
		blob.ActivityAt = &activityAt
		updated = true
	}
	writeJSON(w, http.StatusOK, map[string]bool{"updated": updated})
}

// handleListStatus lists a scope's rows, the last written first, in pages
// of limit from an offset cursor; chats can be narrowed to a project.
func (s *Store) handleListStatus(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	scope, project := query.Get("scope"), query.Get("project_id")
	limit, _ := strconv.Atoi(query.Get("limit"))
	offset, _ := strconv.Atoi(query.Get("cursor"))
	type listed struct {
		id   string
		blob *Blob
	}
	var all []listed
	for key, blob := range s.Blobs {
		rowScope, id, _ := strings.Cut(key, "/")
		if rowScope != scope || project != "" && (blob.ProjectID == nil || *blob.ProjectID != project) {
			continue
		}
		all = append(all, listed{id, blob})
	}
	// Newest activity first, as the control plane lists: a chat's
	// activity time when it has one, else its write time, then id.
	listedAt := func(b *Blob) time.Time {
		if b.ActivityAt != nil {
			return *b.ActivityAt
		}
		return b.UpdatedAt
	}
	sort.Slice(all, func(i, j int) bool {
		if at, bt := listedAt(all[i].blob), listedAt(all[j].blob); !at.Equal(bt) {
			return at.After(bt)
		}
		return all[i].id > all[j].id
	})
	if offset > len(all) {
		offset = len(all)
	}
	page := all[offset:]
	next := ""
	if limit > 0 && len(page) > limit {
		page = page[:limit]
		next = strconv.Itoa(offset + limit)
	}
	updates := []controlplane.BlobMeta{}
	for _, item := range page {
		updates = append(updates, controlplane.BlobMeta{ID: item.id, ETag: FormatETag(item.blob.ETag), KeyID: item.blob.KeyID, ProjectID: item.blob.ProjectID, UpdatedAt: item.blob.UpdatedAt, ActivityAt: item.blob.ActivityAt})
	}
	writeJSON(w, http.StatusOK, controlplane.ListStatusResponse{Updates: updates, NextCursor: next})
}

func (s *Store) handleNeedsMigration(w http.ResponseWriter, r *http.Request) {
	ids := append([]string{}, s.NeedsMigration...)
	writeJSON(w, http.StatusOK, controlplane.ListNeedsMigrationResponse{IDs: ids, RetryableRemaining: len(ids)})
}

func (s *Store) handleMigrationFailure(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Scope string `json:"scope"`
		ID    string `json:"id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	s.MigrationFailures[Key(body.Scope, body.ID)]++
	w.WriteHeader(http.StatusNoContent)
}

func (s *Store) handleCurrentKey(w http.ResponseWriter, r *http.Request) {
	if s.CurrentKeyID == "" {
		if s.NoKeyHasData {
			// The newer control plane's no-key answer, bytes as sent,
			// with the empty created_at the enclave must tolerate.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"key_id":"","etag":"","bundles":{},"created_via":"","created_at":"","has_data":true}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, controlplane.CurrentKeyResponse{KeyID: s.CurrentKeyID, Bundles: s.Bundles[s.CurrentKeyID]})
}

// handleLegacyAttachment serves an attachment as older clients stored it,
// with the control plane's claim on it when a secret is set.
func (s *Store) handleLegacyAttachment(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	if s.GoneAttachments[aid] {
		w.WriteHeader(http.StatusGone)
		return
	}
	body, ok := s.LegacyAttachments[aid]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if s.Secret != "" {
		w.Header().Set(controlplane.HeaderLegacyClaim, legacyClaim(s.Secret, r.Header.Get(controlplane.HeaderClerkUserID), aid, body))
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(body)
}

// legacyClaim signs who may move an attachment: the user, the id and a
// digest of the bytes, as the control plane does.
func legacyClaim(secret, clerkUserID, aid string, ciphertext []byte) string {
	digest := sha256.Sum256(ciphertext)
	payload, _ := json.Marshal(struct {
		ClerkUserID string `json:"clerk_user_id"`
		ID          string `json:"id"`
		Scope       string `json:"scope"`
		SHA256      string `json:"sha256"`
	}{clerkUserID, aid, "attachment", hex.EncodeToString(digest[:])})
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// handleRegisterAttachmentIndex records an attachment as held in buckets
// and drops the legacy bytes, so a later legacy read answers not found.
func (s *Store) handleRegisterAttachmentIndex(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	var body struct {
		ChatID string `json:"chat_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if s.AttachmentIndex == nil {
		s.AttachmentIndex = map[string]string{}
	}
	s.AttachmentIndex[aid] = body.ChatID
	delete(s.LegacyAttachments, aid)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Store) handleDeleteAttachmentIndex(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	if s.DeleteAttachmentIndexFailures[aid] > 0 {
		s.DeleteAttachmentIndexFailures[aid]--
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if _, ok := s.AttachmentIndex[aid]; !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	delete(s.AttachmentIndex, aid)
	w.WriteHeader(http.StatusNoContent)
}

// The pending-write ledger accepts every reservation and never has
// anything to sweep.
func (s *Store) handleReservePendingAttachment(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (s *Store) handleSweepPendingAttachments(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"rows": []any{}})
}
