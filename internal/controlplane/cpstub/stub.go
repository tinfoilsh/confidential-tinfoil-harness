// Package cpstub is an in-memory stand-in for the control plane's storage
// API as the harness uses it: sealed blobs with their revisions and
// metadata, the key registry with its passkey bundles, the legacy
// attachments and the attachment index, chat activity, and the rows left
// to reseal with their cooldowns. Tests seed its state, read it back after
// a call, and inject the failures the control plane can answer with.
package cpstub

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"slices"
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
	Blobs map[string]*Blob
	// The registered current key: its id, the revision a register names
	// to replace it, how and when it was made, and the passkey bundles by
	// key id and credential id.
	CurrentKeyID      string
	KeyETag           int64
	KeyCreatedVia     string
	KeyRegisteredAt   time.Time
	Bundles           map[string]map[string]controlplane.CurrentKeyBundle
	LegacyAttachments map[string][]byte // attachment id to ciphertext
	GoneAttachments   map[string]bool   // attachment ids already moved to buckets
	AttachmentIndex   map[string]string // attachment id to chat id
	AttachmentOwners  map[string]string // attachment id to the user it was registered for
	// MigrationFailures counts the recorded reseal failures by "scope/id",
	// the id as the needs-migration list names it; a row with one is in
	// its cooldown and is not listed.
	MigrationFailures map[string]int
	SourceRevision    int64
	// MinimumProfileSyncProtocol refuses profile overwrites by older
	// writers when set.
	MinimumProfileSyncProtocol int
	ActivityBackfills          []ActivityBackfill
	// Secret signs the claim on a legacy attachment, as the control plane
	// does when it shares one with the enclave. Empty omits the claim.
	Secret string

	// Failure injection. A count is how many calls fail before the next
	// succeeds; a list of statuses is answered in turn. A reservation
	// purge refuses the reservation of an id while its earlier copy is
	// still being purged.
	BeforePutBlob    func(scope, id string)
	BeforeDeleteBlob func(scope, id string)
	PutBlobFailures  map[string]int
	GetBlobFailures  map[string][]int
	// NeedsMigrationFailures is the status a listing of rows to reseal
	// answers with, by the listing's number since the stub was made.
	NeedsMigrationFailures          map[int]int
	needsMigrationListings          int
	DeleteAttachmentIndexFailures   map[string]int
	RegisterAttachmentIndexFailures map[string][]int
	ReserveAttachmentPurges         map[string]int
	// CaptureHeaders sees every request before it is served, outside Mu.
	CaptureHeaders func(r *http.Request)

	mux *http.ServeMux
}

func New() *Store {
	s := &Store{
		Blobs:                           map[string]*Blob{},
		Bundles:                         map[string]map[string]controlplane.CurrentKeyBundle{},
		AttachmentIndex:                 map[string]string{},
		AttachmentOwners:                map[string]string{},
		MigrationFailures:               map[string]int{},
		PutBlobFailures:                 map[string]int{},
		GetBlobFailures:                 map[string][]int{},
		NeedsMigrationFailures:          map[int]int{},
		DeleteAttachmentIndexFailures:   map[string]int{},
		RegisterAttachmentIndexFailures: map[string][]int{},
		ReserveAttachmentPurges:         map[string]int{},
		mux:                             http.NewServeMux(),
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
	s.mux.HandleFunc("DELETE "+controlplane.DeleteAllProjectsPath, s.handleDeleteAllProjects)
	s.mux.HandleFunc("POST "+controlplane.RewrapPath, s.handleRewrap)
	s.mux.HandleFunc("POST "+controlplane.ChatActivityPath, s.handleBackfillChatActivity)
	s.mux.HandleFunc("GET /api/sync/list-status", s.handleListStatus)
	s.mux.HandleFunc("GET "+controlplane.RevisionSummaryPath, s.handleRevisionSummary)
	s.mux.HandleFunc("GET /api/sync/needs-migration", s.handleNeedsMigration)
	s.mux.HandleFunc("POST /api/sync/migration-failure", s.handleMigrationFailure)
	s.mux.HandleFunc("GET /api/sync/keys/current", s.handleCurrentKey)
	s.mux.HandleFunc("POST "+controlplane.RegisterKeyPath, s.handleRegisterKey)
	s.mux.HandleFunc("POST /api/sync/keys/{kid}/bundles", s.handleAddBundle)
	s.mux.HandleFunc("DELETE /api/sync/keys/{kid}/bundles/{cid}", s.handleRemoveBundle)
	s.mux.HandleFunc("GET /api/storage/attachment/{aid}", s.handleLegacyAttachment)
	s.mux.HandleFunc("POST /api/sync/attachment-index/{aid}", s.handleRegisterAttachmentIndex)
	s.mux.HandleFunc("DELETE /api/sync/attachment-index/{aid}", s.handleDeleteAttachmentIndex)
	s.mux.HandleFunc("GET /api/sync/attachment-owner/{aid}", s.handleAttachmentOwner)
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
					delete(s.AttachmentOwners, aid)
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "wiped_v2_attachments": wiped, "source_revision": s.SourceRevision})
	}
}

// handleDeleteAllProjects removes every project and project document, and
// answers how many projects went.
func (s *Store) handleDeleteAllProjects(w http.ResponseWriter, r *http.Request) {
	deleted := 0
	for key := range s.Blobs {
		switch scope, _, _ := strings.Cut(key, "/"); scope {
		case "project":
			deleted++
			delete(s.Blobs, key)
		case "project_document":
			delete(s.Blobs, key)
		}
	}
	writeJSON(w, http.StatusOK, controlplane.DeleteAllProjectsResponse{OK: true, Deleted: deleted})
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

// handleListStatus lists a scope's rows, the last written first unless
// asked for the oldest, in pages of limit from an offset cursor; chats can
// be narrowed to a project.
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
	if query.Get("direction") == "asc" {
		slices.Reverse(all)
	}
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

// handleRevisionSummary is where the chats' ledger stands.
func (s *Store) handleRevisionSummary(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, controlplane.RevisionSummaryResponse{CurrentRevision: strconv.FormatInt(s.SourceRevision, 10)})
}

// The needs-migration list is at most maxMigrationPage ids, by id, and
// defaultMigrationPage without a limit.
const (
	maxMigrationPage     = 500
	defaultMigrationPage = 100
)

// handleNeedsMigration lists a scope's rows not sealed under the current
// key and outside their cooldown, by id, with how many such rows remain
// and how many are in a cooldown. The control plane files a profile under
// its user, so it is named by the user id.
func (s *Store) handleNeedsMigration(w http.ResponseWriter, r *http.Request) {
	listing := s.needsMigrationListings
	s.needsMigrationListings++
	if status, failed := s.NeedsMigrationFailures[listing]; failed {
		w.WriteHeader(status)
		return
	}
	scope := r.URL.Query().Get("scope")
	switch scope {
	case "chat", "profile", "project", "project_document":
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid scope"})
		return
	}
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		limit = defaultMigrationPage
	}
	limit = min(limit, maxMigrationPage)
	resp := controlplane.ListNeedsMigrationResponse{IDs: []string{}}
	for key, blob := range s.Blobs {
		rowScope, id, _ := strings.Cut(key, "/")
		if rowScope != scope || blob.KeyID != "" && blob.KeyID == s.CurrentKeyID {
			continue
		}
		if scope == "profile" {
			id = r.Header.Get(controlplane.HeaderClerkUserID)
		}
		if s.MigrationFailures[Key(scope, id)] > 0 {
			resp.BlockedUnmigrated++
			continue
		}
		resp.RetryableRemaining++
		resp.IDs = append(resp.IDs, id)
	}
	sort.Strings(resp.IDs)
	resp.IDs = resp.IDs[:min(len(resp.IDs), limit)]
	writeJSON(w, http.StatusOK, resp)
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

// keyTimestamp is a time as the control plane reports when a key or a
// bundle was registered.
func keyTimestamp(at time.Time) string { return at.UTC().Format("2006-01-02T15:04:05.000Z") }

// currentKeyAnswer is the control plane's answer about the current key;
// without one it names no key and leaves out when it was made.
type currentKeyAnswer struct {
	KeyID      string                                   `json:"key_id"`
	ETag       string                                   `json:"etag"`
	Bundles    map[string]controlplane.CurrentKeyBundle `json:"bundles"`
	CreatedVia string                                   `json:"created_via"`
	CreatedAt  string                                   `json:"created_at,omitempty"`
	HasData    bool                                     `json:"has_data"`
}

func (s *Store) handleCurrentKey(w http.ResponseWriter, r *http.Request) {
	answer := currentKeyAnswer{Bundles: map[string]controlplane.CurrentKeyBundle{}, HasData: s.hasDataUnderOtherKey(s.CurrentKeyID, true)}
	if s.CurrentKeyID != "" {
		answer.KeyID, answer.ETag, answer.CreatedVia, answer.CreatedAt = s.CurrentKeyID, FormatETag(s.KeyETag), s.KeyCreatedVia, keyTimestamp(s.KeyRegisteredAt)
		for cid, bundle := range s.Bundles[s.CurrentKeyID] {
			answer.Bundles[cid] = bundle
		}
	}
	writeJSON(w, http.StatusOK, answer)
}

// hasDataUnderOtherKey is whether any row is sealed under a key other
// than claim, counting rows sealed under none when legacy is set.
func (s *Store) hasDataUnderOtherKey(claim string, legacy bool) bool {
	for _, blob := range s.Blobs {
		if blob.KeyID == "" && legacy || blob.KeyID != "" && blob.KeyID != claim {
			return true
		}
	}
	return false
}

func keyConflict(w http.ResponseWriter, code string, details map[string]string) {
	body := map[string]string{"code": code, "error": strings.ToLower(strings.ReplaceAll(code, "_", " "))}
	for k, v := range details {
		body[k] = v
	}
	writeJSON(w, http.StatusConflict, body)
}

func isLowerHex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// handleRegisterKey registers a key as the control plane does. Without a
// key, If-Match must be "*", and a key that is not a recovery or a manual
// one is refused over existing rows unless it starts fresh, which wipes
// them. Over a key, If-Match must be its revision; "*" re-asserts the same
// key and refuses another. A key change drops the bundles of the old key
// and the cooldowns of the rows left to reseal.
func (s *Store) handleRegisterKey(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(controlplane.HeaderIdempotency) == "" || r.Header.Get(controlplane.HeaderOperationHash) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "X-Idempotency-Key and X-Operation-Hash are required"})
		return
	}
	var body struct {
		KeyID         string                          `json:"key_id"`
		CreatedVia    string                          `json:"created_via"`
		InitialBundle *controlplane.RegisterKeyBundle `json:"initial_bundle"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !isLowerHex32(body.KeyID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid key_id"})
		return
	}
	switch body.CreatedVia {
	case "passkey", "manual", "recovery", "start_fresh":
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid created_via"})
		return
	}
	ifMatch := r.Header.Get(controlplane.HeaderIfMatch)
	if ifMatch == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "If-Match is required"})
		return
	}
	if body.InitialBundle != nil && (body.InitialBundle.CredentialID == "" || body.InitialBundle.KEKIV == "" || body.InitialBundle.EncryptedKeys == "") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "initial_bundle requires credential_id, kek_iv, encrypted_keys"})
		return
	}
	asserting := body.CreatedVia == "recovery" || body.CreatedVia == "manual"
	wiped := []string{}
	switch {
	case s.CurrentKeyID == "":
		if ifMatch != controlplane.IfMatchAnyKey {
			keyConflict(w, controlplane.StatusStaleKey, map[string]string{"current_etag": ""})
			return
		}
		if !asserting && s.hasDataUnderOtherKey(body.KeyID, true) {
			if body.CreatedVia != "start_fresh" {
				keyConflict(w, controlplane.StatusExistingDataUnderOtherKey, map[string]string{"current_key_id": ""})
				return
			}
			wiped = s.wipeAll()
		}
	case ifMatch == controlplane.IfMatchAnyKey:
		if s.CurrentKeyID == body.KeyID && body.CreatedVia != "start_fresh" {
			s.addBundle(body.KeyID, body.InitialBundle)
			writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key_id": s.CurrentKeyID, "etag": FormatETag(s.KeyETag), "wiped_v2_attachments": wiped})
			return
		}
		keyConflict(w, controlplane.StatusExistingDataUnderOtherKey, map[string]string{"current_key_id": s.CurrentKeyID})
		return
	default:
		if ifMatch != FormatETag(s.KeyETag) {
			keyConflict(w, controlplane.StatusStaleKey, map[string]string{"current_etag": FormatETag(s.KeyETag), "current_key_id": s.CurrentKeyID})
			return
		}
		if s.CurrentKeyID != body.KeyID && s.hasDataUnderOtherKey(body.KeyID, !asserting) {
			if asserting || body.CreatedVia != "start_fresh" {
				keyConflict(w, controlplane.StatusExistingDataUnderOtherKey, map[string]string{"current_key_id": s.CurrentKeyID})
				return
			}
			wiped = s.wipeAll()
		}
	}
	changed := s.CurrentKeyID != body.KeyID
	if changed {
		s.KeyRegisteredAt = time.Now()
	}
	s.CurrentKeyID, s.KeyCreatedVia = body.KeyID, body.CreatedVia
	s.KeyETag++
	if changed {
		for kid := range s.Bundles {
			if kid != body.KeyID {
				delete(s.Bundles, kid)
			}
		}
		clear(s.MigrationFailures)
	}
	s.addBundle(body.KeyID, body.InitialBundle)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key_id": s.CurrentKeyID, "etag": FormatETag(s.KeyETag), "wiped_v2_attachments": wiped})
}

// wipeAll drops every row and attachment, and names the attachments held
// in buckets so the caller can drop those too.
func (s *Store) wipeAll() []string {
	for key := range s.Blobs {
		if strings.HasPrefix(key, "chat/") {
			s.SourceRevision++
		}
		delete(s.Blobs, key)
	}
	wiped := []string{}
	for aid := range s.AttachmentIndex {
		wiped = append(wiped, aid)
	}
	sort.Strings(wiped)
	clear(s.AttachmentIndex)
	clear(s.AttachmentOwners)
	clear(s.LegacyAttachments)
	return wiped
}

func (s *Store) addBundle(kid string, bundle *controlplane.RegisterKeyBundle) {
	if bundle == nil {
		return
	}
	if s.Bundles[kid] == nil {
		s.Bundles[kid] = map[string]controlplane.CurrentKeyBundle{}
	}
	s.Bundles[kid][bundle.CredentialID] = controlplane.CurrentKeyBundle{CredentialID: bundle.CredentialID, KEKIV: bundle.KEKIV, EncryptedKeys: bundle.EncryptedKeys, RegisteredAt: keyTimestamp(time.Now())}
}

// bundleKey is the key a bundle request names, once it is the current
// one: without a key the request is not found, and with another it is
// stale.
func (s *Store) bundleKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	kid := r.PathValue("kid")
	switch {
	case r.Header.Get(controlplane.HeaderIdempotency) == "" || r.Header.Get(controlplane.HeaderOperationHash) == "":
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "X-Idempotency-Key and X-Operation-Hash are required"})
	case !isLowerHex32(kid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid key id"})
	case s.CurrentKeyID == "":
		writeJSON(w, http.StatusNotFound, map[string]string{"code": "NOT_FOUND", "error": "no current key registered"})
	case kid != s.CurrentKeyID:
		keyConflict(w, controlplane.StatusStaleKey, map[string]string{"current_key_id": s.CurrentKeyID, "current_etag": FormatETag(s.KeyETag)})
	default:
		return kid, true
	}
	return "", false
}

func (s *Store) handleAddBundle(w http.ResponseWriter, r *http.Request) {
	var bundle controlplane.RegisterKeyBundle
	if err := json.NewDecoder(r.Body).Decode(&bundle); err != nil || bundle.CredentialID == "" || bundle.KEKIV == "" || bundle.EncryptedKeys == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "credential_id, kek_iv, encrypted_keys are required"})
		return
	}
	kid, ok := s.bundleKey(w, r)
	if !ok {
		return
	}
	s.addBundle(kid, &bundle)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleRemoveBundle drops a credential's bundle; one the key does not
// have is already gone.
func (s *Store) handleRemoveBundle(w http.ResponseWriter, r *http.Request) {
	kid, ok := s.bundleKey(w, r)
	if !ok {
		return
	}
	delete(s.Bundles[kid], r.PathValue("cid"))
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
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
// for the user the request names, and drops the legacy bytes, so a later
// legacy read answers not found.
func (s *Store) handleRegisterAttachmentIndex(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	var body struct {
		ChatID string `json:"chat_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if pending := s.RegisterAttachmentIndexFailures[aid]; len(pending) > 0 {
		s.RegisterAttachmentIndexFailures[aid] = pending[1:]
		w.WriteHeader(pending[0])
		return
	}
	if s.AttachmentIndex == nil {
		s.AttachmentIndex = map[string]string{}
	}
	s.AttachmentIndex[aid] = body.ChatID
	s.AttachmentOwners[aid] = r.Header.Get(controlplane.HeaderClerkUserID)
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
	delete(s.AttachmentOwners, aid)
	w.WriteHeader(http.StatusNoContent)
}

// handleAttachmentOwner names the user an indexed attachment belongs to.
func (s *Store) handleAttachmentOwner(w http.ResponseWriter, r *http.Request) {
	owner, ok := s.AttachmentOwners[r.PathValue("aid")]
	if !ok || owner == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"clerk_user_id": owner})
}

// The pending-write ledger accepts every reservation, except while an
// earlier copy of the id is being purged, and never has anything to sweep.
func (s *Store) handleReservePendingAttachment(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	if s.ReserveAttachmentPurges[aid] > 0 {
		s.ReserveAttachmentPurges[aid]--
		writeJSON(w, http.StatusConflict, map[string]string{"code": controlplane.StatusAttachmentPurgeInProgress})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Store) handleSweepPendingAttachments(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"rows": []any{}})
}
