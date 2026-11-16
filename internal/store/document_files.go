package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// A project document keeps the file it was read from beside its text: in
// buckets under an id and key of its own, which its sealed row names, and
// known to the control plane under the document, which keeps the file
// while the document's latest write names it. A file no write names, as
// one replaced, one whose document was deleted, or one whose document was
// never written, the control plane queues for the attachment purge.

var (
	errDocumentFileNotStored = api.Err(502, "UPSTREAM_REFUSED", "The document's file could not be stored")
	errDocumentFileMissing   = api.Err(404, "THREAD_NOT_FOUND", "The document's file is not kept")
)

// PutDocumentFile stores data as the file of a project's document and is
// where it is kept. Each call keeps a new object under a new key: the
// control plane learns of it before buckets holds its bytes, so one a
// failure leaves unnamed is reclaimed.
func (s *Store) PutDocumentFile(ctx context.Context, p *auth.Principal, projectID, documentID string, data []byte) (*domain.DocumentFile, error) {
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsPutDocumentFile(projectID+"/"+documentID) {
		return nil, errRunScope
	}
	id := make([]byte, attachmentIDBytes)
	key := make([]byte, attachmentKeySize)
	defer seal.Zero(key)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	file := &domain.DocumentFile{ID: hex.EncodeToString(id), Key: base64.StdEncoding.EncodeToString(key)}
	if err := s.Controlplane.RegisterDocumentFile(ctx, string(p.JWT), p.ID, file.ID, projectID, documentID); err != nil {
		return nil, FromControlplane(err)
	}
	if err := s.Buckets.Put(ctx, p.ID, file.ID, data, key); err != nil {
		return nil, errDocumentFileNotStored
	}
	return file, nil
}

// DocumentFile reads the file a document keeps, which only its owner reads.
func (s *Store) DocumentFile(ctx context.Context, p *auth.Principal, doc *domain.ProjectDocument) ([]byte, error) {
	if doc.File == nil {
		return nil, errDocumentFileMissing
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opGetAttachment) {
		return nil, errRunScope
	}
	key, err := base64.StdEncoding.DecodeString(doc.File.Key)
	if err != nil || len(key) != attachmentKeySize {
		return nil, errAttachmentKeyInvalid
	}
	defer seal.Zero(key)
	data, err := s.Buckets.Get(ctx, p.ID, doc.File.ID, key)
	switch {
	case errors.Is(err, buckets.ErrNotFound):
		return nil, errDocumentFileMissing
	case errors.Is(err, buckets.ErrForbidden):
		return nil, errAttachmentKeyMismatch
	case err != nil:
		return nil, errAttachmentNotRead
	}
	return data, nil
}

// documentFileRef is the file a document row's plaintext keeps, or empty.
func documentFileRef(plaintext []byte) string {
	var row struct {
		File *domain.DocumentFile `json:"file"`
	}
	if json.Unmarshal(plaintext, &row) != nil || row.File == nil {
		return ""
	}
	return row.File.ID
}
