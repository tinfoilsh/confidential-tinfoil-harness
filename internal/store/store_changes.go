package store

import (
	"context"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// Change tokens tell a client whether what it stores changed, from the
// control plane's ledger alone, without opening any row.

// changeLedger is what a client's change token for a scope is a digest
// of: the edge of the scope's ledger, as listed. A token a client holds
// stays valid only while this shape does.
type changeLedger struct {
	Updates []ledgerUpdate `json:"updates"`
	Deletes []ledgerDelete `json:"deletes"`
}

type ledgerUpdate struct {
	ID        string `json:"id"`
	ETag      string `json:"etag"`
	UpdatedAt string `json:"updated_at"`
}

type ledgerDelete struct {
	ID        string `json:"id"`
	DeletedAt string `json:"deleted_at"`
}

// StorageChanges is a token per kind of row: the chats' revision, and a
// digest of the ledger's edge for the profile and for projects and their
// documents.
func (s *Store) StorageChanges(ctx context.Context, p *auth.Principal) (any, error) {
	summary, err := s.Controlplane.RevisionSummary(ctx, string(p.JWT), p.ID)
	if err != nil {
		return nil, FromControlplane(err)
	}
	tokens := map[string]string{"chats": summary.CurrentRevision}
	for _, scope := range []string{"profile", "project", "project_document"} {
		direction := "desc"
		if scope == "profile" {
			direction = "asc"
		}
		list, err := s.Controlplane.ListStatus(ctx, scope, "", 1, string(p.JWT), p.ID, "", direction)
		if err != nil {
			return nil, FromControlplane(err)
		}
		var ledger changeLedger
		for _, update := range list.Updates {
			ledger.Updates = append(ledger.Updates, ledgerUpdate{update.ID, update.ETag, update.UpdatedAt.UTC().Format(time.RFC3339Nano)})
		}
		for _, deletion := range list.Deletes {
			ledger.Deletes = append(ledger.Deletes, ledgerDelete{deletion.ID, deletion.DeletedAt.UTC().Format(time.RFC3339Nano)})
		}
		tokens[scope] = api.HashHex(api.Raw(ledger))
	}
	return struct {
		Chats    string `json:"chats"`
		Profile  string `json:"profile"`
		Projects string `json:"projects"`
	}{tokens["chats"], tokens["profile"], api.HashHex([]byte(tokens["project"] + tokens["project_document"]))}, nil
}
