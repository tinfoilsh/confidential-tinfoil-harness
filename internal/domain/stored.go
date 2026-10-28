package domain

import (
	"encoding/json"
	"slices"
	"time"
)

// StoredRow is a row as the control plane holds it: sealed JSON, opened
// only by the store's readers.
type StoredRow struct {
	ID, ETag string
	Data     json.RawMessage
	// project is the project the control plane files a chat or document
	// under, apart from its sealed data; projectSet says it reported one,
	// even none.
	Project    *string `json:"-"`
	ProjectSet bool    `json:"-"`
	// UpdatedAt is when the control plane last wrote the row. A row an
	// older client sealed carries no time of its own, and is read as
	// updated then.
	UpdatedAt time.Time `json:"-"`
}

// rowTime is when a row was last written, as RFC 3339, or "" when the
// control plane did not say.
func (row *StoredRow) rowTime() string {
	if row.UpdatedAt.IsZero() {
		return ""
	}
	return row.UpdatedAt.UTC().Format(time.RFC3339Nano)
}

func (row StoredRow) Copy() *StoredRow {
	row.Data = slices.Clone(row.Data)
	return &row
}

// ImportOrigin is what an imported row came from: the import's source and
// the source's own id for it, and which copy of it this is when an earlier
// copy was in the way.
type ImportOrigin struct {
	Format     string `json:"format"`
	Source     string `json:"sourceBackupId"`
	Kind       string `json:"kind"`
	SourceID   string `json:"sourceId"`
	Generation int    `json:"generation"`
}

// ReadImportOrigin is where a row came from when an import stored it, or
// nil for a row that was not imported or cannot be read.
func ReadImportOrigin(row *StoredRow) *ImportOrigin {
	var stored struct {
		ImportedFrom *ImportOrigin `json:"_restore"`
	}
	if json.Unmarshal(row.Data, &stored) != nil {
		return nil
	}
	return stored.ImportedFrom
}

// rowJSON is a stored row as JSON, to check which fields it holds.
