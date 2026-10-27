package domain

import (
	"encoding/json"
	"slices"
)

// StoredRow is a row as sync holds it: sealed JSON, opened only by the
// store's readers.
type StoredRow struct {
	ID, ETag string
	Data     json.RawMessage
	// project is the project sync files a chat or document under, apart
	// from its sealed data; projectSet says sync reported one, even none.
	Project    *string `json:"-"`
	ProjectSet bool    `json:"-"`
}

func (row StoredRow) Copy() *StoredRow {
	row.Data = slices.Clone(row.Data)
	return &row
}

// rowJSON is a stored row as JSON, to check which fields it holds.
