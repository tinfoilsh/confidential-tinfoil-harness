package main

import (
	"encoding/json"
	"slices"
)

// storedRow is a row as sync holds it: sealed JSON, opened only by the
// store's readers.
type storedRow struct {
	ID, ETag string
	Data     json.RawMessage
	// project is the project sync files a chat or document under, apart
	// from its sealed data; projectSet says sync reported one, even none.
	project    *string
	projectSet bool
	// restore lets a create reuse the id of a chat deleted from sync, as a
	// chat moved to the device and back does.
	restore bool
}

func (row storedRow) copy() *storedRow {
	row.Data = slices.Clone(row.Data)
	return &row
}

// rowJSON is a stored row as JSON, to check which fields it holds.
