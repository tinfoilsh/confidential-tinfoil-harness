package store

import (
	"context"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A chat list shows a summary of each chat, which only the sealed chat
// holds. So that a listing opens only the chats that changed, the store
// keeps each chat's summary as last read or written, bound to the caller,
// the key that opened it and the revision it was read at. A summary is
// served only while the control plane lists the chat at that revision.
//
// Summaries (titles, counts and times, never messages or key material)
// stay in the same attested memory that holds chat plaintext during a
// request, for at most summaryLifetime, and are gone when the process ends.
const (
	// maxSummaries bounds the summaries kept, across every caller.
	maxSummaries = 100_000
	// summaryLifetime is how long a summary is kept after it was read.
	summaryLifetime = time.Hour
)

// summaryKey is whose a kept summary is: the caller, the key that opened
// the chat, and the chat.
type summaryKey struct {
	owner, keyID, id string
}

// keptSummary is a chat's summary and the revision it was read at.
type keptSummary struct {
	etag    string
	summary domain.ThreadSummary
}

func (s *Store) keptSummaries() *expirable.LRU[summaryKey, keptSummary] {
	s.summariesOnce.Do(func() {
		s.summaries = expirable.NewLRU[summaryKey, keptSummary](maxSummaries, nil, summaryLifetime)
	})
	return s.summaries
}

// keepSummary keeps a chat's summary at the revision row holds, and is it.
func (s *Store) keepSummary(p *auth.Principal, kid string, row *domain.ThreadRow) domain.ThreadSummary {
	summary := domain.Summarize(&row.Thread)
	s.keptSummaries().Add(summaryKey{p.Scope(), kid, row.ID}, keptSummary{row.ETag, summary})
	return summary
}

// ThreadSummaryBatch is the chats among ids as their list shows them,
// opened now and kept for listings. A chat ThreadBatch leaves out is left
// out.
func (s *Store) ThreadSummaryBatch(ctx context.Context, p *auth.Principal, key auth.ContentKey, ids []string) ([]domain.ThreadSummary, error) {
	rows, err := s.ThreadBatch(ctx, p, key, ids)
	if err != nil {
		return nil, err
	}
	kid := keyID(key)
	summaries := make([]domain.ThreadSummary, 0, len(rows))
	for _, row := range rows {
		summaries = append(summaries, s.keepSummary(p, kid, row))
	}
	return summaries, nil
}

// ThreadSummaries is one page of a user's chats as their list shows them,
// newest first, or of a project's. It opens only the chats whose summary
// under this key is not kept at their listed revision. A chat this
// harness cannot read is left out, as one the key cannot open is.
func (s *Store) ThreadSummaries(ctx context.Context, p *auth.Principal, key auth.ContentKey, cursor, project string, limit int) ([]domain.ThreadSummary, string, error) {
	listed, next, err := s.Rows.Revisions(ctx, p, "chat", cursor, project, limit)
	if err != nil {
		return nil, "", err
	}
	kid, kept := keyID(key), s.keptSummaries()
	found := make(map[string]domain.ThreadSummary, len(listed))
	stale := []string{}
	for _, row := range listed {
		if summary, ok := kept.Get(summaryKey{p.Scope(), kid, row.ID}); ok && summary.etag == row.ETag {
			// The control plane can move a chat out of a deleted project
			// without a new revision, so its listed project is current.
			summary.summary.ProjectID = row.Project
			found[row.ID] = summary.summary
		} else {
			stale = append(stale, row.ID)
		}
	}
	rows, _, err := s.Rows.Pull(ctx, p, key, "chat", stale)
	if err != nil {
		return nil, "", err
	}
	for i := range rows {
		if row, err := domain.ReadThreadRow(&rows[i]); err == nil {
			found[row.ID] = s.keepSummary(p, kid, row)
		}
	}
	summaries := make([]domain.ThreadSummary, 0, len(found))
	for _, row := range listed {
		if summary, ok := found[row.ID]; ok {
			summaries = append(summaries, summary.Current())
			delete(found, row.ID)
		}
	}
	return summaries, next, nil
}
