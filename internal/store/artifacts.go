package store

import (
	"context"
	"slices"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A project's artifacts are read from its chats, so a chat deleted, moved
// out of the project or changed takes its artifacts with it. So that a
// listing opens only the chats that changed, the store keeps each chat's
// artifacts as last read, as it keeps its summary: bound to the caller, the
// key that opened the chat and the revision it was read at, for at most
// summaryLifetime. They hold where each artifact is, its title and the type
// of its source, never the source.

// keptArtifacts are a chat's artifacts and the revision they were read at.
type keptArtifacts struct {
	etag      string
	artifacts []domain.ProjectArtifact
}

func (s *Store) keptArtifacts() *expirable.LRU[summaryKey, keptArtifacts] {
	s.artifactsOnce.Do(func() {
		s.artifacts = expirable.NewLRU[summaryKey, keptArtifacts](maxSummaries, nil, summaryLifetime)
	})
	return s.artifacts
}

// ProjectArtifacts are the artifacts every chat of a project shows, newest
// first. It opens only the chats whose artifacts under this key are not
// kept at their listed revision. A chat this harness cannot read is left
// out, as one the key cannot open is.
func (s *Store) ProjectArtifacts(ctx context.Context, p *auth.Principal, key auth.ContentKey, projectID string) ([]domain.ProjectArtifact, error) {
	kid, kept := keyID(key), s.keptArtifacts()
	artifacts, seen := []domain.ProjectArtifact{}, map[string]bool{}
	for cursor := ""; ; {
		listed, next, err := s.Rows.Revisions(ctx, p, "chat", cursor, projectID, maxListPage)
		if err != nil {
			return nil, err
		}
		stale := []string{}
		for _, row := range listed {
			// A chat the control plane files again while it is listed
			// is listed again.
			if seen[row.ID] {
				continue
			}
			seen[row.ID] = true
			if held, ok := kept.Get(summaryKey{p.Scope(), kid, row.ID}); ok && held.etag == row.ETag {
				artifacts = append(artifacts, held.artifacts...)
			} else {
				stale = append(stale, row.ID)
			}
		}
		rows, _, err := s.Rows.Pull(ctx, p, key, "chat", stale)
		if err != nil {
			return nil, err
		}
		for i := range rows {
			if row, err := domain.ReadThreadRow(&rows[i]); err == nil {
				found := row.Artifacts()
				kept.Add(summaryKey{p.Scope(), kid, row.ID}, keptArtifacts{row.ETag, found})
				artifacts = append(artifacts, found...)
			}
		}
		if next == "" {
			break
		}
		if next == cursor {
			return nil, api.Err(502, "UPSTREAM_REFUSED", "The storage cursor did not advance")
		}
		cursor = next
	}
	// A time that does not parse reads as the zero time, which sorts last.
	slices.SortStableFunc(artifacts, func(a, b domain.ProjectArtifact) int {
		at, _ := time.Parse(time.RFC3339Nano, a.CreatedAt)
		bt, _ := time.Parse(time.RFC3339Nano, b.CreatedAt)
		return bt.Compare(at)
	})
	return artifacts, nil
}
