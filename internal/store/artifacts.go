package store

import (
	"context"
	"errors"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Artifacts are read from the chats, so a chat deleted, moved or changed
// takes its artifacts with it. So that a listing opens only the chats that
// changed, the store keeps each chat's artifacts as last read, as it keeps
// its summary: bound to the caller, the key that opened the chat and the
// revision it was read at, for at most summaryLifetime. They hold where each
// artifact is, its title and the type of its source, never the source.

const (
	// A page of artifacts reads chats in the order the chat list shows
	// them, a page of chats at a time, until it holds artifactPageTarget
	// artifacts or has read artifactPageChats chats.
	artifactPageTarget = 50
	artifactPageChats  = 500
)

// keptArtifacts are a chat's artifacts and the revision they were read at.
type keptArtifacts struct {
	etag      string
	artifacts []domain.Artifact
}

func (s *Store) keptArtifacts() *expirable.LRU[summaryKey, keptArtifacts] {
	s.artifactsOnce.Do(func() {
		s.artifacts = expirable.NewLRU[summaryKey, keptArtifacts](maxSummaries, nil, summaryLifetime)
	})
	return s.artifacts
}

// Artifacts is one page of the artifacts a user's chats show, or a
// project's: each chat's newest first, the chats in the order their list
// shows them. It opens only the chats whose artifacts under this key are
// not kept at their listed revision. A chat this harness cannot read is
// left out, as one the key cannot open is.
func (s *Store) Artifacts(ctx context.Context, p *auth.Principal, key auth.ContentKey, projectID, cursor string) ([]domain.Artifact, string, error) {
	kid, kept := keyID(key), s.keptArtifacts()
	artifacts, seen, read := []domain.Artifact{}, map[string]bool{}, 0
	for len(artifacts) < artifactPageTarget && read < artifactPageChats {
		listed, next, err := s.Rows.Revisions(ctx, p, "chat", cursor, projectID, maxListPage)
		if err != nil {
			return nil, "", err
		}
		read += len(listed)
		found, stale := map[string][]domain.Artifact{}, []string{}
		for _, row := range listed {
			if held, ok := kept.Get(summaryKey{p.Scope(), kid, row.ID}); ok && held.etag == row.ETag {
				found[row.ID] = held.artifacts
			} else {
				stale = append(stale, row.ID)
			}
		}
		rows, failed, err := s.Rows.Pull(ctx, p, key, "chat", stale)
		if err != nil {
			return nil, "", err
		}
		for _, failure := range failed {
			if errors.Is(failure, ErrRowNotFetched) {
				return nil, "", failure
			}
		}
		for i := range rows {
			if row, err := domain.ReadThreadRow(&rows[i]); err == nil && (projectID == "" || domain.ValueOr(row.ProjectID, "") == projectID) {
				found[row.ID] = row.Artifacts()
				kept.Add(summaryKey{p.Scope(), kid, row.ID}, keptArtifacts{row.ETag, found[row.ID]})
			}
		}
		for _, row := range listed {
			// A chat the control plane files again while it is listed is
			// listed again.
			if seen[row.ID] {
				continue
			}
			seen[row.ID] = true
			for _, artifact := range found[row.ID] {
				// The control plane can move a chat out of a deleted project
				// without a new revision, so its listed project is current.
				artifact.ProjectID = row.Project
				artifacts = append(artifacts, artifact)
			}
		}
		if next == "" {
			return artifacts, "", nil
		}
		if next == cursor {
			return nil, "", api.Err(502, "UPSTREAM_REFUSED", "The storage cursor did not advance")
		}
		cursor = next
	}
	return artifacts, cursor, nil
}
