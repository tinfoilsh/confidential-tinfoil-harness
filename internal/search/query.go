package search

import (
	"context"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

const (
	maxQueryChars = 1000
	defaultLimit  = 20
	maxLimit      = 100
)

// Query is the ids of the chats that best match query, best first, and
// whether the index must be rebuilt before it answers for every chat.
// The query is embedded outside the caller's lock, since it is a round
// trip that must not hold up their chat writes; one that cannot be
// embedded is answered by keyword alone.
func (s *Service) Query(ctx context.Context, p *auth.Principal, key auth.ContentKey, query string, limit int) (ids []string, rebuildNeeded bool, err error) {
	defer clear(key[:])
	if !s.configured() {
		return nil, false, errUnavailable
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, false, api.Err(400, "BAD_REQUEST", "query is required")
	}
	if len(query) > maxQueryChars {
		return nil, false, api.Err(400, "BAD_REQUEST", "query is too long")
	}
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	c, err := s.callerOf(p, key)
	if err != nil {
		return nil, false, err
	}
	defer c.zero()
	owner := c.owner()

	publication, objectKey, err := s.publication(ctx, c)
	if err != nil {
		return nil, false, err
	}
	runlock := s.rlock(owner)
	ix, state, err := s.load(ctx, owner, objectKey, c.indexKey)
	if err != nil {
		runlock()
		return nil, false, err
	}
	if len(ix.Chats) == 0 {
		runlock()
		return []string{}, s.rebuildNeeded(publication, objectKey, ix, state), nil
	}
	runlock()

	var queryVector []float32
	vectors, err := s.embed(ctx, owner, []string{queryPrefix + query}, false)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
	} else {
		queryVector = vectors[0]
	}

	// The index is loaded again for the search itself rather than kept
	// across the embedding: the earlier pointer could see a change in
	// place whose store failed, while a fresh load is a cache hit that
	// reflects only what is stored.
	publication, objectKey, err = s.publication(ctx, c)
	if err != nil {
		return nil, false, err
	}
	runlock = s.rlock(owner)
	ix, state, err = s.load(ctx, owner, objectKey, c.indexKey)
	if err != nil {
		runlock()
		return nil, false, err
	}
	rebuildNeeded = s.rebuildNeeded(publication, objectKey, ix, state)
	repairEmbeddings := len(queryVector) > 0 && !rebuildNeeded && ix.NeedsEmbeddingRepairFor(c.kid)
	results := ix.Search(queryVector, searchindex.Tokenize(query), limit)
	runlock()
	ids = make([]string, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.ID)
	}
	if repairEmbeddings {
		// Entries indexed while the embedding service was down are
		// repaired now that it answers, best effort.
		s.startEmbeddingRepair(ctx, c, key)
	}
	return ids, rebuildNeeded, nil
}

// rebuildNeeded is whether the index as loaded does not answer for every
// chat: the publication says so, the object is under another key or
// model, the index says so, the object cannot be read, or chats exist
// and no index was ever published.
func (s *Service) rebuildNeeded(publication *controlplane.SearchIndexState, objectKey string, ix *searchindex.Index, state loadState) bool {
	return publication.Incomplete || publication.ObjectKey != "" && objectKey == "" || ix.Incomplete ||
		publication.ObjectKey != "" && state != loadOK ||
		publication.ObjectKey == "" && publication.SourceRevision > 0
}

// needsRepair is whether a rebuild, or with embeddingOnly an embedding
// repair under this key, has work left: the index as stored now, read
// past the cache since another replica may have published since.
func (s *Service) needsRepair(ctx context.Context, c caller, embeddingOnly bool) (bool, error) {
	owner := c.owner()
	state, objectKey, err := s.publication(ctx, c)
	if err != nil {
		return false, err
	}
	if state.ObjectKey == "" {
		return state.Incomplete || state.SourceRevision > 0, nil
	}
	if objectKey == "" {
		return true, nil
	}
	runlock := s.rlock(owner)
	defer runlock()
	s.cache.drop(owner)
	ix, loadState, err := s.load(ctx, owner, state.ObjectKey, c.indexKey)
	if err != nil {
		return false, err
	}
	pending := ix.NeedsEmbeddingRepair()
	if embeddingOnly {
		pending = ix.NeedsEmbeddingRepairFor(c.kid)
	}
	return loadState != loadOK || ix.Incomplete || state.Incomplete || pending, nil
}
