package search

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// errIndexPanicked is an index update that panicked: the chat's write
// has already landed, so the caller sees an ordinary failure, which only
// degrades search until the next rebuild.
var errIndexPanicked = errors.New("search: the index update panicked")

// ChatStored indexes a chat just written, at the revision its write
// returned and the source revision it left. The write has landed, so
// nothing here may fail it: an error degrades search until a rebuild,
// and a panic is contained the same way.
//
// The push's revision is trusted as the live one while the control plane
// has seen no later chat write, which the publication's source revision
// tells; only a gap costs a read of the chat's metadata, which says
// whether the chat was overwritten or deleted since.
func (s *Service) ChatStored(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, plaintext []byte, etag string, at time.Time, revision int64) (err error) {
	defer clear(key[:])
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errIndexPanicked
		}
	}()
	if !s.configured() {
		return nil
	}
	c, err := s.callerOf(p, key)
	if err != nil {
		return err
	}
	defer c.zero()
	hook := func(ctx context.Context, ix *searchindex.Index, publication *controlplane.SearchIndexState) (bool, bool, error) {
		if revision > 0 && publication.SourceRevision == revision {
			return true, false, nil
		}
		switch state, err := s.snapshotState(ctx, c, id, etag); {
		case err != nil:
			return false, false, err
		case state == snapshotCurrent:
			return true, false, nil
		case state == snapshotDeleted:
			if _, ok := ix.Chats[id]; ok {
				ix.Remove(id)
				return false, true, nil
			}
			return false, false, nil
		default:
			return false, false, nil
		}
	}
	return s.indexChat(ctx, c, id, plaintext, etag, at, revision, hook)
}

// writeHook decides, under the owner's lock with the index loaded,
// whether a chat is still to be indexed: cont to go on, mutated when the
// hook changed the index instead, which is then published as it is.
type writeHook func(ctx context.Context, ix *searchindex.Index, publication *controlplane.SearchIndexState) (cont, mutated bool, err error)

// indexChat embeds one chat and merges it into the caller's index. at is
// when the chat's write landed, which orders this entry against other
// writes of the same chat well before the embedding round trip can
// reorder their completion.
func (s *Service) indexChat(ctx context.Context, c caller, id string, plaintext []byte, etag string, at time.Time, revision int64, hook writeHook) error {
	owner := c.owner()
	chunks := chatChunks(plaintext)
	entry := searchindex.Entry{ETag: etag, SourceRevision: revision, UpdatedAt: at.UTC().Format(time.RFC3339Nano)}
	tokens := searchindex.Tokenize(strings.Join(chunks, "\n"))
	if len(chunks) > 0 {
		// Without vectors the chat is still found by keyword, and a
		// later query repairs the embedding.
		entry.EmbeddingPending, entry.EmbeddingKeyID = true, c.kid
		if vectors, err := s.embedChunks(ctx, owner, docPrefix, chunks, false); err == nil {
			entry.Vectors, entry.EmbeddingPending, entry.EmbeddingKeyID = quantizeAll(vectors), false, ""
		}
	}

	unlock := s.lock(owner)
	defer unlock()
	for attempt := 0; attempt < publishAttempts; attempt++ {
		publication, objectKey, err := s.publication(ctx, c)
		if err != nil {
			return err
		}
		ix, state, err := s.load(ctx, owner, objectKey, c.indexKey)
		if err != nil {
			return err
		}
		if publication.ObjectKey != "" && objectKey == "" {
			state = loadUnreadable
		}
		if hook != nil {
			cont, mutated, err := hook(ctx, ix, publication)
			if err != nil {
				return err
			}
			if !cont && !mutated {
				return nil
			}
			if !cont {
				ix.Incomplete = true
				if _, err := s.publish(ctx, c, publication, publication.PublishedSourceRevision, true, ix); err != nil {
					if isConflict(err) {
						continue
					}
					return err
				}
				return nil
			}
		}
		// A slower write must not replace the entry a newer write, or a
		// rebuild, already made for this chat.
		if existing, ok := ix.Chats[id]; ok && (entrySupersedes(existing, etag, revision, at) || existing.SourceRevision == revision && existing.ETag == etag) {
			return nil
		}
		if err := ix.Upsert(id, entry, tokens); err != nil {
			return err
		}
		// The publication advances to this write's revision only when it
		// is the very next one over a complete, readable index; any gap
		// leaves the index incomplete, for a rebuild to close.
		publicationReadable := publication.ObjectKey == "" || state == loadOK
		covered, incomplete := publication.PublishedSourceRevision, true
		if revision == publication.PublishedSourceRevision+1 && !publication.PublicationIncomplete && publicationReadable {
			covered, incomplete = revision, false
		}
		ix.Incomplete = incomplete
		if _, err := s.publish(ctx, c, publication, covered, incomplete, ix); err != nil {
			if isConflict(err) {
				continue
			}
			return err
		}
		return nil
	}
	return errIndexContended
}

// ChatRemoved drops a deleted chat from the caller's index. A stale entry
// only means a deleted chat can still come up until the next rebuild, so
// a failure is let go, as is a panic.
func (s *Service) ChatRemoved(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, revision int64) {
	defer clear(key[:])
	defer func() { _ = recover() }()
	if !s.configured() {
		return
	}
	c, err := s.callerOf(p, key)
	if err != nil {
		return
	}
	defer c.zero()
	_ = s.removeChat(ctx, c, id, revision)
}

// removeChat drops a deleted chat from the index, unless a newer chat was
// made under the same id while this was waiting to run.
func (s *Service) removeChat(ctx context.Context, c caller, id string, revision int64) error {
	owner := c.owner()
	unlock := s.lock(owner)
	defer unlock()
	if _, err := s.Controlplane.GetBlob(ctx, "chat", id, string(c.p.JWT), owner); err == nil {
		return nil
	} else if !controlplaneStatus(err, http.StatusNotFound) {
		return store.FromControlplane(err)
	}
	for attempt := 0; attempt < publishAttempts; attempt++ {
		publication, objectKey, err := s.publication(ctx, c)
		if err != nil {
			return err
		}
		ix, state, err := s.load(ctx, owner, objectKey, c.indexKey)
		if err != nil {
			return err
		}
		existing, exists := ix.Chats[id]
		if !exists && state == loadOK {
			return nil
		}
		if exists && revision > 0 && existing.SourceRevision > revision {
			// The entry is from a write after this deletion: a chat made
			// again under the id, which stays.
			return nil
		}
		ix.Remove(id)
		publicationReadable := publication.ObjectKey == "" || state == loadOK
		covered, incomplete := publication.PublishedSourceRevision, true
		if revision == publication.PublishedSourceRevision+1 && !publication.PublicationIncomplete && publicationReadable {
			covered, incomplete = revision, false
		}
		ix.Incomplete = incomplete
		if _, err := s.publish(ctx, c, publication, covered, incomplete, ix); err != nil {
			if isConflict(err) {
				continue
			}
			return err
		}
		return nil
	}
	return errIndexContended
}

// snapshotState is whether a chat is still at the revision an index entry
// was built from, has changed since, or is gone.
type snapshotState int

const (
	snapshotCurrent snapshotState = iota
	snapshotStale
	snapshotDeleted
)

// snapshotState compares a chat's stored revision with etag.
func (s *Service) snapshotState(ctx context.Context, c caller, id, etag string) (snapshotState, error) {
	current, exists, err := s.currentETag(ctx, c, id)
	switch {
	case err != nil:
		return snapshotStale, err
	case !exists:
		return snapshotDeleted, nil
	case current != etag:
		return snapshotStale, nil
	}
	return snapshotCurrent, nil
}

// currentETag is a chat's stored revision; a deleted chat has none. It
// runs under the owner's lock, so it reads the chat's metadata rather
// than its bytes. A failed metadata read is ambiguous, though: a control
// plane that does not route it answers not found for live chats, and
// taking that for deleted would silently drop the chat from the index,
// so the chat itself is read before a miss counts.
func (s *Service) currentETag(ctx context.Context, c caller, id string) (etag string, exists bool, err error) {
	owner := c.owner()
	if meta, err := s.Controlplane.HeadBlob(ctx, "chat", id, string(c.p.JWT), owner); err == nil {
		return meta.ETag, true, nil
	}
	blob, err := s.Controlplane.GetBlob(ctx, "chat", id, string(c.p.JWT), owner)
	if err != nil {
		if controlplaneStatus(err, http.StatusNotFound) {
			return "", false, nil
		}
		return "", false, store.FromControlplane(err)
	}
	return blob.ETag, true, nil
}

func controlplaneStatus(err error, status int) bool {
	var cpe *controlplane.Error
	return errors.As(err, &cpe) && cpe.StatusCode == status
}

// etagGeneration reads a revision as the control plane's per-row counter;
// ok is false for any other shape, which is ordered by time instead.
func etagGeneration(etag string) (int64, bool) {
	generation, err := strconv.ParseInt(etag, 10, 64)
	return generation, err == nil
}

// entrySupersedes is whether an index entry is from a strictly newer
// revision of its chat than a write at etag. The source revision orders
// writes across replicas; the per-row revision, then the write time,
// order entries without one.
func entrySupersedes(existing searchindex.Entry, etag string, revision int64, at time.Time) bool {
	if existing.SourceRevision > 0 && revision > 0 {
		return existing.SourceRevision > revision
	}
	if eg, ok := etagGeneration(existing.ETag); ok {
		if cg, ok := etagGeneration(etag); ok {
			return eg > cg
		}
	}
	return entryTime(existing.UpdatedAt).After(at)
}
