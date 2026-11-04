package search

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// A chat's write reaches the caller's index after the write has returned.
// Its update waits with the caller's writer, one per caller and key,
// which applies every update waiting in one publication, then those that
// arrived meanwhile in the next: a caller's writes take turns for the
// index rather than race for it, and a burst of them is published once.
// The writer holds the key the index is sealed with, and nothing else of
// the caller's, until no update waits.

// updateTimeout bounds how long after a write its update may still be
// applied, and so how long a writer holds the index key after the
// caller's last write; an update not applied by then is left to a rebuild.
const updateTimeout = time.Minute

type writerKey struct{ owner, kid string }

// writer is one caller's updates under one key: the latest update of each
// chat, waiting to be applied together.
type writer struct {
	c       caller
	pending map[string]chatUpdate
	// deadline is when the oldest pending update is applied by.
	deadline time.Time
	// waiting is closed once the pending updates are applied, applied
	// once the updates last taken are, and ended once the writer is.
	waiting, applied, ended chan struct{}
}

// enqueue leaves an update with the caller's writer, starting one when
// they have none.
func (s *Service) enqueue(c caller, u chatUpdate) {
	name := writerKey{c.owner(), c.kid}
	s.writersMu.Lock()
	defer s.writersMu.Unlock()
	w := s.writers[name]
	if w == nil {
		// The writer needs the index key, not the content key.
		w = &writer{c: c.detached(auth.ContentKey{}), pending: map[string]chatUpdate{}, waiting: make(chan struct{}), ended: make(chan struct{})}
		s.writers[name] = w
		go s.runWriter(name, w)
	}
	if len(w.pending) == 0 {
		w.deadline = time.Now().Add(updateTimeout)
	}
	w.add(u)
}

// add keeps one update per chat: the later revision's, or the later to
// arrive when either has none, which stands for the other's writes too.
// A removal without a revision, of a row already gone when it was read,
// does not take the place of a write with one, which the index takes only
// while it is the chat's latest.
func (w *writer) add(u chatUpdate) {
	if prev, ok := w.pending[u.id]; ok {
		if prev.revision > u.revision && (u.revision > 0 || u.removed) {
			prev, u = u, prev
		}
		u.replaced = append(u.replaced, prev.revisions()...)
	}
	w.pending[u.id] = u
}

// runWriter applies the writer's updates a batch at a time until none
// waits, then ends.
func (s *Service) runWriter(name writerKey, w *writer) {
	defer close(w.ended)
	defer w.c.zero()
	for {
		s.writersMu.Lock()
		if len(w.pending) == 0 {
			delete(s.writers, name)
			s.writersMu.Unlock()
			return
		}
		updates := slices.SortedFunc(maps.Values(w.pending), func(a, b chatUpdate) int {
			return cmp.Or(cmp.Compare(a.revision, b.revision), cmp.Compare(a.id, b.id))
		})
		deadline, applied := w.deadline, w.waiting
		w.pending, w.waiting, w.applied = map[string]chatUpdate{}, make(chan struct{}), applied
		s.writersMu.Unlock()
		behind := s.applyBy(deadline, w.c, updates)
		s.writersMu.Lock()
		w.fold(behind)
		s.writersMu.Unlock()
		close(applied)
	}
}

// applyBy applies a writer's updates by deadline, and is the revisions
// of those a later write of their chat superseded. A failure, or a
// panic, leaves the updates to the rebuild a search starts on finding
// the index short of the writes.
func (s *Service) applyBy(deadline time.Time, c caller, updates []chatUpdate) (behind map[string][]int64) {
	defer func() { api.Rescued("search index update", recover()) }()
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	behind, _ = s.apply(ctx, c, updates)
	return behind
}

// fold hands the revisions of superseded updates to the update of the
// later write of their chat that waits here, which then stands for them.
func (w *writer) fold(behind map[string][]int64) {
	for id, revisions := range behind {
		if u, ok := w.pending[id]; ok && u.revision > slices.Max(revisions) {
			u.replaced = append(u.replaced, revisions...)
			w.pending[id] = u
		}
	}
}

// settled waits until the updates this process holds for the caller's
// index are applied, or ctx ends, so a query sees the caller's own writes
// rather than an index short of them.
func (s *Service) settled(ctx context.Context, c caller) {
	s.writersMu.Lock()
	var applied chan struct{}
	if w := s.writers[writerKey{c.owner(), c.kid}]; w != nil {
		applied = w.applied
		if len(w.pending) > 0 {
			applied = w.waiting
		}
	}
	s.writersMu.Unlock()
	if applied == nil {
		return
	}
	select {
	case <-applied:
	case <-ctx.Done():
	}
}

// Flush waits until every update this process holds for any caller's
// index is applied, or ctx ends, and is whether they were.
func (s *Service) Flush(ctx context.Context) bool {
	for {
		s.writersMu.Lock()
		ended := make([]chan struct{}, 0, len(s.writers))
		for _, w := range s.writers {
			ended = append(ended, w.ended)
		}
		s.writersMu.Unlock()
		if len(ended) == 0 {
			return true
		}
		for _, done := range ended {
			select {
			case <-done:
			case <-ctx.Done():
				return false
			}
		}
	}
}
