// Package search keeps each caller's encrypted chat index in step with
// their stored chats and answers queries from it. The index is one sealed
// object per caller in the search bucket, under a key derived from the
// caller's content key, and the control plane's publication record names
// the object every replica reads. Chats are indexed as they are stored,
// while the harness holds their plaintext and key; a query is embedded
// the same way and ranked here, so no chat text leaves the enclave.
package search

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// Embedder turns text into vectors through the attested inference service.
type Embedder interface {
	Configured() bool
	Model() string
	Embed(ctx context.Context, inputs []string) ([][]float32, error)
}

// Service keeps each caller's search index: one sealed object per caller
// in the search bucket, under a key derived from the caller's content key,
// named by the control plane's publication record so every replica reads
// the same one. Chats are indexed as they are stored, while the harness
// holds their plaintext and key; queries are embedded the same way and
// ranked in the enclave.
type Service struct {
	Controlplane *controlplane.Client
	Buckets      *buckets.Client // the search index bucket
	Embedder     Embedder
	Rows         store.RowStore // what a rebuild reads
	Rebuilds     *jobs.Runner[RebuildReport]

	// Decoded indexes are kept between requests; the object a caller's
	// index is read from is rewritten whole, so one caller's reads and
	// writes take turns on their lock.
	cache *indexCache
	locks sync.Map // owner to *sync.RWMutex
	// The inference service is shared by every caller, so embedding is
	// rationed per caller and overall.
	gate     *inferenceGate
	timeouts inferenceTimeouts
	// started is what each caller's last rebuild was for, so a request
	// for another kind of rebuild replaces it rather than joins it.
	mu      sync.Mutex
	started map[string]startedRebuild
}

func New(cp *controlplane.Client, bk *buckets.Client, embedder Embedder, rows store.RowStore) *Service {
	return &Service{
		Controlplane: cp,
		Buckets:      bk,
		Embedder:     embedder,
		Rows:         rows,
		Rebuilds:     jobs.New[RebuildReport](RebuildBudget, RebuildRetention),
		cache:        newIndexCache(cacheBudgetBytes),
		gate:         newInferenceGate(defaultInferenceLimits),
		timeouts:     defaultInferenceTimeouts,
		started:      map[string]startedRebuild{},
	}
}

// errUnavailable refuses a search while the index bucket or the embedding
// service is not configured; chats are then stored without being indexed.
var errUnavailable = api.Err(503, "SEARCH_INDEX_FAILED", "Search is not configured")

func (s *Service) configured() bool {
	return s != nil && s.Buckets != nil && s.Buckets.Configured() && s.Embedder != nil && s.Embedder.Configured()
}

// caller is who an index belongs to, with the keys it is kept under: the
// id of their content key, which the publication names, and the key the
// index object is sealed with, derived from it. A rebuild's caller also
// holds the content key, which opens the chats it reads.
type caller struct {
	p        *auth.Principal
	kid      string
	indexKey []byte
	key      auth.ContentKey
}

func (s *Service) callerOf(p *auth.Principal, key auth.ContentKey) (caller, error) {
	indexKey, err := seal.DeriveSearchIndexKey(key[:])
	if err != nil {
		return caller{}, api.Err(400, "BAD_REQUEST", "invalid content key")
	}
	return caller{p: p, kid: keyID(key), indexKey: indexKey}, nil
}

// detached is the caller as a background job keeps them: who they are and
// their credential, without the request's grant, with their own copies
// of the keys.
func (c caller) detached(key auth.ContentKey) caller {
	return caller{p: &auth.Principal{ID: c.p.ID, JWT: c.p.JWT}, kid: c.kid, indexKey: bytes.Clone(c.indexKey), key: key}
}

func (c caller) owner() string { return c.p.ID }

func (c *caller) zero() {
	seal.Zero(c.indexKey)
	clear(c.key[:])
}

// keyID is the hex id a content key is registered and sealed under.
func keyID(key auth.ContentKey) string {
	id, _ := seal.DeriveKeyID(key[:])
	return seal.KeyIDHex(id)
}

// lock serializes the read-modify-write cycles on one caller's index
// within this process: the bucket has no compare-and-swap, so two writes
// that read the same generation would lose one entry. Readers take the
// shared side, since a cached index is changed in place by writers.
func (s *Service) lock(owner string) func() {
	mu := s.lockOf(owner)
	mu.Lock()
	return mu.Unlock
}

func (s *Service) rlock(owner string) func() {
	mu := s.lockOf(owner)
	mu.RLock()
	return mu.RUnlock
}

func (s *Service) lockOf(owner string) *sync.RWMutex {
	mu, _ := s.locks.LoadOrStore(owner, &sync.RWMutex{})
	return mu.(*sync.RWMutex)
}

// Forget drops what this process holds for a caller whose index the
// control plane fenced off (a start-fresh key), and the fenced object.
func (s *Service) Forget(ctx context.Context, owner, objectKey string) {
	if !s.configured() {
		return
	}
	s.forgetRebuild(owner)
	unlock := s.lock(owner)
	defer unlock()
	s.cache.drop(owner)
	s.deleteObject(ctx, owner, objectKey)
}

// entryTime is when an entry was written, for ordering; an entry without
// a time, or with one that cannot be read, is older than any other.
func entryTime(updatedAt string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return time.Time{}
	}
	return t
}
