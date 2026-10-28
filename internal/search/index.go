package search

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// The index object: the encoded index, gzipped before the bucket seals it
// (sealed bytes do not compress), under a key that names the fence it
// was published under and a random suffix, so a publication never
// overwrites the object another replica reads.

const (
	objectKeyPrefix = "search-index-v1"
	// objectKeySuffixBytes is the random part of an object key.
	objectKeySuffixBytes = 16

	// maxDecodedBytes caps the inflated index, so a corrupted or hostile
	// object cannot balloon memory: searchindex.MaxChats entries at their
	// worst footprint, with ample margin.
	maxDecodedBytes = 512 << 20
	// maxStoredBytes caps the object read from the bucket before it is
	// inflated: the decoded cap plus gzip framing.
	maxStoredBytes = maxDecodedBytes + (1 << 20)

	// A publication that lost to another replica's is tried again, from
	// that replica's object, up to publishAttempts times.
	publishAttempts = 3
	// An object left behind by a publication is deleted within
	// orphanCleanupTimeout; a publication whose answer was lost is
	// reconciled against the control plane within reconcileTimeout.
	orphanCleanupTimeout = 5 * time.Second
	reconcileTimeout     = 5 * time.Second
)

var (
	errIndexUnreadable = api.Err(502, "UPSTREAM_REFUSED", "The search index could not be read")
	errIndexNotStored  = api.Err(502, "UPSTREAM_REFUSED", "The search index could not be stored")
	errIndexContended  = api.Err(503, "SEARCH_INDEX_FAILED", "The search index changed during every publication attempt")
)

// loadState is how an index was obtained. Missing and unreadable both
// give a fresh empty index, but say different things about coverage: a
// missing object may be a caller who never had one, while an unreadable
// one existed and its contents are lost (another key, corruption, a
// model change), so anything built on it is known to be incomplete.
type loadState int

const (
	loadOK loadState = iota
	loadMissing
	loadUnreadable
)

// maxStoredIndexBytes is what load reads at most; a test lowers it.
var maxStoredIndexBytes int64 = maxStoredBytes

// load is the caller's decoded index, from the cache when it has it and
// from the bucket otherwise. A missing object, an index sealed under
// another key, an unknown format or a model change give a fresh empty
// index with a degraded state, which is never cached, so callers can
// report a rebuild rather than fail. Callers hold the owner's lock,
// shared or exclusive, since a cached index is shared across requests.
func (s *Service) load(ctx context.Context, owner, objectKey string, indexKey []byte) (*searchindex.Index, loadState, error) {
	model := s.Embedder.Model()
	if objectKey == "" {
		return searchindex.New(model), loadMissing, nil
	}
	key := cacheKey(indexKey, objectKey)
	if ix, ok := s.cache.get(owner, key); ok {
		if ix.Compatible(model) {
			return ix, loadOK, nil
		}
		s.cache.drop(owner)
	}
	raw, err := s.Buckets.GetLimited(ctx, owner, objectKey, indexKey, maxStoredIndexBytes)
	switch {
	case errors.Is(err, buckets.ErrNotFound):
		return searchindex.New(model), loadMissing, nil
	case errors.Is(err, buckets.ErrForbidden), errors.Is(err, buckets.ErrTooLarge):
		return searchindex.New(model), loadUnreadable, nil
	case err != nil:
		return nil, loadUnreadable, errIndexUnreadable
	}
	encoded, err := gunzipIndex(raw)
	if err != nil {
		return searchindex.New(model), loadUnreadable, nil
	}
	ix, err := searchindex.Decode(encoded)
	if err != nil || !ix.Compatible(model) {
		return searchindex.New(model), loadUnreadable, nil
	}
	s.cache.put(owner, key, ix, len(encoded))
	return ix, loadOK, nil
}

func gunzipIndex(compressed []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	out, err := io.ReadAll(io.LimitReader(zr, maxDecodedBytes+1))
	if err != nil {
		return nil, err
	}
	if len(out) > maxDecodedBytes {
		return nil, errors.New("search: the index exceeds the decoded size limit")
	}
	return out, nil
}

// save writes the index, gzipped, to the bucket under objectKey. A saved
// index is cached; a failed save drops the cache, so the next operation
// reloads the last stored state rather than serve a change that was
// never stored.
func (s *Service) save(ctx context.Context, owner, objectKey string, indexKey []byte, ix *searchindex.Index) error {
	encoded, err := ix.Encode()
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(encoded); err != nil {
		_ = zw.Close()
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	if err := s.Buckets.Put(ctx, owner, objectKey, buf.Bytes(), indexKey); err != nil {
		s.cache.drop(owner)
		return errIndexNotStored
	}
	s.cache.put(owner, cacheKey(indexKey, objectKey), ix, len(encoded))
	return nil
}

// deleteObject drops an index object no publication names, best effort
// and detached from the request: one left behind is also swept later.
func (s *Service) deleteObject(ctx context.Context, owner, objectKey string) {
	if objectKey == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), orphanCleanupTimeout)
	defer cancel()
	_ = s.Buckets.Delete(ctx, owner, objectKey)
}

func newObjectKey(fence int64) (string, error) {
	var suffix [objectKeySuffixBytes]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%d-%s", objectKeyPrefix, fence, hex.EncodeToString(suffix[:])), nil
}

// publish stores the index as a new object and names it in the control
// plane's publication record at the generation state was read at, as
// covering the source through covered. The object the record named
// before is dropped. A publication another replica's got ahead of is a
// *controlplane.SearchIndexConflictError, with this one's object dropped;
// one whose answer was lost is read back, so a publication that landed
// is not taken for a failure.
func (s *Service) publish(ctx context.Context, c caller, state *controlplane.SearchIndexState, covered int64, incomplete bool, ix *searchindex.Index) (*controlplane.SearchIndexState, error) {
	owner := c.owner()
	objectKey, err := newObjectKey(state.FenceGeneration)
	if err != nil {
		return nil, err
	}
	if err := s.save(ctx, owner, objectKey, c.indexKey, ix); err != nil {
		return nil, err
	}
	published, err := s.Controlplane.PublishSearchIndex(ctx, controlplane.PublishSearchIndexRequest{
		JWT:                   string(c.p.JWT),
		ClerkUserID:           owner,
		ExpectedGeneration:    state.PublicationGeneration,
		ExpectedFence:         state.FenceGeneration,
		CoveredSourceRevision: covered,
		ObjectKey:             objectKey,
		KeyID:                 c.kid,
		Model:                 s.Embedder.Model(),
		Incomplete:            incomplete,
	})
	if err != nil {
		s.cache.drop(owner)
		var conflict *controlplane.SearchIndexConflictError
		if errors.As(err, &conflict) {
			s.deleteObject(ctx, owner, objectKey)
			return nil, err
		}
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconcileTimeout)
		defer cancel()
		if current, reconcileErr := s.Controlplane.GetSearchIndexState(reconcileCtx, string(c.p.JWT), owner); reconcileErr == nil {
			if current.ObjectKey == objectKey {
				s.deleteObject(ctx, owner, state.ObjectKey)
				return current, nil
			}
			s.deleteObject(ctx, owner, objectKey)
		}
		return nil, store.FromControlplane(err)
	}
	s.deleteObject(ctx, owner, state.ObjectKey)
	return published, nil
}

// isConflict is whether a publication lost to another replica's, which
// a caller tries again from that replica's state.
func isConflict(err error) bool {
	var conflict *controlplane.SearchIndexConflictError
	return errors.As(err, &conflict)
}

// publication is the control plane's record of the caller's index, and
// which object it names for this caller's key and model: none when the
// record names an index under another key or model, which this caller
// cannot read.
func (s *Service) publication(ctx context.Context, c caller) (*controlplane.SearchIndexState, string, error) {
	state, err := s.Controlplane.GetSearchIndexState(ctx, string(c.p.JWT), c.owner())
	if err != nil {
		return nil, "", store.FromControlplane(err)
	}
	return state, s.readableObject(state, c), nil
}

// readableObject is the published object when it is sealed under the
// caller's key and built with the current model.
func (s *Service) readableObject(state *controlplane.SearchIndexState, c caller) string {
	if state.ObjectKey != "" && (state.KeyID != c.kid || state.Model != s.Embedder.Model()) {
		return ""
	}
	return state.ObjectKey
}
