package search

import (
	"container/list"
	"crypto/sha256"
	"sync"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

// cacheBudgetBytes bounds the decoded indexes kept in memory, counted by
// the encoded size of each, which the decoded structures exceed by a
// small factor. An index is a few megabytes, so every active caller
// stays cached and eviction only sets in under extreme aggregate load.
const cacheBudgetBytes = 4 << 30

// indexCache keeps decoded indexes between requests, so the hot path
// skips the bucket read, the decompression and the decode, evicting the
// least recently used under a byte budget.
//
// Index contents (tokens and quantized vectors derived from chat text,
// never the text or key material) stay in the same attested memory that
// holds chat plaintext during a request, capped by the budget and gone
// when the process ends. Storage stays the source of truth: every change
// is stored before it is served.
//
// An entry is bound to a digest of the object key the publication names
// and the key it is sealed with, so another replica's publication or a
// key change misses and reloads from storage.
//
// An entry this process published as complete keeps the generation it
// was published at: the control plane marks a publication incomplete
// when another write lands while it is made, and the update that brings
// that write from here then knows the index was complete before it.
type indexCache struct {
	mu      sync.Mutex
	budget  int
	total   int
	lru     *list.List // front is the most recently used
	byOwner map[string]*list.Element
}

type cacheEntry struct {
	owner string
	ix    *searchindex.Index
	key   [sha256.Size]byte
	size  int
	// completeAt is the generation this process published the object at
	// as complete, or 0.
	completeAt int64
}

func newIndexCache(budget int) *indexCache {
	return &indexCache{budget: budget, lru: list.New(), byOwner: map[string]*list.Element{}}
}

// cacheKey is what a cached index is bound to: the key it is sealed with
// and the object it was read from.
func cacheKey(indexKey []byte, objectKey string) [sha256.Size]byte {
	digest := sha256.New()
	digest.Write(indexKey)
	digest.Write([]byte{0})
	digest.Write([]byte(objectKey))
	var out [sha256.Size]byte
	copy(out[:], digest.Sum(nil))
	return out
}

// get is the owner's cached index when it is bound to key; one bound to
// another key is dropped.
func (c *indexCache) get(owner string, key [sha256.Size]byte) (*searchindex.Index, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byOwner[owner]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*cacheEntry)
	if entry.key != key {
		c.removeLocked(el)
		return nil, false
	}
	c.lru.MoveToFront(el)
	return entry.ix, true
}

// put caches the owner's index as read from, or written to, the object
// key names; one larger than the whole budget is not kept.
func (c *indexCache) put(owner string, key [sha256.Size]byte, ix *searchindex.Index, size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byOwner[owner]; ok {
		c.removeLocked(el)
	}
	if size > c.budget {
		return
	}
	c.byOwner[owner] = c.lru.PushFront(&cacheEntry{owner: owner, ix: ix, key: key, size: size})
	c.total += size
	for c.total > c.budget {
		c.removeLocked(c.lru.Back())
	}
}

// publishedComplete records that this process published the owner's
// cached object, bound to key, as complete at generation.
func (c *indexCache) publishedComplete(owner string, key [sha256.Size]byte, generation int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byOwner[owner]; ok && el.Value.(*cacheEntry).key == key {
		el.Value.(*cacheEntry).completeAt = generation
	}
}

// completeAt is whether this process published the owner's cached object,
// bound to key, as complete at generation.
func (c *indexCache) completeAt(owner string, key [sha256.Size]byte, generation int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byOwner[owner]
	return ok && generation > 0 && el.Value.(*cacheEntry).key == key && el.Value.(*cacheEntry).completeAt == generation
}

func (c *indexCache) drop(owner string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byOwner[owner]; ok {
		c.removeLocked(el)
	}
}

func (c *indexCache) removeLocked(el *list.Element) {
	entry := c.lru.Remove(el).(*cacheEntry)
	delete(c.byOwner, entry.owner)
	c.total -= entry.size
}
