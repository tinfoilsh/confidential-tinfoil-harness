package search

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"hash/fnv"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets/bucketstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

const (
	testAttachmentsBucket = "test-attachments-bucket"
	testSearchBucket      = "test-search-bucket"
	testSecret            = "test-enclave-secret"
)

// stubEmbedder makes deterministic bag-of-words vectors, with a small
// synonym table so "animal" lands near "duck" and "dog" as a real model
// would, and no network.
type stubEmbedder struct {
	mu    sync.Mutex
	model string
	fail  error
	calls int
}

const stubDims = 16

var stubSynonyms = map[string]string{"duck": "animal", "dog": "animal"}

func (e *stubEmbedder) Configured() bool { return true }

func (e *stubEmbedder) Model() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.model
}

func (e *stubEmbedder) setModel(model string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.model = model
}

func (e *stubEmbedder) setFail(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fail = err
}

func (e *stubEmbedder) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (e *stubEmbedder) Embed(_ context.Context, inputs []string) ([][]float32, error) {
	e.mu.Lock()
	e.calls++
	fail := e.fail
	e.mu.Unlock()
	if fail != nil {
		return nil, fail
	}
	out := make([][]float32, len(inputs))
	for i, in := range inputs {
		out[i] = stubVector(strings.TrimPrefix(strings.TrimPrefix(in, docPrefix), queryPrefix))
	}
	return out, nil
}

func stubVector(text string) []float32 {
	vec := make([]float32, stubDims)
	for _, tok := range strings.Fields(strings.ToLower(text)) {
		tok = strings.Trim(tok, ".,!?;:")
		if canon, ok := stubSynonyms[tok]; ok {
			tok = canon
		}
		h := fnv.New64a()
		h.Write([]byte(tok))
		seed := h.Sum64()
		for d := range vec {
			seed = seed*6364136223846793005 + 1442695040888963407
			vec[d] += float32(int64(seed>>33)%1000) / 1000
		}
	}
	var norm float64
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	if norm > 0 {
		n := float32(math.Sqrt(norm))
		for d := range vec {
			vec[d] /= n
		}
	}
	return vec
}

// blockingEmbedder holds every embedding until released, so a test can
// act while one is under way.
type blockingEmbedder struct {
	base    *stubEmbedder
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingEmbedder(base *stubEmbedder) *blockingEmbedder {
	return &blockingEmbedder{base: base, started: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingEmbedder) Configured() bool { return true }
func (b *blockingEmbedder) Model() string    { return b.base.Model() }
func (b *blockingEmbedder) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.base.Embed(ctx, inputs)
}

// searchHarness is a Service over a stub of the control plane, a bucket
// for attachments and one for the index, the stub embedder, and real
// rows over the same control plane, for one caller with one key.
type searchHarness struct {
	cp        *cpstub.Store
	bk        *bucketstub.Store // the search index bucket
	searchURL string
	rows      *store.Rows
	store     *store.Store
	service   *Service
	embedder  *stubEmbedder
	p         *auth.Principal
	key       auth.ContentKey
	kid       string
}

// bucketServer serves a bucket stub at the paths the buckets client uses.
func bucketServer(t *testing.T, bucket string, bk *bucketstub.Store) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/"+bucket+"/{key}", bk.Handle)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func newSearchHarness(t *testing.T) *searchHarness {
	t.Helper()
	cp := cpstub.New()
	cp.Secret = testSecret
	cpServer := httptest.NewServer(cp)
	t.Cleanup(cpServer.Close)
	attachments := bucketServer(t, testAttachmentsBucket, bucketstub.NewStore())
	searchBk := bucketstub.NewStore()
	searchServer := bucketServer(t, testSearchBucket, searchBk)
	var key auth.ContentKey
	rand.Read(key[:])
	kid := keyID(key)
	// The control plane publishes an index only under the registered key.
	cp.CurrentKeyID = kid
	cpClient := controlplane.NewClient(cpServer.URL, nil, controlplane.WithServiceSecret(testSecret))
	rows := &store.Rows{Controlplane: cpClient, Buckets: buckets.NewClient(attachments.URL, testAttachmentsBucket, nil)}
	embedder := &stubEmbedder{model: "test-embed"}
	service := New(cpClient, buckets.NewClient(searchServer.URL, testSearchBucket, nil), embedder, rows)
	rows.Index = service
	st := &store.Store{Rows: rows, Controlplane: cpClient, Buckets: rows.Buckets, Search: service}
	return &searchHarness{cp: cp, bk: searchBk, searchURL: searchServer.URL, rows: rows, store: st, service: service, embedder: embedder, p: &auth.Principal{ID: "user_abc", JWT: "clerk-abc"}, key: key, kid: kid}
}

func chatJSON(t *testing.T, id, title string, contents ...string) []byte {
	t.Helper()
	messages := make([]map[string]any, 0, len(contents))
	for _, content := range contents {
		messages = append(messages, map[string]any{"role": "user", "content": content})
	}
	out, err := json.Marshal(map[string]any{"id": id, "title": title, "messages": messages})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// push stores a chat, which indexes it inline, and is its revision.
func (h *searchHarness) push(t *testing.T, id, title, content string) string {
	t.Helper()
	return h.pushRow(t, &domain.StoredRow{ID: id, Data: chatJSON(t, id, title, content)})
}

func (h *searchHarness) pushRow(t *testing.T, row *domain.StoredRow) string {
	t.Helper()
	etag, err := h.rows.Push(context.Background(), h.p, h.key, "chat", row, "nonce-"+row.ID+"-"+row.ETag)
	if err != nil {
		t.Fatalf("push %s: %v", row.ID, err)
	}
	return etag
}

func (h *searchHarness) remove(t *testing.T, id string) {
	t.Helper()
	if err := h.rows.Remove(context.Background(), h.p, h.key, "chat", id); err != nil {
		t.Fatalf("remove %s: %v", id, err)
	}
}

func (h *searchHarness) query(t *testing.T, query string) (ids []string, rebuildNeeded bool) {
	t.Helper()
	ids, rebuildNeeded, err := h.service.Query(context.Background(), h.p, h.key, query, 0)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return ids, rebuildNeeded
}

func (h *searchHarness) caller(t *testing.T) caller {
	t.Helper()
	c, err := h.service.callerOf(h.p, h.key)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (h *searchHarness) sourceRevision() int64 {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	return h.cp.SourceRevision
}

func (h *searchHarness) publication() controlplane.SearchIndexState {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	state := h.cp.SearchState
	state.SourceRevision = h.cp.SourceRevision
	state.Incomplete = state.PublicationIncomplete || state.PublishedSourceRevision < state.SourceRevision
	return state
}

func (h *searchHarness) objectKey(t *testing.T) string {
	t.Helper()
	key := h.publication().ObjectKey
	if key == "" {
		t.Fatal("no search index is published")
	}
	return key
}

// index is the caller's index as stored, read past the cache.
func (h *searchHarness) index(t *testing.T) *searchindex.Index {
	t.Helper()
	c := h.caller(t)
	defer c.zero()
	raw, err := h.service.Buckets.Get(context.Background(), h.p.ID, h.objectKey(t), c.indexKey)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := gunzipIndex(raw)
	if err != nil {
		t.Fatal(err)
	}
	ix, err := searchindex.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return ix
}

// loseIndex drops the stored index object and the cache, as a restart
// after a loss in storage would.
func (h *searchHarness) loseIndex(t *testing.T) {
	t.Helper()
	if err := h.service.Buckets.Delete(context.Background(), h.p.ID, h.objectKey(t)); err != nil {
		t.Fatal(err)
	}
	h.service.cache.drop(h.p.ID)
}

// lastJob is the caller's last rebuild, or nil.
func (h *searchHarness) lastJob() *jobs.Job[RebuildReport] {
	h.service.mu.Lock()
	defer h.service.mu.Unlock()
	return h.service.started[h.p.ID].job
}

// rebuildEnded waits for the caller's rebuild to end and is its status.
func (h *searchHarness) rebuildEnded(t *testing.T) jobs.Status[RebuildReport] {
	t.Helper()
	job, started := h.service.Rebuilds.Start(context.Background(), h.p.ID, func(context.Context, func(RebuildReport)) (RebuildReport, error) { return RebuildReport{}, nil })
	if started {
		t.Fatal("no rebuild was running for the caller")
	}
	<-job.Done()
	return job.Status()
}

// rebuild runs a rebuild to its end and is its status.
func (h *searchHarness) rebuild(t *testing.T) jobs.Status[RebuildReport] {
	t.Helper()
	if _, err := h.service.Rebuild(context.Background(), h.p, h.key); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	return h.rebuildEnded(t)
}

func codeOf(err error) string {
	if err == nil {
		return ""
	}
	return api.AsError(err).Code
}
