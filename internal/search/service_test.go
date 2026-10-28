package search

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets/bucketstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

func TestAStoredChatIsFoundUntilItIsRemoved(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.push(t, "chat_duck", "Pond visit", "I saw a duck at the pond today")
	h.push(t, "chat_tax", "Paperwork", "filing my tax return before the deadline")

	objectKey := h.objectKey(t)
	item, ok := h.bk.Item(objectKey)
	if !ok || len(item.Value) < 2 || item.Value[0] != 0x1f || item.Value[1] != 0x8b {
		t.Fatalf("the index object %s is not stored gzipped in the search bucket: present=%v", objectKey, ok)
	}
	if ids, rebuildNeeded := h.query(t, "animal"); rebuildNeeded || len(ids) == 0 || ids[0] != "chat_duck" {
		t.Fatalf("query for a meaning = %v (rebuild %v), want the duck chat first", ids, rebuildNeeded)
	}
	if ids, _ := h.query(t, "tax deadline"); len(ids) == 0 || ids[0] != "chat_tax" {
		t.Fatalf("query for a keyword = %v, want the tax chat first", ids)
	}
	h.remove(t, "chat_duck")
	if ids, rebuildNeeded := h.query(t, "duck"); rebuildNeeded || slices.Contains(ids, "chat_duck") {
		t.Fatalf("query after the chat was removed = %v (rebuild %v), want it gone", ids, rebuildNeeded)
	}
	// A removal that reaches the index after the chat was made again
	// under its id leaves the new chat indexed.
	first := h.push(t, "chat_same", "Old", "a goose honked")
	h.pushRow(t, &domain.StoredRow{ID: "chat_same", ETag: first, Data: chatJSON(t, "chat_same", "New", "a heron flew past")})
	c := h.caller(t)
	defer c.zero()
	if err := h.service.removeChat(ctx, c, "chat_same", 0); err != nil {
		t.Fatal(err)
	}
	if ids, _ := h.query(t, "heron"); len(ids) == 0 || ids[0] != "chat_same" {
		t.Fatalf("query after a late removal of a remade chat = %v, want the chat first", ids)
	}
	// A removal of a chat already gone still drops its entry, as after
	// a delete that got no further than the row.
	if err := h.service.indexChat(ctx, c, "chat_ghost", chatJSON(t, "chat_ghost", "", "a left over ghost"), "ghost", time.Now(), 0, nil); err != nil {
		t.Fatal(err)
	}
	h.remove(t, "chat_ghost")
	if _, ok := h.index(t).Chats["chat_ghost"]; ok {
		t.Fatal("the entry of a chat already gone survived its removal")
	}
}

// A deletion that reaches the index after a chat was made again under its
// id, by a write it never saw, leaves the newer entry; one from after that
// write removes it.
func TestADeletionOlderThanTheEntryKeepsIt(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.push(t, "chat_other", "Elsewhere", "a duck swam by")
	h.push(t, "chat_same", "Remade", "a heron flew past")
	entry := h.index(t).Chats["chat_same"]
	if entry.SourceRevision < 2 {
		t.Fatalf("the entry's source revision is %d; the test needs an earlier revision to delete at", entry.SourceRevision)
	}
	// The row is gone from the control plane, as after a delete on
	// another replica, while the index holds the entry a later write made.
	h.cp.Mu.Lock()
	delete(h.cp.Blobs, "chat/chat_same")
	h.cp.SourceRevision++
	h.cp.Mu.Unlock()
	c := h.caller(t)
	defer c.zero()
	if err := h.service.removeChat(ctx, c, "chat_same", entry.SourceRevision-1); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.index(t).Chats["chat_same"]; !ok {
		t.Fatal("a deletion older than the entry removed it")
	}
	if err := h.service.removeChat(ctx, c, "chat_same", entry.SourceRevision+1); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.index(t).Chats["chat_same"]; ok {
		t.Fatal("a deletion newer than the entry left it")
	}
}

func TestAStaleWriteDoesNotReplaceANewerEntry(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	c := h.caller(t)
	defer c.zero()
	index := func(content, etag string, at time.Time) {
		t.Helper()
		if err := h.service.indexChat(ctx, c, "chat_a", chatJSON(t, "chat_a", "", content), etag, at, 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	index("a goose on the lake", "5", time.Now())
	// An older revision with a later write time is a straggler.
	index("a duck on the pond", "3", time.Now().Add(time.Hour))
	ix := h.index(t)
	if found := ix.Search(nil, []string{"goose"}, 5); len(found) != 1 {
		t.Fatalf("the newer entry was replaced by an older revision: %+v", found)
	}
	if found := ix.Search(nil, []string{"duck"}, 5); len(found) != 0 {
		t.Fatalf("the older revision was indexed: %+v", found)
	}
	// A newer revision with an earlier write time is applied.
	index("a heron flew past", "7", time.Now().Add(-time.Hour))
	if found := h.index(t).Search(nil, []string{"heron"}, 5); len(found) != 1 {
		t.Fatalf("the newer revision was not indexed: %+v", found)
	}
	// Without revisions to compare, the write time orders entries.
	index("an owl at dusk", "later", time.Now().Add(time.Hour))
	index("a swan at dawn", "latest", time.Now())
	if found := h.index(t).Search(nil, []string{"owl"}, 5); len(found) != 1 {
		t.Fatalf("an entry written later was replaced by an earlier write: %+v", found)
	}
}

func TestAnIndexUnderAnotherKeyOrModelIsNotServed(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.push(t, "chat_duck", "Pond", "a duck swam by")

	var other [32]byte
	other[0] = 1
	for range 2 {
		ids, rebuildNeeded, err := h.service.Query(ctx, h.p, other, "duck", 0)
		if err != nil || !rebuildNeeded || len(ids) != 0 {
			t.Fatalf("query under another key = %v (rebuild %v, %v), want nothing and a rebuild", ids, rebuildNeeded, err)
		}
		// The other key's query did not poison what the right one sees.
		if ids, rebuildNeeded := h.query(t, "duck"); rebuildNeeded || !slices.Equal(ids, []string{"chat_duck"}) {
			t.Fatalf("query under the right key = %v (rebuild %v), want the chat", ids, rebuildNeeded)
		}
	}

	h.embedder.setModel("test-embed-v2")
	if ids, rebuildNeeded := h.query(t, "duck"); !rebuildNeeded || len(ids) != 0 {
		t.Fatalf("query after a model change = %v (rebuild %v), want nothing and a rebuild", ids, rebuildNeeded)
	}
}

func TestAChatIndexedOverAnUnreadableIndexReportsARebuildUntilOneRuns(t *testing.T) {
	h := newSearchHarness(t)
	h.push(t, "chat_duck", "Pond", "a duck swam by")
	c := h.caller(t)
	defer c.zero()
	if err := h.service.Buckets.Put(context.Background(), h.p.ID, h.objectKey(t), []byte("garbage"), c.indexKey); err != nil {
		t.Fatal(err)
	}
	h.service.cache.drop(h.p.ID)

	h.push(t, "chat_tax", "Paperwork", "tax return time")
	if ids, rebuildNeeded := h.query(t, "tax"); !rebuildNeeded || !slices.Equal(ids, []string{"chat_tax"}) {
		t.Fatalf("query over an index rebuilt from a chat = %v (rebuild %v), want the chat and a rebuild", ids, rebuildNeeded)
	}
	if status := h.rebuild(t); status.State != jobs.Complete || status.Report.Partial || status.Report.TotalIndexed != 2 {
		t.Fatalf("rebuild = %+v, want complete over both chats", status)
	}
	if ids, rebuildNeeded := h.query(t, "duck"); rebuildNeeded || len(ids) != 2 || ids[0] != "chat_duck" {
		t.Fatalf("query after the rebuild = %v (rebuild %v), want the chat the loss had hidden first", ids, rebuildNeeded)
	}
}

func TestARebuildDropsDeletedChatsAndKeepsEntriesWrittenMeanwhile(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	c := h.caller(t)
	defer c.zero()
	h.push(t, "chat_old", "Pond", "a duck swam by")
	// In the index alone, its chat gone from the control plane.
	if err := h.service.indexChat(ctx, c, "chat_deleted", chatJSON(t, "chat_deleted", "", "left over ghost"), "g1", time.Now(), 0, nil); err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now().UTC()
	// Written after the rebuild began, by a chat its listing does not see.
	if err := h.service.indexChat(ctx, c, "chat_racer", chatJSON(t, "chat_racer", "", "zebra crossing photos"), "g2", time.Now(), 0, nil); err != nil {
		t.Fatal(err)
	}
	// Updated inline during the rebuild: the index has the newer text,
	// the listing the older.
	if err := h.service.indexChat(ctx, c, "chat_old", chatJSON(t, "chat_old", "", "actually a heron flew past"), "g3", time.Now(), 0, nil); err != nil {
		t.Fatal(err)
	}

	c = c.detached(h.key)
	defer c.zero()
	for cursor := ""; ; {
		page, err := h.service.rebuildPage(ctx, c, cursor, startedAt, h.sourceRevision(), false, true)
		if err != nil {
			t.Fatal(err)
		}
		if page.done {
			break
		}
		cursor = page.next
	}
	ix := h.index(t)
	if ix.Incomplete {
		t.Fatal("a drained rebuild left the index incomplete")
	}
	if _, ok := ix.Chats["chat_deleted"]; ok {
		t.Fatal("the rebuild kept an entry whose chat is gone")
	}
	if found := ix.Search(nil, []string{"zebra"}, 5); len(found) != 1 || found[0].ID != "chat_racer" {
		t.Fatalf("the rebuild dropped an entry written meanwhile: %+v", found)
	}
	if found := ix.Search(nil, []string{"heron"}, 5); len(found) != 1 || found[0].ID != "chat_old" {
		t.Fatalf("the rebuild replaced a newer entry with the older chat: %+v", found)
	}
}

func TestARebuildResumesFromItsCheckpoint(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	const chats = rebuildPage + 4
	for i := range chats {
		h.push(t, fmt.Sprintf("chat_resume_%02d", i), "Resume", fmt.Sprintf("checkpoint topic %d", i))
	}
	h.loseIndex(t)

	c := h.caller(t).detached(h.key)
	defer c.zero()
	page, err := h.service.rebuildPage(ctx, c, "", time.Now().UTC(), h.sourceRevision(), false, true)
	if err != nil {
		t.Fatal(err)
	}
	if page.done || page.next == "" || page.total != rebuildPage {
		t.Fatalf("the first page = %+v, want one that checkpoints its cursor", page)
	}
	if checkpoint := h.index(t).Reindex; checkpoint == nil || checkpoint.NextCursor != page.next {
		t.Fatalf("stored checkpoint = %+v, want the cursor %q", checkpoint, page.next)
	}
	calls := h.embedder.callCount()

	status := h.rebuild(t)
	if status.State != jobs.Complete || status.Report.Partial || status.Report.Failed != 0 || status.Report.TotalIndexed != chats {
		t.Fatalf("the resumed rebuild = %+v, want complete over every chat", status)
	}
	if made := h.embedder.callCount() - calls; made != 1 {
		t.Fatalf("the resumed rebuild embedded %d times, want once for the page left", made)
	}
	if ix := h.index(t); ix.Incomplete || ix.Reindex != nil || len(ix.Chats) != chats {
		t.Fatalf("the index after the resumed rebuild: incomplete=%v checkpoint=%+v chats=%d", ix.Incomplete, ix.Reindex, len(ix.Chats))
	}
}

func TestARebuildIsCompleteOnlyWhenEveryPageSucceeded(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.push(t, "chat_ok", "Pond", "a duck swam by")
	h.push(t, "chat_bad", "Park", "a goose flew past")
	h.cp.Mu.Lock()
	sealed := h.cp.Blobs["chat/chat_bad"].Body
	h.cp.Blobs["chat/chat_bad"].Body = []byte("malformed")
	h.cp.Mu.Unlock()
	h.loseIndex(t)

	if _, err := h.service.Rebuild(ctx, h.p, h.key); err != nil {
		t.Fatal(err)
	}
	status := h.rebuildEnded(t)
	if status.State != jobs.Failed || !errors.Is(status.Err, errRebuildIncomplete) || !status.Report.Partial || status.Report.Failed != 1 || status.Report.TotalIndexed != 1 {
		t.Fatalf("a rebuild over a chat it cannot read = %+v, want failed as incomplete with the other chat indexed", status)
	}
	if ids, rebuildNeeded := h.query(t, "duck"); !rebuildNeeded || !slices.Equal(ids, []string{"chat_ok"}) {
		t.Fatalf("query after the failed rebuild = %v (rebuild %v), want the chat read and a rebuild", ids, rebuildNeeded)
	}
	// A rebuild that failed is reported once, to the next request, and
	// the one after starts over.
	h.cp.Mu.Lock()
	h.cp.Blobs["chat/chat_bad"].Body = sealed
	h.cp.Mu.Unlock()
	if indexing, err := h.service.Rebuild(ctx, h.p, h.key); codeOf(err) != "SEARCH_INDEX_FAILED" {
		t.Fatalf("the next rebuild request = (%v, %v), want the failure reported", indexing, err)
	}
	if status := h.rebuild(t); status.State != jobs.Complete || status.Report.Partial || status.Report.Failed != 0 || status.Report.TotalIndexed != 2 {
		t.Fatalf("the rebuild after the chat was repaired = %+v, want complete over both", status)
	}
	if ids, rebuildNeeded := h.query(t, "goose"); rebuildNeeded || len(ids) != 2 || ids[0] != "chat_bad" {
		t.Fatalf("query after the repair = %v (rebuild %v), want the repaired chat first", ids, rebuildNeeded)
	}
	// A final page is not complete over a failure of an earlier one.
	c := h.caller(t).detached(h.key)
	defer c.zero()
	if page, err := h.service.rebuildPage(ctx, c, "", time.Now().UTC(), h.sourceRevision(), true, true); err != nil || !page.done || !page.partial {
		t.Fatalf("a final page after an earlier failure = %+v (%v), want done and partial", page, err)
	}
	if _, rebuildNeeded := h.query(t, "duck"); !rebuildNeeded {
		t.Fatal("a final page cleared an earlier page's failure")
	}
}

func TestARebuildUnderAnotherKeyReplacesTheRunningOne(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.push(t, "chat_duck", "Pond", "a duck swam by")
	h.loseIndex(t)
	blocker := newBlockingEmbedder(h.embedder)
	h.service.Embedder = blocker

	if indexing, err := h.service.Rebuild(ctx, h.p, h.key); err != nil || !indexing {
		t.Fatalf("rebuild = (%v, %v), want one under way", indexing, err)
	}
	first := h.lastJob()
	select {
	case <-blocker.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the rebuild did not reach embedding")
	}
	if _, err := h.service.Rebuild(ctx, h.p, h.key); err != nil {
		t.Fatal(err)
	}
	if h.lastJob() != first {
		t.Fatal("a rebuild under the same key did not join the running one")
	}

	// The other key becomes the registered one, as after a recovery.
	var other [32]byte
	other[0] = 1
	h.cp.Mu.Lock()
	h.cp.CurrentKeyID = keyID(other)
	h.cp.Mu.Unlock()
	if _, err := h.service.Rebuild(ctx, h.p, other); err != nil {
		t.Fatal(err)
	}
	second := h.lastJob()
	if second == first {
		t.Fatal("a rebuild under another key joined the running one")
	}
	select {
	case <-first.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the replaced rebuild was not stopped")
	}
	if status := first.Status(); status.State != jobs.Complete || !status.Report.Partial || status.Report.Indexed != 0 {
		t.Fatalf("the replaced rebuild = %+v, want it stopped short with nothing indexed", status)
	}
	close(blocker.release)
	<-second.Done()
	// The other key opens no chat, so its rebuild left the chat out.
	if status := second.Status(); status.State != jobs.Failed || status.Report.Failed != 1 {
		t.Fatalf("the rebuild under the other key = %+v, want it to have left the chat out", status)
	}
}

func TestAPublicationConflictIsRetriedFromTheWinner(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.push(t, "chat_a", "Alpha", "alpha term")
	initial := h.publication()
	c := h.caller(t)
	defer c.zero()

	// Another replica publishes an index with one more chat just before
	// this one does.
	winner := h.index(t)
	if err := winner.Upsert("chat_b", searchindex.Entry{ETag: "1", SourceRevision: 2, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}, searchindex.Tokenize("bravo term")); err != nil {
		t.Fatal(err)
	}
	winnerKey, err := newObjectKey(initial.FenceGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.service.save(ctx, h.p.ID, winnerKey, c.indexKey, winner); err != nil {
		t.Fatal(err)
	}
	h.cp.Mu.Lock()
	h.cp.SourceRevision = 3
	h.cp.SearchConflict = func(state controlplane.SearchIndexState) controlplane.SearchIndexState {
		state.PublicationGeneration++
		state.PublishedSourceRevision = 2
		state.ObjectKey, state.KeyID, state.Model = winnerKey, initial.KeyID, initial.Model
		state.PublicationIncomplete = false
		return state
	}
	h.cp.Mu.Unlock()

	if err := h.service.indexChat(ctx, c, "chat_c", chatJSON(t, "chat_c", "", "charlie term"), "1", time.Now(), 3, nil); err != nil {
		t.Fatal(err)
	}
	ix := h.index(t)
	for _, id := range []string{"chat_a", "chat_b", "chat_c"} {
		if _, ok := ix.Chats[id]; !ok {
			t.Fatalf("the publication retried after the conflict lost %s", id)
		}
	}
	if state := h.publication(); state.PublishedSourceRevision != 3 || state.Incomplete {
		t.Fatalf("publication = %+v, want it complete through revision 3", state)
	}
	// The object that lost the publication and the winner's it replaced
	// are gone; the one from before the winner is that replica's to drop.
	tokens := h.bk.Tokens()
	slices.Sort(tokens)
	if want := []string{initial.ObjectKey, h.objectKey(t)}; slices.Compare(tokens, slices.Sorted(slices.Values(want))) != 0 {
		t.Fatalf("the search bucket holds %v, want %v", tokens, want)
	}
}

// bucketProxy is a search bucket whose PUTs or DELETEs fail while told to.
func (h *searchHarness) bucketProxy(t *testing.T, failing *atomic.Bool, method string) {
	t.Helper()
	target, err := url.Parse(h.searchURL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failing.Load() && r.Method == method {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	h.service.Buckets = buckets.NewClient(server.URL, testSearchBucket, nil)
}

func TestAChangeWhoseSaveFailedIsNotServedFromTheCache(t *testing.T) {
	h := newSearchHarness(t)
	var failing atomic.Bool
	h.bucketProxy(t, &failing, http.MethodPut)
	h.push(t, "chat_a", "Pond", "a duck swam by")

	failing.Store(true)
	etag, err := h.rows.Push(context.Background(), h.p, h.key, "chat", &domain.StoredRow{ID: "chat_b", Data: chatJSON(t, "chat_b", "Lake", "a goose honked")}, "nonce-b")
	if err != nil || etag == "" {
		t.Fatalf("a push whose index save failed = (%q, %v), want the chat stored", etag, err)
	}
	failing.Store(false)

	ids, rebuildNeeded := h.query(t, "goose")
	if slices.Contains(ids, "chat_b") {
		t.Fatalf("query served a change that was never stored: %v", ids)
	}
	if !rebuildNeeded {
		t.Fatal("the failed save did not leave the index incomplete")
	}
	if ids, _ := h.query(t, "duck"); !slices.Equal(ids, []string{"chat_a"}) {
		t.Fatalf("the stored entry was lost: %v", ids)
	}
	if ix := h.index(t); len(ix.Chats) != 1 {
		t.Fatalf("the stored index holds %d chats, want the one whose save landed", len(ix.Chats))
	}
}

// panickingEmbedder is a defect in the search pipeline, which runs after
// a chat's write has landed.
type panickingEmbedder struct{ panicOnModel bool }

func (panickingEmbedder) Configured() bool { return true }
func (e panickingEmbedder) Model() string {
	if e.panicOnModel {
		panic("embedder defect")
	}
	return "panic-embed"
}
func (panickingEmbedder) Embed(context.Context, []string) ([][]float32, error) {
	panic("embedder defect")
}

func TestAPanicWhileIndexingDoesNotFailTheStore(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.service.Embedder = panickingEmbedder{}
	etag, err := h.rows.Push(ctx, h.p, h.key, "chat", &domain.StoredRow{ID: "chat_1", Data: chatJSON(t, "chat_1", "T", "hello")}, "nonce-1")
	if err != nil || etag != "1" {
		t.Fatalf("push = (%q, %v), want the chat stored with search degraded", etag, err)
	}
	if err := h.service.ChatStored(ctx, h.p, h.key, "chat_1", chatJSON(t, "chat_1", "T", "hello"), etag, time.Now(), 1); !errors.Is(err, errIndexPanicked) {
		t.Fatalf("indexing = %v, want the panic reported as a failure", err)
	}
	h.cp.Mu.Lock()
	_, stored := h.cp.Blobs["chat/chat_1"]
	h.cp.Mu.Unlock()
	if !stored {
		t.Fatal("the chat was not stored")
	}
	// The removal consults the model while reading the publication.
	h.service.Embedder = panickingEmbedder{panicOnModel: true}
	if err := h.rows.RemoveRevision(ctx, h.p, h.key, "chat_1", etag); err != nil {
		t.Fatalf("remove = %v, want the chat deleted with search degraded", err)
	}
	h.cp.Mu.Lock()
	_, stored = h.cp.Blobs["chat/chat_1"]
	h.cp.Mu.Unlock()
	if stored {
		t.Fatal("the chat was not deleted")
	}
}

func TestTheGateRefusesTheForegroundAtOnceAndGivesBackWhatItTook(t *testing.T) {
	gate := newInferenceGate(inferenceLimits{globalRate: 0, globalBurst: 1, globalConcurrent: 1, callerRate: 0, callerBurst: 1, callerConcurrent: 1})
	ctx := context.Background()
	release, err := gate.acquire(ctx, "winner", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	release()
	// The process's rate is spent; the refused caller keeps their own.
	if _, err := gate.acquire(ctx, "denied", 1, false); codeOf(err) != "RATE_LIMITED" {
		t.Fatalf("acquire over the process's rate = %v, want RATE_LIMITED", err)
	}
	gate.mu.Lock()
	denied := gate.callers["denied"]
	gate.mu.Unlock()
	if tokens := denied.foreground.Tokens(); tokens != 1 {
		t.Fatalf("the refused caller's rate = %v, want the 1 they had", tokens)
	}

	// A call in flight fills the caller's slot: the foreground is refused
	// at once, and background work waits only as long as its context.
	busy := newInferenceGate(inferenceLimits{globalRate: 100, globalBurst: 100, globalConcurrent: 2, callerRate: 100, callerBurst: 100, callerConcurrent: 1})
	release, err = busy.acquire(ctx, "alice", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := busy.acquire(ctx, "alice", 1, false); codeOf(err) != "RATE_LIMITED" {
		t.Fatalf("a foreground acquire with the caller's slot taken = %v, want RATE_LIMITED without waiting", err)
	}
	ended, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := busy.acquire(ended, "alice", 1, true); codeOf(err) != "RATE_LIMITED" {
		t.Fatalf("a background acquire whose context ended = %v, want RATE_LIMITED", err)
	}
	if _, err := busy.acquire(ctx, "bob", 1, false); err != nil {
		t.Fatalf("another caller was refused for the first one's slot: %v", err)
	}
}

func TestAQueryOverTheInferenceLimitIsAnsweredByKeyword(t *testing.T) {
	h := newSearchHarness(t)
	h.push(t, "chat_duck", "Pond", "a duck swam by")
	// A foreground embedding that waited would hang for the whole timeout.
	h.service.timeouts.foreground = time.Minute
	h.service.gate = newInferenceGate(inferenceLimits{globalRate: 100, globalBurst: 100, globalConcurrent: 2, callerRate: 0, callerBurst: 1, callerConcurrent: 1})
	calls := h.embedder.callCount()
	if ids, _ := h.query(t, "duck"); !slices.Equal(ids, []string{"chat_duck"}) {
		t.Fatalf("the first query = %v, want the chat", ids)
	}
	if ids, rebuildNeeded := h.query(t, "duck"); rebuildNeeded || !slices.Equal(ids, []string{"chat_duck"}) {
		t.Fatalf("the query over the limit = %v (rebuild %v), want the chat by keyword", ids, rebuildNeeded)
	}
	if made := h.embedder.callCount() - calls; made != 1 {
		t.Fatalf("the queries embedded %d times, want only the one within the limit", made)
	}
}

func TestTheDeletionWorkerAcknowledgesOnlyWhatItDeleted(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.bk.Put("object-1", bucketstub.Item{Tenant: tenantOf(t, "user-1"), Value: []byte("index")})
	h.bk.Put("object-3", bucketstub.Item{Tenant: tenantOf(t, "user-1"), Value: []byte("index")})
	retired := []controlplane.SearchIndexDeletion{
		{ID: "row-1", ClerkUserID: "user-1", ObjectKey: "object-1"},
		{ID: "row-2", ClerkUserID: "user-1", ObjectKey: "already-missing"},
	}
	h.cp.Mu.Lock()
	h.cp.SearchIndexDeletions = slices.Clone(retired)
	h.cp.Mu.Unlock()
	if count, err := h.service.deleteRetired(ctx); err != nil || count != 2 {
		t.Fatalf("sweep = (%d, %v), want both retired objects acknowledged: the missing one is gone already", count, err)
	}
	if h.bk.Has("object-1") {
		t.Fatal("the retired object was not deleted")
	}
	h.cp.Mu.Lock()
	left := slices.Clone(h.cp.SearchIndexDeletions)
	h.cp.Mu.Unlock()
	if len(left) != 0 {
		t.Fatalf("the control plane still holds %+v after the acknowledgment", left)
	}

	var failing atomic.Bool
	h.bucketProxy(t, &failing, http.MethodDelete)
	failing.Store(true)
	h.cp.Mu.Lock()
	h.cp.SearchIndexDeletions = []controlplane.SearchIndexDeletion{{ID: "row-3", ClerkUserID: "user-1", ObjectKey: "object-3"}}
	h.cp.Mu.Unlock()
	if count, err := h.service.deleteRetired(ctx); err == nil || count != 0 {
		t.Fatalf("sweep with the bucket down = (%d, %v), want nothing acknowledged and the failure", count, err)
	}
	h.cp.Mu.Lock()
	left = slices.Clone(h.cp.SearchIndexDeletions)
	h.cp.Mu.Unlock()
	if len(left) != 1 || left[0].ID != "row-3" || !h.bk.Has("object-3") {
		t.Fatalf("after a failed delete the control plane holds %+v and the object is present=%v, want the retirement kept for a later sweep", left, h.bk.Has("object-3"))
	}
}

func tenantOf(t *testing.T, owner string) string {
	t.Helper()
	tenant, err := buckets.TenantForUser(owner)
	if err != nil {
		t.Fatal(err)
	}
	return tenant
}

func TestAnEmbeddingOutageLeavesEntriesPendingUntilAQueryRepairsThem(t *testing.T) {
	h := newSearchHarness(t)
	h.embedder.setFail(errors.New("embedding backend down"))
	h.push(t, "chat_duck", "Pond", "a duck swam by")
	if ix := h.index(t); !ix.NeedsEmbeddingRepair() || len(ix.Chats["chat_duck"].Vectors) != 0 {
		t.Fatal("a chat indexed during the outage was not left pending an embedding")
	}
	// Found by keyword meanwhile, with no rebuild asked for.
	if ids, rebuildNeeded := h.query(t, "duck"); rebuildNeeded || !slices.Equal(ids, []string{"chat_duck"}) {
		t.Fatalf("query during the outage = %v (rebuild %v), want the chat by keyword", ids, rebuildNeeded)
	}
	if h.lastJob() != nil {
		t.Fatal("a query during the outage started a repair")
	}

	h.embedder.setFail(nil)
	if ids, rebuildNeeded := h.query(t, "duck"); rebuildNeeded || !slices.Equal(ids, []string{"chat_duck"}) {
		t.Fatalf("query after the outage = %v (rebuild %v), want the chat", ids, rebuildNeeded)
	}
	if status := h.rebuildEnded(t); status.State != jobs.Complete || status.Report.Partial || status.Report.Indexed != 1 {
		t.Fatalf("the repair the query started = %+v, want it complete over the pending entry", status)
	}
	if ix := h.index(t); ix.NeedsEmbeddingRepair() || len(ix.Chats["chat_duck"].Vectors) == 0 {
		t.Fatal("the repair did not give the entry its vectors")
	}
	repair := h.lastJob()
	if ids, _ := h.query(t, "duck"); !slices.Equal(ids, []string{"chat_duck"}) {
		t.Fatalf("query after the repair = %v, want the chat", ids)
	}
	if h.lastJob() != repair {
		t.Fatal("a query after the repair started another")
	}
}

// A chat deleted while its entry waited for an embedding is dropped by the
// repair rather than left findable by keyword for good.
func TestARepairDropsAChatDeletedSinceItWasIndexed(t *testing.T) {
	h := newSearchHarness(t)
	h.embedder.setFail(errors.New("embedding backend down"))
	h.push(t, "chat_duck", "Pond", "a duck swam by")
	h.push(t, "chat_goose", "Pond", "a goose honked")
	// The duck chat's row goes while the index still holds its entry, as
	// after a delete whose index update got no further.
	h.cp.Mu.Lock()
	delete(h.cp.Blobs, "chat/chat_duck")
	h.cp.Mu.Unlock()
	h.embedder.setFail(nil)
	if ids, _ := h.query(t, "goose"); !slices.Equal(ids, []string{"chat_goose"}) {
		t.Fatalf("query = %v, want the goose chat", ids)
	}
	if status := h.rebuildEnded(t); status.State != jobs.Complete || status.Report.Partial || status.Report.Indexed != 1 {
		t.Fatalf("the repair = %+v, want it complete over the one chat left", status)
	}
	ix := h.index(t)
	if _, ok := ix.Chats["chat_duck"]; ok || ix.NeedsEmbeddingRepair() {
		t.Fatalf("after the repair the deleted chat is indexed=%v and repair needed=%v", ok, ix.NeedsEmbeddingRepair())
	}
}

func TestAFreshKeyForgetsTheIndexItFencedOff(t *testing.T) {
	h := newSearchHarness(t)
	ctx := context.Background()
	h.push(t, "chat_duck", "Pond", "a duck swam by")
	oldObject := h.objectKey(t)
	var fresh [32]byte
	fresh[0] = 9
	h.cp.Mu.Lock()
	etag := cpstub.FormatETag(h.cp.KeyETag)
	h.cp.Mu.Unlock()
	if _, err := h.store.RegisterKey(ctx, h.p, fresh, etag, "start_fresh", "start-fresh-search", nil); err != nil {
		t.Fatalf("register a fresh key: %v", err)
	}
	if state := h.publication(); state.ObjectKey != "" {
		t.Fatalf("the fresh key left the old index published: %+v", state)
	}
	if h.bk.Has(oldObject) {
		t.Fatal("the fresh key left the old index object in the bucket")
	}
	ids, rebuildNeeded, err := h.service.Query(ctx, h.p, fresh, "duck", 0)
	if err != nil || len(ids) != 0 || !rebuildNeeded {
		t.Fatalf("query under the fresh key = %v (rebuild %v, %v), want nothing and a rebuild", ids, rebuildNeeded, err)
	}
}

func TestChunksCutOnMessageBoundariesAndNeverSplitARune(t *testing.T) {
	// A message larger than a chunk is split with no text lost.
	big := strings.Repeat("a", chunkChars*2+500)
	chunks := chatChunks(chatJSON(t, "x", "", big))
	if len(chunks) != 3 || strings.Join(chunks, "") != big {
		t.Fatalf("an oversized message became %d chunks of %v, want 3 with every byte", len(chunks), chunkLengths(chunks))
	}
	// A message that fits a fresh chunk is not split across the title's.
	nearLimit := strings.Repeat("c", chunkChars-1)
	if chunks := chatChunks(chatJSON(t, "x", "T", nearLimit)); len(chunks) != 2 || chunks[0] != "T" || chunks[1] != nearLimit {
		t.Fatalf("a message that fits a chunk was split: %v", chunkLengths(chunks))
	}
	// The separator counts toward the chunk.
	half := chunkChars / 2
	if chunks := chatChunks(chatJSON(t, "x", strings.Repeat("t", half), strings.Repeat("m", chunkChars-half-1))); len(chunks) != 1 || len(chunks[0]) != chunkChars {
		t.Fatalf("a title and message that fill a chunk with their separator became %v", chunkLengths(chunks))
	}
	// A chunk filled to the byte closes before an oversized part.
	exactTitle, exactBody, overflow := strings.Repeat("t", 38), strings.Repeat("b", chunkChars-39), strings.Repeat("o", chunkChars+571)
	chunks = chatChunks(chatJSON(t, "x", exactTitle, exactBody, overflow))
	if len(chunks) != 3 || chunks[0] != exactTitle+"\n"+exactBody || strings.Join(chunks[1:], "") != overflow {
		t.Fatalf("an exactly filled chunk followed by an oversized part became %v", chunkLengths(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk) > chunkChars {
			t.Fatalf("chunk %d is %d bytes, over the size", i, len(chunk))
		}
	}
	// Multibyte runes are never split.
	euros := strings.Repeat("\u20ac", chunkChars)
	chunks = chatChunks(chatJSON(t, "x", "", euros))
	if strings.Join(chunks, "") != euros {
		t.Fatal("splitting multibyte text lost bytes")
	}
	for i, chunk := range chunks {
		if !utf8.ValidString(chunk) {
			t.Fatalf("chunk %d splits a rune", i)
		}
	}
	// Coverage stops at the cap, keeping the first chunks whole.
	var contents []string
	for i := range searchindex.MaxChunksPerChat + 1 {
		contents = append(contents, strings.Repeat(string(rune('a'+i)), chunkChars))
	}
	chunks = chatChunks(chatJSON(t, "x", "", contents...))
	if len(chunks) != searchindex.MaxChunksPerChat || !slices.Equal(chunks, contents[:searchindex.MaxChunksPerChat]) {
		t.Fatalf("a chat over the cap became %d chunks", len(chunks))
	}
	if chunks := chatChunks(chatJSON(t, "x", "", "   ", "\n\t")); chunks != nil {
		t.Fatalf("whitespace became chunks: %q", chunks)
	}
}

func chunkLengths(chunks []string) []int {
	lengths := make([]int, len(chunks))
	for i, chunk := range chunks {
		lengths[i] = len(chunk)
	}
	return lengths
}

func TestTheCacheIsBoundToTheObjectAndItsKeyAndEvictsTheLeastUsed(t *testing.T) {
	c := newIndexCache(300)
	ix := searchindex.New("m")
	c.put("alice", cacheKey([]byte{1}, "object-1"), ix, 100)
	if got, ok := c.get("alice", cacheKey([]byte{1}, "object-1")); !ok || got != ix {
		t.Fatal("the cached index was not served for its object and key")
	}
	// Another replica's publication, or another key, misses and drops
	// what was cached.
	if _, ok := c.get("alice", cacheKey([]byte{1}, "object-2")); ok {
		t.Fatal("an index was served for another object")
	}
	if _, ok := c.get("alice", cacheKey([]byte{1}, "object-1")); ok {
		t.Fatal("the index bound to the old object survived the miss")
	}
	c.mu.Lock()
	entries, total := len(c.byOwner), c.total
	c.mu.Unlock()
	if entries != 0 || total != 0 {
		t.Fatalf("after the miss the cache accounts for %d entries and %d bytes, want none", entries, total)
	}

	for i := range 3 {
		c.put(fmt.Sprintf("user%d", i), cacheKey(nil, "o"), searchindex.New("m"), 100)
	}
	if _, ok := c.get("user0", cacheKey(nil, "o")); !ok {
		t.Fatal("user0 is not cached")
	}
	c.put("user3", cacheKey(nil, "o"), searchindex.New("m"), 100)
	if _, ok := c.get("user1", cacheKey(nil, "o")); ok {
		t.Fatal("the least recently used index was not evicted for the budget")
	}
	for _, owner := range []string{"user0", "user2", "user3"} {
		if _, ok := c.get(owner, cacheKey(nil, "o")); !ok {
			t.Fatalf("%s was evicted though more recently used", owner)
		}
	}
	// An index over the whole budget is not kept, and replacing one
	// replaces its accounting.
	c.put("user0", cacheKey(nil, "o"), searchindex.New("m"), 301)
	if _, ok := c.get("user0", cacheKey(nil, "o")); ok {
		t.Fatal("an index over the whole budget was cached")
	}
	c.mu.Lock()
	entries, total = len(c.byOwner), c.total
	c.mu.Unlock()
	if entries != 2 || total != 200 {
		t.Fatalf("the cache accounts for %d entries and %d bytes, want 2 and 200", entries, total)
	}
}

func TestQueryServedFromTheCacheWhileTheBucketIsDown(t *testing.T) {
	h := newSearchHarness(t)
	h.push(t, "chat_duck", "Pond", "a duck swam by")
	h.service.Buckets = buckets.NewClient("http://127.0.0.1:1", testSearchBucket, nil)
	if ids, rebuildNeeded := h.query(t, "duck"); rebuildNeeded || !slices.Equal(ids, []string{"chat_duck"}) {
		t.Fatalf("query with the bucket down = %v (rebuild %v), want the cached index to answer", ids, rebuildNeeded)
	}
}

func TestARebuildWithinAnExpiredBudgetIsPartial(t *testing.T) {
	h := newSearchHarness(t)
	h.push(t, "chat_duck", "Pond", "a duck swam by")
	h.loseIndex(t)
	h.service.Rebuilds = jobs.New[RebuildReport](-time.Second, RebuildRetention)
	if indexing, err := h.service.Rebuild(context.Background(), h.p, h.key); err != nil || !indexing {
		t.Fatalf("rebuild = (%v, %v), want indexing under way", indexing, err)
	}
	if status := h.rebuildEnded(t); status.State != jobs.Complete || !status.Report.Partial || status.Report.Indexed != 0 {
		t.Fatalf("a rebuild whose budget ran out = %+v, want complete, partial and nothing indexed", status)
	}
	// A partial rebuild leaves indexing under way for the next request.
	if indexing, err := h.service.Rebuild(context.Background(), h.p, h.key); err != nil || !indexing {
		t.Fatalf("the next rebuild request = (%v, %v), want indexing still under way", indexing, err)
	}
}
