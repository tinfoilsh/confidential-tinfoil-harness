package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// The harness holds a chat only while its reply runs: there is no limit on
// how many it runs at once, and nothing stays behind.
func TestManyChatsRunWithoutARegistryLimit(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	const chats, turnsPerChat = 150, 2
	var wg sync.WaitGroup
	results := make(chan []event, chats*turnsPerChat)
	for chat := range chats {
		id := fmt.Sprintf("%013d_local", chat)
		for range turnsPerChat {
			wg.Go(func() {
				w := postRoute(h, "/v1/threads/turn", localInput(id, priorThread()), "alice")
				if w.Code != 200 {
					t.Errorf("%s: HTTP %d %s", id, w.Code, w.Body.String())
					return
				}
				results <- eventsIn(t, w)
			})
		}
	}
	wg.Wait()
	close(results)
	finished := 0
	for events := range results {
		if last := events[len(events)-1]; last.Type == "RUN_FINISHED" {
			finished++
		} else {
			t.Errorf("a turn ended with %+v", last)
		}
	}
	if finished != chats*turnsPerChat {
		t.Fatalf("%d of %d turns finished", finished, chats*turnsPerChat)
	}
	if threads, runs, requests := liveCount(h); threads+runs+requests != 0 {
		t.Fatalf("finished runs left %d chats, %d runs and %d requests behind", threads, runs, requests)
	}
}

// A queued turn that cannot start ends with its own error, and the turns
// queued after it still run.
func TestAQueuedTurnThatCannotStartDoesNotStallTheQueue(t *testing.T) {
	model, release := heldModel()
	h, _, key := testChatHarness(t, model)
	t.Cleanup(release)
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	first, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, first.log, "TEXT_MESSAGE_CHUNK")
	broken := continueInput(key, first.thread.id)
	broken["kind"], broken["messageId"] = "edit", "no-such-message"
	queue := []*chatRun{}
	for _, in := range []api.Object{broken, continueInput(key, first.thread.id)} {
		run, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](in))
		if err != nil || !run.queued {
			t.Fatalf("turn not queued: %v", err)
		}
		queue = append(queue, run)
	}
	release()
	awaitFrame(t, first.log, "RUN_FINISHED")
	awaitFrame(t, queue[0].log, "RUN_ERROR")
	awaitFrame(t, queue[1].log, "RUN_FINISHED")
	<-queue[1].finished
	if threads, runs, requests := liveCount(h); threads+runs+requests != 0 {
		t.Fatalf("the queue left %d chats, %d runs and %d requests behind", threads, runs, requests)
	}
}

// A turn past a chat's full queue is refused as such, not as the caller's
// exhausted quota.
func TestAFullQueueIsNotAnExhaustedQuota(t *testing.T) {
	model, release := heldModel()
	h, _, key := testChatHarness(t, model)
	t.Cleanup(release)
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	first, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, first.log, "TEXT_MESSAGE_CHUNK")
	for range maxQueuedTurns {
		if run, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](continueInput(key, first.thread.id))); err != nil || !run.queued {
			t.Fatalf("turn not queued: %v", err)
		}
	}
	_, err = h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](continueInput(key, first.thread.id)))
	if e := api.AsError(err); e.HTTP != 429 || e.Code != "QUEUE_FULL" {
		t.Fatalf("a turn past a full queue was refused with %v", err)
	}
}

// Deleting a chat ends every turn queued on it with a terminal frame.
func TestDeletingAChatEndsItsQueuedTurns(t *testing.T) {
	model, release := heldModel()
	h, rows, key := testChatHarness(t, model)
	t.Cleanup(release)
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	first, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, first.log, "TEXT_MESSAGE_CHUNK")
	queued, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](continueInput(key, first.thread.id)))
	if err != nil || !queued.queued {
		t.Fatalf("turn not queued: %v", err)
	}
	if w := postRoute(h, "/v1/threads/delete", api.Object{"key": keyBase64(key), "id": first.thread.id}, "alice"); w.Code != 200 {
		t.Fatalf("delete: HTTP %d %s", w.Code, w.Body.String())
	}
	awaitFrame(t, queued.log, "RUN_FINISHED")
	awaitFrame(t, first.log, "RUN_FINISHED")
	<-first.finished
	if _, err := rows.load(context.Background(), p, key, "chat", first.thread.id); api.AsError(err).HTTP != 404 {
		t.Fatalf("the deleted chat came back: %v", err)
	}
	if threads, runs, requests := liveCount(h); threads+runs+requests != 0 {
		t.Fatalf("the deleted chat left %d chats, %d runs and %d requests behind", threads, runs, requests)
	}
}

// A chat deleted while its turn starts stays deleted, whether this instance
// deletes it or another does between the turn's read and its write.
func TestAChatDeletedAsItsTurnStartsStaysDeleted(t *testing.T) {
	t.Run("here", func(t *testing.T) {
		h, rows, key := testChatHarness(t, nil)
		// The turn's flag lookup is held while the chat is deleted.
		asked, answer := make(chan struct{}), make(chan struct{})
		var once sync.Once
		cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			once.Do(func() {
				close(asked)
				<-answer
			})
			io.WriteString(w, `{"flags":[]}`)
		}))
		t.Cleanup(cp.Close)
		release := sync.OnceFunc(func() { close(answer) })
		t.Cleanup(release)
		h.auth = testAuth(cp)
		in := sendInput(key)
		refused := make(chan *httptest.ResponseRecorder, 1)
		go func() { refused <- postRoute(h, "/v1/threads/turn", in, "alice") }()
		<-asked
		if w := postRoute(h, "/v1/threads/delete", api.Object{"key": keyBase64(key), "id": in["threadId"]}, "alice"); w.Code != 200 {
			t.Fatalf("delete: HTTP %d %s", w.Code, w.Body.String())
		}
		release()
		if w := <-refused; w.Code != 404 {
			t.Fatalf("a turn on a deleted chat: HTTP %d %s", w.Code, w.Body.String())
		}
		if _, err := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", api.Str(in["threadId"])); api.AsError(err).HTTP != 404 {
			t.Fatalf("the deleted chat came back: %v", err)
		}
	})
	t.Run("elsewhere", func(t *testing.T) {
		h, rows, key := testChatHarness(t, nil)
		id := domain.RowID()
		name := rowMapKey(&auth.Principal{ID: "alice"}, "chat", id)
		rows.rows[name] = domain.StoredRow{ID: id, ETag: "1", Data: api.Raw(api.Object{"title": "Old", "messages": []any{}})}
		// Sync removes the chat for another instance just before the
		// turn's write, which then conflicts.
		rows.refuse = func(*domain.StoredRow) error {
			delete(rows.rows, name)
			rows.refuse = nil
			return nil
		}
		in := continueInput(key, id)
		if w := postRoute(h, "/v1/threads/turn", in, "alice"); w.Code != 404 {
			t.Fatalf("a turn on a deleted chat: HTTP %d %s", w.Code, w.Body.String())
		}
		if _, err := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", id); api.AsError(err).HTTP != 404 {
			t.Fatalf("the deleted chat came back: %v", err)
		}
	})
}

// within fails the test if call does not return promptly, or fails.
func within(t *testing.T, what string, call func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- call() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not return promptly", what)
	}
}

// A finishing run's writes hold no lock: while its commit waits on sync, its
// chat can still be followed and cancelled, and other chats still run.
func TestAFinishingRunsWritesBlockNothing(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	committing, commit := make(chan struct{}), make(chan struct{})
	var once, released sync.Once
	release := func() { released.Do(func() { close(commit) }) }
	t.Cleanup(release)
	rows.refuse = func(row *domain.StoredRow) error {
		if chat := mustRead(domain.ReadThreadRow(row)); len(chat.Messages) == 2 && chat.ActiveRun == nil {
			once.Do(func() {
				close(committing)
				<-commit
			})
		}
		return nil
	}
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	run, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	<-committing
	follow := func(r *chatRun) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/threads/follow", bytes.NewReader(api.Raw(api.Object{"key": keyBase64(key), "threadId": r.thread.id, "runId": r.log.id})))
		signIn(req, "alice")
		return replayOf(h, req)
	}
	within(t, "following", func() error {
		if w := follow(run); w.Code != 200 || !strings.HasPrefix(w.Body.String(), `id: 0`+"\n"+`data: {"type":"RUN_STARTED"`) {
			return fmt.Errorf("HTTP %d %s", w.Code, w.Body.String())
		}
		return nil
	})
	within(t, "cancelling", func() error {
		if _, err := h.cancelThread(context.Background(), p, key, requestOf[runRequest](api.Object{"key": keyBase64(key), "threadId": run.thread.id, "runId": run.log.id})); api.AsError(err).Code != "RUN_IN_FLIGHT" {
			return fmt.Errorf("a finishing run answered %v", err)
		}
		return nil
	})
	within(t, "another chat", func() error {
		if w := postRoute(h, "/v1/threads/turn", localInput("0000000000007_local", priorThread()), "alice"); !strings.Contains(w.Body.String(), `"type":"RUN_FINISHED"`) {
			return fmt.Errorf("HTTP %d %s", w.Code, w.Body.String())
		}
		return nil
	})
	release()
	awaitFrame(t, run.log, "RUN_FINISHED")
}

// A run names the instance holding it, the same for every run of the
// process, so requests about any of its runs can be routed back to it. The
// chat's hint that the run is streaming names it too, for a client that
// opens the chat again.
func TestARunNamesTheInstanceHoldingIt(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	var named []string
	for range 2 {
		events := eventsIn(t, postRoute(h, "/v1/threads/turn", sendInput(key), "alice"))
		run := api.Obj(api.Obj(api.Obj(eventOfType(events, "STATE_SNAPSHOT").Snapshot)["thread"])["activeRun"])
		if run["instance"] != events[0].Instance {
			t.Fatalf("the chat's running hint is %v, not the run's instance %q", run, events[0].Instance)
		}
		named = append(named, events[0].Instance)
	}
	if named[0] == "" || named[0] != named[1] || named[0] != instanceID {
		t.Fatalf("runs named instances %q, not this one, %q", named, instanceID)
	}
}

// Re-attaching to a live run resumes after the last frame the client applied,
// as Last-Event-ID names it, or replays it from its first frame when the
// client names none. A cursor the run never sent is refused. A run that is no
// longer live is not found.
func TestFollowResumesALiveRunAfterTheClientsCursor(t *testing.T) {
	model, release := heldModel()
	h, _, key := testChatHarness(t, model)
	t.Cleanup(release)
	run, err := h.acceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.log, "TEXT_MESSAGE_CHUNK")
	request := func(lastEventID string) *http.Request {
		in := api.Object{"key": keyBase64(key), "threadId": run.thread.id, "runId": run.log.id}
		req := httptest.NewRequest(http.MethodPost, "/v1/threads/follow", bytes.NewReader(api.Raw(in)))
		signIn(req, "alice")
		if lastEventID != "" {
			req.Header.Set("Last-Event-ID", lastEventID)
		}
		return req
	}
	frames, _, _ := run.log.log.read(0)
	for _, tc := range []struct {
		lastEventID string
		first       int
	}{{"", 0}, {"1", 2}} {
		w := replayOf(h, request(tc.lastEventID))
		ids := regexp.MustCompile(`(?m)^id: (\d+)$`).FindAllStringSubmatch(w.Body.String(), -1)
		events := eventsIn(t, w)
		if len(ids) != len(frames)-tc.first || len(events) != len(ids) || eventOfType(events, "TEXT_MESSAGE_CHUNK") == nil {
			t.Fatalf("after %q: replayed %d of %d frames: %s", tc.lastEventID, len(events), len(frames), w.Body.String())
		}
		for i, id := range ids {
			if id[1] != strconv.Itoa(tc.first+i) {
				t.Fatalf("after %q: frame %d replayed as id %s", tc.lastEventID, tc.first+i, id[1])
			}
		}
	}
	for _, lastEventID := range []string{strconv.Itoa(len(frames)), "-1", "first"} {
		w := replayOf(h, request(lastEventID))
		if e := api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["error"]); w.Code != 400 || e["code"] != "BAD_REQUEST" || e["action"] != "stop" {
			t.Fatalf("following after %q: HTTP %d %s", lastEventID, w.Code, w.Body.String())
		}
	}
	release()
	<-run.finished
	if w := replayOf(h, request("")); w.Code != 404 || !strings.Contains(w.Body.String(), "RUN_NOT_FOUND") {
		t.Fatalf("following a finished run: HTTP %d %s", w.Code, w.Body.String())
	}
}

// A retried request is told apart through the chat itself: while its run is
// live it follows that run, and afterwards it is refused as already applied,
// by any instance, without adding its message twice.
func TestARetriedRequestFollowsItsRunOrIsAlreadyApplied(t *testing.T) {
	model, release := heldModel()
	h, rows, key := testChatHarness(t, model)
	t.Cleanup(release)
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	in := sendInput(key)
	run, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](in))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.log, "TEXT_MESSAGE_CHUNK")
	retry := func(h *harness) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(api.Raw(in)))
		signIn(req, "alice")
		return req
	}
	w := replayOf(h, retry(h))
	events := eventsIn(t, w)
	if frames, _, _ := run.log.log.read(0); events[0].Type != "RUN_STARTED" || events[0].RunID != run.log.id || len(events) != len(frames) {
		t.Fatalf("a retry during the run was not its replay: %s", w.Body.String())
	}
	release()
	<-run.finished
	fresh, _, _ := testChatHarness(t, nil)
	fresh.storeRows = rows
	for _, instance := range []*harness{h, fresh} {
		w := httptest.NewRecorder()
		instance.routes().ServeHTTP(w, retry(instance))
		if w.Code != 409 || !strings.Contains(w.Body.String(), "ALREADY_APPLIED") {
			t.Fatalf("a retry after the run: HTTP %d %s", w.Code, w.Body.String())
		}
	}
	row, _ := rows.load(context.Background(), p, key, "chat", run.thread.id)
	chat := mustRead(domain.ReadThreadRow(row))
	if len(chat.Messages) != 2 || chat.Messages[0].ID != in["clientRequestId"] || chat.LastRequestID != in["clientRequestId"] {
		t.Fatalf("the chat holds %s", row.Data)
	}
	// A device chat names its last request in the copy its client sends.
	local := localInput("", priorThread())
	api.Obj(local["thread"])["lastRequestId"] = local["clientRequestId"]
	if w := postRoute(h, "/v1/threads/turn", local, "alice"); w.Code != 409 || !strings.Contains(w.Body.String(), "ALREADY_APPLIED") {
		t.Fatalf("a retried device chat turn: HTTP %d %s", w.Code, w.Body.String())
	}
}

// A new chat's first turn creates its row under the id its client chose; an
// id it did not make that way names a chat that must exist.
func TestAFirstTurnCreatesItsChatUnderTheClientsID(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	in := sendInput(key)
	events := finishedRun(t, h, "/v1/threads/turn", in)
	if events[0].ThreadID != in["threadId"] {
		t.Fatalf("the chat was stored as %s, not %s", events[0].ThreadID, in["threadId"])
	}
	if _, err := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", api.Str(in["threadId"])); err != nil {
		t.Fatal(err)
	}
	in = sendInput(key)
	in["threadId"] = "not-a-new-chat-id"
	if w := postRoute(h, "/v1/threads/turn", in, "alice"); w.Code != 404 {
		t.Fatalf("a turn on a missing chat with a foreign id: HTTP %d %s", w.Code, w.Body.String())
	}
	delete(in, "threadId")
	if w := postRoute(h, "/v1/threads/turn", in, "alice"); w.Code != 400 {
		t.Fatalf("a turn without a chat id: HTTP %d %s", w.Code, w.Body.String())
	}
}

// A run submits its conversation for screening after its terminal frame,
// so a sidecar that is slow to answer holds up neither the run nor its chat.
func TestASlowSafeguardsSidecarHoldsUpNothing(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	hold := make(chan struct{})
	var once sync.Once
	answer := func() { once.Do(func() { close(hold) }) }
	drained := heldScreening(t, h, hold)
	t.Cleanup(answer)
	first := sendInput(key)
	for _, in := range []api.Object{first, continueInput(key, api.Str(first["threadId"]))} {
		within(t, "a turn", func() error {
			if w := postRoute(h, "/v1/threads/turn", in, "alice"); !strings.Contains(w.Body.String(), `"type":"RUN_FINISHED"`) {
				return fmt.Errorf("HTTP %d %s", w.Code, w.Body.String())
			}
			return nil
		})
	}
	answer()
	if got := drained(); len(got) != 2 {
		t.Fatalf("the sidecar received %d submissions", len(got))
	}
}

// Every route about a live run finds it under its caller, so another
// account's request names nothing, and the same request nonce from another
// account is that account's own new turn.
func TestAnotherCallerReachesNothingOfALiveRun(t *testing.T) {
	// Only the first account's reply is held; the other account's is answered.
	held, release := heldModel()
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		for _, m := range lastRequest(r) {
			if api.Str(api.Obj(m)["content"]) == "mine" {
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\ndata: [DONE]\n\n")
				return
			}
		}
		held(w, r)
	})
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	in := sendInput(key)
	run, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](in))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { release(); <-run.finished })
	awaitFrame(t, run.log, "RUN_STARTED")
	queued := sendInput(key)
	queued["threadId"] = run.thread.id
	next, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](queued))
	if err != nil || !next.queued {
		t.Fatalf("turn not queued: %v", err)
	}
	for path, body := range map[string]api.Object{
		"/v1/threads/cancel":       {"key": keyBase64(key), "threadId": run.thread.id, "runId": run.log.id},
		"/v1/threads/queue/send":   {"key": keyBase64(key), "threadId": run.thread.id, "queueId": next.log.id},
		"/v1/threads/queue/remove": {"key": keyBase64(key), "threadId": run.thread.id, "queueId": next.log.id},
		"/v1/threads/transcript":   {"key": keyBase64(key), "threadId": run.thread.id},
	} {
		if w := postRoute(h, path, body, "bob"); w.Code != 404 {
			t.Fatalf("%s for another account's chat answered HTTP %d %s", path, w.Code, w.Body.String())
		}
	}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", api.Object{"key": keyBase64(key), "threadId": domain.RowID(), "clientRequestId": in["clientRequestId"], "content": "mine", "kind": "send", "options": api.Object{"model": "kimi-k3", "webSearch": false}}, "bob"))
	if events[0].Type != "RUN_STARTED" || events[0].RunID == run.log.id || events[0].ThreadID == run.thread.id || events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatalf("another account's turn with the same nonce was not its own: %v", events)
	}
	h.live.mu.Lock()
	stillQueued := slices.Contains(run.thread.queue, next)
	h.live.mu.Unlock()
	if !stillQueued || run.thread.active != run {
		t.Fatal("another account's requests changed the run")
	}
}

// A finished run's log is wiped once nothing drains it, whether its last
// reader leaves after the run ends or before. A follower attached as the run
// ends is still sent every frame, and the frames are zeroed, not only
// dropped.
func TestAFinishedRunsLogIsWipedOnceNothingDrainsIt(t *testing.T) {
	for _, tc := range []struct {
		name         string
		leavesBefore bool
	}{{"last reader leaves after the run ends", false}, {"last reader left before the run ends", true}} {
		t.Run(tc.name, func(t *testing.T) {
			model, release := heldModel()
			h, _, key := testChatHarness(t, model)
			p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
			run, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
			if err != nil {
				t.Fatal(err)
			}
			readers := func() int {
				run.log.log.mu.Lock()
				defer run.log.log.mu.Unlock()
				return run.log.log.readers
			}
			awaitFrame(t, run.log, "RUN_STARTED")
			first, _, _ := run.log.log.read(0)
			kept := first[0]
			if tc.leavesBefore {
				run.log.log.detach()
				release()
				<-run.finished
			} else {
				followed := make(chan *httptest.ResponseRecorder, 1)
				go func() {
					followed <- postRoute(h, "/v1/threads/follow", api.Object{"key": keyBase64(key), "threadId": run.thread.id, "runId": run.log.id}, "alice")
				}()
				for deadline := time.Now().Add(5 * time.Second); readers() < 2; {
					if time.Now().After(deadline) {
						t.Fatal("the follower never attached")
					}
					time.Sleep(time.Millisecond)
				}
				release()
				<-run.finished
				events := eventsIn(t, <-followed)
				frames, _, closed := run.log.log.read(0)
				if closed != errDone || len(frames) == 0 || len(events) != len(frames) || events[len(events)-1].Type != "RUN_FINISHED" {
					t.Fatalf("the follower got %d of %d frames, ending in %v", len(events), len(frames), closed)
				}
				run.log.log.detach()
			}
			if frames, _, _ := run.log.log.read(0); frames != nil || readers() != 0 || bytes.ContainsFunc(kept, func(b rune) bool { return b != 0 }) {
				t.Fatalf("the log kept %d frames for %d readers; first frame %q", len(frames), readers(), kept)
			}
		})
	}
}
