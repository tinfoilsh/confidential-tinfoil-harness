package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
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
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	first, err := h.acceptTurn(context.Background(), p, key, sendInput(key))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, first.log, "TEXT_MESSAGE_CHUNK")
	broken := continueInput(key, first.thread.id)
	broken["kind"], broken["messageId"] = "edit", "no-such-message"
	queue := []*chatRun{}
	for _, in := range []object{broken, continueInput(key, first.thread.id)} {
		run, err := h.acceptTurn(context.Background(), p, key, in)
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

// Deleting a chat ends every turn queued on it with a terminal frame.
func TestDeletingAChatEndsItsQueuedTurns(t *testing.T) {
	model, release := heldModel()
	h, rows, key := testChatHarness(t, model)
	t.Cleanup(release)
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	first, err := h.acceptTurn(context.Background(), p, key, sendInput(key))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, first.log, "TEXT_MESSAGE_CHUNK")
	queued, err := h.acceptTurn(context.Background(), p, key, continueInput(key, first.thread.id))
	if err != nil || !queued.queued {
		t.Fatalf("turn not queued: %v", err)
	}
	if w := postRoute(h, "/v1/threads/delete", object{"key": key.base64(), "id": first.thread.id}, "alice"); w.Code != 200 {
		t.Fatalf("delete: HTTP %d %s", w.Code, w.Body.String())
	}
	awaitFrame(t, queued.log, "RUN_FINISHED")
	awaitFrame(t, first.log, "RUN_FINISHED")
	<-first.finished
	if _, err := rows.load(context.Background(), p, key, "chat", first.thread.id); asAPIError(err).HTTP != 404 {
		t.Fatalf("the deleted chat came back: %v", err)
	}
	if threads, runs, requests := liveCount(h); threads+runs+requests != 0 {
		t.Fatalf("the deleted chat left %d chats, %d runs and %d requests behind", threads, runs, requests)
	}
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
	case <-time.After(5 * time.Second):
		t.Fatalf("%s waited for the finishing run", what)
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
	rows.refuse = func(row *storedRow) error {
		if len(arr(row.Data["messages"])) == 2 && row.Data["activeRun"] == nil {
			once.Do(func() {
				close(committing)
				<-commit
			})
		}
		return nil
	}
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	run, err := h.acceptTurn(context.Background(), p, key, sendInput(key))
	if err != nil {
		t.Fatal(err)
	}
	<-committing
	follow := func(r *chatRun) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/threads/follow", bytes.NewReader(raw(object{"key": key.base64(), "threadId": r.thread.id, "runId": r.log.id})))
		req.Header.Set("Authorization", "Bearer clerk-alice")
		return replayOf(h, req)
	}
	within(t, "following", func() error {
		if w := follow(run); w.Code != 200 || !strings.Contains(w.Body.String(), `"type":"RUN_STARTED"`) {
			return fmt.Errorf("HTTP %d %s", w.Code, w.Body.String())
		}
		return nil
	})
	within(t, "cancelling", func() error {
		if _, err := h.cancelThread(context.Background(), p, key, object{"key": key.base64(), "threadId": run.thread.id, "runId": run.log.id}); asAPIError(err).Code != "RUN_IN_FLIGHT" {
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
