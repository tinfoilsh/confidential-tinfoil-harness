package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// A flag lookup the control plane is slow to answer flags nothing rather
// than holding up the chats it was for.
func TestASlowFlagLookupFlagsNothing(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	stuck := make(chan struct{})
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-stuck:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(cp.Close)
	t.Cleanup(func() { close(stuck) })
	h.auth.client, h.auth.controlplane = cp.Client(), cp.URL
	saved := flagsTimeout
	flagsTimeout = 50 * time.Millisecond
	t.Cleanup(func() { flagsTimeout = saved })
	storedThread(rows, "alice", "chat-1")
	within(t, "listing", func() error {
		var list struct {
			Threads []threadSummary `json:"threads"`
		}
		w := postRoute(h, "/v1/threads/list", api.Object{"key": key.base64()}, "alice")
		if json.Unmarshal(w.Body.Bytes(), &list); len(list.Threads) != 1 || list.Threads[0].ID != "chat-1" || list.Threads[0].Flagged {
			return fmt.Errorf("HTTP %d %s", w.Code, w.Body.String())
		}
		return nil
	})
}

// A chat the safeguards flagged takes no more turns, on the device or in
// the cloud, and every view of it says so.
func TestAFlaggedChatTakesNoTurns(t *testing.T) {
	var modelCalls atomic.Int32
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		modelCalls.Add(1)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != safeguardFlagsPath || r.Header.Get("Authorization") != "Bearer clerk-alice" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		io.WriteString(w, `{"flags":[{"id":"f1","conversation_id":"flagged","created_at":"2026-10-01T00:00:00Z"},{"id":"f2","conversation_id":"listed","created_at":"2026-10-01T00:00:00Z"}],"in_window":2,"window_hours":720,"warn_threshold":2,"ban_threshold":3}`)
	}))
	t.Cleanup(cp.Close)
	h.auth.client, h.auth.controlplane = cp.Client(), cp.URL
	storedThread(rows, "alice", "flagged")
	storedThread(rows, "alice", "fine")
	// A flagged chat that takes no turn is flagged only by the control plane.
	storedThread(rows, "alice", "listed")
	rows.rows["user:alice/profile/profile"] = storedRow{ID: "profile", ETag: "1", Data: api.Raw(api.Object{"pinnedChatIds": []string{"listed", "fine"}})}
	code := func(w *httptest.ResponseRecorder) string {
		var refused struct {
			Error api.Error `json:"error"`
		}
		json.Unmarshal(w.Body.Bytes(), &refused)
		return refused.Error.Code
	}
	ask := continueInput(key, "flagged")
	ask["kind"], ask["quote"] = "ask", "ok"
	delete(ask, "content")
	device := localInput("flagged", priorThread())
	for name, in := range map[string]api.Object{"stored": continueInput(key, "flagged"), "ask": ask, "device": device} {
		if w := postRoute(h, "/v1/threads/turn", in, "alice"); w.Code != 403 || code(w) != "CHAT_FLAGGED" {
			t.Fatalf("%s turn on a flagged chat: HTTP %d %s", name, w.Code, w.Body.String())
		}
	}
	if modelCalls.Load() != 0 {
		t.Fatalf("a flagged chat reached the model %d times", modelCalls.Load())
	}
	if !storedChat(t, rows, key, "flagged").Flagged {
		t.Fatal("the refused chat was not stored as flagged")
	}
	var list struct {
		Threads []threadSummary `json:"threads"`
		Pinned  []threadSummary `json:"pinned"`
	}
	json.Unmarshal(postRoute(h, "/v1/threads/list", api.Object{"key": key.base64()}, "alice").Body.Bytes(), &list)
	pinned := map[string]bool{}
	for _, summary := range list.Pinned {
		pinned[summary.ID] = summary.Flagged
	}
	if !reflect.DeepEqual(pinned, map[string]bool{"listed": true, "fine": false}) {
		t.Fatalf("favorite flags: %v", pinned)
	}
	listed := map[string]bool{}
	for _, summary := range list.Threads {
		listed[summary.ID] = summary.Flagged
	}
	if len(list.Threads) != 3 || !reflect.DeepEqual(listed, map[string]bool{"flagged": true, "fine": false, "listed": true}) {
		t.Fatalf("the list shows %+v", list.Threads)
	}
	var got threadSummary
	json.Unmarshal(postRoute(h, "/v1/threads/get", api.Object{"key": key.base64(), "id": "listed"}, "alice").Body.Bytes(), &got)
	if got.ID != "listed" || !got.Flagged {
		t.Fatalf("the chat reads as %+v", got)
	}
	if renamed := deviceOperation(t, h, "/v1/threads/update", priorThread(), api.Object{"id": "flagged", "title": "Renamed"}, "alice"); renamed["flagged"] != true {
		t.Fatalf("the device chat came back as %v", renamed)
	}
	finishedRun(t, h, "/v1/threads/turn", continueInput(key, "fine"))
}

// A caller's flags are read once a minute, so the control plane does not
// hear of each chat read, and they never mark another caller's chats.
func TestFlagsAreReusedOnlyForTheirCaller(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	var reads atomic.Int32
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		if r.Header.Get("Authorization") == "Bearer clerk-alice" {
			io.WriteString(w, `{"flags":[{"conversation_id":"chat-1"}]}`)
			return
		}
		io.WriteString(w, `{"flags":[]}`)
	}))
	t.Cleanup(cp.Close)
	h.auth.client, h.auth.controlplane = cp.Client(), cp.URL
	storedThread(rows, "alice", "chat-1")
	storedThread(rows, "bob", "chat-1")
	flagged := func(user string) bool {
		var got threadSummary
		json.Unmarshal(postRoute(h, "/v1/threads/get", api.Object{"key": key.base64(), "id": "chat-1"}, user).Body.Bytes(), &got)
		return got.Flagged
	}
	if seen := []bool{flagged("alice"), flagged("alice"), flagged("bob"), flagged("bob")}; !reflect.DeepEqual(seen, []bool{true, true, false, false}) {
		t.Fatalf("flagged as read by alice, alice, bob, bob: %v", seen)
	}
	if reads.Load() != 2 {
		t.Fatalf("the control plane was asked for flags %d times", reads.Load())
	}
}
