package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func mustRun(t *testing.T, id string, key secret) *run {
	t.Helper()
	rn, err := newRun(id, key)
	if err != nil {
		t.Fatal(err)
	}
	return rn
}

func TestFrameIsBoundToItsIndex(t *testing.T) {
	rn := mustRun(t, "00", "aa")
	first, second := rn.seal(0, []byte(`{"a":1}`)), rn.seal(1, []byte(`{"a":2}`))
	if _, err := rn.open(0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := rn.open(1, first); err == nil {
		t.Fatal("a frame opened at the wrong index")
	}
	if _, err := mustRun(t, "00", "bb").open(1, second); err == nil {
		t.Fatal("a frame opened under the wrong secret")
	}
	if _, err := mustRun(t, "ff", "aa").open(1, second); err == nil {
		t.Fatal("a frame opened under the wrong storage id")
	}
}

func TestFollowReplaysFromCursorAndEndsOnClose(t *testing.T) {
	rn := mustRun(t, "00", "aa")
	out := newStream(rn, "thread", "run")
	out.emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "one"})
	out.emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "two"})
	out.done()

	whole := httptest.NewRecorder()
	follow(whole, httptest.NewRequest("POST", "/agui", nil), rn, 0)
	if got := strings.Count(whole.Body.String(), "data: "); got != 4 {
		t.Fatalf("whole log: got %d frames, want 4", got)
	}
	if !strings.HasPrefix(whole.Body.String(), "id: 0\ndata: {\"type\":\"RUN_STARTED\"") {
		t.Fatalf("whole log did not open with RUN_STARTED: %q", whole.Body.String())
	}

	rest := httptest.NewRecorder()
	follow(rest, httptest.NewRequest("POST", "/agui", nil), rn, 2)
	body := rest.Body.String()
	if strings.Contains(body, "one") || !strings.Contains(body, "two") {
		t.Fatalf("resume from 2 replayed the wrong frames: %q", body)
	}
	if !strings.Contains(body, `"id": 3`) && !strings.Contains(body, "id: 3") {
		t.Fatalf("resume lost its ids: %q", body)
	}
}

func TestRefusalBeforeTheRunOpensStaysAStatus(t *testing.T) {
	rn := mustRun(t, "00", "aa")
	newStream(rn, "thread", "run").fail(&refusal{status: http.StatusTooManyRequests, detail: "slow down"})
	rec := httptest.NewRecorder()
	follow(rec, httptest.NewRequest("POST", "/agui", nil), rn, 0)
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), "slow down") {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestSpilledRunComesBackFromTheStore(t *testing.T) {
	var stored bytes.Buffer
	created, completed := false, false
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			w.Write(stored.Bytes())
		case strings.HasSuffix(r.URL.Path, "/chunks"):
			io.Copy(&stored, r.Body)
		case strings.HasSuffix(r.URL.Path, "/complete"):
			completed = true
		default:
			created = true
		}
	}))
	defer store.Close()
	h := &harness{controlplane: store.URL, cpClient: store.Client(), runs: map[string]*run{}}

	rn := mustRun(t, strings.Repeat("a", 32), "bb")
	out := newStream(rn, "thread", "run")
	out.emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "hello"})
	out.done()
	h.spill(context.Background(), rn)
	if !created || !completed || stored.Len() == 0 {
		t.Fatalf("spill: created=%v completed=%v bytes=%d", created, completed, stored.Len())
	}

	req := &request{storageID: strings.Repeat("a", 32), secret: "bb"}
	rec := httptest.NewRecorder()
	h.cold(rec, httptest.NewRequest("POST", "/agui", nil), req)
	if !strings.Contains(rec.Body.String(), "hello") || !strings.Contains(rec.Body.String(), "RUN_FINISHED") {
		t.Fatalf("cold resume lost the run: %q", rec.Body.String())
	}

	wrong := httptest.NewRecorder()
	h.cold(wrong, httptest.NewRequest("POST", "/agui", nil), &request{storageID: req.storageID, secret: "cc"})
	if wrong.Code != http.StatusForbidden {
		t.Fatalf("wrong secret got %d", wrong.Code)
	}
	missing := httptest.NewRecorder()
	h.cold(missing, httptest.NewRequest("POST", "/agui", nil), &request{storageID: strings.Repeat("b", 32), secret: "bb"})
	if missing.Code != wrong.Code {
		t.Fatalf("absent log answered %d, wrong secret answered %d", missing.Code, wrong.Code)
	}
}

func TestWarmReattachIsAuthorizedByOpeningTheLog(t *testing.T) {
	id := strings.Repeat("c", 32)
	h := &harness{runs: map[string]*run{}}
	rn := mustRun(t, id, "bb")
	newStream(rn, "thread", "run").emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "live"})
	if err := h.enlist(id, rn); err != nil {
		t.Fatal(err)
	}

	if live, err := h.lookup(id, "bb"); live != rn || err != nil {
		t.Fatalf("the right secret did not reattach: %v", err)
	}
	if live, err := h.lookup(id, "zz"); live != nil || err != errNotYours {
		t.Fatalf("wrong secret reattached: live=%v err=%v", live != nil, err)
	}
	if live, err := h.lookup(strings.Repeat("d", 32), "bb"); live != nil || err != nil {
		t.Fatalf("an unknown run reported itself: live=%v err=%v", live != nil, err)
	}
	h.retire(id, rn)
	if live, err := h.lookup(id, "bb"); live != nil || err != nil {
		t.Fatalf("a retired run stayed in the registry: live=%v err=%v", live != nil, err)
	}
}

// A secret is the whole of the key, so the same secret twice must not be the
// same keystream twice: two runs sealing one plaintext at one index must differ.
func TestOneSecretTwiceIsNotOneKeystreamTwice(t *testing.T) {
	plain := []byte(`{"type":"RUN_STARTED"}`)
	if bytes.Equal(mustRun(t, "00", "aa").seal(0, plain), mustRun(t, "00", "aa").seal(0, plain)) {
		t.Fatal("frame 0 sealed identically under two runs: key and nonce repeat")
	}
}

func TestAStorageIDBelongsToOneRun(t *testing.T) {
	id := strings.Repeat("e", 32)
	h := &harness{runs: map[string]*run{}}
	held, loser := mustRun(t, id, "bb"), mustRun(t, id, "bb")
	if err := h.enlist(id, held); err != nil {
		t.Fatal(err)
	}
	if err := h.enlist(id, loser); err != errTaken {
		t.Fatalf("a second run took a live storage id: %v", err)
	}
	h.retire(id, loser)
	if h.runs[id] != held {
		t.Fatal("retiring one run unregistered another")
	}
}

func TestARunTooYoungToAuthorizeIsToldToComeBack(t *testing.T) {
	id := strings.Repeat("f", 32)
	h := &harness{runs: map[string]*run{}}
	if err := h.enlist(id, mustRun(t, id, "bb")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.lookup(id, "bb"); err != errPending {
		t.Fatalf("a run with no frames yet answered %v", err)
	}
	rec := httptest.NewRecorder()
	refuse(rec, errPending)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("got %d, Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestOutgrowingTheLogStopsTheRun(t *testing.T) {
	rn := mustRun(t, "00", "aa")
	stopped := false
	rn.stop = func() { stopped = true }
	out := newStream(rn, "thread", "run")
	for i := 0; !stopped && i <= maxRunLog; i += 1 << 16 {
		out.text("msg", strings.Repeat("x", 1<<16))
	}
	if !stopped {
		t.Fatal("a run that outgrew its log was left producing into it")
	}
	if rn.log.closed != errTooLong {
		t.Fatalf("closed = %v, want errTooLong", rn.log.closed)
	}
}

func TestDroppingALogIsAuthorizedAndReported(t *testing.T) {
	id, key := strings.Repeat("a", 32), strings.Repeat("b", 32)
	var stored bytes.Buffer
	deletes, refuses := 0, true
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete:
			deletes++
			if refuses {
				w.WriteHeader(http.StatusInternalServerError)
			}
		case r.Method == http.MethodGet:
			w.Write(stored.Bytes())
		case strings.HasSuffix(r.URL.Path, "/chunks"):
			io.Copy(&stored, r.Body)
		}
	}))
	defer store.Close()
	h := &harness{controlplane: store.URL, cpClient: store.Client(), runs: map[string]*run{}}

	rn := mustRun(t, id, secret(key))
	out := newStream(rn, "thread", "run")
	out.emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "hello"})
	out.done()
	h.spill(context.Background(), rn)

	call := func(auth, secret string) int {
		r := httptest.NewRequest("DELETE", "/agui",
			strings.NewReader(`{"storageId":"`+id+`","resumeSecret":"`+secret+`"}`))
		if auth != "" {
			r.Header.Set("Authorization", "Bearer "+auth)
		}
		rec := httptest.NewRecorder()
		h.drop(rec, r)
		return rec.Code
	}
	if got := call("", key); got != http.StatusUnauthorized {
		t.Fatalf("an unauthenticated delete answered %d", got)
	}
	if got := call("key", strings.Repeat("c", 32)); got != http.StatusForbidden {
		t.Fatalf("a delete under the wrong secret answered %d", got)
	}
	if deletes != 0 {
		t.Fatalf("%d unauthorized deletes reached the store", deletes)
	}
	if got := call("key", key); got != http.StatusBadGateway {
		t.Fatalf("a store that refused the delete answered %d", got)
	}
	refuses = false
	if got := call("key", key); got != http.StatusNoContent {
		t.Fatalf("a dropped log answered %d", got)
	}
	if deletes != 2 {
		t.Fatalf("deletes = %d, want 2", deletes)
	}
}

func TestLogWithoutATerminalFrameReplaysAsAbandoned(t *testing.T) {
	rn := mustRun(t, "00", "aa")
	newStream(rn, "thread", "run").emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "half"})
	var wire []byte
	for i, frame := range rn.log.frames {
		_ = i
		wire = append(wire, byte(len(frame)>>24), byte(len(frame)>>16), byte(len(frame)>>8), byte(len(frame)))
		wire = append(wire, frame...)
	}
	back := mustRun(t, "00", "aa")
	if err := back.rehydrate(wire); err != nil {
		t.Fatal(err)
	}
	if back.log.closed != errAbandoned {
		t.Fatalf("closed = %v, want errAbandoned", back.log.closed)
	}
	rec := httptest.NewRecorder()
	follow(rec, httptest.NewRequest("POST", "/agui", nil), back, 0)
	if !strings.Contains(rec.Body.String(), "RUN_ERROR") {
		t.Fatalf("abandoned run did not report itself: %q", rec.Body.String())
	}
}
