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

// caller is a request context carrying one API key, which is what a run log is
// scoped to on top of its secret.
func caller(apiKey string) context.Context {
	return context.WithValue(context.Background(), apiKeyKey{}, apiKey)
}

func mustRun(t *testing.T, id string, key secret) *run {
	t.Helper()
	rn, err := newRun(caller("key"), id, key)
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

// The run is started before the gateway is dialled, so a refusal can no longer
// be a status: it reaches the caller as a RUN_ERROR carrying the same detail.
func TestARefusalReachesTheCallerAsARunError(t *testing.T) {
	rn := mustRun(t, "00", "aa")
	newStream(rn, "thread", "run").fail(&refusal{status: http.StatusTooManyRequests, detail: "slow down"})
	rec := httptest.NewRecorder()
	follow(rec, httptest.NewRequest("POST", "/agui", nil), rn, 0)
	body := rec.Body.String()
	if !strings.HasPrefix(body, "id: 0\ndata: {\"type\":\"RUN_STARTED\"") {
		t.Fatalf("a refused run never started: %q", body)
	}
	if !strings.Contains(body, "RUN_ERROR") || !strings.Contains(body, "429") || !strings.Contains(body, "slow down") {
		t.Fatalf("the refusal lost its detail: %q", body)
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

	mine := httptest.NewRequest("POST", "/agui", nil).WithContext(caller("key"))
	req := &request{storageID: strings.Repeat("a", 32), secret: "bb"}
	rec := httptest.NewRecorder()
	h.cold(rec, mine, req)
	if !strings.Contains(rec.Body.String(), "hello") || !strings.Contains(rec.Body.String(), "RUN_FINISHED") {
		t.Fatalf("cold resume lost the run: %q", rec.Body.String())
	}

	wrong := httptest.NewRecorder()
	h.cold(wrong, mine, &request{storageID: req.storageID, secret: "cc"})
	if wrong.Code != http.StatusForbidden {
		t.Fatalf("wrong secret got %d", wrong.Code)
	}
	missing := httptest.NewRecorder()
	h.cold(missing, mine, &request{storageID: strings.Repeat("b", 32), secret: "bb"})
	if missing.Code != wrong.Code {
		t.Fatalf("absent log answered %d, wrong secret answered %d", missing.Code, wrong.Code)
	}
	// The right pair under another API key is the same refusal as a wrong secret.
	foreign := httptest.NewRecorder()
	h.cold(foreign, httptest.NewRequest("POST", "/agui", nil).WithContext(caller("other")), req)
	if foreign.Code != wrong.Code {
		t.Fatalf("another key read the stored log: %d, %q", foreign.Code, foreign.Body.String())
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

	if live, err := h.lookup(caller("key"), id, "bb"); live != rn || err != nil {
		t.Fatalf("the right secret did not reattach: %v", err)
	}
	if live, err := h.lookup(caller("key"), id, "zz"); live != nil || err != errNotYours {
		t.Fatalf("wrong secret reattached: live=%v err=%v", live != nil, err)
	}
	if live, err := h.lookup(caller("other"), id, "bb"); live != nil || err != errNotYours {
		t.Fatalf("another key reattached with the right secret: live=%v err=%v", live != nil, err)
	}
	if live, err := h.lookup(caller("key"), strings.Repeat("d", 32), "bb"); live != nil || err != nil {
		t.Fatalf("an unknown run reported itself: live=%v err=%v", live != nil, err)
	}
	h.retire(id, rn)
	if live, err := h.lookup(caller("key"), id, "bb"); live != nil || err != nil {
		t.Fatalf("a retired run stayed in the registry: live=%v err=%v", live != nil, err)
	}
}

// A reused secret must not seal one plaintext at one index to the same bytes twice.
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

// Starting the run frames the log, so there is no window in which a caller
// holding the right secret is told its own run is not recoverable.
func TestAStartedRunIsReattachableBeforeItProducesAnything(t *testing.T) {
	id := strings.Repeat("f", 32)
	h := &harness{runs: map[string]*run{}}
	rn := mustRun(t, id, "bb")
	newStream(rn, "thread", "run")
	if err := h.enlist(id, rn); err != nil {
		t.Fatal(err)
	}
	if live, err := h.lookup(caller("key"), id, "bb"); live != rn || err != nil {
		t.Fatalf("a run that had only started answered live=%v err=%v", live != nil, err)
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
			strings.NewReader(`{"sessionId":"`+id+`","recoveryToken":"`+secret+`"}`))
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

// The whole of the authorization is opening the log, and the caller's API key
// scopes which log that is. A foreign key holding the right pair is refused
// like any other, and refused before it can reach a model or the store.
func TestALogBelongsToTheKeyThatSealedIt(t *testing.T) {
	id, tok := strings.Repeat("a", 32), strings.Repeat("b", 32)
	h := &harness{runs: map[string]*run{}}
	rn := mustRun(t, id, secret(tok))
	out := newStream(rn, "victim-thread", "victim-run")
	out.emit(event{Type: "TEXT_MESSAGE_CHUNK", Delta: "the victim's answer"})
	out.done()
	if err := h.enlist(id, rn); err != nil {
		t.Fatal(err)
	}

	// Not a resume: the live-run branch is taken on any request naming the pair.
	body := `{"messages":[{"role":"user","content":"hi"}],"sessionId":"` + id +
		`","recoveryToken":"` + tok + `"}`
	post := func(apiKey string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/agui", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+apiKey)
		rec := httptest.NewRecorder()
		h.agui(rec, r)
		return rec
	}
	if got := post("key"); !strings.Contains(got.Body.String(), "the victim's answer") {
		t.Fatalf("the key that sealed the log could not reattach: %d %q", got.Code, got.Body.String())
	}
	got := post("another-key")
	if got.Code != http.StatusForbidden || strings.Contains(got.Body.String(), "victim") {
		t.Fatalf("another key reattached to a live run: %d %q", got.Code, got.Body.String())
	}
}

func TestARunIsNamedByABoundedIdentifier(t *testing.T) {
	body := func(field, value string) []byte {
		return []byte(`{"messages":[{"role":"user","content":"hi"}],"` + field + `":"` + value + `"}`)
	}
	for _, field := range []string{"runId", "threadId"} {
		if _, err := parseRequest(body(field, strings.Repeat("z", maxIDLength)), "key"); err != nil {
			t.Fatalf("%s of the largest allowed size was refused: %v", field, err)
		}
		if _, err := parseRequest(body(field, strings.Repeat("z", maxIDLength+1)), "key"); err == nil {
			t.Fatalf("an oversized %s was accepted", field)
		}
		if _, err := parseRequest(body(field, `a\u0000b`), "key"); err == nil {
			t.Fatalf("a %s carrying a control character was accepted", field)
		}
	}
	// Absent is still allowed: the harness names the run itself.
	req, err := parseRequest(body("model", "auto"), "key")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(req.runID, "run_") {
		t.Fatalf("an unnamed run was not named by the harness: %q", req.runID)
	}
}

func TestAPanickingToolCallDoesNotTakeTheRun(t *testing.T) {
	// byName holds the call's name, so invoke gets past the widget branch and
	// panics on the nil session behind it.
	set := &toolset{byName: map[string]*session{"boom": nil}}
	set.byName["boom"] = &session{}
	rn := mustRun(t, "00", "aa")
	out := newStream(rn, "thread", "run")
	message := invoke(context.Background(), out, set, toolCall{id: "call_1", name: "boom"})
	if !strings.Contains(string(message), "tool boom failed") {
		t.Fatalf("a panicking tool call answered %q", message)
	}
	if rn.log.closed != nil {
		t.Fatalf("a panicking tool call closed the log: %v", rn.log.closed)
	}
}
