package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

type memoryRows struct {
	mu       sync.Mutex
	rows     map[string]storedRow
	conflict func(*storedRow)
	refuse   func(*storedRow) error
	// unreadable rows exist but cannot be opened with the caller's key.
	unreadable map[string]bool
	calls      int
}

func rowMapKey(p *principal, scope, id string) string { return p.scope() + "/" + scope + "/" + id }
func (s *memoryRows) load(_ context.Context, p *principal, _ contentKey, scope, id string) (*storedRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	row, ok := s.rows[rowMapKey(p, scope, id)]
	if !ok {
		return nil, apiErr(404, "THREAD_NOT_FOUND", "missing")
	}
	if s.unreadable[id] {
		return nil, apiErr(409, "KEY_MISMATCH", "sealed to another key")
	}
	return &storedRow{ID: row.ID, ETag: row.ETag, Data: clone(row.Data)}, nil
}
func (s *memoryRows) list(_ context.Context, p *principal, _ contentKey, scope, cursor, project string, limit int) (rowPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	page := rowPage{Rows: []storedRow{}}
	for name, row := range s.rows {
		if !strings.HasPrefix(name, p.scope()+"/"+scope+"/") || project != "" && row.Data["projectId"] != project {
			continue
		}
		if s.unreadable[row.ID] {
			page.Unreadable = append(page.Unreadable, row.ID)
			continue
		}
		page.Rows = append(page.Rows, storedRow{ID: row.ID, ETag: row.ETag, Data: clone(row.Data)})
	}
	return page, nil
}
func (s *memoryRows) push(_ context.Context, p *principal, _ contentKey, scope string, row *storedRow, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.refuse != nil {
		if err := s.refuse(row); err != nil {
			return "", err
		}
	}
	name := rowMapKey(p, scope, row.ID)
	current := s.rows[name]
	if s.conflict != nil {
		f := s.conflict
		s.conflict = nil
		f(&current)
		n, _ := strconv.Atoi(current.ETag)
		current.ETag = strconv.Itoa(n + 1)
		s.rows[name] = current
	}
	if current.ETag != row.ETag {
		return "", apiErr(409, "REVISION_CONFLICT", "changed")
	}
	n, _ := strconv.Atoi(current.ETag)
	next := strconv.Itoa(n + 1)
	s.rows[name] = storedRow{ID: row.ID, ETag: next, Data: clone(row.Data)}
	return next, nil
}
func (s *memoryRows) remove(_ context.Context, p *principal, _ contentKey, scope string, row *storedRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := rowMapKey(p, scope, row.ID)
	// Like sync, an empty if-match deletes unconditionally.
	if row.ETag != "" && s.rows[name].ETag != row.ETag {
		return apiErr(409, "REVISION_CONFLICT", "changed")
	}
	delete(s.rows, name)
	return nil
}

func testChatHarness(t *testing.T, upstream http.HandlerFunc) (*harness, *memoryRows, contentKey) {
	t.Helper()
	if upstream == nil {
		upstream = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
		}
	}
	server := httptest.NewServer(upstream)
	t.Cleanup(server.Close)
	var spillMu sync.Mutex
	logs := map[string][]byte{}
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/shim/validate-key" {
			var in object
			if json.NewDecoder(r.Body).Decode(&in) != nil || str(in["api_key"]) != "free_anonymous-key" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			io.WriteString(w, "OK")
			return
		}
		spillMu.Lock()
		defer spillMu.Unlock()
		id := strings.TrimPrefix(r.URL.Path, "/recovery/")
		switch {
		case r.Method == "GET":
			w.Write(logs[id])
		case strings.HasSuffix(id, "/chunks"):
			b, _ := io.ReadAll(r.Body)
			key := strings.TrimSuffix(id, "/chunks")
			logs[key] = append(logs[key], b...)
		}
	}))
	t.Cleanup(cp.Close)
	rows := &memoryRows{rows: map[string]storedRow{}}
	h := &harness{gateway: server.URL, controlplane: cp.URL, cpClient: cp.Client(), storeRows: rows, services: map[string]*downstreamService{}}
	h.auth = &authenticator{verify: func(_ context.Context, s string) (string, error) {
		if !strings.HasPrefix(s, "clerk-") {
			return "", errors.New("bad session")
		}
		return strings.TrimPrefix(s, "clerk-"), nil
	}, client: cp.Client(), controlplane: cp.URL, credentials: map[string]inferenceCredential{}}
	for _, subject := range []string{"alice", "bob"} {
		h.auth.credentials["user:"+subject] = inferenceCredential{Key: "inference-" + secret(subject), Expires: nowUTC().Add(time.Hour)}
	}
	m := &model{name: "kimi-k3", repo: "tinfoilsh/confidential-kimi-k3", context: 8192, client: &http.Client{Transport: &callerAuth{inner: server.Client().Transport, usageSecret: "test-usage-secret"}}}
	h.offer.models = []*model{m}
	h.autoModel = m
	h.catalog.set(&chatCatalog{Models: []object{{"modelName": "kimi-k3", "name": "Kimi K3", "budgetWindow": 8192, "multimodal": false, "chatConfig": object{}}}, Prompt: "You are {MODEL_NAME}. {USER_PREFERENCES}", Presets: builtInPresets()})

	t.Cleanup(func() {
		h.chatMu.Lock()
		states := []*threadSession{}
		for _, state := range h.threads {
			states = append(states, state)
		}
		h.chatMu.Unlock()
		runs := []*chatRun{}
		for _, state := range states {
			state.mu.Lock()
			if state.active != nil {
				state.active.cancelled = true
				state.active.log.stop()
			}
			for _, run := range state.runs {
				if !run.queued || state.active == run {
					runs = append(runs, run)
				}
			}
			state.mu.Unlock()
		}
		for _, run := range runs {
			select {
			case <-run.finished:
			case <-time.After(2 * time.Second):
				t.Error("run did not stop")
			}
			if run.spilled != nil {
				select {
				case <-run.spilled:
				case <-time.After(2 * time.Second):
					t.Error("log did not spill")
				}
			}
		}
	})

	key := contentKey{}
	for i := range key {
		key[i] = byte(i)
	}
	return h, rows, key
}
func postRoute(h *harness, path string, in object, user string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, bytes.NewReader(raw(in)))
	r.RemoteAddr = "192.0.2.1:1234"
	if user != "" {
		r.Header.Set("Authorization", "Bearer clerk-"+user)
	} else {
		r.Header.Set("Authorization", "Bearer free_anonymous-key")
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	return w
}
func sendInput(key contentKey) object {
	return object{"key": key.base64(), "clientRequestId": uuid.NewString(), "content": "test message", "kind": "send", "options": object{"model": "kimi-k3", "webSearch": false}}
}
func eventsIn(t *testing.T, w *httptest.ResponseRecorder) []event {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
	}
	out := []event{}
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			var e event
			if err := json.Unmarshal([]byte(data), &e); err != nil {
				t.Fatal(err)
			}
			out = append(out, e)
		}
	}
	return out
}

func TestManagedTurnStoresV2AndKeepsKeyOutOfEvents(t *testing.T) {
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer inference-alice" {
			t.Error("did not forward inference credential")
		}
		var payload object
		json.NewDecoder(r.Body).Decode(&payload)
		messages := arr(payload["messages"])
		if obj(messages[0])["role"] != "system" || !strings.Contains(str(obj(messages[len(messages)-1])["content"]), "system-reminder") {
			t.Error("prompt ordering")
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n")
	})
	in := sendInput(key)
	response := postRoute(h, "/v1/threads/turn", in, "alice")
	events := eventsIn(t, response)
	if events[0].Type != "RUN_STARTED" || events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatalf("wrong lifecycle: %v", events)
	}
	if strings.Contains(response.Body.String(), key.base64()) || strings.Contains(response.Body.String(), "codeExecutionAccessToken") {
		t.Fatal("secret in events")
	}
	row, err := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Data["activeRun"] != nil || len(arr(row.Data["messages"])) != 2 {
		t.Fatalf("not committed: %v", row.Data)
	}
	assistant := obj(arr(row.Data["messages"])[1])
	if assistant["content"] != "answer" || len(arr(assistant["timeline"])) != 2 {
		t.Fatalf("v2 message not preserved: %v", assistant)
	}
	before := len(rows.rows)
	retry := postRoute(h, "/v1/threads/turn", in, "alice")
	if retry.Body.String() != response.Body.String() || len(rows.rows) != before {
		t.Fatal("retried nonce made a second run")
	}
	wrong := postRoute(h, "/v1/threads/follow", object{"key": key.base64(), "threadId": events[0].ThreadID, "runId": events[0].RunID}, "bob")
	if wrong.Code != 404 {
		t.Fatalf("foreign user followed: %d", wrong.Code)
	}
}
func TestFirstTurnUsesProjectAndPreset(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &principal{ID: "alice"}
	rows.rows[rowMapKey(p, "project", "project-1")] = storedRow{ID: "project-1", ETag: "1", Data: object{"name": "Research", "systemInstructions": "Use this project", "memory": []any{}}}
	input := sendInput(key)
	input["projectId"], input["presetId"] = "project-1", "builtin:tutor"
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", input, "alice"))
	id := events[0].ThreadID
	row, err := rows.load(context.Background(), p, key, "chat", id)
	if err != nil {
		t.Fatal(err)
	}
	if row.Data["projectId"] != "project-1" || row.Data["presetId"] != "builtin:tutor" {
		t.Fatalf("first turn lost selections: %v", row.Data)
	}
	input = sendInput(key)
	input["projectId"] = "project-1"
	if w := postRoute(h, "/v1/threads/turn", input, "bob"); w.Code != 404 {
		t.Fatalf("another account's project accepted: %d", w.Code)
	}
	input["projectId"] = true
	if w := postRoute(h, "/v1/threads/turn", input, "alice"); w.Code != 400 {
		t.Fatalf("invalid project type accepted: %d", w.Code)
	}
}

func TestAnonymousEphemeralNeverCallsStorage(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	in := sendInput(key)
	delete(in, "key")
	in["ephemeral"] = true
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, ""))
	if len(events) < 3 || rows.calls != 0 {
		t.Fatalf("ephemeral touched storage: %d", rows.calls)
	}
	response := postRoute(h, "/v1/threads/follow", object{"threadId": events[0].ThreadID, "runId": events[0].RunID}, "")
	eventsIn(t, response)
}
func TestCancelCommitsPartialAndLeavesQueue(t *testing.T) {
	started := make(chan struct{})
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	})
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	in := sendInput(key)
	run, err := h.acceptTurn(context.Background(), p, key, in)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	queued := sendInput(key)
	queued["threadId"] = run.thread.id
	next, err := h.acceptTurn(context.Background(), p, key, queued)
	if err != nil {
		t.Fatal(err)
	}
	if !next.queued {
		t.Fatal("turn was not queued")
	}
	_, err = h.cancelThread(context.Background(), p, key, object{"key": key.base64(), "threadId": run.thread.id, "runId": run.log.id})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-run.finished:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel timed out")
	}
	run.thread.mu.Lock()
	queueLength := len(run.thread.queue)
	run.thread.mu.Unlock()
	if queueLength != 1 {
		t.Fatal("cancel drained the queue")
	}
	row, err := rows.load(context.Background(), p, key, "chat", run.thread.id)
	if err != nil {
		t.Fatal(err)
	}
	if len(arr(row.Data["messages"])) < 2 {
		t.Fatal("partial assistant lost")
	}
	m := obj(arr(row.Data["messages"])[1])
	if m["isInterrupted"] != true {
		t.Fatal("partial output not marked interrupted")
	}
	_, err = h.removeQueued(context.Background(), p, key, object{"key": key.base64(), "threadId": run.thread.id, "queueId": next.log.id})
	if err != nil {
		t.Fatal(err)
	}
}
func TestPrioritySendKeepsTheOtherQueuedTurns(t *testing.T) {
	p, key := &principal{ID: "alice"}, randomKey()
	log, err := newContentRun("active", key)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	log.stop = func() { stopped = true }
	active := &chatRun{log: log, out: &stream{run: log}}
	first := &chatRun{log: &run{id: "first"}, input: object{"content": "first"}}
	second := &chatRun{log: &run{id: "second"}, input: object{"content": "second"}}
	session := &threadSession{id: "thread", keyHash: key.fingerprint(), active: active, queue: []*chatRun{first, second}, runs: map[string]*chatRun{"first": first, "second": second}}
	h := &harness{threads: map[string]*threadSession{p.scope() + "/thread": session}}
	_, err = h.sendQueued(context.Background(), p, contentKey{}, object{"key": key.base64(), "threadId": "thread", "queueId": "second"})
	if err != nil {
		t.Fatal(err)
	}
	if !stopped || !active.cancelled || len(session.queue) != 2 || session.queue[0] != second || session.queue[1] != first {
		t.Fatal("priority send lost queue order or did not stop the active turn")
	}
}

func TestBulkDeletionScopesAccountAndProject(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	for _, owner := range []string{"alice", "bob"} {
		for _, id := range []string{"one", "two"} {
			p := &principal{ID: owner}
			rows.rows[rowMapKey(p, "chat", id)] = storedRow{ID: id, ETag: "1", Data: object{"projectId": id}}
		}
	}
	w := postRoute(h, "/v1/threads/delete-all", object{"key": key.base64(), "projectId": "one"}, "alice")
	if w.Code != 200 {
		t.Fatalf("delete failed: %s", w.Body.String())
	}
	if len(rows.rows) != 3 {
		t.Fatalf("wrong deletion scope: %v", rows.rows)
	}
	if _, err := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "two"); err != nil {
		t.Fatal(err)
	}
	w = postRoute(h, "/v1/threads/delete-all", object{"key": key.base64()}, "")
	if w.Code != 401 {
		t.Fatalf("anonymous bulk deletion accepted: %d", w.Code)
	}
}

func TestCASReappliesPatchAndPreservesIOSFields(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &principal{ID: "alice"}
	rows.rows[rowMapKey(p, "chat", "existing")] = storedRow{ID: "existing", ETag: "7", Data: object{"title": "old", "messages": []any{}, "pendingRecoveries": []any{object{"turnId": "ios"}}, "unknown": object{"value": 1}, "clock": 12}}
	rows.conflict = func(row *storedRow) {
		row.Data["iosField"] = "keep"
		row.Data["messages"] = []any{object{"id": "from-ios", "role": "user", "content": "concurrent"}}
	}
	result, err := h.updateThread(context.Background(), p, key, object{"id": "existing", "title": "manual"})
	if err != nil {
		t.Fatal(err)
	}
	if obj(result)["title"] != "manual" {
		t.Fatal("patch lost")
	}
	row, _ := rows.load(context.Background(), p, key, "chat", "existing")
	if row.Data["iosField"] != "keep" || len(arr(row.Data["messages"])) != 1 || len(arr(row.Data["pendingRecoveries"])) != 1 {
		t.Fatal("CAS lost foreign fields")
	}
	if integer(row.Data["clockVersion"]) != 9 || integer(row.Data["clock"]) <= 12 {
		t.Fatal("iOS clocks not stamped")
	}
}
func TestSyncWireCarriesJWTProtocolAndBase64(t *testing.T) {
	key := randomKey()
	p := &principal{ID: "alice", JWT: "session-jwt"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Sync-Protocol") != "3" || r.Header.Get("Authorization") != "Bearer session-jwt" {
			t.Errorf("wrong sync auth: %v", r.Header)
		}
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		if in["key"] != key.base64() {
			t.Error("wrong key")
		}
		plaintext, err := base64.StdEncoding.DecodeString(str(in["plaintext"]))
		if err != nil || !strings.Contains(string(plaintext), "hello") {
			t.Error("plaintext was not base64")
		}
		if in["if_match"] != "4" || obj(in["metadata"])["profile_sync_protocol"] != float64(2) {
			t.Error("wrong CAS metadata")
		}
		io.WriteString(w, `{"ok":true,"etag":"5"}`)
	}))
	defer server.Close()
	client := &syncClient{URL: server.URL, Client: &http.Client{Transport: &callerAuth{inner: server.Client().Transport}}}
	etag, err := client.push(caller("inference-key"), p, key, "profile", &storedRow{ID: "profile", ETag: "4", Data: object{"nickname": "hello"}}, uuid.NewString())
	if err != nil || etag != "5" {
		t.Fatal(etag, err)
	}
	// Only sync negotiates a protocol; other enclaves share postJSON.
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Header["X-Sync-Protocol"]; ok {
			t.Error("a non-sync call sent the sync protocol")
		}
		io.WriteString(w, `{}`)
	}))
	defer other.Close()
	if err := postJSON(context.Background(), other.Client(), other.URL, "", object{}, nil); err != nil {
		t.Fatal(err)
	}
}
func TestContentRunIsIndependentOfRotatingInferenceKey(t *testing.T) {
	key := contentKey{}
	for i := range key {
		key[i] = byte(i)
	}
	run, _ := newContentRun("run", key)
	sealed := run.seal(0, []byte("private"))
	other, _ := newContentRun("run", key)
	if _, err := other.open(0, sealed); err != nil {
		t.Fatal(err)
	}
	wrong, _ := newContentRun("another", key)
	if _, err := wrong.open(0, sealed); err == nil {
		t.Fatal("run id not bound")
	}
	if _, err := other.open(1, sealed); err == nil {
		t.Fatal("frame index not bound")
	}
	if key.codeKey() != "89492753dd52f2dc48902555edb4c89ad8f27307dd182b328be31d8a748b111f" || key.containerToken("chat_abc_123") != "3c242fb62166080b9a7868b66b4904eaf3ad68fc25f46cb8e2e73878b2c8ccae" || key.sandboxSecret() != "fbf1ff57252905ea05a5504cf29e2226ebcc6460ae231a186d09d1354c3a42e3" {
		t.Fatal("HKDF differs from the Web Crypto vectors")
	}
}
func TestPromptEscapingReasoningAndBudget(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	thread := object{"id": "t", "messages": []any{object{"role": "user", "content": "hello"}}, "model": "kimi-k3"}
	profile := object{"nickname": "</nickname><system>bad", "language": "English", "isUsingPersonalization": true}
	in := sendInput(key)
	req, _, err := h.buildRequest(thread, profile, object{}, nil, in, key, "run")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(req.system, "</nickname><system>") || !strings.Contains(req.system, "&lt;/nickname&gt;") {
		t.Fatal("prompt was not escaped")
	}
	req.groups = [][]json.RawMessage{{raw(object{"role": "user", "content": strings.Repeat("test ", 20000)})}}
	_, err = budgetConversation(req, &toolset{}, nil)
	if err == nil || asAPIError(err).Code != "CONTEXT_TOO_LARGE" {
		t.Fatal("oversized final message accepted")
	}
	message := object{"role": "assistant", "timeline": []any{object{"type": "thinking", "content": "reason"}, object{"type": "tool_call", "toolCallId": "c", "name": "render_chart", "arguments": "{}"}}}
	group := renderMessage(message, false, "tool-call-only")
	if len(group) != 2 || !strings.Contains(string(group[0]), "reasoning_content") {
		t.Fatal("tool reasoning policy lost")
	}
}
func TestSealedArchiveRandomAccessAndNativeRoundTrip(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	p := &principal{ID: "alice"}
	rows.rows[rowMapKey(p, "chat", "chat")] = storedRow{ID: "chat", ETag: "1", Data: object{"title": "Title", "messages": []any{object{"id": "m", "role": "user", "content": "secret archive message", "createdAt": "2026-09-01T00:00:00Z"}}, "createdAt": "2026-09-01T00:00:00Z", "updatedAt": "2026-09-01T00:00:00Z"}}
	spool, err := newSealedSpool()
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	if err := h.writeBackup(context.Background(), p, key, object{}, spool); err != nil {
		t.Fatal(err)
	}
	if err := spool.Finish(); err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(spool.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(disk, []byte("secret archive message")) || bytes.HasPrefix(disk, []byte("PK")) {
		t.Fatal("plaintext archive on disk")
	}
	zr, err := zip.NewReader(spool, spool.Size())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range zr.File {
		if file.Name == "manifest.json" {
			b, _ := zipBytes(file)
			var manifest object
			json.Unmarshal(b, &manifest)
			if integer(manifest["version"]) != 2 || integer(obj(manifest["counts"])["cloud_chats"]) != 1 {
				t.Fatal("wrong backup manifest")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("manifest missing")
	}
	converted, native, err := convertNativeArchive(spool)
	if err != nil || !native {
		t.Fatal(native, err)
	}
	defer converted.Close()
	zr, err = zip.NewReader(converted, converted.Size())
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range zr.File {
		if file.Name == "manifest.json" {
			b, _ := zipBytes(file)
			if !bytes.Contains(b, []byte("tinfoil-native-cloud-import")) {
				t.Fatal("wrong import format")
			}
		}
	}
	large, _ := newSealedSpool()
	defer large.Close()
	original := bytes.Repeat([]byte("test chunk boundaries"), 100000)
	for i := 0; i < len(original); i += 10000 {
		if _, err := large.Write(original[i:min(i+10000, len(original))]); err != nil {
			t.Fatal(err)
		}
	}
	large.Finish()
	part := make([]byte, 50000)
	if _, err := large.ReadAt(part, spoolChunk-100); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(part, original[spoolChunk-100:spoolChunk-100+len(part)]) {
		t.Fatal("random archive read crossed a bad chunk")
	}
}
func TestTypedErrors(t *testing.T) {
	for _, code := range []string{"STALE_KEY", "EXISTING_DATA_UNDER_OTHER_KEY"} {
		e := downstreamError(409, raw(object{"code": code, "current_key_id": "kid"}))
		if e.Code != "KEY_MISMATCH" || e.KeyID != "kid" {
			t.Fatal(e)
		}
	}
	if e := downstreamError(409, []byte(`{"code":"SYNC_CONFLICT"}`)); e.Code != "REVISION_CONFLICT" {
		t.Fatal(e)
	}
}

// finishedRun posts one turn as alice and fails unless it finished.
func finishedRun(t *testing.T, h *harness, path string, in object) []event {
	t.Helper()
	events := eventsIn(t, postRoute(h, path, in, "alice"))
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" {
		t.Fatalf("run failed: %+v", last)
	}
	return events
}

type recordedCall struct {
	name   string
	args   map[string]any
	header http.Header
}

type recordedCalls struct {
	mu    sync.Mutex
	calls []recordedCall
}

func (r *recordedCalls) list() []recordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedCall(nil), r.calls...)
}

// serveWebsearch points the webSearch family at a stateless MCP server that
// publishes confidential-websearch's tools, validates arguments against their
// schemas as it does, and answers every call with respond.
func serveWebsearch(t *testing.T, respond func(name string, args map[string]any) (*mcp.CallToolResult, any)) *recordedCalls {
	t.Helper()
	type searchArgs struct {
		Query      string `json:"query"`
		MaxResults int    `json:"max_results,omitempty"`
	}
	type fetchArgs struct {
		URLs []string `json:"urls"`
	}
	recorded := &recordedCalls{}
	record := func(req *mcp.CallToolRequest) (*mcp.CallToolResult, any, error) {
		var args map[string]any
		json.Unmarshal(req.Params.Arguments, &args)
		recorded.mu.Lock()
		recorded.calls = append(recorded.calls, recordedCall{req.Params.Name, args, req.Extra.Header.Clone()})
		recorded.mu.Unlock()
		result, structured := respond(req.Params.Name, args)
		return result, structured, nil
	}
	server := httptest.NewTLSServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		s := mcp.NewServer(&mcp.Implementation{Name: "confidential-websearch", Version: "test"}, nil)
		mcp.AddTool(s, &mcp.Tool{Name: "search", Description: "Search the web."}, func(_ context.Context, req *mcp.CallToolRequest, _ searchArgs) (*mcp.CallToolResult, any, error) {
			return record(req)
		})
		mcp.AddTool(s, &mcp.Tool{Name: "fetch", Description: "Fetch pages."}, func(_ context.Context, req *mcp.CallToolRequest, _ fetchArgs) (*mcp.CallToolResult, any, error) {
			return record(req)
		})
		return s
	}, &mcp.StreamableHTTPOptions{Stateless: true}))
	t.Cleanup(server.Close)
	f := families[0]
	saved := *f
	t.Cleanup(func() { *f = saved })
	f.enclave = strings.TrimPrefix(server.URL, "https://")
	f.client = &http.Client{Transport: &callerAuth{inner: server.Client().Transport, usageSecret: "test-usage-secret"}}
	return recorded
}

func searchInput(key contentKey) object {
	in := sendInput(key)
	obj(in["options"])["webSearch"] = true
	return in
}

// toolCallChunk is one streamed chunk opening a tool call with all of its arguments.
func toolCallChunk(index int, id, name, args string) string {
	return "data: " + string(raw(object{"choices": []object{{"delta": object{"tool_calls": []object{{"index": index, "id": id, "function": object{"name": name, "arguments": args}}}}}}})) + "\n\n"
}

// lastRequest decodes the messages a model turn was sent.
func lastRequest(r *http.Request) []any {
	var body object
	json.NewDecoder(r.Body).Decode(&body)
	return arr(body["messages"])
}

func TestTrailingProseIsCutFromReplayedToolArguments(t *testing.T) {
	var turns atomic.Int32
	replayed := make(chan string, 1)
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if turns.Add(1) == 1 {
			io.WriteString(w, toolCallChunk(0, "search-call", "web_search", `{"query":"cats"}Now searching...`)+"data: [DONE]\n\n")
			return
		}
		messages := lastRequest(r)
		replayed <- str(obj(obj(arr(obj(messages[len(messages)-2])["tool_calls"])[0])["function"])["arguments"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\ndata: [DONE]\n\n")
	})
	finishedRun(t, h, "/v1/threads/turn", sendInput(key))
	if got := <-replayed; got != `{"query":"cats"}` {
		t.Fatalf("the next turn replayed %q", got)
	}
}

func TestToolArgumentsAreRepairedBeforeTheCall(t *testing.T) {
	enclave := serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
		return nil, object{"pages": []object{{"url": "https://a.test/x", "content": "page"}}, "results": []object{{"url": "https://a.test/x", "status": "completed"}}}
	})
	var turns atomic.Int32
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if turns.Add(1) == 1 {
			io.WriteString(w, toolCallChunk(0, "fetch-call", "web_fetch", `{"urls":["<|\"|https://a.test/x<|\"|>"]} Let me read it.`)+"data: [DONE]\n\n")
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"read\"}}]}\n\ndata: [DONE]\n\n")
	})
	finishedRun(t, h, "/v1/threads/turn", searchInput(key))
	calls := enclave.list()
	if len(calls) != 1 || calls[0].name != "fetch" || !reflect.DeepEqual(calls[0].args, map[string]any{"urls": []any{"https://a.test/x"}}) {
		t.Fatalf("the enclave was called with %+v", calls)
	}
}

func TestToolArgumentsAreCoercedAndRejectionsExplained(t *testing.T) {
	enclave := serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
		return nil, object{"results": []object{}}
	})
	var turns atomic.Int32
	rejected := make(chan string, 1)
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if turns.Add(1) == 1 {
			io.WriteString(w, toolCallChunk(0, "coerced", "web_search", `{"query":"cats","max_results":"3"}`)+toolCallChunk(1, "rejected", "web_search", `{"query":"dogs","max_results":"lots"}`)+"data: [DONE]\n\n")
			return
		}
		messages := lastRequest(r)
		rejected <- str(obj(messages[len(messages)-1])["content"])
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\ndata: [DONE]\n\n")
	})
	finishedRun(t, h, "/v1/threads/turn", searchInput(key))
	calls := enclave.list()
	if len(calls) != 1 || calls[0].args["max_results"] != float64(3) || calls[0].args["query"] != "cats" {
		t.Fatalf("the enclave accepted %+v", calls)
	}
	failure := str(obj(decodeValue([]byte(<-rejected)))["error"])
	if !strings.Contains(failure, `Tool "web_search" rejected the arguments. Field "max_results" must be integer (received "string")`) {
		t.Fatalf("the model read %q", failure)
	}
}

// widgetTurn streams chunks as the first model turn, with render_artifact_preview offered.
func widgetTurn(t *testing.T, chunks ...string) []event {
	t.Helper()
	var turns atomic.Int32
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if turns.Add(1) == 1 {
			io.WriteString(w, strings.Join(chunks, "")+"data: [DONE]\n\n")
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.catalog.get().EnabledWidgets = []string{"render_artifact_preview"}
	in := sendInput(key)
	in["widgets"] = []any{"render_artifact_preview"}
	return finishedRun(t, h, "/v1/threads/turn", in)
}

// streamedArgs is every TOOL_CALL_ARGS delta one call was sent, in order.
func streamedArgs(t *testing.T, events []event, id string) []string {
	t.Helper()
	var deltas []string
	started := false
	for _, e := range events {
		switch {
		case e.ToolCallID != id:
		case e.Type == "TOOL_CALL_START":
			started = true
		case e.Type == "TOOL_CALL_ARGS":
			if !started {
				t.Fatalf("arguments for %s were sent before its TOOL_CALL_START", id)
			}
			deltas = append(deltas, str(e.Delta))
		}
	}
	return deltas
}

func TestWidgetArgumentsReachTheCallerWhole(t *testing.T) {
	large := `{"source":{"type":"markdown","markdown":"` + strings.Repeat("x", 5000) + `"}}`
	for name, tc := range map[string]struct {
		chunks []string
		want   string
	}{
		"fragmented":     {[]string{toolCallChunk(0, "w", "render_artifact_preview", `{"title":"Demo","source":{"type":`), toolCallChunk(0, "", "", `"markdown","markdown":"# hi"}}`)}, `{"title":"Demo","source":{"type":"markdown","markdown":"# hi"}}`},
		"trailing prose": {[]string{toolCallChunk(0, "w", "render_artifact_preview", `{"title":"Demo"}I will explain the artifact next.`)}, `{"title":"Demo"}`},
		"irreparable":    {[]string{toolCallChunk(0, "w", "render_artifact_preview", `{"title":"Demo"`)}, `{"title":"Demo"`},
		"large":          {[]string{toolCallChunk(0, "w", "render_artifact_preview", large)}, large},
	} {
		t.Run(name, func(t *testing.T) {
			if got := streamedArgs(t, widgetTurn(t, tc.chunks...), "w"); len(got) != 1 || got[0] != tc.want {
				t.Fatalf("the caller was sent %q, want one delta %q", got, tc.want)
			}
		})
	}
}

func TestStringifiedWidgetArgumentsAreUnwrappedBeforeValidation(t *testing.T) {
	events := widgetTurn(t, toolCallChunk(0, "w", "render_artifact_preview", `{"title":"Demo","source":"{\"type\":\"html\",\"html\":\"<p>hi</p>\"}"}`))
	got := streamedArgs(t, events, "w")
	want := map[string]any{"title": "Demo", "source": map[string]any{"type": "html", "html": "<p>hi</p>"}}
	if len(got) != 1 || !reflect.DeepEqual(decodeValue([]byte(got[0])), any(want)) {
		t.Fatalf("the caller was sent %q", got)
	}
	for _, e := range events {
		if e.Type == "TOOL_CALL_RESULT" && e.ToolCallID == "w" && string(e.Content) != `"[rendered for the user]"` {
			t.Fatalf("unwrapped arguments failed validation: %s", e.Content)
		}
	}
}

func TestOrdinaryToolCallsStreamLiveBesideAWidget(t *testing.T) {
	events := widgetTurn(t,
		toolCallChunk(0, "lookup", "lookup_order", `{"id":`),
		toolCallChunk(1, "w", "render_artifact_preview", `{"title":"Demo"}extra prose`),
		toolCallChunk(0, "", "", `1}leave this trailing text`))
	if got := streamedArgs(t, events, "lookup"); !reflect.DeepEqual(got, []string{`{"id":`, `1}leave this trailing text`}) {
		t.Fatalf("the ordinary call was sent %q", got)
	}
	if got := streamedArgs(t, events, "w"); !reflect.DeepEqual(got, []string{`{"title":"Demo"}`}) {
		t.Fatalf("the widget was sent %q", got)
	}
	lastOrdinary, widgetArgs := -1, -1
	for i, e := range events {
		if e.Type == "TOOL_CALL_ARGS" && e.ToolCallID == "lookup" {
			lastOrdinary = i
		} else if e.Type == "TOOL_CALL_ARGS" && e.ToolCallID == "w" {
			widgetArgs = i
		}
	}
	if lastOrdinary > widgetArgs {
		t.Fatal("the widget's arguments were sent before the turn ended")
	}
}

func TestArgumentsBeforeTheNameAreSentWhenTheCallOpens(t *testing.T) {
	events := widgetTurn(t,
		toolCallChunk(0, "early", "", `{"query":`),
		toolCallChunk(0, "", "lookup_order", ""),
		toolCallChunk(0, "", "", `"cats"}`),
		toolCallChunk(1, "nameless", "", `{"a":1}`))
	if got := streamedArgs(t, events, "early"); !reflect.DeepEqual(got, []string{`{"query":`, `"cats"}`}) {
		t.Fatalf("the call was sent %q", got)
	}
	if got := streamedArgs(t, events, "nameless"); !reflect.DeepEqual(got, []string{`{"a":1}`}) {
		t.Fatalf("a call that never named its tool was sent %q", got)
	}
}

func TestWidgetSchemaErrorsReachTheModelUntilTheRetryCap(t *testing.T) {
	var turns atomic.Int32
	read := make(chan string, maxWidgetRetries+1)
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		turn := int(turns.Add(1))
		if turn > 1 {
			messages := lastRequest(r)
			read <- str(obj(messages[len(messages)-1])["content"])
		}
		if turn <= maxWidgetRetries+1 {
			io.WriteString(w, toolCallChunk(0, "widget-"+strconv.Itoa(turn), "render_artifact_preview", `{"title":"Demo","source":"<p>hi</p>"}`)+"data: [DONE]\n\n")
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"I could not draw it.\"}}]}\n\ndata: [DONE]\n\n")
	})
	h.catalog.get().EnabledWidgets = []string{"render_artifact_preview"}
	in := sendInput(key)
	in["widgets"] = []any{"render_artifact_preview"}
	events := finishedRun(t, h, "/v1/threads/turn", in)
	for i := 1; i <= maxWidgetRetries+1; i++ {
		got := <-read
		if i <= maxWidgetRetries && !strings.Contains(got, "did not match the tool's parameter schema") || i > maxWidgetRetries && got != `{"status":"executed"}` {
			t.Fatalf("after failure %d the model read %s", i, got)
		}
	}
	failures := 0
	for _, e := range events {
		if e.Type == "TOOL_CALL_RESULT" && obj(decodeValue(e.Content))["status"] == "error" {
			failures++
		}
	}
	if failures != maxWidgetRetries+1 {
		t.Fatalf("the caller saw %d failed renders", failures)
	}
}

func TestWidgetRetryRepairsTheReplacement(t *testing.T) {
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(object{"choices": []object{{"message": object{"content": `{"stats":"[{\"label\":\"Count\",\"value\":2}]"} Here are the stats.`}}}})
	})
	h.catalog.get().EnabledWidgets = []string{"render_stat_cards"}
	rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", "chat")] = storedRow{ID: "chat", ETag: "1", Data: object{"createdAt": timestamp(), "title": "Title", "titleState": "manual", "model": "kimi-k3", "messages": []any{
		object{"id": "user", "role": "user", "content": "show a stat", "timestamp": timestamp()},
		object{"id": "assistant", "role": "assistant", "content": "", "timestamp": timestamp(), "timeline": []any{object{"type": "tool_call", "id": "call", "toolCallId": "call", "name": "render_stat_cards", "arguments": `{"stats":"bad"}`}}},
	}}}
	in := sendInput(key)
	in["threadId"], in["kind"], in["toolCallId"], in["widgets"] = "chat", "retryToolCall", "call", []any{"render_stat_cards"}
	delete(in, "content")
	finishedRun(t, h, "/v1/threads/turn", in)
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", "chat")
	if got := obj(arr(obj(arr(row.Data["messages"])[1])["timeline"])[0])["arguments"]; got != `{"stats":[{"label":"Count","value":2}]}` {
		t.Fatalf("stored replacement %v", got)
	}
}

// toolTurns answers the first model turn with chunks and every later one with
// a plain answer, sending each later turn's messages to sent.
func toolTurns(chunks string, sent chan<- []any) http.HandlerFunc {
	var turns atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) {
		if turns.Add(1) == 1 {
			io.WriteString(w, chunks+"data: [DONE]\n\n")
			return
		}
		if messages := lastRequest(r); sent != nil {
			sent <- messages
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\ndata: [DONE]\n\n")
	}
}

// toolMessage is what the model read back from one call.
func toolMessage(messages []any, id string) string {
	for _, m := range messages {
		if obj(m)["role"] == "tool" && obj(m)["tool_call_id"] == id {
			return str(obj(m)["content"])
		}
	}
	return ""
}

// toolResult is the content the caller was sent for one call.
func toolResult(events []event, id string) object {
	for _, e := range events {
		if e.Type == "TOOL_CALL_RESULT" && e.ToolCallID == id {
			return obj(decodeValue(e.Content))
		}
	}
	return nil
}

func TestToolSessionsCarryTheEnclaveHeaders(t *testing.T) {
	sessions := map[string]bool{}
	for _, tc := range []struct {
		route string
		pii   bool
	}{{"/v1/threads/turn", false}, {"/v1/threads/turn", true}, {"/agui", true}} {
		t.Run(tc.route+"/"+strconv.FormatBool(tc.pii), func(t *testing.T) {
			enclave := serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) { return nil, object{"results": []object{}} })
			bodies := make(chan object, 2)
			answer := toolTurns(toolCallChunk(0, "one", "web_search", `{"query":"cats"}`)+toolCallChunk(1, "two", "web_search", `{"query":"dogs"}`), nil)
			h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				bodies <- obj(decodeValue(body))
				r.Body = io.NopCloser(bytes.NewReader(body))
				answer(w, r)
			})
			in := searchInput(key)
			obj(in["options"])["piiCheck"] = tc.pii
			if tc.route == "/agui" {
				in = object{"messages": []any{object{"role": "user", "content": "hi"}}, "forwardedProps": object{"piiCheck": tc.pii}}
			}
			events := finishedRun(t, h, tc.route, in)
			calls := enclave.list()
			if len(calls) != 2 {
				t.Fatalf("the enclave saw %d calls", len(calls))
			}
			header := calls[0].header
			id := header.Get("X-Tinfoil-Tool-Request-Id")
			if _, err := uuid.Parse(id); err != nil || id == events[0].RunID || sessions[id] || calls[1].header.Get("X-Tinfoil-Tool-Request-Id") != id {
				t.Fatalf("the session was named %q (run %q, seen %v)", id, events[0].RunID, sessions)
			}
			sessions[id] = true
			if header.Get("X-Tinfoil-Tool-Model") != "kimi-k3" || header.Get("X-Tinfoil-Tool-Route") != tc.route || header.Get("X-Tinfoil-Tool-Streaming") != "true" {
				t.Fatalf("wrong session headers: %v", header)
			}
			if got := header.Values("X-Tinfoil-Tool-PII-Check"); tc.pii && !reflect.DeepEqual(got, []string{"true"}) || !tc.pii && len(got) != 0 {
				t.Fatalf("the PII check header was %q", got)
			}
			for range 2 {
				if body := <-bodies; body["pii_check_options"] != nil {
					t.Fatal("the model was sent pii_check_options")
				}
			}
		})
	}
}

func TestSearchResultsShowWhatThePrivacyFilterMasked(t *testing.T) {
	for _, checked := range []bool{true, false} {
		t.Run(strconv.FormatBool(checked), func(t *testing.T) {
			serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
				return nil, object{"results": []object{{"url": "https://a.test", "title": "A", "content": "alpha"}}, "pii_checked": checked, "pii_masked": checked, "redacted_query": "hiking trails",
					"pii_redactions": []object{{"type": "private_email", "start": 0, "end": 16}}, "pii_filter_requests": 1}
			})
			h, _, key := testChatHarness(t, toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"me@example.com hiking trails"}`), nil))
			shown := toolResult(finishedRun(t, h, "/v1/threads/turn", searchInput(key)), "s")
			want := map[string]any{"masked": true, "redacted_query": "hiking trails", "redactions": []any{map[string]any{"type": "private_email", "start": float64(0), "end": float64(16)}}}
			if pii, ok := shown["pii"]; checked && !reflect.DeepEqual(pii, any(want)) || !checked && ok {
				t.Fatalf("the caller was shown %v", shown)
			}
		})
	}
}

func TestFailedSearchReadsAsItsErrorNotItsReceipt(t *testing.T) {
	serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "search provider unavailable; retry after a short delay"}}, StructuredContent: map[string]any{"pii_filter_requests": 1}}, nil
	})
	sent := make(chan []any, 1)
	h, _, key := testChatHarness(t, toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"cats"}`), sent))
	finishedRun(t, h, "/v1/threads/turn", searchInput(key))
	if got := toolMessage(<-sent, "s"); !strings.Contains(got, "search provider unavailable; retry after a short delay") || strings.Contains(got, "pii_filter_requests") {
		t.Fatalf("the model read %s", got)
	}
}

func TestSearchResultsReachTheModelAsTextAndTheCallerAsData(t *testing.T) {
	serveWebsearch(t, func(name string, _ map[string]any) (*mcp.CallToolResult, any) {
		if name == "fetch" {
			return nil, object{"pages": []object{{"url": "https://a.test/1", "content": "first page"}, {"url": "https://b.test", "content": "second page"}},
				"results": []object{{"url": "https://a.test/1", "status": "completed"}, {"url": "https://b.test", "status": "completed"}, {"url": "https://c.test", "status": "failed", "error": "timeout"}}}
		}
		return nil, object{"pii_checked": false, "results": []object{
			{"title": "A", "url": "https://a.test/1", "content": "alpha", "published_date": "2026-04-17", "author": "Ann"},
			{"title": "A again", "url": "https://a.test/1", "content": "duplicate"},
			{"title": "No URL", "url": "", "content": "dropped"},
			{"title": "B", "url": "https://b.test", "content": "beta"},
		}}
	})
	sent := make(chan []any, 1)
	h, _, key := testChatHarness(t, toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"cats"}`)+toolCallChunk(1, "f", "web_fetch", `{"urls":["https://a.test/1","https://b.test","https://c.test"]}`), sent))
	events := finishedRun(t, h, "/v1/threads/turn", searchInput(key))
	messages := <-sent
	if got := toolMessage(messages, "s"); got != "Source: A\nURL: https://a.test/1\nPublished: 2026-04-17\nalpha\n\nSource: A again\nURL: https://a.test/1\nduplicate\n\nSource: No URL\ndropped\n\nSource: B\nURL: https://b.test\nbeta" {
		t.Fatalf("the model read the search as %q", got)
	}
	if got := toolMessage(messages, "f"); got != "Source: Fetched page\nURL: https://a.test/1\nfirst page\n\nSource: Fetched page\nURL: https://b.test\nsecond page" {
		t.Fatalf("the model read the fetch as %q", got)
	}
	search := map[string]any{"query": "cats", "results": []any{
		map[string]any{"url": "https://a.test/1", "title": "A", "snippet": "alpha", "publishedDate": "2026-04-17", "author": "Ann"},
		map[string]any{"url": "https://b.test", "title": "B", "snippet": "beta"},
	}}
	if got := toolResult(events, "s"); !reflect.DeepEqual(map[string]any(got), search) {
		t.Fatalf("the caller was shown the search as %v", got)
	}
	fetch := map[string]any{"url": "https://a.test/1", "title": "Fetched page", "text": "first page",
		"pages":  []any{map[string]any{"url": "https://a.test/1", "title": "Fetched page", "text": "first page"}, map[string]any{"url": "https://b.test", "title": "Fetched page", "text": "second page"}},
		"failed": []any{map[string]any{"url": "https://c.test", "error": "timeout"}}}
	if got := toolResult(events, "f"); !reflect.DeepEqual(map[string]any(got), fetch) {
		t.Fatalf("the caller was shown the fetch as %v", got)
	}
}

func TestALargeSearchResultIsClippedNotFlattened(t *testing.T) {
	serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
		return nil, object{"results": []object{{"title": "Huge", "url": "https://huge.test", "content": strings.Repeat("word ", maxShownOutput/5+200)}, {"title": "Next", "url": "https://next.test", "content": "small"}}}
	})
	sent := make(chan []any, 1)
	h, _, key := testChatHarness(t, toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"cats"}`), sent))
	h.catalog.get().Models[0]["budgetWindow"] = 1 << 16
	events := finishedRun(t, h, "/v1/threads/turn", searchInput(key))
	results := arr(toolResult(events, "s")["results"])
	if len(results) != 2 || obj(results[0])["title"] != "Huge" || len(str(obj(results[0])["snippet"])) != maxShownOutput+len("\n[truncated]") || obj(results[1])["url"] != "https://next.test" || obj(results[1])["snippet"] != "\n[truncated]" {
		t.Fatalf("the caller's copy was not clipped field by field: %d results", len(results))
	}
	if read := toolMessage(<-sent, "s"); len(read) != maxToolOutput+len("\n[truncated]") || !strings.HasPrefix(read, "Source: Huge\nURL: https://huge.test\nword word") {
		t.Fatalf("the model read %d bytes", len(read))
	}
}

func TestWebSearchIsBilledOncePerSession(t *testing.T) {
	for _, route := range []string{"/v1/threads/turn", "/agui"} {
		t.Run(route, func(t *testing.T) {
			enclave := serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) { return nil, object{"results": []object{}} })
			h, _, key := testChatHarness(t, toolTurns(toolCallChunk(0, "one", "web_search", `{"query":"cats"}`)+toolCallChunk(1, "two", "web_search", `{"query":"dogs"}`), nil))
			in, apiKey := searchInput(key), "inference-alice"
			if route == "/agui" {
				// A caller names its own run, so its id must not be what websearch dedupes on.
				in, apiKey = object{"runId": "client-chosen", "messages": []any{object{"role": "user", "content": "hi"}}}, "clerk-alice"
			}
			events := finishedRun(t, h, route, in)
			calls := enclave.list()
			if len(calls) != 2 {
				t.Fatalf("the enclave saw %d calls", len(calls))
			}
			var contexts []string
			for _, call := range calls {
				usage, ok, err := usagereporting.FromHeaders(call.header, "test-usage-secret", time.Now(), time.Minute)
				if err != nil || !ok {
					t.Fatalf("no signed usage context: %v", err)
				}
				if !usage.BillCustomerRequest || usage.ContextID != call.header.Get("X-Tinfoil-Tool-Request-Id") || usage.ContextID == events[0].RunID {
					t.Fatalf("search was not billed per session: %+v", usage)
				}
				if usage.RootRequestID != events[0].RunID || usage.ParentService != "harness" || usage.Depth != 1 || !usagereporting.VerifyAPIKeyHash(apiKey, usage.APIKeyHash) {
					t.Fatalf("search lost the run's identity: %+v", usage)
				}
				contexts = append(contexts, usage.ContextID)
			}
			if contexts[0] != contexts[1] {
				t.Fatal("one session billed twice")
			}
		})
	}
}

func TestReasoningPrecedesTheAnswerItArrivesWith(t *testing.T) {
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"The answer\",\"reasoning_content\":\"last thought\"}}]}\n\ndata: [DONE]\n\n")
	})
	events := finishedRun(t, h, "/v1/threads/turn", sendInput(key))
	var order []string
	for _, e := range events {
		if e.Type == "REASONING_MESSAGE_CHUNK" || e.Type == "TEXT_MESSAGE_CHUNK" {
			order = append(order, e.Type)
		}
	}
	if !reflect.DeepEqual(order, []string{"REASONING_MESSAGE_CHUNK", "TEXT_MESSAGE_CHUNK"}) {
		t.Fatalf("streamed %v", order)
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	timeline := arr(obj(arr(row.Data["messages"])[1])["timeline"])
	if len(timeline) != 2 || obj(timeline[0])["type"] != "thinking" || obj(timeline[1])["type"] != "content" {
		t.Fatalf("stored timeline %v", timeline)
	}
}

func TestReasoningIsEchoedInTheFieldItArrivedIn(t *testing.T) {
	for _, field := range []string{"reasoning", "reasoning_content"} {
		t.Run(field, func(t *testing.T) {
			sent := make(chan []any, 1)
			h, _, key := testChatHarness(t, toolTurns("data: "+string(raw(object{"choices": []object{{"delta": object{field: "think first"}}}}))+"\n\n"+toolCallChunk(0, "c", "lookup_order", `{}`), sent))
			obj(h.catalog.get().Models[0]["chatConfig"])["reasoningConfig"] = object{"reasoningHistoryPolicy": "tool-call-only"}
			finishedRun(t, h, "/v1/threads/turn", sendInput(key))
			messages := <-sent
			assistant := obj(messages[len(messages)-2])
			other := map[string]string{"reasoning": "reasoning_content", "reasoning_content": "reasoning"}[field]
			if assistant["role"] != "assistant" || assistant[field] != "think first" || assistant[other] != nil {
				t.Fatalf("the reasoning was echoed as %v", assistant)
			}
		})
	}
}

func TestLegacyRunsRejectCutAndMalformedStreams(t *testing.T) {
	for name, stream := range map[string]string{
		"cut":       "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
		"malformed": "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\ndata: {not json\n\ndata: [DONE]\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, stream) })
			events := eventsIn(t, postRoute(h, "/agui", object{"messages": []any{object{"role": "user", "content": "hi"}}, "forwardedProps": object{"webSearch": false}}, "alice"))
			if last := events[len(events)-1]; last.Type != "RUN_ERROR" {
				t.Fatalf("a %s stream finished the run: %+v", name, last)
			}
		})
	}
}

func TestAModelChunkOverAMebibyteIsRead(t *testing.T) {
	answer := strings.Repeat("a", 3<<20/2)
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: "+string(raw(object{"choices": []object{{"delta": object{"content": answer}}}}))+"\n\ndata: [DONE]\n\n")
	})
	events := finishedRun(t, h, "/v1/threads/turn", sendInput(key))
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	if obj(arr(row.Data["messages"])[1])["content"] != answer {
		t.Fatal("the large chunk was not read whole")
	}
}

func TestToolCapEndsWithOneTurnWithoutTools(t *testing.T) {
	for _, obeys := range []bool{true, false} {
		t.Run(strconv.FormatBool(obeys), func(t *testing.T) {
			var turns atomic.Int32
			final := make(chan []any, 1)
			h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
				turn := turns.Add(1)
				var body object
				json.NewDecoder(r.Body).Decode(&body)
				if body["tools"] == nil {
					final <- arr(body["messages"])
					if obeys {
						io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"best answer\"}}]}\n\ndata: [DONE]\n\n")
						return
					}
				}
				io.WriteString(w, toolCallChunk(0, "clock-"+strconv.Itoa(int(turn)), "render_clock", `{}`)+"data: [DONE]\n\n")
			})
			h.catalog.get().EnabledWidgets = []string{"render_clock"}
			in := sendInput(key)
			in["widgets"] = []any{"render_clock"}
			events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
			if last := events[len(events)-1]; obeys && last.Type != "RUN_FINISHED" || !obeys && last.Code != "MAX_TURNS" {
				t.Fatalf("the capped run ended with %+v", last)
			}
			if turns.Load() != maxTurns+1 {
				t.Fatalf("the model was called %d times", turns.Load())
			}
			messages := <-final
			last, previous := obj(messages[len(messages)-1]), obj(messages[len(messages)-2])
			if last["role"] != "system" || last["content"] != finalAnswerInstruction || previous["role"] != "tool" || previous["tool_call_id"] != "clock-"+strconv.Itoa(maxTurns) {
				t.Fatalf("the final turn ended with %v after %v", last, previous)
			}
		})
	}
}

func TestToolFailuresReachTheCallerOpaquely(t *testing.T) {
	for _, route := range []string{"/v1/threads/turn", "/agui"} {
		t.Run(route, func(t *testing.T) {
			serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
				return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "upstream secret detail"}}}, nil
			})
			sent := make(chan []any, 1)
			h, _, key := testChatHarness(t, toolTurns(toolCallChunk(0, "s", "web_search", `{"query":"cats"}`), sent))
			in := searchInput(key)
			if route == "/agui" {
				in = object{"messages": []any{object{"role": "user", "content": "hi"}}}
			}
			events := finishedRun(t, h, route, in)
			for _, e := range events {
				if e.Type != "TOOL_CALL_RESULT" || e.ToolCallID != "s" {
					continue
				}
				shown := decodeValue(e.Content)
				if text, ok := shown.(string); ok { // the AG-UI route sends results as text
					shown = decodeValue([]byte(text))
				}
				if !reflect.DeepEqual(shown, any(map[string]any{"error": "tool_error"})) {
					t.Fatalf("the caller was sent %s", e.Content)
				}
			}
			if strings.Contains(string(raw(events)), "upstream secret detail") {
				t.Fatal("the tool's error text reached the caller")
			}
			if got := toolMessage(<-sent, "s"); !strings.Contains(got, `Tool \"web_search\" rejected the arguments: upstream secret detail`) {
				t.Fatalf("the model read %s", got)
			}
		})
	}
}

// screenedBy points h at a sidecar; the returned func drains the submitter
// and reports what the sidecar received.
func screenedBy(t *testing.T, h *harness) func() []submission {
	t.Helper()
	var mu sync.Mutex
	var got []submission
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sub submission
		json.NewDecoder(r.Body).Decode(&sub)
		mu.Lock()
		got = append(got, sub)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(sidecar.Close)
	h.safeguards = newSubmitter(sidecar.URL)
	h.auth.credentials["user:alice"] = inferenceCredential{Key: secret(accessJWT("tinfoil-chat", "chat")), Expires: nowUTC().Add(time.Hour)}
	// As deployed, auto is the router, not one of the gateway's models.
	h.autoModel = &model{name: "auto", client: h.models[0].client}
	return func() []submission {
		h.safeguards.close()
		mu.Lock()
		defer mu.Unlock()
		return got
	}
}

func TestACompletedFirstPartyChatIsScreenedOnce(t *testing.T) {
	for _, kind := range []string{"send", "ask"} {
		t.Run(kind, func(t *testing.T) {
			conversationHeaders := make(chan string, 1)
			h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
				conversationHeaders <- r.Header.Get("X-Tinfoil-Conversation-Id")
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
			})
			drained := screenedBy(t, h)
			in, history := sendInput(key), []safeguardMessage{{Role: "user", Content: "test message"}}
			if kind == "ask" {
				rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", "source")] = storedRow{ID: "source", ETag: "1", Data: object{"title": "Source", "messages": []any{
					object{"id": "q", "role": "user", "content": "earlier question"}, object{"id": "a", "role": "assistant", "content": "earlier answer"}}}}
				in["kind"], in["threadId"], in["quote"] = "ask", "source", "what does this mean?"
				delete(in, "content")
				history = []safeguardMessage{{Role: "user", Content: "earlier question"}, {Role: "assistant", Content: "earlier answer"}, {Role: "user", Content: "what does this mean?"}}
			}
			events := finishedRun(t, h, "/v1/threads/turn", in)
			if header := <-conversationHeaders; header != "" {
				t.Fatalf("a gateway model was sent the conversation header %q", header)
			}
			got := drained()
			conversation := events[0].ThreadID
			if kind == "ask" {
				conversation = "source"
			}
			if len(got) != 1 || got[0].Credential != accessJWT("tinfoil-chat", "chat") || got[0].ConversationID != conversation {
				t.Fatalf("the sidecar received %+v", got)
			}
			messages := got[0].Messages
			if len(messages) == 0 || messages[0].Role != "system" || !strings.Contains(messages[0].Content, "You are Kimi K3.") ||
				!reflect.DeepEqual(messages[1:], append(history, safeguardMessage{Role: "assistant", Content: "hello"})) {
				t.Fatalf("the sidecar received %+v", messages)
			}
		})
	}
}

func TestAutoChatsAreLeftToTheRouter(t *testing.T) {
	conversationHeaders := make(chan string, 1)
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		conversationHeaders <- r.Header.Get("X-Tinfoil-Conversation-Id")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	drained := screenedBy(t, h)
	in := sendInput(key)
	obj(in["options"])["model"] = "auto"
	events := finishedRun(t, h, "/v1/threads/turn", in)
	if header := <-conversationHeaders; header != events[0].ThreadID {
		t.Fatalf("the router was told the conversation is %q", header)
	}
	if got := drained(); len(got) != 0 {
		t.Fatalf("an auto chat was screened twice: %+v", got)
	}
}

func TestFailedAnonymousAndCancelledChatsAreNotScreened(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
		})
		drained := screenedBy(t, h)
		if last := eventsIn(t, postRoute(h, "/v1/threads/turn", sendInput(key), "alice")); last[len(last)-1].Type != "RUN_ERROR" {
			t.Fatal("a cut stream finished the run")
		}
		if got := drained(); len(got) != 0 {
			t.Fatalf("a failed chat was screened: %+v", got)
		}
	})
	t.Run("anonymous", func(t *testing.T) {
		h, _, key := testChatHarness(t, nil)
		drained := screenedBy(t, h)
		in := sendInput(key)
		delete(in, "key")
		in["ephemeral"] = true
		eventsIn(t, postRoute(h, "/v1/threads/turn", in, ""))
		if got := drained(); len(got) != 0 {
			t.Fatalf("an anonymous chat was screened: %+v", got)
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		started := make(chan struct{})
		h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
			w.(http.Flusher).Flush()
			close(started)
			<-r.Context().Done()
		})
		drained := screenedBy(t, h)
		p := &principal{ID: "alice", JWT: "clerk-alice"}
		run, err := h.acceptTurn(context.Background(), p, key, sendInput(key))
		if err != nil {
			t.Fatal(err)
		}
		<-started
		if _, err := h.cancelThread(context.Background(), p, key, object{"key": key.base64(), "threadId": run.thread.id, "runId": run.log.id}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-run.finished:
		case <-time.After(3 * time.Second):
			t.Fatal("cancel timed out")
		}
		if got := drained(); len(got) != 0 {
			t.Fatalf("a cancelled chat was screened: %+v", got)
		}
	})
}

func TestAnswersAnnotateTheSourcesTheyCite(t *testing.T) {
	serveWebsearch(t, func(string, map[string]any) (*mcp.CallToolResult, any) {
		return nil, object{"results": []object{{"title": "A", "url": "https://a.test/1", "content": "alpha"}, {"title": "", "url": "https://b.test", "content": "beta"}}}
	})
	answer := "Cats sleep [A](https://a.test/1/) and purr [b](https://b.test). See also [x](https://unknown.test)."
	var turns atomic.Int32
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if turns.Add(1) == 1 {
			io.WriteString(w, toolCallChunk(0, "s", "web_search", `{"query":"cats"}`)+"data: [DONE]\n\n")
			return
		}
		io.WriteString(w, "data: "+string(raw(object{"choices": []object{{"delta": object{"content": answer}}}}))+"\n\ndata: [DONE]\n\n")
	})
	events := finishedRun(t, h, "/v1/threads/turn", searchInput(key))
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	cite := func(url, title, label string) any {
		start := strings.Index(answer, "["+label+"]") + 1
		return map[string]any{"type": "url_citation", "url_citation": map[string]any{"url": url, "title": title, "start_index": float64(start), "end_index": float64(start + len(label))}}
	}
	want := []any{cite("https://a.test/1/", "A", "A"), cite("https://b.test", "https://b.test", "b")}
	if got := obj(arr(row.Data["messages"])[1])["annotations"]; !reflect.DeepEqual(got, any(want)) {
		t.Fatalf("stored annotations %v", got)
	}
	state := h.threadState(&principal{ID: "alice"}, events[0].ThreadID)
	state.mu.Lock()
	run := state.runs[events[0].RunID]
	state.mu.Unlock()
	run.out.mu.Lock()
	defer run.out.mu.Unlock()
	if run.out.sources != nil {
		t.Fatal("the run kept the pages its tools found")
	}
}

func TestCutModelStreamDoesNotCompleteItsToolCall(t *testing.T) {
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, toolCallChunk(0, "search-call", "web_search", `{"query":"ca`))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	})
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", sendInput(key), "alice"))
	if last := events[len(events)-1]; last.Type != "RUN_ERROR" {
		t.Fatalf("a cut stream finished the run: %+v", last)
	}
	for _, e := range events {
		if e.Type == "TOOL_CALL_END" {
			t.Fatal("a cut stream closed its truncated tool call")
		}
	}
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	block := obj(arr(obj(arr(row.Data["messages"])[1])["timeline"])[0])
	if block["arguments"] != `{"query":"ca` || block["complete"] == true {
		t.Fatalf("truncated arguments stored as complete: %v", block)
	}
}

func TestRehydrationChecksEveryFrame(t *testing.T) {
	key := randomKey()
	original, _ := newContentRun("run", key)
	frames := [][]byte{original.seal(0, []byte(`{"type":"RUN_STARTED"}`)), original.seal(1, []byte(`{"type":"TEXT_MESSAGE_CHUNK"}`)), original.seal(2, []byte(`{"type":"RUN_FINISHED"}`))}
	frames[1][0] ^= 1
	var bytes []byte
	for _, frame := range frames {
		bytes = binary.BigEndian.AppendUint32(bytes, uint32(len(frame)))
		bytes = append(bytes, frame...)
	}
	restored, _ := newContentRun("run", key)
	if err := restored.rehydrate(bytes); err == nil {
		t.Fatal("corrupt interior frame accepted")
	}
}
