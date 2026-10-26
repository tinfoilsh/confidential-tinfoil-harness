package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// chatToken is a subscriber's Tinfoil Chat credential.
var chatToken = auth.Credential{Key: "chat-token", FirstParty: true}

func TestSubmitter_DeliversCompletedConversation(t *testing.T) {
	var mu sync.Mutex
	var got []submission
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sub submission
		if r.URL.Path != "/ingest" || json.NewDecoder(r.Body).Decode(&sub) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		got = append(got, sub)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer sidecar.Close()
	s := newSubmitter(sidecar.URL + "/")
	s.submit("tok", "chat-1", []safeguardMessage{{Role: "user", Content: "hi"}, {Role: "assistant", Content: "hello"}})
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].Credential != "tok" || got[0].ConversationID != "chat-1" || len(got[0].Messages) != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestSubmitter_TrimsOversizedHistory(t *testing.T) {
	big := strings.Repeat("x", maxSubmissionBytes)
	for _, tc := range []struct {
		name     string
		messages []safeguardMessage
		kept     int
	}{
		{"older turns", []safeguardMessage{{Role: "user", Content: big}, {Role: "assistant", Content: big}, {Role: "user", Content: "latest"}, {Role: "assistant", Content: "reply"}}, 2},
		{"escaped reply", []safeguardMessage{{Role: "user", Content: "question"}, {Role: "assistant", Content: strings.Repeat("<\x00", maxSubmissionBytes/2)}}, 1},
		{"multibyte reply", []safeguardMessage{{Role: "assistant", Content: strings.Repeat("é", maxSubmissionBytes)}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := encodeSubmission("token", "chat", tc.messages)
			if len(body) > maxSubmissionBytes {
				t.Fatalf("body is %d bytes, want at most %d", len(body), maxSubmissionBytes)
			}
			var got submission
			if err := json.Unmarshal(body, &got); err != nil || len(got.Messages) != tc.kept {
				t.Fatalf("kept %d messages, want %d (%v)", len(got.Messages), tc.kept, err)
			}
			offset := len(tc.messages) - len(got.Messages)
			for i, m := range got.Messages {
				want := tc.messages[offset+i]
				if m.Role != want.Role {
					t.Fatalf("message %d role = %q, want %q", i, m.Role, want.Role)
				}
				if i == 0 {
					if !utf8.ValidString(m.Content) || m.Content == "" && want.Content != "" || !strings.HasSuffix(want.Content, m.Content) {
						t.Fatal("the oldest kept message must be a valid trailing part of the original")
					}
				} else if m.Content != want.Content {
					t.Fatalf("message %d must be kept whole", i)
				}
			}
		})
	}
}

func TestScreeningIsThePromptAndHistoryWithoutToolResults(t *testing.T) {
	h := &engine{safeguards: &submitter{}}
	req := &request{system: "be helpful", conversation: "chat-1", groups: [][]json.RawMessage{
		{api.Raw(api.Object{"role": "user", "content": []any{api.Object{"type": "text", "text": "look"}, api.Object{"type": "image_url", "image_url": api.Object{"url": "data:..."}}}})},
		{api.Raw(api.Object{"role": "assistant", "content": "", "reasoning_content": "hidden", "tool_calls": []any{api.Object{"id": "c1"}}}), api.Raw(api.Object{"role": "tool", "tool_call_id": "c1", "content": "42"})},
		{api.Raw(api.Object{"role": "user", "content": "thanks"})},
	}}
	want := &screening{conversation: "chat-1", messages: []safeguardMessage{{Role: "system", Content: "be helpful"}, {Role: "user", Content: "look\n[image_url]"}, {Role: "assistant", Content: ""}, {Role: "user", Content: "thanks"}}}
	if got := h.screening(req); !reflect.DeepEqual(got, want) {
		t.Fatalf("captured %+v\nwant %+v", got, want)
	}
	if (&engine{}).screening(req) != nil {
		t.Fatal("a conversation was captured with no sidecar configured")
	}
}

func TestOnlyCompletedFirstPartyChatsAreScreened(t *testing.T) {
	reply := []domain.Message{{Content: "hello"}}
	for name, tc := range map[string]struct {
		change func(*chatRun)
		reply  []domain.Message
		err    error
		want   bool
	}{
		"completed":    {func(*chatRun) {}, reply, nil, true},
		"failed":       {func(*chatRun) {}, reply, errors.New("model failed"), false},
		"cancelled":    {func(r *chatRun) { r.cancelled = true }, reply, nil, false},
		"deleted":      {func(r *chatRun) { r.thread.deleted = true }, reply, nil, false},
		"widget retry": {func(r *chatRun) { r.retryTarget = &retryTarget{block: domain.Block{Name: "render_chart"}} }, reply, nil, false},
		"anonymous":    {func(r *chatRun) { r.principal.Anonymous = true }, reply, nil, false},
		"api key":      {func(r *chatRun) { r.credential.FirstParty = false }, reply, nil, false},
		"not captured": {func(r *chatRun) { r.screening = nil }, reply, nil, false},
		"no reply":     {func(*chatRun) {}, []domain.Message{{Content: ""}}, nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			var submissions atomic.Int32
			sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				submissions.Add(1)
				w.WriteHeader(http.StatusAccepted)
			}))
			defer sidecar.Close()
			h := &engine{safeguards: newSubmitter(sidecar.URL)}
			r := &chatRun{thread: &threadSession{id: "chat-1"}, turn: &turnRequest{Kind: "send"}, principal: &auth.Principal{ID: "alice"}, credential: chatToken,
				screening: &screening{conversation: "chat-1", messages: []safeguardMessage{{Role: "user", Content: "hi"}}}}
			tc.change(r)
			h.screen(r, tc.reply, tc.err)
			if submitted := submissions.Load() == 1; submitted != tc.want {
				t.Fatalf("submitted = %v, want %v", submitted, tc.want)
			}
		})
	}
}
