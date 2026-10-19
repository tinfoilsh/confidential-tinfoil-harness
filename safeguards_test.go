package main

import (
	"encoding/base64"
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
)

func accessJWT(clientID, product string) string {
	header, _ := json.Marshal(map[string]string{"alg": "none", "typ": "at+jwt"})
	payload, _ := json.Marshal(map[string]string{"client_id": clientID, "product": product})
	return base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
}

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
	}{
		{"history", []safeguardMessage{{Role: "user", Content: big + "the question"}, {Role: "assistant", Content: "reply"}}},
		{"older turns", []safeguardMessage{{Role: "user", Content: big}, {Role: "assistant", Content: big}, {Role: "user", Content: "latest"}, {Role: "assistant", Content: "reply"}}},
		{"escaping", []safeguardMessage{{Role: "assistant", Content: strings.Repeat("<\x00", maxSubmissionBytes/2)}}},
		{"escaped reply", []safeguardMessage{{Role: "user", Content: big}, {Role: "assistant", Content: strings.Repeat("<", maxSubmissionBytes/3)}}},
		{"multibyte", []safeguardMessage{{Role: "user", Content: strings.Repeat("é", maxSubmissionBytes)}, {Role: "assistant", Content: "reply"}}},
		{"many messages", append(make([]safeguardMessage, maxSubmissionBytes/messageJSONOverhead), safeguardMessage{Role: "assistant", Content: "reply"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, ok := encodeSubmission("token", "chat", tc.messages)
			if !ok {
				t.Fatal("oversized history must be trimmed, not dropped")
			}
			if len(body) > maxSubmissionBytes {
				t.Fatalf("body is %d bytes, want at most %d", len(body), maxSubmissionBytes)
			}
			var got submission
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatal(err)
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

func TestIsFirstPartyChatAccessJWT(t *testing.T) {
	if !isFirstPartyChatAccessJWT(accessJWT("tinfoil-chat", "chat")) {
		t.Fatal("expected first-party Chat at+jwt token to be recognized")
	}
	header, _ := json.Marshal(map[string]string{"typ": "application/at+jwt"})
	payload, _ := json.Marshal(map[string]string{"client_id": "tinfoil-chat", "product": "chat"})
	if !isFirstPartyChatAccessJWT(base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload) + ".signature") {
		t.Fatal("expected the application/at+jwt type to be recognized")
	}
	for _, token := range []string{"opaque-key", "e30.e30.signature", "bad.payload.signature", "free_anonymous-key", accessJWT("third-party", "chat"), accessJWT("tinfoil-chat", "api")} {
		if isFirstPartyChatAccessJWT(token) {
			t.Fatalf("ineligible credential %q must not be classified", token)
		}
	}
}

func TestScreeningIsThePromptAndHistoryWithoutToolResults(t *testing.T) {
	h := &harness{safeguards: &submitter{}}
	req := &request{system: "be helpful", conversation: "chat-1", groups: [][]json.RawMessage{
		{raw(object{"role": "user", "content": []any{object{"type": "text", "text": "look"}, object{"type": "image_url", "image_url": object{"url": "data:..."}}}})},
		{raw(object{"role": "assistant", "content": "", "reasoning_content": "hidden", "tool_calls": []any{object{"id": "c1"}}}), raw(object{"role": "tool", "tool_call_id": "c1", "content": "42"})},
		{raw(object{"role": "user", "content": "thanks"})},
	}}
	want := &screening{conversation: "chat-1", messages: []safeguardMessage{{Role: "system", Content: "be helpful"}, {Role: "user", Content: "look\n[image_url]"}, {Role: "assistant", Content: ""}, {Role: "user", Content: "thanks"}}}
	if got := h.screening(req); !reflect.DeepEqual(got, want) {
		t.Fatalf("captured %+v\nwant %+v", got, want)
	}
	if (&harness{}).screening(req) != nil {
		t.Fatal("a conversation was captured with no sidecar configured")
	}
}

func TestOnlyCompletedFirstPartyChatsAreScreened(t *testing.T) {
	reply := []object{{"content": "hello"}}
	for name, tc := range map[string]struct {
		change func(*chatRun)
		reply  []object
		err    error
		want   bool
	}{
		"completed":    {func(*chatRun) {}, reply, nil, true},
		"failed":       {func(*chatRun) {}, reply, errors.New("model failed"), false},
		"cancelled":    {func(r *chatRun) { r.cancelled = true }, reply, nil, false},
		"deleted":      {func(r *chatRun) { r.thread.deleted = true }, reply, nil, false},
		"widget retry": {func(r *chatRun) { r.retryTarget = object{"name": "render_chart"} }, reply, nil, false},
		"anonymous":    {func(r *chatRun) { r.principal.Anonymous = true }, reply, nil, false},
		"api key":      {func(r *chatRun) { r.credential.Key = "tk_api_key" }, reply, nil, false},
		"not captured": {func(r *chatRun) { r.screening = nil }, reply, nil, false},
		"no reply":     {func(*chatRun) {}, []object{{"content": ""}}, nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			var submissions atomic.Int32
			sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				submissions.Add(1)
				w.WriteHeader(http.StatusAccepted)
			}))
			defer sidecar.Close()
			h := &harness{safeguards: newSubmitter(sidecar.URL)}
			r := &chatRun{thread: &threadSession{id: "chat-1"}, principal: &principal{ID: "alice"}, credential: inferenceCredential{Key: secret(accessJWT("tinfoil-chat", "chat"))},
				screening: &screening{conversation: "chat-1", messages: []safeguardMessage{{Role: "user", Content: "hi"}}}}
			tc.change(r)
			h.screen(r, tc.reply, tc.err)
			if submitted := submissions.Load() == 1; submitted != tc.want {
				t.Fatalf("submitted = %v, want %v", submitted, tc.want)
			}
		})
	}
}
