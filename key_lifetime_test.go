package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

func TestInvalidKeyEncodingNamesTheKey(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	for _, key := range []any{true, "not-base64"} {
		w := postRoute(h, "/v1/threads/get", api.Object{"key": key, "id": "chat-1"}, "alice")
		var response struct {
			Error api.Error `json:"error"`
		}
		json.Unmarshal(w.Body.Bytes(), &response)
		if w.Code != http.StatusBadRequest || response.Error.Code != "BAD_REQUEST" || !strings.Contains(response.Error.Message, "key") {
			t.Fatalf("invalid key: HTTP %d %s", w.Code, w.Body.String())
		}
	}
}

// The streaming handler must not retain the encoded key after decoding it.
func TestRequestKeysAreClearedAfterDecoding(t *testing.T) {
	key := randomKey()
	body := api.Raw(api.Object{"key": keyBase64(key), "threadId": "chat-1"})
	var received *runRequest
	handle := decoded(func(_ context.Context, _ *auth.Principal, _ auth.ContentKey, in *runRequest) (any, error) {
		received = in
		var wire api.Object
		json.Unmarshal(api.Raw(in), &wire)
		if wire["key"] != keyBase64(key) || in.ThreadID != "chat-1" {
			t.Fatal("the decoded request lost its key or thread")
		}
		if !bytes.Equal(body, make([]byte, len(body))) {
			t.Error("the encoded content key is still in the request buffer")
		}
		return nil, nil
	})
	if _, err := handle(context.Background(), &auth.Principal{ID: "alice"}, key, body); err != nil {
		t.Fatal(err)
	}
	if received == nil || len(received.Key) != 0 {
		t.Fatal("the completed handler retained its decoded key")
	}
}

// A slow safeguards submission must not prolong the storage key's lifetime.
func TestRunDropsKeyBeforeScreening(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	hold := make(chan struct{})
	drained := heldScreening(t, h, hold)
	defer func() {
		close(hold)
		if got := drained(); len(got) != 1 {
			t.Errorf("the completed conversation produced %d submissions", len(got))
		}
	}()
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	r, err := h.acceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[turnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, r.log, "RUN_FINISHED")
	if r.key != (auth.ContentKey{}) {
		t.Error("the completed run holds its storage key while screening")
	}
	row, err := rows.Load(context.Background(), p, key, "chat", r.thread.id)
	if err != nil {
		t.Fatal(err)
	}
	thread := mustRead(domain.ReadThreadRow(row))
	if len(thread.Messages) != 2 || thread.Messages[1].Content != "hello" || thread.ActiveRun != nil {
		t.Fatal("the reply was not committed before the key was dropped")
	}
}

// The wire key must survive retries during a call, then disappear from all
// readers, including one a transport retains after returning a refusal.
func TestSyncRequestKeyReadersAreCleared(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			key := randomKey()
			var sent *http.Request
			var late io.ReadCloser
			s := &store.Client{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(r *http.Request) (*http.Response, error) {
				sent = r
				first, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				var in api.Object
				if json.Unmarshal(first, &in) != nil || in["key"] != keyBase64(key) || in["id"] != "chat-1" || in["scope"] != "chat" {
					t.Fatal("sync did not receive the requested key and row")
				}
				replay, err := r.GetBody()
				if err != nil {
					t.Fatal(err)
				}
				again, err := io.ReadAll(replay)
				replay.Close()
				if err != nil || !bytes.Equal(first, again) {
					t.Fatal("the request could not be replayed during the call")
				}
				late, err = r.GetBody()
				if err != nil {
					t.Fatal(err)
				}
				return syncTestResponse(status, api.Object{})
			})}}
			err := s.Remove(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, key, "chat", "chat-1")
			if (err == nil) != (status == http.StatusOK) || sent == nil || late == nil {
				t.Fatalf("sync call: %v", err)
			}
			defer late.Close()
			remaining, err := io.ReadAll(late)
			if err != nil || !bytes.Equal(remaining, make([]byte, len(remaining))) {
				t.Fatal("the completed sync call left key bytes in a transport reader")
			}
		})
	}
}
