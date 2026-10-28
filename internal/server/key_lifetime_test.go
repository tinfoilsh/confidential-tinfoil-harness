package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
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
	var received *engine.RunRequest
	handle := decoded(func(_ context.Context, _ *auth.Principal, _ auth.ContentKey, in *engine.RunRequest) (any, error) {
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
	r, err := h.engine.AcceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[engine.TurnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, r.Log, "RUN_FINISHED")
	if r.HoldsKey() {
		t.Error("the completed run holds its storage key while screening")
	}
	row, err := rows.Load(context.Background(), p, key, "chat", r.Thread.ID)
	if err != nil {
		t.Fatal(err)
	}
	thread := mustRead(domain.ReadThreadRow(row))
	if len(thread.Messages) != 2 || thread.Messages[1].Content != "hello" || thread.ActiveRun != nil {
		t.Fatal("the reply was not committed before the key was dropped")
	}
}
