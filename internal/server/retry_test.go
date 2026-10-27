package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
)

func TestARetryRepeatsWhatTheChatLeftUnfinished(t *testing.T) {
	prompts := make(chan []any, 1)
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		prompts <- lastRequest(r)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\" violets are blue.\"}}]}\n\ndata: [DONE]\n\n")
	})
	p := &auth.Principal{ID: "alice"}
	for _, tc := range []struct {
		name, lastKind string
		more           []any
		kind, reply    string
		messages       int
	}{
		{"a stopped continuation", "continue", nil, "continue", "Roses are red, violets are blue.", 2},
		{"a stopped reply", "send", nil, "regenerate", " violets are blue.", 2},
		{"an unanswered question", "continue", []any{api.Object{"id": "u2", "role": "user", "content": "Another?"}}, "answer", " violets are blue.", 4},
	} {
		poemThread(rows, "chat-1", tc.more...)
		row := rows.rows[rowMapKey(p, "chat", "chat-1")]
		editRow(&row, func(data api.Object) { data["lastRequestKind"] = tc.lastKind })
		rows.rows[rowMapKey(p, "chat", "chat-1")] = row
		in := sendInput(key)
		in["threadId"], in["kind"] = "chat-1", "retry"
		delete(in, "content")
		finishedRun(t, h, "/v1/threads/turn", in)
		prompt := <-prompts
		continued := strings.HasSuffix(api.Str(api.Obj(prompt[len(prompt)-1])["content"]), engine.ContinueInstruction)
		chat := storedChat(t, rows, key, "chat-1")
		reply := chat.Messages[len(chat.Messages)-1]
		if continued != (tc.kind == "continue") || len(chat.Messages) != tc.messages || reply.Content != tc.reply || (reply.ID == "a1") != (tc.kind == "continue") || chat.LastRequestKind != tc.kind {
			t.Fatalf("%s: retried as %q (continued %v) into %+v", tc.name, chat.LastRequestKind, continued, chat.Messages)
		}
	}
	// A device chat's first turn starts it without messages.
	w := postRoute(h, "/v1/threads/turn", api.Object{"threadId": domain.RowID(), "clientRequestId": "6f1c4a8e-0d55-4b8f-9a8e-2f0c7b9d1e11", "kind": "retry", "local": true}, "")
	var refused struct {
		Error api.Error `json:"error"`
	}
	json.Unmarshal(w.Body.Bytes(), &refused)
	if w.Code != 400 || refused.Error.Message != "The chat has nothing to retry" {
		t.Fatalf("a chat without messages was retried: HTTP %d %s", w.Code, w.Body.String())
	}
}
