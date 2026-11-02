package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
)

// quizCall is a questions widget's call: a single-choice question and a
// checklist.
func quizCall() api.Object {
	quiz := api.Object{"title": "Driving quiz", "questions": []any{
		api.Object{"id": "q1", "prompt": "A red curb means", "selection": "single", "options": []any{
			api.Object{"id": "a", "label": "No stopping"}, api.Object{"id": "b", "label": "Buses only"}, api.Object{"id": "c", "label": "Loading only"}}},
		api.Object{"id": "q2", "prompt": "Which lights are on at night?", "selection": "multiple", "options": []any{
			api.Object{"id": "x", "label": "Headlights"}, api.Object{"id": "y", "label": "Hazards"}}},
	}}
	return api.Object{"id": "quiz", "toolCallId": "quiz", "type": "tool_call", "name": domain.QuestionsWidget, "arguments": string(api.Raw(quiz)), "complete": true}
}

// quizThread is a chat whose reply asks the quiz, beside a widget that takes
// no answers.
func quizThread() api.Object {
	return api.Object{"title": "Quiz", "titleState": "manual", "createdAt": "2026-01-01T00:00:00Z", "model": "kimi-k3", "messages": []any{
		api.Object{"id": "u1", "role": "user", "content": "quiz me", "createdAt": "2026-01-01T00:00:01Z"},
		api.Object{"id": "a1", "role": "assistant", "content": "Here you go.", "createdAt": "2026-01-01T00:00:02Z", "timeline": []any{
			api.Object{"id": "text", "type": "content", "content": "Here you go."},
			quizCall(),
			api.Object{"id": "clock", "toolCallId": "clock", "type": "tool_call", "name": "render_clock", "arguments": "{}", "complete": true},
		}},
	}}
}

func storeQuiz(rows *memoryRows, thread api.Object) {
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")] = domain.StoredRow{ID: "chat-1", ETag: "1", Data: api.Raw(thread)}
}

// answer saves answers to the stored quiz's call as alice.
func answer(h *harness, key auth.ContentKey, call string, answers api.Object) *httptest.ResponseRecorder {
	return postRoute(h, "/v1/threads/messages/answer", api.Object{"key": keyBase64(key), "threadId": "chat-1", "messageId": "a1", "toolCallId": call, "answers": answers}, "alice")
}

func savedAnswers(t *testing.T, rows *memoryRows, key auth.ContentKey) map[string][]string {
	t.Helper()
	return storedChat(t, rows, key, "chat-1").Messages[1].Timeline[1].Answers
}

// An answer the widget does not offer is refused and leaves the answers
// saved before it; null leaves a question unanswered.
func TestAnswersTheWidgetDoesNotOfferAreRefused(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	storeQuiz(rows, quizThread())
	if w := answer(h, key, "quiz", api.Object{"q1": []any{"a"}, "q2": []any{"x"}}); w.Code != 200 {
		t.Fatalf("answering: HTTP %d %s", w.Code, w.Body.String())
	}
	for _, tc := range []struct {
		name, call string
		answers    api.Object
		code       string
	}{
		{"an option the widget does not offer", "quiz", api.Object{"q1": []any{"d"}}, "REVISION_CONFLICT"},
		{"a question the widget does not ask", "quiz", api.Object{"q9": []any{"a"}}, "REVISION_CONFLICT"},
		{"two options for a single choice", "quiz", api.Object{"q1": []any{"a", "b"}}, "BAD_REQUEST"},
		{"a widget that asks nothing", "clock", api.Object{"q1": []any{"a"}}, "BAD_REQUEST"},
	} {
		w := answer(h, key, tc.call, tc.answers)
		if code := api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["error"])["code"]; w.Code == 200 || code != tc.code {
			t.Errorf("%s: HTTP %d %s", tc.name, w.Code, w.Body.String())
		}
	}
	if saved := savedAnswers(t, rows, key); !reflect.DeepEqual(saved, map[string][]string{"q1": {"a"}, "q2": {"x"}}) {
		t.Fatalf("a refused answer left %v", saved)
	}
	if w := answer(h, key, "quiz", api.Object{"q1": nil}); w.Code != 200 {
		t.Fatalf("clearing an answer: HTTP %d %s", w.Code, w.Body.String())
	}
	if saved := savedAnswers(t, rows, key); !reflect.DeepEqual(saved, map[string][]string{"q2": {"x"}}) {
		t.Fatalf("a cleared answer left %v", saved)
	}
}

// Answers given while a reply is continued stay with it once the
// continuation is saved.
func TestAnswersGivenDuringAContinuationAreKept(t *testing.T) {
	model, release := heldModel()
	h, rows, key := testChatHarness(t, model)
	t.Cleanup(release)
	storeQuiz(rows, quizThread())
	in := continueInput(key, "chat-1")
	in["kind"], in["messageId"] = "continue", "a1"
	delete(in, "content")
	run, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{ID: "alice", JWT: "clerk-alice"}, chatCredential("alice"), key, requestOf[engine.TurnRequest](in))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.Log, "TEXT_MESSAGE_CHUNK")
	if w := answer(h, key, "quiz", api.Object{"q1": []any{"a"}}); w.Code != 200 {
		t.Fatalf("answering during the run: HTTP %d %s", w.Code, w.Body.String())
	}
	release()
	<-run.Finished
	reply := storedChat(t, rows, key, "chat-1").Messages[1]
	if !strings.HasSuffix(reply.Content, "partial done") || !reflect.DeepEqual(reply.Timeline[1].Answers, map[string][]string{"q1": {"a"}}) {
		t.Fatalf("the continued reply was saved as %+v", reply)
	}
}

// The model is told when its questions repeat an id, which would make an
// answer ambiguous, and otherwise that the user answers them in place.
func TestQuestionIDsAreUnique(t *testing.T) {
	set := &engine.Toolset{Widgets: catalogWidgets(domain.QuestionsWidget), Local: func(context.Context, engine.ToolCall) (any, error) { return "[rendered for the user]", nil }}
	for _, args := range []string{
		`{"questions":[{"id":"q1","prompt":"One","selection":"single","options":[{"id":"a","label":"A"}]},{"id":"q1","prompt":"Two","selection":"single","options":[{"id":"a","label":"A"}]}]}`,
		`{"questions":[{"id":"q1","prompt":"One","selection":"multiple","options":[{"id":"a","label":"A"},{"id":"a","label":"B"}]}]}`,
	} {
		if model, _ := invokeWidget(t, set, engine.ToolCall{ID: "c1", Name: domain.QuestionsWidget, Args: args}); !strings.Contains(model, "is used more than once") {
			t.Errorf("questions %s read as %s", args, model)
		}
	}
	if model, _ := invokeWidget(t, set, engine.ToolCall{ID: "c2", Name: domain.QuestionsWidget, Args: api.Str(quizCall()["arguments"])}); model != domain.QuestionsShown {
		t.Fatalf("valid questions read as %s", model)
	}
}

// Regenerated questions start unanswered: the answers were to the
// questions the user was shown.
func TestRetriedQuestionsStartUnanswered(t *testing.T) {
	replacement := `{"questions":[{"id":"q1","prompt":"A blue curb means","selection":"single","options":[{"id":"a","label":"Disabled parking"}]}]}`
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(api.Object{"choices": []api.Object{{"message": api.Object{"content": replacement}}}})
	})
	h.models.Catalog.Get().EnabledWidgets = []string{domain.QuestionsWidget}
	thread := quizThread()
	call := api.Obj(api.Arr(api.Obj(api.Arr(thread["messages"])[1])["timeline"])[1])
	call["answers"] = api.Object{"q1": []any{"a"}}
	storeQuiz(rows, thread)
	in := continueInput(key, "chat-1")
	in["kind"], in["toolCallId"], in["widgets"] = "retryToolCall", "quiz", []any{domain.QuestionsWidget}
	delete(in, "content")
	finishedRun(t, h, "/v1/threads/turn", in)
	if retried := storedChat(t, rows, key, "chat-1").Messages[1].Timeline[1]; retried.Arguments != replacement || retried.Answers != nil {
		t.Fatalf("the retried questions were saved as %s with answers %v", retried.Arguments, retried.Answers)
	}
}
