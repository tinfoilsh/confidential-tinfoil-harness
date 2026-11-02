package server

import (
	"context"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
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
