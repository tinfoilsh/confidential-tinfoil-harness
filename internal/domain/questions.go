package domain

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// QuestionsWidget is the widget whose questions the user answers in place.
const QuestionsWidget = "render_questions"

// QuestionsShown is what the model reads once it has shown its questions.
const QuestionsShown = "The questions are shown to the user, who answers them in place. Their answers reach you with their next message."

// SingleSelection is how a question that takes one option says so.
const SingleSelection = "single"

// questions are a questions widget's arguments.
type questions struct {
	Title     string     `json:"title"`
	Questions []question `json:"questions"`
}

type question struct {
	ID        string           `json:"id"`
	Prompt    string           `json:"prompt"`
	Selection string           `json:"selection"`
	Options   []QuestionOption `json:"options"`
}

// QuestionOption is one answer a question offers.
type QuestionOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// readQuestions reads a questions widget's arguments, refusing ids its
// schema cannot say are unique: a client keeps each answer by them.
func readQuestions(arguments string) (questions, error) {
	var q questions
	if err := json.Unmarshal([]byte(arguments), &q); err != nil {
		return questions{}, err
	}
	asked := map[string]bool{}
	for _, item := range q.Questions {
		if asked[item.ID] {
			return questions{}, fmt.Errorf("question id %q is used more than once", item.ID)
		}
		asked[item.ID] = true
		offered := map[string]bool{}
		for _, o := range item.Options {
			if offered[o.ID] {
				return questions{}, fmt.Errorf("option id %q is used more than once in question %q", o.ID, item.ID)
			}
			offered[o.ID] = true
		}
	}
	return q, nil
}

// answerableQuestions are the questions of a widget call the user can
// answer: one whose arguments the widget draws.
func answerableQuestions(b *Block) (questions, bool) {
	if b.Type != "tool_call" || b.Name != QuestionsWidget {
		return questions{}, false
	}
	for _, w := range WidgetCatalog() {
		if w.Name == QuestionsWidget && WidgetFailure(w, b.Arguments) == nil {
			q, err := readQuestions(b.Arguments)
			return q, err == nil
		}
	}
	return questions{}, false
}

// picked is the options of a question that ids name, in the order the
// question offers them, or why they cannot be its answer.
func (q *question) picked(ids []string) ([]string, error) {
	chosen := []string{}
	for _, o := range q.Options {
		if slices.Contains(ids, o.ID) {
			chosen = append(chosen, o.ID)
		}
	}
	for _, id := range ids {
		if !slices.Contains(chosen, id) {
			return nil, api.Err(409, "REVISION_CONFLICT", "The question no longer offers that option")
		}
	}
	if q.Selection == SingleSelection && len(chosen) != 1 {
		return nil, api.Err(400, "BAD_REQUEST", "A single-choice question takes one option")
	}
	return chosen, nil
}

// AnswerQuestions changes the answers the user gave in a questions widget of
// a reply: each question named takes the options picked, or none to leave it
// unanswered. Questions not named keep their answers.
func AnswerQuestions(m *Message, callID string, changes map[string]*[]string) error {
	index := slices.IndexFunc(m.Timeline, func(b Block) bool { return b.Type == "tool_call" && b.CallID() == callID })
	if index < 0 {
		return api.Err(409, "REVISION_CONFLICT", "The widget was removed")
	}
	block := &m.Timeline[index]
	if block.Name != QuestionsWidget {
		return api.Err(400, "BAD_REQUEST", "The widget takes no answers")
	}
	asked, ok := answerableQuestions(block)
	if !ok {
		return api.Err(400, "BAD_REQUEST", "The widget's questions cannot be answered")
	}
	answers := maps.Clone(block.Answers)
	if answers == nil {
		answers = map[string][]string{}
	}
	for id, ids := range changes {
		at := slices.IndexFunc(asked.Questions, func(q question) bool { return q.ID == id })
		if at < 0 {
			return api.Err(409, "REVISION_CONFLICT", "The widget no longer asks that question")
		}
		if ids == nil {
			delete(answers, id)
			continue
		}
		chosen, err := asked.Questions[at].picked(*ids)
		if err != nil {
			return err
		}
		answers[id] = chosen
	}
	block.Answers = answers
	if len(answers) == 0 {
		block.Answers = nil
	}
	return nil
}

// KeepAnswers gives the questions widgets of a reply the answers another
// copy of it holds, as a continued reply takes those given while it ran.
func (m *Message) KeepAnswers(from *Message) {
	given := map[string]map[string][]string{}
	for _, b := range from.Timeline {
		if b.Type == "tool_call" && b.Name == QuestionsWidget {
			given[b.CallID()] = b.Answers
		}
	}
	if len(given) == 0 {
		return
	}
	m.Timeline = clonedSlice(m.Timeline)
	for i := range m.Timeline {
		b := &m.Timeline[i]
		if answers, ok := given[b.CallID()]; ok && b.Type == "tool_call" && b.Name == QuestionsWidget {
			b.Answers = answers
		}
	}
}
