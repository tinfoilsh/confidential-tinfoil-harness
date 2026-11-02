package domain

import (
	"encoding/json"
	"fmt"
)

// QuestionsWidget is the widget whose questions the user answers in place.
const QuestionsWidget = "render_questions"

// QuestionsShown is what the model reads once it has shown its questions.
const QuestionsShown = "The questions are shown to the user, who answers them in place. Their answers reach you with their next message."

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
