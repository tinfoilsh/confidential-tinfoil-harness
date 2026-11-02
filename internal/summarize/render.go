package summarize

import (
	"fmt"
	"regexp"
	"strings"
)

// Document is one item a summary reads: text, in a tag named by Kind whose
// attributes tell the model what it is.
type Document struct {
	Kind  string
	Attrs []Attr
	Text  string
}

// Attr is one attribute of a document's tag.
type Attr struct {
	Name, Value string
}

// earlierTag holds what a summary of the input before a part said.
const earlierTag = "previous_summary"

// sectionsOpening introduces a task's sections in its system prompt.
const sectionsOpening = `Write the summary in these sections, in this order, each under a level-two heading written exactly as here, whatever language the rest is in. Write "None" under a section with nothing in it.`

// system is the task's system prompt.
func (t *Task) system() string {
	prompt := t.Instructions
	if len(t.Sections) > 0 {
		prompt += "\n\n" + sectionsOpening
		for _, s := range t.Sections {
			prompt += "\n\n## " + s.Heading + "\n" + s.Description
		}
	}
	if t.MaxWords > 0 {
		prompt += fmt.Sprintf(" Use between %d and %d words.", t.MinWords, t.MaxWords)
	}
	return prompt
}

// user is the user message a call sends: what the parts before said, then
// the part's documents in the task's frame, then its closing. A task without
// a frame sends its documents' text as it is.
func (t *Task) user(earlier string, docs []Document) string {
	if t.Frame == "" {
		texts := make([]string, len(docs))
		for i, d := range docs {
			texts[i] = d.Text
		}
		return strings.Join(texts, "\n\n")
	}
	tags := []string{t.Frame, earlierTag}
	for _, d := range docs {
		tags = append(tags, d.Kind)
	}
	neutral := neutralizer(tags)
	var b strings.Builder
	if earlier != "" {
		b.WriteString("<" + earlierTag + ">\n" + neutral(earlier) + "\n</" + earlierTag + ">\n\n")
	}
	b.WriteString("<" + t.Frame + ">")
	for _, d := range docs {
		b.WriteString("\n<" + d.Kind)
		for _, a := range d.Attrs {
			b.WriteString(" " + a.Name + `="` + attrEscaper.Replace(a.Value) + `"`)
		}
		b.WriteString(">\n" + neutral(d.Text) + "\n</" + d.Kind + ">")
	}
	b.WriteString("\n</" + t.Frame + ">")
	if t.Closing != "" {
		b.WriteString("\n\n" + t.Closing)
	}
	return b.String()
}

var attrEscaper = strings.NewReplacer("&", "&amp;", `"`, "&quot;", "<", "&lt;", ">", "&gt;")

// neutralizer escapes the opening of any of the tags in text, so text cannot
// close the tag it is read in or pose as another document, while code and
// other angle brackets stay as written.
func neutralizer(tags []string) func(string) string {
	names := make([]string, len(tags))
	for i, tag := range tags {
		names[i] = regexp.QuoteMeta(tag)
	}
	pattern := regexp.MustCompile(`(?i)<(/?(?:` + strings.Join(names, "|") + `)\b)`)
	return func(text string) string { return pattern.ReplaceAllString(text, "&lt;$1") }
}

// check is the summary a call wrote, refused when the model was cut off,
// wrote nothing, or left out a section or wrote them out of order.
func (t *Task) check(text, finishReason string) (string, error) {
	if finishReason == "length" {
		return "", fmt.Errorf("%w: it reached the output limit", ErrInvalid)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return "", fmt.Errorf("%w: it is empty", ErrInvalid)
	}
	at := 0
	for _, s := range t.Sections {
		found := regexp.MustCompile(`(?m)^## ` + regexp.QuoteMeta(s.Heading) + `[ \t]*$`).FindStringIndex(text[at:])
		if found == nil {
			return "", fmt.Errorf("%w: it has no %q section in its place", ErrInvalid, s.Heading)
		}
		at += found[1]
	}
	return text, nil
}
