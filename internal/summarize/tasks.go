package summarize

import "time"

// Role is the model a task runs on, which configuration names.
type Role string

const (
	// Fast is a quick model for short text, SUMMARY_MODEL.
	Fast Role = "fast"
	// Long is a model whose window holds a long conversation.
	Long Role = "long"
)

// Task is one use the harness has of summaries.
type Task struct {
	// Name and Version say which instructions wrote a summary.
	Name    string
	Version int
	Role    Role
	// Instructions open the system prompt, which Sections and the word
	// range complete.
	Instructions string
	// Sections are the level-two headings a summary holds, in this order
	// and as written here.
	Sections []Section
	// MinWords and MaxWords, when set, are the length the prompt asks for.
	MinWords, MaxWords int
	// Frame is the tag the documents are read in. A task without one reads
	// its document's text as it is.
	Frame string
	// Closing follows the documents, restating the task after a long input.
	Closing string
	// MaxTokens bounds each call's output, reasoning included. A summary
	// that reaches it is cut off, and fails.
	MaxTokens   int64
	Temperature *float64
	// Timeout bounds each call, and Attempts is how many calls a summary
	// takes when one fails for a while or writes a summary that fails its
	// check.
	Timeout  time.Duration
	Attempts int
	// Fold reads an input too large for one call in parts, folding each
	// into the summary of the parts before it. Without it, such an input
	// fails.
	Fold bool
}

// Section is a heading a summary holds, and what goes under it.
type Section struct {
	Heading, Description string
}

// Title names a chat from its first message.
var Title = &Task{
	Name: "title", Version: 1, Role: Fast,
	Instructions: "Generate a concise, descriptive title for the following text. ONLY output plain text - never output markdown or code.",
	MinWords:     2, MaxWords: 5,
	MaxTokens: 1024, Timeout: 30 * time.Second, Attempts: 1,
}

// Thoughts labels what a reply is reasoning about as it thinks.
var Thoughts = &Task{
	Name: "thoughts", Version: 1, Role: Fast,
	Instructions: `You produce a brief status label describing what an AI assistant is currently reasoning about. The label is shown to a user who is waiting for the AI to finish thinking.

Focus on the LAST part of the reasoning text — that reflects what the AI is working on right now. Ignore earlier parts that have already been resolved.

Format rules:
- Start with a present participle verb (Analyzing, Considering, Evaluating, Comparing, Weighing, Planning, etc.)
- Be specific to the actual content — never write vague filler like "Thinking about the question"
- Output ONLY the label, nothing else — no quotes, no punctuation, no markdown, no preamble
- Never refer to "the AI", "the model", "the assistant", "the person", or "the user"

Good examples:
- Evaluating post-surgery recovery risks
- Comparing authentication middleware approaches
- Breaking down database query performance
- Weighing privacy versus usability tradeoffs
- Planning step-by-step explanation structure
- Assessing UV exposure risks from skiing after laser treatment
- Deciding how to structure the response`,
	MinWords: 4, MaxWords: 12,
	MaxTokens: 1024, Temperature: new(0.2), Timeout: 30 * time.Second, Attempts: 1,
}
