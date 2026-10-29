package inference

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/param"
	tinfoil "github.com/tinfoilsh/tinfoil-go"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// SummaryStyle is what a summary says of its text.
type SummaryStyle string

const (
	// TitleSummary names a chat from its first message.
	TitleSummary SummaryStyle = "title"
	// ThoughtsSummary labels what a reply is reasoning about as it thinks.
	ThoughtsSummary SummaryStyle = "thoughts"
)

// A summary is written within summaryTimeout, or not at all.
const summaryTimeout = 30 * time.Second

type summaryPrompt struct {
	system             string
	minWords, maxWords int
	maxTokens          int64
	temperature        param.Opt[float64]
}

var summaryPrompts = map[SummaryStyle]summaryPrompt{
	TitleSummary: {
		system:    "Generate a concise, descriptive title for the following text. ONLY output plain text - never output markdown or code.",
		minWords:  2,
		maxWords:  5,
		maxTokens: 1024,
	},
	ThoughtsSummary: {
		system: `You produce a brief status label describing what an AI assistant is currently reasoning about. The label is shown to a user who is waiting for the AI to finish thinking.

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
		minWords:    4,
		maxWords:    12,
		maxTokens:   1024,
		temperature: openai.Float(0.2),
	},
}

var errEmptySummary = errors.New("inference: the summary is empty")

// Summarizer writes short summaries of a chat's text.
type Summarizer interface {
	Summarize(ctx context.Context, style SummaryStyle, text string) (string, error)
}

type completionService interface {
	New(context.Context, openai.ChatCompletionNewParams, ...option.RequestOption) (*openai.ChatCompletion, error)
}

// modelSummarizer asks a model of the inference service for summaries under
// the harness's own account, so a summary bills no caller.
type modelSummarizer struct {
	model       string
	completions completionService
}

// NewSummarizer summarizes with model through the Tinfoil SDK's attested,
// enclave-bound transport.
func NewSummarizer(apiKey, model string) (Summarizer, error) {
	secureClient, err := tinfoil.NewClientWithOptions(
		cacheFallback,
		tinfoil.WithOpenAIOptions(
			option.WithAPIKey(apiKey),
			option.WithMaxRetries(0),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("inference: initialize attested summarizer: %w", err)
	}
	return &modelSummarizer{model: model, completions: &secureClient.Chat.Completions}, nil
}

func (s *modelSummarizer) Summarize(ctx context.Context, style SummaryStyle, text string) (string, error) {
	prompt, ok := summaryPrompts[style]
	if !ok {
		return "", fmt.Errorf("inference: no summary style %q", style)
	}
	ctx, cancel := context.WithTimeout(ctx, summaryTimeout)
	defer cancel()
	params := openai.ChatCompletionNewParams{
		Model:               s.model,
		MaxCompletionTokens: openai.Int(prompt.maxTokens),
		ReasoningEffort:     openai.ReasoningEffortLow,
		Temperature:         prompt.temperature,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(fmt.Sprintf("%s Use between %d and %d words.", prompt.system, prompt.minWords, prompt.maxWords)),
			openai.UserMessage(text),
		},
	}
	// The account summarizes for every caller, so each summary caches its
	// prompt apart.
	response, err := s.completions.New(ctx, params, option.WithJSONSet("user_cache_secret", api.Token()))
	if err != nil {
		return "", err
	}
	if len(response.Choices) == 0 {
		return "", errEmptySummary
	}
	summary := strings.TrimSpace(response.Choices[0].Message.Content)
	if summary == "" {
		return "", errEmptySummary
	}
	return summary, nil
}
