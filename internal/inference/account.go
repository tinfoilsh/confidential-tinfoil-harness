package inference

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	tinfoil "github.com/tinfoilsh/tinfoil-go"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Account calls models of the inference service under the harness's own
// account, so what it asks for bills no caller, unless its caller pays.
type Account interface {
	Complete(ctx context.Context, c Completion) (Completed, error)
}

// Completion is one request to a model on the harness's account: a system
// prompt and one user message.
type Completion struct {
	// Task is what asks, which the account's log of its spending names.
	Task         string
	Model        string
	System, User string
	MaxTokens    int64
	// Temperature is the model's own unless set.
	Temperature *float64
	// Params are the request members the catalog gives the model, such as
	// its reasoning settings.
	Params api.Object
	// CacheScope partitions the enclave's prompt cache: requests that share
	// one may reuse each other's prompt, and no others.
	CacheScope string
	// CallerPays bills the completion to the caller whose key the context
	// carries, as part of the run its usage names, instead of the harness.
	CallerPays bool
}

// Completed is a model's answer to a Completion.
type Completed struct {
	Text         string
	FinishReason string
	Usage        api.Usage
}

// Refused is a completion the inference service answered with an error
// status, and how long it asked to wait before another.
type Refused struct {
	Status     int
	RetryAfter time.Duration
	err        error
}

func (r *Refused) Error() string {
	return fmt.Sprintf("inference: the model refused the request with status %d: %v", r.Status, r.err)
}
func (r *Refused) Unwrap() error { return r.err }

type completionService interface {
	New(context.Context, openai.ChatCompletionNewParams, ...option.RequestOption) (*openai.ChatCompletion, error)
}

type modelAccount struct {
	completions completionService
	// callers takes the completions their callers pay for.
	callers completionService
}

// NewAccount calls models with apiKey through the Tinfoil SDK's attested,
// enclave-bound transport, and those a caller pays for through router, whose
// client signs each request with the caller's key and usage.
func NewAccount(apiKey string, router *Service) (Account, error) {
	secureClient, err := tinfoil.NewClientWithOptions(
		attestedTLS,
		cacheFallback,
		tinfoil.WithOpenAIOptions(
			option.WithAPIKey(apiKey),
			option.WithMaxRetries(0),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("inference: initialize the attested account client: %w", err)
	}
	return &modelAccount{completions: &secureClient.Chat.Completions, callers: callerCompletions(router)}, nil
}

// callerCompletions takes completions through router, whose client signs
// each with the caller's key and usage.
func callerCompletions(router *Service) completionService {
	client := openai.NewClient(option.WithBaseURL(router.URL+"/v1"), option.WithHTTPClient(router.Client), option.WithMaxRetries(0))
	return &client.Chat.Completions
}

func (a *modelAccount) Complete(ctx context.Context, c Completion) (Completed, error) {
	params := openai.ChatCompletionNewParams{
		Model:               c.Model,
		MaxCompletionTokens: openai.Int(c.MaxTokens),
		Messages:            []openai.ChatCompletionMessageParamUnion{openai.SystemMessage(c.System), openai.UserMessage(c.User)},
	}
	if c.Temperature != nil {
		params.Temperature = openai.Float(*c.Temperature)
	}
	options := []option.RequestOption{option.WithJSONSet("user_cache_secret", c.CacheScope)}
	for key, value := range c.Params {
		options = append(options, option.WithJSONSet(key, value))
	}
	completions := a.completions
	if c.CallerPays {
		completions = a.callers
	}
	started := time.Now()
	response, err := completions.New(ctx, params, options...)
	if err != nil {
		var refused *openai.Error
		if errors.As(err, &refused) {
			r := &Refused{Status: refused.StatusCode, err: err}
			if refused.Response != nil {
				r.RetryAfter = time.Duration(api.RetryAfter(refused.Response.Header)) * time.Second
			}
			return Completed{}, r
		}
		return Completed{}, err
	}
	if len(response.Choices) == 0 {
		return Completed{}, errors.New("inference: the model answered with no choices")
	}
	done := Completed{Text: response.Choices[0].Message.Content, FinishReason: string(response.Choices[0].FinishReason),
		Usage: api.Usage{Prompt: response.Usage.PromptTokens, Completion: response.Usage.CompletionTokens, Total: response.Usage.TotalTokens}}
	// What each completion spends, by what spent it and who pays.
	slog.Info("account completion", "task", c.Task, "model", c.Model, "callerPays", c.CallerPays, "promptTokens", done.Usage.Prompt, "completionTokens", done.Usage.Completion, "finishReason", done.FinishReason, "duration", time.Since(started))
	return done, nil
}
