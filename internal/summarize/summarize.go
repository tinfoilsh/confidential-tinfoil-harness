package summarize

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
)

var (
	// ErrUnavailable is a summary no model could write: the task's model is
	// not in the catalog, or refused or failed every call.
	ErrUnavailable = errors.New("summarize: no model could write the summary")
	// ErrInvalid is a summary that is not what its task asks for.
	ErrInvalid = errors.New("summarize: the summary does not have the form its task asks for")
	// ErrTooLarge is an input that does not fit its task's model.
	ErrTooLarge = errors.New("summarize: the input does not fit the model")
)

const (
	// A call fills at most nine tenths of its model's window, since the
	// harness counts tokens the way one tokenizer does.
	windowShareTenths = 9
	// minPartTokens is the least room a part of an input needs.
	minPartTokens = 1024
	// documentOverhead is what the line breaks around a document's tags
	// and text cost beside them.
	documentOverhead = 4
	// concurrentCalls bounds the calls each role's model takes at once
	// from this instance; more wait.
	concurrentCalls = 8
	// A failed call is tried again after retryDelay, doubling to
	// maxRetryDelay, or after the delay the model asked for.
	retryDelay    = time.Second
	maxRetryDelay = 30 * time.Second
	// summaryEffort is the reasoning a model gives a summary, on a model
	// whose reasoning cannot be turned off.
	summaryEffort = "low"
)

// Input is what a summary reads: its documents, in order, after what a
// summary of what came before them said, when one did.
type Input struct {
	Earlier   string
	Documents []Document
}

// Result is a summary, the model that wrote it, and how many parts its
// input was read in.
type Result struct {
	Text  string
	Model string
	Parts int
}

// Service writes summaries on the harness's account, with the model
// configuration names for each role.
type Service struct {
	account inference.Account
	catalog *inference.CatalogCache
	models  map[Role]string
	slots   map[Role]chan struct{}
}

// New is a service that runs the fast role's tasks on fast and the long
// role's on long, each as the catalog describes it.
func New(account inference.Account, catalog *inference.CatalogCache, fast, long string) *Service {
	return &Service{account: account, catalog: catalog, models: map[Role]string{Fast: fast, Long: long},
		slots: map[Role]chan struct{}{Fast: make(chan struct{}, concurrentCalls), Long: make(chan struct{}, concurrentCalls)}}
}

// Summarize writes task's summary of in.
func (s *Service) Summarize(ctx context.Context, task *Task, in Input) (Result, error) {
	model := s.models[task.Role]
	result, err := s.summarize(ctx, task, model, in)
	if err != nil && ctx.Err() == nil {
		slog.Warn("summary failed", "task", task.Name, "model", model, "error", err)
	}
	return result, err
}

func (s *Service) summarize(ctx context.Context, task *Task, model string, in Input) (Result, error) {
	var def api.Object
	if c := s.catalog.Get(); c != nil {
		def = c.Definition(model)
	}
	if def == nil {
		return Result{}, fmt.Errorf("%w: the %s model %q is not in the catalog", ErrUnavailable, task.Role, model)
	}
	system := task.system()
	room := api.Int(def["budgetWindow"])*windowShareTenths/10 - inference.Tokens(system) - inference.Tokens(task.Closing) - int(task.MaxTokens)
	completion := inference.Completion{Task: task.Name, Model: model, System: system, MaxTokens: task.MaxTokens, Temperature: task.Temperature,
		Params: domain.ModelParams(def, false, summaryEffort), CacheScope: api.Token()}
	summary, docs, parts := in.Earlier, in.Documents, 0
	for parts == 0 || len(docs) > 0 {
		part, rest, err := take(docs, room-inference.Tokens(summary))
		if err == nil && len(rest) > 0 && !task.Fold {
			err = ErrTooLarge
		}
		if err != nil {
			return Result{}, err
		}
		completion.User = task.user(summary, part)
		if summary, err = s.call(ctx, task, completion); err != nil {
			return Result{}, err
		}
		docs, parts = rest, parts+1
	}
	return Result{Text: summary, Model: model, Parts: parts}, nil
}

// take is the documents that fit budget tokens, in order, and the rest. A
// first document too large on its own is split, and its first piece taken.
func take(docs []Document, budget int) (part, rest []Document, err error) {
	if budget < minPartTokens {
		return nil, nil, ErrTooLarge
	}
	used := 0
	for i, d := range docs {
		open, end := d.tags()
		overhead := inference.Tokens(open) + inference.Tokens(end) + documentOverhead
		cost := inference.Tokens(d.Text) + overhead
		if used+cost <= budget {
			part, used = append(part, d), used+cost
			continue
		}
		if len(part) > 0 {
			return part, docs[i:], nil
		}
		if overhead >= budget {
			return nil, nil, ErrTooLarge
		}
		pieces := split(d.Text, budget-overhead)
		first, more := d, d
		if len(pieces) < 2 {
			// Only the whitespace split leaves out made the text too large.
			first.Text = strings.Join(pieces, "")
			return []Document{first}, docs[i+1:], nil
		}
		first.Text = pieces[0]
		more.Text = strings.Join(pieces[1:], "\n\n")
		more.Attrs = append(append([]Attr{}, d.Attrs...), Attr{"part", "continued"})
		return []Document{first}, append([]Document{more}, docs[i+1:]...), nil
	}
	return part, nil, nil
}

// split cuts text into pieces of at most budget tokens, between paragraphs
// where it can, between words where it cannot, and within a word larger
// than a piece, such as an encoded file, between its runes.
func split(text string, budget int) []string {
	var pieces, current []string
	used := 0
	flush := func(separator string) {
		if len(current) > 0 {
			pieces = append(pieces, strings.Join(current, separator))
			current, used = nil, 0
		}
	}
	for _, paragraph := range strings.Split(text, "\n\n") {
		cost := inference.Tokens(paragraph) + 1
		if cost <= budget {
			if used+cost > budget {
				flush("\n\n")
			}
			current, used = append(current, paragraph), used+cost
			continue
		}
		flush("\n\n")
		for _, word := range strings.Fields(paragraph) {
			cost := inference.Tokens(word) + 1
			if cost > budget {
				flush(" ")
				pieces = append(pieces, chunks(word, budget-1)...)
				continue
			}
			if used+cost > budget {
				flush(" ")
			}
			current, used = append(current, word), used+cost
		}
		flush(" ")
	}
	flush("\n\n")
	return pieces
}

// chunks cuts s between runes into chunks of at most limit bytes, so of at
// most limit tokens, since no token is shorter than a byte. A chunk holds a
// rune even when the rune is longer than limit.
func chunks(s string, limit int) []string {
	var cut []string
	limit = max(1, limit)
	for len(s) > limit {
		at := limit
		for at > 0 && !utf8.RuneStart(s[at]) {
			at--
		}
		if at == 0 {
			_, at = utf8.DecodeRuneInString(s)
		}
		cut, s = append(cut, s[:at]), s[at:]
	}
	return append(cut, s)
}

// call asks for one summary, again after a failure worth retrying, up to
// the task's attempts.
func (s *Service) call(ctx context.Context, task *Task, c inference.Completion) (string, error) {
	var err error
	for attempt := range max(1, task.Attempts) {
		if attempt > 0 {
			select {
			case <-time.After(delayAfter(attempt, err)):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		var summary string
		if summary, err = s.attempt(ctx, task, c); err == nil {
			return summary, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if !retryable(err) {
			return "", err
		}
	}
	return "", err
}

func (s *Service) attempt(ctx context.Context, task *Task, c inference.Completion) (string, error) {
	slots := s.slots[task.Role]
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	defer func() { <-slots }()
	callCtx, cancel := context.WithTimeout(ctx, task.Timeout)
	defer cancel()
	done, err := s.account.Complete(callCtx, c)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return task.check(done.Text, done.FinishReason)
}

// retryable is a failure another call may not repeat: a summary that failed
// its check, a refusal for load, a call that timed out, or a connection
// that failed. Any other refusal would be refused again.
func retryable(err error) bool {
	var refused *inference.Refused
	if errors.As(err, &refused) {
		return refused.Status == 429 || refused.Status >= 500
	}
	return true
}

func delayAfter(attempt int, err error) time.Duration {
	var refused *inference.Refused
	if errors.As(err, &refused) && refused.RetryAfter > 0 {
		return min(refused.RetryAfter, maxRetryDelay)
	}
	return min(retryDelay<<(attempt-1), maxRetryDelay)
}
