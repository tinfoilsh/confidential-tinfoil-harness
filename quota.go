package main

type tokenBudget struct {
	Max       int `json:"max"`
	Used      int `json:"used"`
	Remaining int `json:"remaining"`
}

// Only free keys count requests; a subscriber's hourly cap is in tokens.
type rateLimit struct {
	Kind         any          `json:"kind"`
	MaxRequests  *int         `json:"maxRequests,omitempty"`
	Remaining    *int         `json:"remaining,omitempty"`
	InputTokens  *tokenBudget `json:"inputTokens,omitempty"`
	OutputTokens *tokenBudget `json:"outputTokens,omitempty"`
	ResetsAt     string       `json:"resetsAt,omitempty"`
}

func (l rateLimit) exhausted() bool { return l.Remaining != nil && *l.Remaining <= 0 }

func exhaustedLimit(kind string) rateLimit {
	return rateLimit{Kind: kind, MaxRequests: new(0), Remaining: new(0)}
}

// quota is the rate_limit object both credential exchanges answer with. A
// missing or zero maximum leaves that dimension unlimited.
type quota struct {
	MaxRequests     *int   `json:"max_requests"`
	Remaining       *int   `json:"remaining"`
	MaxInput        *int   `json:"max_input_tokens"`
	InputUsed       *int   `json:"input_tokens_used"`
	InputRemaining  *int   `json:"input_tokens_remaining"`
	MaxOutput       *int   `json:"max_output_tokens"`
	OutputUsed      *int   `json:"output_tokens_used"`
	OutputRemaining *int   `json:"output_tokens_remaining"`
	ResetsAt        string `json:"resets_at"`
}

func (q quota) limit(kind string) rateLimit {
	l := rateLimit{Kind: kind, InputTokens: budget(q.MaxInput, q.InputUsed, q.InputRemaining), OutputTokens: budget(q.MaxOutput, q.OutputUsed, q.OutputRemaining), ResetsAt: q.ResetsAt}
	if q.MaxRequests != nil && *q.MaxRequests > 0 && q.Remaining != nil {
		l.MaxRequests, l.Remaining = q.MaxRequests, q.Remaining
	}
	return l
}

func budget(limit, used, remaining *int) *tokenBudget {
	if limit == nil || used == nil || remaining == nil || *limit <= 0 {
		return nil
	}
	return &tokenBudget{*limit, *used, *remaining}
}

// debited is the quota after a run's request and the tokens it spent.
func (l rateLimit) debited(spent usage) rateLimit {
	if l.Remaining != nil {
		l.Remaining = new(max(0, *l.Remaining-1))
	}
	l.InputTokens = l.InputTokens.spend(spent.Prompt)
	l.OutputTokens = l.OutputTokens.spend(spent.Completion)
	return l
}

// none is the quota with no request left.
func (l rateLimit) none() rateLimit {
	if l.MaxRequests == nil {
		l.MaxRequests = new(0)
	}
	l.Remaining = new(0)
	return l
}

// spend is a token budget after tokens are spent from it.
func (b *tokenBudget) spend(tokens int64) *tokenBudget {
	if b == nil {
		return nil
	}
	return &tokenBudget{Max: b.Max, Used: b.Used + int(tokens), Remaining: max(0, b.Remaining-int(tokens))}
}

type usage struct {
	Prompt     int64 `json:"prompt_tokens"`
	Completion int64 `json:"completion_tokens"`
	Total      int64 `json:"total_tokens"`
}
