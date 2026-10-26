package main

import (
	"context"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A reply's reasoning is summarized while it thinks, as the web client did:
// once its trace has enough words to say something, from the trace's tail,
// at most once per interval, and never after the trace closed.
const (
	minSummaryWords  = 20
	summaryTailWords = 200
	summaryInterval  = 3 * time.Second
	summaryTimeout   = 30 * time.Second
	thoughtsStyle    = "thoughts_summary"
)

// thoughtSummaries follows the thinking block a reply streams into. The
// stream's mutex guards every field.
type thoughtSummaries struct {
	ctx       context.Context
	out       *stream
	assembler *domain.MessageAssembler
	summarize func(context.Context, string) (string, error)

	message, block, trace string
	// epoch changes whenever the block it follows closes or another opens,
	// so a summary of an earlier trace is dropped.
	epoch    int
	inFlight bool
	last     time.Time
	timer    *time.Timer
}

// thoughtSummaries starts following a run's reasoning, unless no summarizer
// verified.
func (h *harness) thoughtSummaries(ctx context.Context, out *stream, assembler *domain.MessageAssembler) *thoughtSummaries {
	if !h.models.ServiceReady("summarizer") {
		return nil
	}
	return &thoughtSummaries{ctx: ctx, out: out, assembler: assembler, summarize: func(ctx context.Context, text string) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, summaryTimeout)
		defer cancel()
		var result struct {
			Summary string `json:"summary"`
		}
		err := h.models.ServiceCall(ctx, "summarizer", "/summarize", api.Object{"content": text, "style": thoughtsStyle}, &result)
		return result.Summary, err
	}}
}

// follow reads a frame the run's reply was built from.
func (s *thoughtSummaries) follow(e domain.Event) {
	switch e := e.(type) {
	case domain.ReasoningChunk:
		message := strings.TrimSuffix(e.MessageID, domain.ReasoningSuffix)
		// A late tail of reasoning the reply's text closed is not followed.
		block := s.assembler.LastBlock(message)
		if block == nil || block.Type != "thinking" {
			return
		}
		if block.ID != s.block {
			s.close()
			s.message, s.block = message, block.ID
		}
		s.trace = block.Content
		s.consider()
	case domain.TextChunk, domain.ToolCallStart:
		s.close()
	}
}

// close stops following a trace the reply moved on from.
func (s *thoughtSummaries) close() {
	s.epoch++
	s.message, s.block, s.trace, s.inFlight = "", "", "", false
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

func (s *thoughtSummaries) consider() {
	if s.inFlight || s.timer != nil || len(strings.Fields(s.trace)) < minSummaryWords {
		return
	}
	wait := summaryInterval - time.Since(s.last)
	if wait <= 0 {
		s.fire()
		return
	}
	epoch := s.epoch
	s.timer = time.AfterFunc(wait, func() {
		s.out.mu.Lock()
		defer s.out.mu.Unlock()
		if s.epoch == epoch {
			s.timer = nil
			s.fire()
		}
	})
}

// fire summarizes the trace's tail and frames the summary if its trace is
// still open when it arrives.
func (s *thoughtSummaries) fire() {
	s.inFlight, s.last = true, time.Now()
	tail := s.trace
	if words := strings.Fields(tail); len(words) > summaryTailWords {
		tail = strings.Join(words[len(words)-summaryTailWords:], " ")
	}
	epoch, message, block := s.epoch, s.message, s.block
	go func() {
		defer func() { api.Rescued("reasoning summary", recover()) }()
		summary, err := s.summarize(s.ctx, tail)
		s.out.mu.Lock()
		defer s.out.mu.Unlock()
		if s.epoch != epoch {
			return
		}
		s.inFlight = false
		if summary = strings.TrimSpace(summary); err == nil && summary != "" {
			s.out.frame(domain.ReasoningSummary{MessageID: message, BlockID: block, Content: summary}, api.Timestamp())
		}
	}()
}
