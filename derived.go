package main

import (
	"context"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// titleSource is the text a title summarizes: the first user message, or the
// content of its documents. It is read from the prompt's copy of the thread,
// since a stored row keeps no document content.
func titleSource(t *domain.Thread) string {
	for _, m := range t.Messages {
		if m.Role != "user" {
			continue
		}
		text := m.Content
		if text == "" {
			for i := range m.Attachments {
				// A document whose blob is gone must not name the chat.
				if a := &m.Attachments[i]; documentText(attachmentContent(a)) != unavailableDocument(a) {
					text += documentText(attachmentContent(a)) + " "
				}
			}
		}
		return text
	}
	return ""
}

func (h *harness) generateTitle(ctx context.Context, r *chatRun, p *auth.Principal, titleState string, text string) {
	if !h.models.ServiceReady("summarizer") || titleState != "placeholder" {
		return
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return
	}
	text = strings.Join(words[:min(len(words), 100)], " ")
	var result struct {
		Summary string `json:"summary"`
	}
	if err := h.models.ServiceCall(ctx, "summarizer", "/summarize", api.Object{"content": text, "style": "title_summary"}, &result); err != nil {
		return
	}
	title := strings.Trim(strings.TrimSpace(result.Summary), "\"'")
	if title == "" {
		return
	}
	name := func(t *domain.Thread) error {
		if t.TitleState == "placeholder" {
			t.Title, t.TitleState = title, "generated"
		}
		return nil
	}
	// A local thread's row lives only in this run, and its client stores the
	// title it reads from the event below.
	if r.thread.local {
		h.live.mu.Lock()
		defer h.live.mu.Unlock()
		name(&r.row.Thread)
		r.out.emit(domain.StateDelta{Delta: titleDelta(&r.row.Thread)})
		return
	}
	updated, err := h.store.MutateThread(ctx, p, r.key, r.thread.id, name)
	if err != nil {
		return
	}
	r.out.emit(domain.StateDelta{Delta: titleDelta(&updated.Thread)})
}

func titleDelta(t *domain.Thread) []domain.StatePatch {
	return []domain.StatePatch{{Op: "replace", Path: "/thread/title", Value: t.Title}, {Op: "replace", Path: "/thread/titleState", Value: t.TitleState}}
}
