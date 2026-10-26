package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

type usageSinkKey struct{}

func (h *harness) completion(ctx context.Context, m *model, payload api.Object) (string, error) {
	endpoint := m.endpoint
	if endpoint == "" {
		endpoint = h.gateway + "/v1/chat/completions"
	}
	payload["stream"] = false
	// Without a scope of its own, a request would share the process-wide
	// cache secret with everyone holding the same credential.
	if _, scoped := payload["user_cache_secret"]; !scoped {
		payload["user_cache_secret"] = token()
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(api.Raw(payload)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return "", api.WithRetryAfter(api.DownstreamError(resp.StatusCode, b), resp.Header)
	}
	if sink, ok := ctx.Value(usageSinkKey{}).(*stream); ok {
		if usage, ok := ctx.Value(usageContextKey{}).(usagereporting.Context); ok && usage.BillCustomerRequest {
			sink.mu.Lock()
			sink.modelAccepted = true
			sink.mu.Unlock()
		}
	}
	var result struct {
		Usage   json.RawMessage `json:"usage"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxShownOutput)).Decode(&result); err != nil {
		return "", err
	}
	if len(result.Choices) == 0 {
		return "", api.Err(502, "UPSTREAM_REFUSED", "The completion is empty")
	}
	if sink, ok := ctx.Value(usageSinkKey{}).(*stream); ok && len(result.Usage) > 0 {
		sink.meter(result.Usage)
	}
	return result.Choices[0].Message.Content, nil
}

// titleSource is the text a title summarizes: the first user message, or the
// content of its documents. It is read from the prompt's copy of the thread,
// since a stored row keeps no document content.
func titleSource(t *Thread) string {
	for _, m := range t.Messages {
		if m.Role != "user" {
			continue
		}
		text := m.Content
		if text == "" {
			for i := range m.Attachments {
				// A document whose blob is gone must not name the chat.
				if a := &m.Attachments[i]; documentText(a.content()) != unavailableDocument(a) {
					text += documentText(a.content()) + " "
				}
			}
		}
		return text
	}
	return ""
}

func (h *harness) generateTitle(ctx context.Context, r *chatRun, p *auth.Principal, titleState string, text string) {
	if !h.serviceReady("summarizer") || titleState != "placeholder" {
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
	if err := h.serviceCall(ctx, "summarizer", "/summarize", api.Object{"content": text, "style": "title_summary"}, &result); err != nil {
		return
	}
	title := strings.Trim(strings.TrimSpace(result.Summary), "\"'")
	if title == "" {
		return
	}
	name := func(t *Thread) error {
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
		r.out.emit(stateDelta{Delta: titleDelta(&r.row.Thread)})
		return
	}
	updated, err := h.mutateThread(ctx, p, r.key, r.thread.id, name)
	if err != nil {
		return
	}
	r.out.emit(stateDelta{Delta: titleDelta(&updated.Thread)})
}

func titleDelta(t *Thread) []statePatch {
	return []statePatch{{"replace", "/thread/title", t.Title}, {"replace", "/thread/titleState", t.TitleState}}
}
