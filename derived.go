package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

type usageSinkKey struct{}

func (h *harness) completion(ctx context.Context, m *model, payload object) (string, error) {
	endpoint := m.endpoint
	if endpoint == "" {
		endpoint = h.gateway + "/v1/chat/completions"
	}
	payload["stream"] = false
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(raw(payload)))
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
		return "", withRetryAfter(downstreamError(resp.StatusCode, b), resp.Header)
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
		return "", apiErr(502, "UPSTREAM_REFUSED", "The completion is empty")
	}
	if sink, ok := ctx.Value(usageSinkKey{}).(*stream); ok && len(result.Usage) > 0 {
		sink.meter(result.Usage)
	}
	return result.Choices[0].Message.Content, nil
}

// titleSource is the text a title summarizes: the first user message, or the
// content of its documents. It is read from the prompt's copy of the thread,
// since a stored row keeps no document content.
func titleSource(thread object) string {
	for _, item := range arr(thread["messages"]) {
		m := obj(item)
		if m["role"] != "user" {
			continue
		}
		text := str(m["content"])
		if text == "" {
			for _, value := range arr(m["attachments"]) {
				// A document whose blob is gone must not name the chat.
				if a := obj(value); documentText(a) != unavailableDocument(a) {
					text += documentText(a) + " "
				}
			}
		}
		return text
	}
	return ""
}

func (h *harness) generateTitle(ctx context.Context, r *chatRun, p *principal, titleState any, text string) {
	if !h.serviceReady("summarizer") || titleState != "placeholder" {
		return
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return
	}
	text = strings.Join(words[:min(len(words), 100)], " ")
	var result object
	if err := h.serviceCall(ctx, "summarizer", "/summarize", object{"content": text, "style": "title_summary"}, &result); err != nil {
		return
	}
	title := strings.Trim(strings.TrimSpace(str(result["summary"])), "\"'")
	if title == "" {
		return
	}
	name := func(data object) error {
		if data["titleState"] == "placeholder" {
			data["title"], data["titleState"] = title, "generated"
		}
		return nil
	}
	// A local thread's row lives only in this run, and its client stores the
	// title it reads from the event below.
	if r.thread.local {
		h.live.mu.Lock()
		defer h.live.mu.Unlock()
		name(r.row.Data)
		r.out.emit(event{Type: "STATE_DELTA", Delta: titleDelta(r.row.Data)})
		return
	}
	updated, err := h.mutate(ctx, p, r.key, "chat", r.thread.id, false, nil, name)
	if err != nil {
		return
	}
	r.out.emit(event{Type: "STATE_DELTA", Delta: titleDelta(updated.Data)})
}

func titleDelta(data object) []object {
	return []object{{"op": "replace", "path": "/thread/title", "value": data["title"]}, {"op": "replace", "path": "/thread/titleState", "value": data["titleState"]}}
}
