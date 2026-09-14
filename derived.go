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
		return "", downstreamError(resp.StatusCode, b)
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

func (h *harness) generateTitle(ctx context.Context, r *chatRun, row *storedRow) {
	if !h.serviceReady("summarizer") || row.Data["titleState"] != "placeholder" {
		return
	}
	text := ""
	for _, item := range arr(row.Data["messages"]) {
		m := obj(item)
		if m["role"] == "user" {
			text = str(m["content"])
			if text == "" {
				for _, value := range arr(m["attachments"]) {
					text += str(obj(value)["textContent"]) + " "
				}
			}
			break
		}
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
	updated, err := h.mutate(ctx, r.principal, r.key, "chat", row.ID, false, nil, func(data object) error {
		if data["titleState"] == "placeholder" {
			data["title"], data["titleState"] = title, "generated"
		}
		return nil
	})
	if err != nil {
		return
	}
	r.thread.mu.Lock()
	defer r.thread.mu.Unlock()
	r.thread.row = updated
	r.out.emit(event{Type: "STATE_DELTA", Delta: []object{{"op": "replace", "path": "/thread/title", "value": updated.Data["title"]}, {"op": "replace", "path": "/thread/titleState", "value": updated.Data["titleState"]}}})
}
