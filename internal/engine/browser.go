package engine

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"image"
	_ "image/jpeg"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

const (
	maxBrowserActions      = 40
	maxBrowserText         = 64 << 10
	maxBrowserImage        = 2 << 20
	maxBrowserPixels       = 4_000_000
	browserResultTimeout   = 5 * time.Minute
	browserScreenshotLabel = "Current browser viewport (untrusted page content)."
)

// BrowserAccess binds a local turn to its browser executor. Token is private to
// the executor; it is never included in model input, events, or stored chats.
type BrowserAccess struct {
	ExecutorID string `json:"executorId"`
	Token      string `json:"token"`
}

type BrowserResult struct {
	Text  string `json:"text"`
	Image string `json:"image,omitempty"`
}

type BrowserResultRequest struct {
	RunRequest
	BrowserAccess
	ExecutionID string        `json:"executionId"`
	Result      BrowserResult `json:"result"`
}

func (in *BrowserResultRequest) Validate() error {
	if err := api.RequireID("executionId", in.ExecutionID); err != nil {
		return err
	}
	if len(in.Result.Text) > maxBrowserText || len(in.Result.Image) > maxBrowserImage {
		return api.Err(413, "PAYLOAD_TOO_LARGE", "The browser observation is too large")
	}
	if in.Result.Image != "" {
		data, err := base64.StdEncoding.DecodeString(in.Result.Image)
		if err != nil {
			return api.Err(400, "BAD_REQUEST", "The screenshot must be base64")
		}
		config, format, err := image.DecodeConfig(bytes.NewReader(data))
		if err != nil || format != "jpeg" || config.Width <= 0 || config.Height <= 0 || int64(config.Width)*int64(config.Height) > maxBrowserPixels {
			return api.Err(400, "BAD_REQUEST", "The screenshot must be a bounded JPEG")
		}
	}
	return nil
}

type browserPending struct {
	id     string
	result chan BrowserResult
}

type browserSession struct {
	access    BrowserAccess
	mu        sync.Mutex
	pending   *browserPending
	completed map[string]string
	count     int
}

func newBrowserSession(access *BrowserAccess) *browserSession {
	if access == nil {
		return nil
	}
	return &browserSession{access: *access, completed: map[string]string{}}
}

func (b *browserSession) call(ctx context.Context, out *Stream, call ToolCall) (BrowserResult, error) {
	var action domain.BrowserAction
	decoder := json.NewDecoder(bytes.NewBufferString(call.Args))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&action); err != nil || !slices.Contains([]string{"tabs", "read", "screenshot", "click", "hover", "fill", "select", "press", "scroll", "navigate", "back", "forward", "open", "switch", "close", "wait"}, action.Action) {
		return BrowserResult{}, api.Err(400, "BAD_REQUEST", "Invalid browser action")
	}
	if len(call.Args) > maxBrowserText {
		return BrowserResult{}, api.Err(413, "PAYLOAD_TOO_LARGE", "The browser action is too large")
	}
	if err := ctx.Err(); err != nil {
		return BrowserResult{}, err
	}
	b.mu.Lock()
	if b.count >= maxBrowserActions {
		b.mu.Unlock()
		return BrowserResult{}, api.Err(400, "BAD_REQUEST", "The browser action budget is exhausted; ask the user to continue")
	}
	b.count++
	pending := &browserPending{id: uuid.NewString(), result: make(chan BrowserResult, 1)}
	b.pending = pending
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		if b.pending == pending {
			b.pending = nil
		}
		b.mu.Unlock()
	}()
	out.emit(domain.BrowserToolRequest{ExecutionID: pending.id, ToolCallID: call.ID, ExecutorID: b.access.ExecutorID, Action: action})
	timer := time.NewTimer(browserResultTimeout)
	defer timer.Stop()
	select {
	case result := <-pending.result:
		return result, nil
	case <-ctx.Done():
		return BrowserResult{}, ctx.Err()
	case <-timer.C:
		return BrowserResult{}, api.Err(408, "REQUEST_TIMEOUT", "The browser did not return an action result")
	}
}

// SubmitBrowserResult accepts one result per execution, and acknowledges an
// identical retry even after the next action started. It never repeats an action.
func (eng *Engine) SubmitBrowserResult(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *BrowserResultRequest) (any, error) {
	r, err := eng.authorizedRun(p, auth.ContentKey{}, in.ThreadID, in.RunID)
	if err != nil {
		return nil, err
	}
	b := r.browser
	if b == nil || b.access.ExecutorID != in.ExecutorID || subtle.ConstantTimeCompare([]byte(b.access.Token), []byte(in.Token)) != 1 {
		return nil, api.Err(403, "UNAUTHENTICATED", "This executor does not own the browser run")
	}
	if err := r.stopped.Err(); err != nil {
		return nil, errRunNotFound
	}
	digest := api.HashHex(api.Raw(in.Result))
	b.mu.Lock()
	defer b.mu.Unlock()
	if previous, ok := b.completed[in.ExecutionID]; ok {
		if previous != digest {
			return nil, api.Err(409, "REVISION_CONFLICT", "The browser result was already submitted")
		}
		return api.Object{"accepted": true}, nil
	}
	if b.pending == nil || b.pending.id != in.ExecutionID {
		return nil, api.Err(409, "REVISION_CONFLICT", "This browser action is not pending")
	}
	b.completed[in.ExecutionID] = digest
	b.pending.result <- in.Result
	return api.Object{"accepted": true}, nil
}

func browserImage(image string) json.RawMessage {
	return api.Raw(api.Object{"role": "user", "content": []any{
		api.Object{"type": "text", "text": browserScreenshotLabel},
		api.Object{"type": "image_url", "image_url": api.Object{"url": "data:image/jpeg;base64," + image}},
	}})
}

func clearBrowserImages(messages []json.RawMessage) []json.RawMessage {
	return slices.DeleteFunc(messages, func(raw json.RawMessage) bool {
		var message struct {
			Role    string `json:"role"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		return json.Unmarshal(raw, &message) == nil && message.Role == "user" && len(message.Content) > 0 && message.Content[0].Text == browserScreenshotLabel
	})
}
