package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

// object is JSON whose shape the harness does not own: tool output, model
// messages, downstream errors, the model catalog, and archives.
type object map[string]any

func raw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

func str(v any) string { s, _ := v.(string); return s }
func obj(v any) object {
	switch m := v.(type) {
	case object:
		return m
	case map[string]any:
		return object(m)
	}
	return object{}
}
func arr(v any) []any {
	a, _ := v.([]any)
	if values, ok := v.([]object); ok {
		for _, value := range values {
			a = append(a, value)
		}
	}
	if a == nil {
		return []any{}
	}
	return a
}
func boolean(v any, fallback bool) bool {
	b, ok := v.(bool)
	if ok {
		return b
	}
	return fallback
}
func integer(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

func nowUTC() time.Time { return time.Now().UTC() }
func timestamp() string { return nowUTC().Format(time.RFC3339Nano) }
func firstString(values ...any) string {
	for _, v := range values {
		if s := str(v); s != "" {
			return s
		}
	}
	return ""
}

type apiError struct {
	HTTP       int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
	Kind       string `json:"kind,omitempty"`
	ResetsAt   string `json:"resetsAt,omitempty"`
	RetryAfter int    `json:"retryAfter,omitempty"`
	Status     int    `json:"status,omitempty"`
	KeyID      string `json:"keyId,omitempty"`
	Upstream   string `json:"upstreamCode,omitempty"`
	// RateLimit is the caller's quota as an exhausted one's refusal leaves it.
	RateLimit *rateLimit `json:"rateLimit,omitempty"`

	missing []string // attachment ids sync did not find under the pushed chat
}

func (e *apiError) Error() string { return e.Message }
func apiErr(status int, code, message string) *apiError {
	return &apiError{HTTP: status, Code: code, Message: message}
}
func asAPIError(err error) *apiError {
	var a *apiError
	if errors.As(err, &a) {
		return a
	}
	if errors.Is(err, errTooLong) {
		return apiErr(413, "RUN_LOG_FULL", "The run log is full")
	}
	var refusal *refusal
	if errors.As(err, &refusal) {
		e := downstreamError(refusal.status, []byte(refusal.detail))
		if refusal.retryAfter > 0 {
			e.RetryAfter = refusal.retryAfter
		}
		return e
	}
	return apiErr(502, "UPSTREAM_REFUSED", "A downstream request failed")
}
func respond(w http.ResponseWriter, result any, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		e := asAPIError(err)
		if e.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(e.RetryAfter))
		}
		w.WriteHeader(e.HTTP)
		result = object{"error": e}
	}
	if result == nil {
		result = object{}
	}
	json.NewEncoder(w).Encode(result)
}

const maxRequestBody = 8 << 20

// maxResponseBody bounds a downstream answer: a pull of sync's largest batch,
// or an attachment, base64 encoded.
const maxResponseBody = 96 << 20

type jsonEndpoint func(context.Context, *principal, contentKey, json.RawMessage) (any, error)

// validated is a request that checks itself once decoded.
type validated interface{ validate() error }

// decoded is a route whose handler takes its request decoded and checked.
func decoded[T any](handle func(context.Context, *principal, contentKey, *T) (any, error)) jsonEndpoint {
	return func(ctx context.Context, p *principal, key contentKey, body json.RawMessage) (any, error) {
		in := new(T)
		if err := decodeRequest(body, in); err != nil {
			return nil, err
		}
		if check, ok := any(in).(validated); ok {
			if err := check.validate(); err != nil {
				return nil, err
			}
		}
		return handle(ctx, p, key, in)
	}
}

type routeSpec struct {
	path        string
	public, key bool
	handler     jsonEndpoint
}

func (h *harness) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, object{"ok": true}, nil) })
	for _, spec := range h.apiRoutes() {
		mux.HandleFunc("POST "+spec.path, func(w http.ResponseWriter, r *http.Request) {
			body, err := readBody(w, r, maxRequestBody)
			if err != nil {
				respond(w, nil, err)
				return
			}
			p, err := h.auth.authenticate(r, spec.public)
			if err != nil {
				respond(w, nil, err)
				return
			}
			var key contentKey
			if spec.key {
				var keyed struct {
					Key any `json:"key"`
				}
				json.Unmarshal(body, &keyed)
				text, _ := keyed.Key.(string)
				key, err = decodeContentKey(text)
				if err != nil {
					respond(w, nil, err)
					return
				}
				defer clear(key[:])
			}
			result, err := spec.handler(r.Context(), p, key, body)
			respond(w, result, h.withQuota(p, err))
		})
	}
	for path, handler := range h.threadOperations() {
		mux.HandleFunc("POST "+path, h.threadOperation(handler))
	}
	mux.HandleFunc("POST /v1/threads/turn", h.handleTurn)
	mux.HandleFunc("POST /v1/threads/import", h.handleThreadImport)
	mux.HandleFunc("POST /v1/threads/follow", h.handleFollow)
	mux.HandleFunc("POST /v1/threads/upgrade", h.handleUpgrade)
	mux.HandleFunc("POST /v1/speech", h.handleSpeech)
	for _, path := range []string{"/v1/attachments/upload", "/v1/projects/documents/upload", "/v1/transcriptions", "/v1/import"} {
		mux.HandleFunc("POST "+path, boundedHandler(uploadSlots, h.handleUpload))
	}
	mux.HandleFunc("POST /v1/export", boundedHandler(archiveSlots, h.handleExport))
	for _, path := range []string{"/v1/attachments/get", "/v1/attachments/get-public"} {
		mux.HandleFunc("POST "+path, h.handleAttachmentGet)
	}
	return mux
}

// threadOperation serves a route that changes a stored chat, named by id
// with the content key, or a device chat, which its client sends as a turn
// sends it, and which needs no account.
func (h *harness) threadOperation(handler jsonEndpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := h.auth.authenticate(r, true)
		if err != nil {
			respond(w, nil, err)
			return
		}
		body, err := readBody(w, r, maxTurnBody)
		if err != nil {
			respond(w, nil, err)
			return
		}
		var target struct {
			Key   string `json:"key"`
			Local bool   `json:"local"`
		}
		json.Unmarshal(body, &target)
		var key contentKey
		if !target.Local {
			if p.Anonymous {
				respond(w, nil, errAnonymousCloud)
				return
			}
			if key, err = decodeContentKey(target.Key); err != nil {
				respond(w, nil, err)
				return
			}
			defer clear(key[:])
		}
		result, err := handler(r.Context(), p, key, body)
		respond(w, result, h.withQuota(p, err))
	}
}

// errAnonymousCloud refuses a signed-out caller a stored chat.
var errAnonymousCloud = apiErr(401, "UNAUTHENTICATED", "Anonymous chats are kept on the device")

func downstreamError(status int, body []byte) *apiError {
	var in object
	json.Unmarshal(body, &in)
	if nested := obj(in["error"]); len(nested) > 0 {
		in = nested
	}
	code := str(in["code"])
	e := apiErr(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	e.Status = status
	switch code {
	case "STALE_KEY", "EXISTING_DATA_UNDER_OTHER_KEY", "UNKNOWN_KEY", "KEY_MISMATCH":
		e = apiErr(409, "KEY_MISMATCH", "The stored data uses another content key")
		e.KeyID = firstString(in["current_key_id"], in["key_id"])
	case "STALE_BLOB", "SYNC_CONFLICT":
		e = apiErr(409, "REVISION_CONFLICT", "The stored row changed")
	case "MISSING_ATTACHMENT":
		e = apiErr(409, "MISSING_ATTACHMENT", "The chat references attachments that are not stored with it")
		for _, id := range arr(in["missing_attachments"]) {
			e.missing = append(e.missing, str(id))
		}
	case "PAYLOAD_TOO_LARGE":
		e = apiErr(413, "THREAD_TOO_LARGE", "The chat is too large to store")
	case "SYNC_PROTOCOL_UPGRADE_REQUIRED":
		e.Message = "The sync service requires a newer protocol"
	case "UPSTREAM":
		if str(in["reason"]) == "outcome_unknown" {
			// The write may have been stored. Retrying reapplies the change
			// to a freshly pulled row, so retrying is safe.
			e = apiErr(503, "SYNC_OUTCOME_UNKNOWN", "The sync service may not have stored the change")
		}
	default:
		if refused := openAIRefusal(status, body); refused != nil {
			if refused.RetryAfter == 0 {
				refused.RetryAfter = integer(in["retryAfter"])
			}
			return refused
		}
		switch status {
		case 401:
			e = apiErr(401, "UNAUTHENTICATED", "Authentication is required")
		case 404:
			e = apiErr(404, "THREAD_NOT_FOUND", "The requested data was not found")
		case 412:
			e = apiErr(409, "REVISION_CONFLICT", "The stored row changed")
		case 429:
			e = apiErr(429, "QUOTA_EXHAUSTED", "The request quota is exhausted")
			e.Kind = firstString(in["kind"], in["type"])
			e.ResetsAt = firstString(in["resetsAt"], in["resets_at"])
			e.RetryAfter = integer(in["retryAfter"])
		}
	}
	return e
}

func postJSON(ctx context.Context, client *http.Client, url, auth string, in, out any) error {
	return postJSONWithHeaders(ctx, client, url, auth, nil, in, out)
}

func postJSONWithHeaders(ctx context.Context, client *http.Client, url, auth string, headers map[string]string, in, out any) error {
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(raw(in)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		// A missing-attachment error lists every missing id.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return withRetryAfter(downstreamError(resp.StatusCode, b), resp.Header)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, maxRequestBody))
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(out)
}

var uploadSlots = make(chan struct{}, 4)
var archiveSlots = make(chan struct{}, 1)

func boundedHandler(slots chan struct{}, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		case <-r.Context().Done():
			return
		default:
			e := apiErr(503, "UPSTREAM_REFUSED", "The service is at capacity; retry shortly")
			e.RetryAfter = 5
			respond(w, nil, e)
			return
		}
		handler(w, r)
	}
}
