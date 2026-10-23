package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// object is JSON whose shape the harness does not own: tool output, model
// messages, downstream errors, and the model catalog.
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
	HTTP    int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
	// Action is what a client does about the error when it has no handling
	// of its own for the code.
	Action     string `json:"action"`
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

// The actions protocol/openapi.yaml describes as ErrorAction.
const (
	actionReauthenticate = "reauthenticate"
	actionReload         = "reload"
	actionRetry          = "retry"
	actionWait           = "wait"
	actionUnlock         = "unlock"
	actionRevise         = "revise"
	actionStop           = "stop"
)

// errorActions is what a client does about each code, unless an error says
// otherwise.
var errorActions = map[string]string{
	"ALREADY_APPLIED":         actionReload,
	"ARCHIVE_TOO_LARGE":       actionRevise,
	"ATTACHMENT_TOO_LARGE":    actionRevise,
	"ATTACHMENT_UNSUPPORTED":  actionRevise,
	"ATTACHMENTS_UNAVAILABLE": actionRetry,
	"BAD_REQUEST":             actionStop,
	"CHAT_FLAGGED":            actionStop,
	"CONTEXT_TOO_LARGE":       actionRevise,
	"CREDENTIAL_REQUIRED":     actionReauthenticate,
	"IMPORT_INVALID":          actionRevise,
	"IMPORT_TOO_LARGE":        actionRevise,
	"KEY_MISMATCH":            actionUnlock,
	"KEY_REQUIRED":            actionUnlock,
	"MAX_TURNS":               actionRetry,
	"MISSING_ATTACHMENT":      actionStop,
	"MODEL_UNAVAILABLE":       actionRevise,
	"PAYLOAD_TOO_LARGE":       actionRevise,
	"PROJECTS_REQUIRE_CLOUD":  actionStop,
	"QUEUE_FULL":              actionRetry,
	"QUOTA_EXHAUSTED":         actionWait,
	"RATE_LIMITED":            actionRetry,
	"REQUEST_TIMEOUT":         actionRetry,
	"REVISION_CONFLICT":       actionReload,
	"RUN_IN_FLIGHT":           actionReload,
	"RUN_LOG_FULL":            actionStop,
	"RUN_NOT_FOUND":           actionReload,
	"SYNC_OUTCOME_UNKNOWN":    actionRetry,
	"TEMP_SPACE_BUSY":         actionRetry,
	"THREAD_EXISTS":           actionStop,
	"THREAD_NOT_FOUND":        actionStop,
	"THREAD_TOO_LARGE":        actionRevise,
	"TOOL_UNAVAILABLE":        actionRetry,
	"UNAUTHENTICATED":         actionReauthenticate,
	"UPSTREAM_REFUSED":        actionRetry,
}

func (e *apiError) Error() string { return e.Message }
func apiErr(status int, code, message string) *apiError {
	return &apiError{HTTP: status, Code: code, Message: message, Action: errorActions[code]}
}

// withAction has a client take another action about a new error than its
// code's.
func (e *apiError) withAction(action string) *apiError {
	e.Action = action
	return e
}
func asAPIError(err error) *apiError {
	var a *apiError
	if errors.As(err, &a) {
		return a
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
		defer clear(key[:])
		in := new(T)
		if keyed, ok := any(in).(interface{ clearKey() }); ok {
			defer keyed.clearKey()
		}
		if err := decodeBody(body, in); err != nil {
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
	for pattern, handler := range h.handlers() {
		mux.HandleFunc(pattern, handler)
	}
	if contractCheck != nil {
		return contractCheck(mux)
	}
	return mux
}

// contractCheck, set only by the tests, checks every request and response
// against the published protocol.
var contractCheck func(http.Handler) http.Handler

// handlers are every route the harness serves, by method and path.
func (h *harness) handlers() map[string]http.HandlerFunc {
	handlers := map[string]http.HandlerFunc{
		"GET /healthz":                       func(w http.ResponseWriter, r *http.Request) { respond(w, object{"ok": true}, nil) },
		"POST /v1/threads/turn":              h.handleTurn,
		"POST /v1/threads/import":            h.handleThreadImport,
		"POST /v1/threads/follow":            h.handleFollow,
		"POST /v1/threads/upgrade":           h.handleUpgrade,
		"POST /v1/speech":                    h.handleSpeech,
		"POST /v1/attachments/upload":        boundedHandler(uploadSlots, h.handleUpload),
		"POST /v1/projects/documents/upload": boundedHandler(uploadSlots, h.handleUpload),
		"POST /v1/transcriptions":            boundedHandler(uploadSlots, h.handleUpload),
		"POST /v1/import":                    h.handleImport,
		"POST /v1/export":                    h.handleExport,
		"POST /v1/attachments/get":           h.handleAttachmentGet,
		"POST /v1/attachments/get-public":    h.handleAttachmentGet,
	}
	add := func(pattern string, handler http.HandlerFunc) {
		if handlers[pattern] != nil {
			panic("two handlers serve " + pattern)
		}
		handlers[pattern] = handler
	}
	for _, spec := range h.apiRoutes() {
		add("POST "+spec.path, h.jsonRoute(spec))
	}
	for path, handler := range h.threadOperations() {
		add("POST "+path, h.threadOperation(handler))
	}
	return handlers
}

// jsonRoute serves a route whose request is one JSON object, after it knows
// the caller.
func (h *harness) jsonRoute(spec routeSpec) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := h.caller(r, spec.public)
		if err != nil {
			respond(w, nil, err)
			return
		}
		body, err := readBody(w, r, maxRequestBody)
		defer clear(body)
		if err != nil {
			respond(w, nil, err)
			return
		}
		var key contentKey
		if spec.key {
			var keyed keyRequest
			defer keyed.clearKey()
			if err := decodeRequest(body, &keyed); err != nil {
				respond(w, nil, err)
				return
			}
			key, err = decodeContentKey(keyed.Key)
			keyed.clearKey()
			if err != nil {
				respond(w, nil, err)
				return
			}
			defer clear(key[:])
		}
		result, err := spec.handler(r.Context(), p, key, body)
		respond(w, result, h.withQuota(p, rateLimit{}, err))
	}
}

// caller is who a request is from, recognized before its body is read.
func (h *harness) caller(r *http.Request, anonymous bool) (*principal, error) {
	p, err := h.auth.authenticate(r, anonymous)
	if err != nil {
		return nil, err
	}
	return p, h.auth.recognize(r.Context(), p)
}

// threadOperation serves a route that changes a stored chat, named by id
// with the content key, or a device chat, which its client sends as a turn
// sends it, and which needs no account.
func (h *harness) threadOperation(handler jsonEndpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := h.caller(r, true)
		if err != nil {
			respond(w, nil, err)
			return
		}
		body, err := readBody(w, r, maxTurnBody)
		defer clear(body)
		if err != nil {
			respond(w, nil, err)
			return
		}
		var target struct {
			keyRequest
			Local bool `json:"local"`
		}
		defer target.clearKey()
		if err := decodeRequest(body, &target); err != nil {
			respond(w, nil, err)
			return
		}
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
		target.clearKey()
		result, err := handler(r.Context(), p, key, body)
		respond(w, result, h.withQuota(p, rateLimit{}, err))
	}
}

// errAnonymousCloud refuses a signed-out caller a stored chat.
var errAnonymousCloud = apiErr(401, "UNAUTHENTICATED", "Anonymous chats are kept on the device").withAction(actionStop)

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
		e.Message, e.Action = "The sync service requires a newer protocol", actionStop
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

// requestBuffer lets a transport finish reading or replaying a request while
// cleanup erases its bytes. RoundTrip may return before its body reader stops.
type requestBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *requestBuffer) clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.data)
	b.data = nil
}

func (b *requestBuffer) open() (io.ReadCloser, error) {
	return &requestReader{buffer: b}, nil
}

type requestReader struct {
	buffer *requestBuffer
	offset int
}

func (r *requestReader) Read(dst []byte) (int, error) {
	r.buffer.mu.Lock()
	defer r.buffer.mu.Unlock()
	if r.offset >= len(r.buffer.data) {
		return 0, io.EOF
	}
	n := copy(dst, r.buffer.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *requestReader) Close() error { return nil }

func postJSONWithHeaders(ctx context.Context, client *http.Client, url, auth string, headers map[string]string, in, out any) error {
	body, encoded := in.(json.RawMessage)
	if !encoded {
		body = raw(in)
	}
	buffer := &requestBuffer{data: body}
	defer buffer.clear()
	reader, _ := buffer.open()
	req, err := http.NewRequestWithContext(ctx, "POST", url, reader)
	if err != nil {
		return err
	}
	req.ContentLength, req.GetBody = int64(len(body)), buffer.open
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
