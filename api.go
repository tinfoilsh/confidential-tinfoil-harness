package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// object preserves fields written by newer web or iOS clients on CAS retries.
type object map[string]any

func raw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func clone(v object) object {
	var copy object
	json.Unmarshal(raw(v), &copy)
	if copy == nil {
		copy = object{}
	}
	return copy
}
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
		return downstreamError(refusal.status, []byte(refusal.detail))
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

func decodeJSON(w http.ResponseWriter, r *http.Request) (object, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	var in object
	if err := decoder.Decode(&in); err != nil {
		return nil, apiErr(400, "BAD_REQUEST", "Expected a JSON object within the request size limit")
	}
	if in == nil {
		return nil, apiErr(400, "BAD_REQUEST", "Expected a JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, apiErr(400, "BAD_REQUEST", "Expected one JSON object")
	}
	return in, nil
}

type jsonEndpoint func(context.Context, *principal, contentKey, object) (any, error)
type routeSpec struct {
	path        string
	public, key bool
	handler     jsonEndpoint
}

func (h *harness) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, object{"ok": true}, nil) })
	// /agui remains available for the shadow and client transition phases.
	mux.HandleFunc("POST /agui", h.agui)
	mux.HandleFunc("DELETE /agui", h.drop)
	for _, spec := range h.apiRoutes() {
		mux.HandleFunc("POST "+spec.path, func(w http.ResponseWriter, r *http.Request) {
			in, err := decodeJSON(w, r)
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
				key, err = decodeContentKey(str(in["key"]))
				if err != nil {
					respond(w, nil, err)
					return
				}
				defer clear(key[:])
			}
			result, err := spec.handler(r.Context(), p, key, in)
			respond(w, result, err)
		})
	}
	mux.HandleFunc("POST /v1/threads/turn", h.handleTurn)
	mux.HandleFunc("POST /v1/threads/follow", h.handleFollow)
	for _, path := range []string{"/v1/attachments/upload", "/v1/projects/documents/upload", "/v1/transcriptions", "/v1/import"} {
		mux.HandleFunc("POST "+path, boundedHandler(uploadSlots, h.handleUpload))
	}
	mux.HandleFunc("POST /v1/export", boundedHandler(archiveSlots, h.handleExport))
	for _, path := range []string{"/v1/attachments/get", "/v1/attachments/get-public"} {
		mux.HandleFunc("POST "+path, h.handleAttachmentGet)
	}
	return mux
}

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
	case "PII_BLOCKED", "PII_DETECTED":
		e = apiErr(422, "PII_BLOCKED", "The privacy screen refused the request")
	default:
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
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(raw(in)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Sync-Protocol", "2")
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return downstreamError(resp.StatusCode, b)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, maxRequestBody))
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 96<<20)).Decode(out)
}

func requiredID(in object, field string) (string, error) {
	id := str(in[field])
	if id == "" || !identifier(id) || strings.ContainsAny(id, "/\\?#") {
		return "", apiErr(400, "BAD_REQUEST", field+" must be a valid identifier")
	}
	return id, nil
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
			respond(w, nil, apiErr(503, "UPSTREAM_REFUSED", "The service is at capacity; retry shortly"))
			return
		}
		handler(w, r)
	}
}
