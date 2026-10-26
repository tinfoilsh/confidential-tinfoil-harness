package main

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

type jsonEndpoint func(context.Context, *auth.Principal, auth.ContentKey, json.RawMessage) (any, error)

// validated is a request that checks itself once decoded.
type validated interface{ Validate() error }

// decoded is a route whose handler takes its request decoded and checked.
func decoded[T any](handle func(context.Context, *auth.Principal, auth.ContentKey, *T) (any, error)) jsonEndpoint {
	return func(ctx context.Context, p *auth.Principal, key auth.ContentKey, body json.RawMessage) (any, error) {
		defer clear(key[:])
		in := new(T)
		if keyed, ok := any(in).(interface{ ClearKey() }); ok {
			defer keyed.ClearKey()
		}
		if err := api.DecodeBody(body, in); err != nil {
			return nil, err
		}
		if check, ok := any(in).(validated); ok {
			if err := check.Validate(); err != nil {
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
		"GET /healthz":                       func(w http.ResponseWriter, r *http.Request) { api.Respond(w, api.Object{"ok": true}, nil) },
		"POST /v1/threads/turn":              h.engine.HandleTurn,
		"POST /v1/threads/import":            h.engine.HandleThreadImport,
		"POST /v1/threads/follow":            h.engine.HandleFollow,
		"POST /v1/threads/upgrade":           h.handleUpgrade,
		"POST /v1/speech":                    h.attachments.HandleSpeech,
		"POST /v1/attachments/upload":        boundedHandler(uploadSlots, h.attachments.HandleUpload),
		"POST /v1/projects/documents/upload": boundedHandler(uploadSlots, h.attachments.HandleUpload),
		"POST /v1/transcriptions":            boundedHandler(uploadSlots, h.attachments.HandleUpload),
		"POST /v1/import":                    h.archives.HandleImport,
		"POST /v1/export":                    h.archives.HandleExport,
		"POST /v1/attachments/get":           h.attachments.HandleAttachmentGet,
		"POST /v1/attachments/get-public":    h.attachments.HandleAttachmentGet,
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
		p, err := h.auth.Caller(r, spec.public)
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		body, err := api.ReadBody(w, r, api.MaxRequestBody)
		defer clear(body)
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		var key auth.ContentKey
		if spec.key {
			var keyed api.KeyRequest
			defer keyed.ClearKey()
			if err := api.DecodeRequest(body, &keyed); err != nil {
				api.Respond(w, nil, err)
				return
			}
			key, err = auth.DecodeContentKey(keyed.Key)
			keyed.ClearKey()
			if err != nil {
				api.Respond(w, nil, err)
				return
			}
			defer clear(key[:])
		}
		result, err := spec.handler(r.Context(), p, key, body)
		api.Respond(w, result, auth.WithQuota(p, api.RateLimit{}, err))
	}
}

// threadOperation serves a route that changes a stored chat, named by id
// with the content key, or a device chat, which its client sends as a turn
// sends it, and which needs no account.
func (h *harness) threadOperation(handler jsonEndpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := h.auth.Caller(r, true)
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		body, err := api.ReadBody(w, r, domain.MaxTurnBody)
		defer clear(body)
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		var target struct {
			api.KeyRequest
			Local bool `json:"local"`
		}
		defer target.ClearKey()
		if err := api.DecodeRequest(body, &target); err != nil {
			api.Respond(w, nil, err)
			return
		}
		var key auth.ContentKey
		if !target.Local {
			if p.Anonymous {
				api.Respond(w, nil, auth.ErrAnonymousCloud)
				return
			}
			if key, err = auth.DecodeContentKey(target.Key); err != nil {
				api.Respond(w, nil, err)
				return
			}
			defer clear(key[:])
		}
		target.ClearKey()
		result, err := handler(r.Context(), p, key, body)
		api.Respond(w, result, auth.WithQuota(p, api.RateLimit{}, err))
	}
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
			e := api.Err(503, "UPSTREAM_REFUSED", "The service is at capacity; retry shortly")
			e.RetryAfter = 5
			api.Respond(w, nil, e)
			return
		}
		handler(w, r)
	}
}
