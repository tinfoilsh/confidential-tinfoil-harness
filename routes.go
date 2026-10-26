package main

import (
	"context"
	"encoding/json"
	"net/http"
)

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
