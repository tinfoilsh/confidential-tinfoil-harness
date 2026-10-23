package main

import "net/http"

func (h *harness) handleTurn(w http.ResponseWriter, r *http.Request) {
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	credential, err := h.auth.inference(r.Context(), p)
	if err != nil {
		respond(w, nil, h.withQuota(p, credential.Limit, err))
		return
	}
	body, err := readBody(w, r, maxTurnBody)
	defer clear(body)
	if err != nil {
		respond(w, nil, err)
		return
	}
	in := &turnRequest{}
	defer in.clearKey()
	if err := decodeBody(body, in); err != nil {
		respond(w, nil, err)
		return
	}
	if in.Options.Locale == "" {
		in.Options.Locale = r.Header.Get("Accept-Language")
	}
	if err := in.validate(); err != nil {
		respond(w, nil, err)
		return
	}
	if p.Anonymous && !in.Local {
		respond(w, nil, errAnonymousCloud)
		return
	}
	var key contentKey
	if !p.Anonymous && !in.Local || len(in.Key) != 0 {
		key, err = decodeContentKey(in.Key)
		if err != nil {
			respond(w, nil, err)
			return
		}
	}
	defer clear(key[:])
	in.clearKey()
	if err := in.validateContent(); err != nil {
		respond(w, nil, err)
		return
	}
	accepted, err := h.acceptTurn(r.Context(), p, credential, key, in)
	clear(key[:])
	if err != nil {
		respond(w, nil, h.withQuota(p, rateLimit{}, err))
		return
	}
	follow(w, r, accepted.log, -1)
}
