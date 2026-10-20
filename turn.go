package main

import "net/http"

func (h *harness) handleTurn(w http.ResponseWriter, r *http.Request) {
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
	in := &turnRequest{}
	if err := decodeRequest(body, in); err != nil {
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
	if !p.Anonymous && !in.Local || in.Key != "" {
		key, err = decodeContentKey(in.Key)
		if err != nil {
			respond(w, nil, err)
			return
		}
	}
	defer clear(key[:])
	if err := in.validateContent(); err != nil {
		respond(w, nil, err)
		return
	}
	accepted, err := h.acceptTurn(r.Context(), p, key, in)
	if err != nil {
		respond(w, nil, h.withQuota(p, rateLimit{}, err))
		return
	}
	follow(w, r, accepted.log)
}
