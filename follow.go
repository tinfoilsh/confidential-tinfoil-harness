package main

import "net/http"

// handleFollow re-attaches a client to a run that is live on this instance.
func (h *harness) handleFollow(w http.ResponseWriter, r *http.Request) {
	in, err := decodeJSON(w, r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	key, err := requestKey(in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	live, err := h.authorizedRun(p, key, in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if !p.Anonymous {
		h.live.mu.Lock()
		if live.thread.active == live {
			live.principal = p
		}
		h.live.mu.Unlock()
	}
	follow(w, r, live.log)
}
