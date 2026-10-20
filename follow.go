package main

import "net/http"

// handleFollow re-attaches a client to a run that is live on this instance.
func (h *harness) handleFollow(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(w, r, maxRequestBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	var in runRequest
	if err := decodeRequest(body, &in); err != nil {
		respond(w, nil, err)
		return
	}
	key, err := requestKey(in.Key)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	live, err := h.authorizedRun(p, key, in.ThreadID, in.RunID)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if !p.Anonymous {
		h.live.mu.Lock()
		if live.thread.active == live {
			p.SyncToken = live.principal.SyncToken
			live.principal = p
		}
		h.live.mu.Unlock()
	}
	follow(w, r, live.log)
}
