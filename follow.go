package main

import "net/http"

// handleFollow re-attaches a client to a run that is live on this instance.
func (h *harness) handleFollow(w http.ResponseWriter, r *http.Request) {
	p, err := h.caller(r, true)
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
	var in struct {
		runRequest
		After *int `json:"after"`
	}
	defer in.clearKey()
	if err := decodeBody(body, &in); err != nil {
		respond(w, nil, err)
		return
	}
	key, err := requestKey(in.Key)
	in.clearKey()
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	live, err := h.authorizedRun(p, key, in.ThreadID, in.RunID)
	clear(key[:])
	if err != nil {
		respond(w, nil, err)
		return
	}
	if !p.Anonymous {
		h.live.mu.Lock()
		if !live.ending && h.live.runs[liveKey(p, in.RunID)] == live {
			p.SyncToken = live.principal.SyncToken
			p.SyncExpires = live.principal.SyncExpires
			live.principal = p
		}
		h.live.mu.Unlock()
	}
	follow(w, r, live.log, valueOr(in.After, -1))
}
