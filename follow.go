package main

import (
	"net/http"
	"strconv"
)

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
	var in runRequest
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
	from, err := resumeFrom(live.log.log, r.Header.Get(lastEventIDHeader))
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
	follow(w, r, live.log, from)
}

// lastEventIDHeader names the last event of a run a client applied, as
// server-sent events name it.
const lastEventIDHeader = "Last-Event-ID"

// resumeFrom is the frame a followed run resumes at: the one after the event
// a client names as the last it applied, or the run's first when it names
// none. A cursor the run never sent is refused.
func resumeFrom(log *runlog, lastEventID string) (int, error) {
	if lastEventID == "" {
		return 0, nil
	}
	after, err := strconv.Atoi(lastEventID)
	sent, _, closed := log.read(0)
	switch {
	case err != nil || after < 0:
		return 0, apiErr(400, "BAD_REQUEST", "Last-Event-ID must be an event's cursor")
	case after < len(sent):
		return after + 1, nil
	// A log that failed ends its streams with the error, under the cursor
	// after its last frame. A client that applied that error is sent it
	// again, which ends this stream too.
	case after == len(sent) && closed != nil && closed != errDone:
		return after, nil
	}
	return 0, apiErr(400, "BAD_REQUEST", "Last-Event-ID names an event this run never sent")
}
