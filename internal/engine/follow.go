package engine

import (
	"net/http"
	"strconv"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// HandleFollow re-attaches a client to a run that is live on this instance.
func (eng *Engine) HandleFollow(w http.ResponseWriter, r *http.Request) {
	p, err := eng.Auth.RunCaller(r)
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
	var in RunRequest
	defer in.ClearKey()
	if err := api.DecodeBody(body, &in); err != nil {
		api.Respond(w, nil, err)
		return
	}
	key, err := auth.RequestKey(in.Key)
	in.ClearKey()
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer clear(key[:])
	live, err := eng.authorizedRun(p, key, in.ThreadID, in.RunID)
	clear(key[:])
	if err == errRunNotFound {
		eng.followHeld(w, r, p, in.ThreadID, in.RunID)
		return
	}
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	eng.Live.mu.Lock()
	// A run that ended since it was found may be held for its client.
	if eng.Live.runs[liveKey(p, in.RunID)] != live {
		eng.Live.mu.Unlock()
		eng.followHeld(w, r, p, in.ThreadID, in.RunID)
		return
	}
	live.Log.Frames.Attach()
	if !p.Anonymous && !live.ending {
		p.Grant = live.Principal.Grant
		live.Principal = p
	}
	eng.Live.mu.Unlock()
	defer live.Log.Frames.Detach()
	from, err := ResumeFrom(live.Log.Frames, r.Header.Get(lastEventIDHeader))
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	if Follow(w, r, live.Log, from) {
		eng.Live.release(liveKey(p, in.RunID), live.Log)
	}
}

// followHeld streams the log of a device chat's run that ended to its
// client, which stores the chat the run returns. A client that has read it
// to the end needs it no longer.
func (eng *Engine) followHeld(w http.ResponseWriter, r *http.Request, p *auth.Principal, threadID, runID string) {
	key := liveKey(p, runID)
	eng.Live.mu.Lock()
	held := eng.Live.held[key]
	if held == nil || held.threadID != threadID {
		eng.Live.mu.Unlock()
		api.Respond(w, nil, errRunNotFound)
		return
	}
	held.log.Frames.Attach()
	eng.Live.mu.Unlock()
	defer held.log.Frames.Detach()
	from, err := ResumeFrom(held.log.Frames, r.Header.Get(lastEventIDHeader))
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	if Follow(w, r, held.log, from) {
		eng.Live.release(key, held.log)
	}
}

// lastEventIDHeader names the last event of a run a client applied, as
// server-sent events name it.
const lastEventIDHeader = "Last-Event-ID"

// ResumeFrom is the frame a followed run resumes at: the one after the event
// a client names as the last it applied, or the run's first when it names
// none. A cursor the run never sent is refused.
func ResumeFrom(log *FrameLog, lastEventID string) (int, error) {
	if lastEventID == "" {
		return 0, nil
	}
	after, err := strconv.Atoi(lastEventID)
	switch {
	case err != nil || after < 0:
		return 0, api.Err(400, "BAD_REQUEST", "Last-Event-ID must be an event's cursor")
	case after < log.sent():
		return after + 1, nil
	}
	return 0, api.Err(400, "BAD_REQUEST", "Last-Event-ID names an event this run never sent")
}
