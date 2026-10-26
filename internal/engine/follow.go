package engine

import (
	"net/http"
	"strconv"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// HandleFollow re-attaches a client to a run that is live on this instance.
func (eng *Engine) HandleFollow(w http.ResponseWriter, r *http.Request) {
	p, err := eng.Auth.Caller(r, true)
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
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	eng.Live.mu.Lock()
	if eng.Live.runs[liveKey(p, in.RunID)] != live {
		eng.Live.mu.Unlock()
		api.Respond(w, nil, errRunNotFound)
		return
	}
	live.Log.Frames.Attach()
	if !p.Anonymous && !live.ending {
		p.SyncToken = live.Principal.SyncToken
		p.SyncExpires = live.Principal.SyncExpires
		live.Principal = p
	}
	eng.Live.mu.Unlock()
	defer live.Log.Frames.Detach()
	from, err := ResumeFrom(live.Log.Frames, r.Header.Get(lastEventIDHeader))
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	Follow(w, r, live.Log, from)
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
	sent, _, closed := log.Read(0)
	switch {
	case err != nil || after < 0:
		return 0, api.Err(400, "BAD_REQUEST", "Last-Event-ID must be an event's cursor")
	case after < len(sent):
		return after + 1, nil
	// A log that failed ends its streams with the error, under the cursor
	// after its last frame. A client that applied that error is sent it
	// again, which ends this stream too.
	case after == len(sent) && closed != nil && closed != ErrDone:
		return after, nil
	}
	return 0, api.Err(400, "BAD_REQUEST", "Last-Event-ID names an event this run never sent")
}
