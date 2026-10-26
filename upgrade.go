package main

import (
	"encoding/json"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// handleUpgrade reads chats the previous web client kept on a device, or in
// an export, into the thread format, in order. It stores nothing. A batch
// holding a chat it cannot read is refused whole, so its client can tell
// which chat that is.
func (h *harness) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	// A signed-out tab upgrades the old chats it holds once its free key is
	// accepted.
	if _, err := h.caller(r, true); err != nil {
		api.Respond(w, nil, err)
		return
	}
	body, err := api.ReadBody(w, r, domain.MaxTurnBody)
	defer clear(body)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	var in upgradeRequest
	if err := api.DecodeBody(body, &in); err != nil || in.Threads == nil {
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "threads must be an array"))
		return
	}
	threads := make([]domain.Thread, 0, len(*in.Threads))
	for _, item := range *in.Threads {
		if !api.IsObject(item) {
			api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Each thread must be an object"))
			return
		}
		var named idRequest
		json.Unmarshal(item, &named)
		if err := named.validate(); err != nil {
			api.Respond(w, nil, err)
			return
		}
		if err := validateLocalThread(item); err != nil {
			api.Respond(w, nil, err)
			return
		}
		// The thread is one its client can send back with a turn.
		thread, err := domain.ReadDeviceThread(named.ID, item)
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		modelCopies(&thread)
		thread.CheckWidgets()
		thread.RecordVersion = domain.RecordVersion
		threads = append(threads, thread)
	}
	api.Respond(w, struct {
		Threads []domain.Thread `json:"threads"`
	}{threads}, nil)
}
