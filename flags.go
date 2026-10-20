package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
)

// safeguardFlagsPath lists the chats the safeguards flagged for the caller.
const safeguardFlagsPath = "/api/users/me/safeguard-flags"

// maxFlagsBody bounds the control plane's list of a caller's flags.
const maxFlagsBody = 1 << 20

var errChatFlagged = apiErr(403, "CHAT_FLAGGED", "This chat was flagged under the acceptable use policy and takes no more turns")

// chatFlags are the ids of the chats the safeguards flagged.
type chatFlags map[string]bool

// flaggedChats asks the control plane which of the caller's chats the
// safeguards flagged. Only signed-in callers' chats are screened. A list
// the control plane cannot give flags none, as the web client's polling
// did.
func (a *authenticator) flaggedChats(ctx context.Context, p *principal) chatFlags {
	if p.Anonymous || p.JWT == "" {
		return nil
	}
	flags, err := a.readFlags(ctx, p)
	if err != nil {
		slog.Warn("safeguard flags unavailable", "error", err)
	}
	return flags
}

func (a *authenticator) readFlags(ctx context.Context, p *principal) (chatFlags, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.controlplane+safeguardFlagsPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+string(p.JWT))
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFlagsBody))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, downstreamError(resp.StatusCode, body)
	}
	var listed struct {
		Flags []struct {
			ConversationID string `json:"conversation_id"`
		} `json:"flags"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		return nil, err
	}
	flags := chatFlags{}
	for _, flag := range listed.Flags {
		flags[flag.ConversationID] = true
	}
	return flags, nil
}

// mark records on each thread whether the safeguards flagged it. A flag
// once stored stays.
func (f chatFlags) mark(threads ...*Thread) {
	for _, t := range threads {
		t.Flagged = t.Flagged || f[t.ID]
	}
}
