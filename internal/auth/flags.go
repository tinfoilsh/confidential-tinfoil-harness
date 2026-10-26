package auth

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// SafeguardFlagsPath lists the chats the safeguards flagged for the caller.
const SafeguardFlagsPath = "/api/users/me/safeguard-flags"

// maxFlagsBody bounds the control plane's list of a caller's flags.
const maxFlagsBody = 1 << 20

// FlagsTimeout bounds the flag lookup that every chat read and turn makes.
var FlagsTimeout = 2 * time.Second

// A caller's flags are reused for flagsTTL, so the control plane does not
// learn of every chat read and turn, for up to maxFlagCallers callers. A chat
// flagged in that time may take one more turn.
const (
	flagsTTL       = time.Minute
	maxFlagCallers = 10000
)

var ErrChatFlagged = api.Err(403, "CHAT_FLAGGED", "This chat was flagged under the acceptable use policy and takes no more turns")

// ChatFlags are the ids of the chats the safeguards flagged.
type ChatFlags map[string]bool

// flaggedChats asks the control plane which of the caller's chats the
// safeguards flagged. Only signed-in callers' chats are screened. A list
// the control plane cannot give in time flags none, as the web client's
// polling did.
func (a *Authenticator) FlaggedChats(ctx context.Context, p *Principal) ChatFlags {
	if p.Anonymous || p.JWT == "" {
		return nil
	}
	if flags, ok := a.recentFlags().Get(p.Scope()); ok {
		return flags
	}
	ctx, cancel := context.WithTimeout(ctx, FlagsTimeout)
	defer cancel()
	flags, err := a.readFlags(ctx, p)
	if err != nil {
		slog.Warn("safeguard flags unavailable", "error", err)
		return nil
	}
	a.recentFlags().Add(p.Scope(), flags)
	return flags
}

// recentFlags are the flags last read for each caller. Callers only read them.
func (a *Authenticator) recentFlags() *expirable.LRU[string, ChatFlags] {
	a.flagsOnce.Do(func() { a.flags = expirable.NewLRU[string, ChatFlags](maxFlagCallers, nil, flagsTTL) })
	return a.flags
}

func (a *Authenticator) readFlags(ctx context.Context, p *Principal) (ChatFlags, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.Controlplane+SafeguardFlagsPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+string(p.JWT))
	resp, err := a.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFlagsBody))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, api.DownstreamError(resp.StatusCode, body)
	}
	var listed struct {
		Flags []struct {
			ConversationID string `json:"conversation_id"`
		} `json:"flags"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		return nil, err
	}
	flags := ChatFlags{}
	for _, flag := range listed.Flags {
		flags[flag.ConversationID] = true
	}
	return flags, nil
}

// mark records on each thread whether the safeguards flagged it. A flag
// once stored stays.
