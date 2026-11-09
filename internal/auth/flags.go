package auth

import (
	"context"
	"log/slog"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

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
// safeguards flagged. Only signed-in callers' chats are screened. The caller
// is named by the id their session proved, not by the session, which expires
// before a turn that waited in a queue starts. A list the control plane
// cannot give in time flags none, as the web client's polling did.
func (a *Authenticator) FlaggedChats(ctx context.Context, p *Principal) ChatFlags {
	if p.Anonymous || p.ID == "" || a.readFlags == nil {
		return nil
	}
	if flags, ok := a.recentFlags().Get(p.Scope()); ok {
		return flags
	}
	ctx, cancel := context.WithTimeout(ctx, FlagsTimeout)
	defer cancel()
	chats, err := a.readFlags(ctx, p.ID)
	if err != nil {
		slog.Warn("safeguard flags unavailable", "error", err)
		return nil
	}
	flags := ChatFlags{}
	for _, chat := range chats {
		flags[chat] = true
	}
	a.recentFlags().Add(p.Scope(), flags)
	return flags
}

// recentFlags are the flags last read for each caller. Callers only read them.
func (a *Authenticator) recentFlags() *expirable.LRU[string, ChatFlags] {
	a.flagsOnce.Do(func() { a.flags = expirable.NewLRU[string, ChatFlags](maxFlagCallers, nil, flagsTTL) })
	return a.flags
}

// mark records on each thread whether the safeguards flagged it. A flag
// once stored stays.
