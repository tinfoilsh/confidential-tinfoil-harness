package main

import (
	"context"
	"slices"
)

// pinnedIDs is the encrypted favorites index, independent of chat pagination.
func (h *harness) pinnedIDs(ctx context.Context, p *principal, key contentKey) ([]string, error) {
	profile, err := h.loadProfile(ctx, p, key)
	if err != nil || profile == nil {
		return nil, err
	}
	return profile.PinnedChatIDs, nil
}

func (h *harness) applyPins(ctx context.Context, p *principal, key contentKey, rows []*threadRow) error {
	pins, err := h.pinnedIDs(ctx, p, key)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if slices.Contains(pins, row.ID) {
			row.Pinned = true
		}
	}
	return nil
}

// setPin updates the encrypted favorites index so favorites can be fetched
// without reading every page of chat history.
func (h *harness) setPin(ctx context.Context, p *principal, key contentKey, id string, pinned bool) error {
	_, err := h.mutateProfile(ctx, p, key, func(profile *storedProfile) error {
		profile.PinnedChatIDs = slices.DeleteFunc(profile.PinnedChatIDs, func(pin string) bool { return pin == id })
		if pinned {
			profile.PinnedChatIDs = append(profile.PinnedChatIDs, id)
		}
		return nil
	})
	return err
}
