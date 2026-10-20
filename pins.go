package main

import (
	"context"
	"slices"
)

// legacyPins are the favorites iOS kept in the profile before each chat
// stored its own pin. A chat the profile lists reads as pinned.
func (h *harness) legacyPins(ctx context.Context, p *principal, key contentKey) ([]string, error) {
	profile, err := h.loadProfile(ctx, p, key)
	if err != nil || profile == nil {
		return nil, err
	}
	return profile.PinnedChatIDs, nil
}

func (h *harness) readLegacyPins(ctx context.Context, p *principal, key contentKey, rows []*threadRow) error {
	pins, err := h.legacyPins(ctx, p, key)
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

// unpinLegacy takes a chat the user unpins off the profile's list, which
// would otherwise keep showing it pinned.
func (h *harness) unpinLegacy(ctx context.Context, p *principal, key contentKey, id string) error {
	pins, err := h.legacyPins(ctx, p, key)
	if err != nil || !slices.Contains(pins, id) {
		return err
	}
	_, err = h.mutateProfile(ctx, p, key, func(profile *storedProfile) {
		profile.PinnedChatIDs = slices.DeleteFunc(profile.PinnedChatIDs, func(pin string) bool { return pin == id })
	})
	return err
}
