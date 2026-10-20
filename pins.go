package main

import (
	"context"
	"slices"
)

// legacyPins are the favorites iOS kept in the profile before each chat
// stored its own pin. A chat the profile lists reads as pinned.
func (h *harness) legacyPins(ctx context.Context, p *principal, key contentKey) ([]any, error) {
	profile, err := h.storeRows.load(ctx, p, key, "profile", "profile")
	if err != nil {
		if asAPIError(err).HTTP == 404 {
			return nil, nil
		}
		return nil, err
	}
	return arr(profile.Data["pinnedChatIds"]), nil
}

func (h *harness) readLegacyPins(ctx context.Context, p *principal, key contentKey, rows []storedRow) error {
	pins, err := h.legacyPins(ctx, p, key)
	if err != nil {
		return err
	}
	for i := range rows {
		if slices.Contains(pins, any(rows[i].ID)) {
			rows[i].Data["pinned"] = true
		}
	}
	return nil
}

// unpinLegacy takes a chat the user unpins off the profile's list, which
// would otherwise keep showing it pinned.
func (h *harness) unpinLegacy(ctx context.Context, p *principal, key contentKey, id string) error {
	pins, err := h.legacyPins(ctx, p, key)
	if err != nil || !slices.Contains(pins, any(id)) {
		return err
	}
	_, err = h.mutate(ctx, p, key, "profile", "profile", false, func(data object) error {
		data["pinnedChatIds"] = slices.DeleteFunc(arr(data["pinnedChatIds"]), func(value any) bool { return value == id })
		return nil
	})
	return err
}
