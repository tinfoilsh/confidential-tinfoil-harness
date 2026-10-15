package main

import (
	"context"
	"log/slog"
	"slices"
)

// iOS still writes favorites in the profile. During that transition, reflect
// those writes in thread.pinned and maintain the legacy field on harness edits.
func (h *harness) syncPins(ctx context.Context, p *principal, key contentKey, rows []storedRow) error {
	profile, err := h.profile(ctx, p, key)
	if err != nil {
		return err
	}
	values, exists := profile["pinnedChatIds"]
	if !exists {
		return nil
	}
	mirror := true
	for i := range rows {
		row := &rows[i]
		pinned := slices.ContainsFunc(arr(values), func(value any) bool { return value == row.ID })
		if boolean(row.Data["pinned"], false) == pinned {
			continue
		}
		if mirror {
			updated, err := h.mutate(ctx, p, key, "chat", row.ID, false, nil, func(data object) error { data["pinned"] = pinned; return nil })
			if err == nil {
				*row = *updated
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Mirroring is a write during a read. After one failure the rest
			// of this read shows the profile's pins without writing them back.
			slog.Warn("pin not mirrored", "error", asAPIError(err).Code)
			mirror = false
		}
		row.Data["pinned"] = pinned
	}
	return nil
}
func (h *harness) writeLegacyPin(ctx context.Context, p *principal, key contentKey, id string, pinned bool) error {
	_, err := h.mutate(ctx, p, key, "profile", "profile", true, []string{"pinnedChatIds"}, func(data object) error {
		pins := slices.DeleteFunc(arr(data["pinnedChatIds"]), func(value any) bool { return value == id })
		if pinned {
			pins = append(pins, id)
		}
		data["pinnedChatIds"] = pins
		return nil
	})
	return err
}
