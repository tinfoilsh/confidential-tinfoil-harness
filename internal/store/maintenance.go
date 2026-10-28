package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Attachment maintenance reclaims the bytes the control plane's index no
// longer needs. Every chat write tells the control plane which
// attachments the chat still names; those no chat has named for a while
// it queues for deletion, and a sweep claims them under a lease, deletes
// each from buckets and acknowledges it, so a delete that fails or a
// lease that lapses is tried again later and no deletion is lost between
// the index and buckets. Writes whose chat was never saved it releases
// once they have been pending for a while, and index entries left without
// an owner it drops, naming both for a sweep to delete from buckets.

const (
	// Attachments no chat names are purged every purgeInterval: the
	// control plane queues up to purgeQueueBatch, and the sweep claims
	// them purgeClaimBatch at a time for up to purgeClaimRounds, so a large
	// backlog drains across sweeps rather than holding one for long.
	purgeInterval    = 15 * time.Minute
	purgeQueueBatch  = 100
	purgeClaimBatch  = 25
	purgeClaimRounds = 20
	// purgeLeaseBudget is how long after a claim the sweep keeps deleting,
	// and bounds each delete it starts: well inside the control plane's
	// lease, so no delete outlives the claim that lets it.
	purgeLeaseBudget = 3 * time.Minute
	// Pending writes are released every pendingSweepInterval, up to
	// pendingSweepBatch at a time.
	pendingSweepInterval = 5 * time.Minute
	pendingSweepBatch    = 200
	// Orphans are dropped every orphanSweepInterval, up to
	// orphanSweepBatch at a time.
	orphanSweepInterval = time.Hour
	orphanSweepBatch    = 500
	// maintenanceStepTimeout bounds each call a sweep makes.
	maintenanceStepTimeout = 5 * time.Minute
)

var errPurgeClaimUnnamed = errors.New("store: the attachment purge claim names no token")

// RunAttachmentMaintenance runs the three sweeps until ctx ends, each
// once at the start and then at its interval. A sweep's failure is
// logged and the next sweep tries again.
func (s *Store) RunAttachmentMaintenance(ctx context.Context) {
	var wg sync.WaitGroup
	for _, sweep := range []struct {
		name  string
		every time.Duration
		run   func(context.Context) (int, error)
	}{
		{"attachment purge", purgeInterval, s.purgeUnreferenced},
		{"pending attachment", pendingSweepInterval, s.releasePending},
		{"orphan attachment", orphanSweepInterval, s.dropOrphans},
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { api.Rescued(sweep.name+" sweep", recover()) }()
			repeat(ctx, sweep.every, func() {
				if deleted, err := sweep.run(ctx); err != nil && ctx.Err() == nil {
					slog.Warn(sweep.name+" sweep", "deleted", deleted, "error", err)
				}
			})
		}()
	}
	wg.Wait()
}

// repeat runs sweep now and then every interval until ctx ends.
func repeat(ctx context.Context, interval time.Duration, sweep func()) {
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

// purgeUnreferenced has the control plane queue the attachments no chat
// names any more, then claims them in rounds, deletes each from buckets
// and acknowledges those deleted, which the control plane then forgets;
// one not deleted stays claimed until its lease ends and is tried again.
// It is how many were acknowledged.
func (s *Store) purgeUnreferenced(ctx context.Context) (int, error) {
	queueCtx, cancel := context.WithTimeout(ctx, maintenanceStepTimeout)
	_, err := s.Controlplane.PurgeUnreferencedAttachments(queueCtx, purgeQueueBatch)
	cancel()
	if err != nil {
		return 0, err
	}
	deleted := 0
	var failed []error
	for round := 0; round < purgeClaimRounds; round++ {
		claimCtx, cancel := context.WithTimeout(ctx, maintenanceStepTimeout)
		claim, err := s.Controlplane.ClaimAttachmentPurges(claimCtx, purgeClaimBatch)
		cancel()
		if err != nil {
			return deleted, errors.Join(append(failed, err)...)
		}
		if len(claim.Purges) == 0 {
			break
		}
		if claim.ClaimToken == "" {
			return deleted, errors.Join(append(failed, errPurgeClaimUnnamed)...)
		}
		leaseEnds := time.Now().Add(purgeLeaseBudget)
		acknowledged := make([]string, 0, len(claim.Purges))
		for _, purge := range claim.Purges {
			remaining := time.Until(leaseEnds)
			if remaining <= 0 {
				break
			}
			deleteCtx, cancel := context.WithTimeout(ctx, remaining)
			err := s.Buckets.Delete(deleteCtx, purge.ClerkUserID, purge.AttachmentID)
			cancel()
			if err != nil {
				failed = append(failed, fmt.Errorf("delete attachment %s: %w", purge.AttachmentID, err))
				continue
			}
			acknowledged = append(acknowledged, purge.AttachmentID)
		}
		if len(acknowledged) > 0 {
			ackCtx, cancel := context.WithTimeout(ctx, maintenanceStepTimeout)
			err := s.Controlplane.AckAttachmentPurges(ackCtx, claim.ClaimToken, acknowledged)
			cancel()
			if err != nil {
				return deleted, errors.Join(append(failed, err)...)
			}
			deleted += len(acknowledged)
		}
		if len(claim.Purges) < purgeClaimBatch {
			break
		}
	}
	return deleted, errors.Join(failed...)
}

// releasePending deletes the bytes of the writes the control plane has
// released: it forgets each as it hands it over, so this sweep alone
// deletes the bytes, and one it cannot delete is left in buckets, named
// by no chat. It is how many were deleted.
func (s *Store) releasePending(ctx context.Context) (int, error) {
	listCtx, cancel := context.WithTimeout(ctx, maintenanceStepTimeout)
	rows, err := s.Controlplane.SweepPendingAttachmentWrites(listCtx, pendingSweepBatch)
	cancel()
	if err != nil {
		return 0, err
	}
	deleted := 0
	var failed []error
	for _, row := range rows {
		deleteCtx, cancel := context.WithTimeout(ctx, maintenanceStepTimeout)
		err := s.Buckets.Delete(deleteCtx, row.ClerkUserID, row.AttachmentID)
		cancel()
		if err != nil {
			failed = append(failed, fmt.Errorf("delete attachment %s: %w", row.AttachmentID, err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(failed...)
}

// dropOrphans deletes the bytes of the index entries the control plane
// drops for having no owner. It is how many were deleted.
func (s *Store) dropOrphans(ctx context.Context) (int, error) {
	sweepCtx, cancel := context.WithTimeout(ctx, maintenanceStepTimeout)
	defer cancel()
	rows, err := s.Controlplane.DeleteOrphanedV2Attachments(sweepCtx, orphanSweepBatch)
	if err != nil {
		return 0, err
	}
	deleted := 0
	var failed []error
	for _, row := range rows {
		if err := s.Buckets.Delete(sweepCtx, row.ClerkUserID, row.AttachmentID); err != nil {
			failed = append(failed, fmt.Errorf("delete attachment %s: %w", row.AttachmentID, err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(failed...)
}
