package search

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

const (
	// Retired index objects are swept every deletionInterval, in batches
	// of deletionBatch.
	deletionInterval = 5 * time.Minute
	deletionBatch    = 25
	// Each step of a sweep is bounded by deletionStepTimeout.
	deletionStepTimeout = 5 * time.Second
)

// RunDeletionWorker deletes the index objects the control plane has
// retired, in batches, until ctx ends. An account's deletion retires its
// object; the publication record it was named in is gone by then, so
// only this sweep knows to delete it.
func (s *Service) RunDeletionWorker(ctx context.Context) {
	defer func() { api.Rescued("search index deletion", recover()) }()
	if !s.configured() {
		return
	}
	s.sweepDeletions(ctx)
	ticker := time.NewTicker(deletionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweepDeletions(ctx)
		}
	}
}

func (s *Service) sweepDeletions(ctx context.Context) {
	if _, err := s.deleteRetired(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("search index deletion sweep", "error", err)
	}
}

// deleteRetired claims a batch of retired objects, deletes each, and
// acknowledges those deleted, which the control plane then forgets; one
// not deleted stays claimed until its lease ends and is tried again. It
// is how many were acknowledged.
func (s *Service) deleteRetired(ctx context.Context) (int, error) {
	claimCtx, cancelClaim := context.WithTimeout(ctx, deletionStepTimeout)
	claim, err := s.Controlplane.ClaimSearchIndexDeletions(claimCtx, deletionBatch)
	cancelClaim()
	if err != nil {
		return 0, err
	}
	if len(claim.Deletions) == 0 {
		return 0, nil
	}
	if claim.ClaimToken == "" {
		return 0, errors.New("search: the deletion claim has no token")
	}
	acked := make([]string, 0, len(claim.Deletions))
	var failures []error
	for _, deletion := range claim.Deletions {
		deleteCtx, cancelDelete := context.WithTimeout(ctx, deletionStepTimeout)
		err := s.Buckets.Delete(deleteCtx, deletion.ClerkUserID, deletion.ObjectKey)
		cancelDelete()
		if err != nil {
			failures = append(failures, fmt.Errorf("delete search index %s: %w", deletion.ID, err))
			continue
		}
		acked = append(acked, deletion.ID)
	}
	if len(acked) > 0 {
		ackCtx, cancelAck := context.WithTimeout(ctx, deletionStepTimeout)
		err := s.Controlplane.AckSearchIndexDeletions(ackCtx, claim.ClaimToken, acked)
		cancelAck()
		if err != nil {
			return 0, errors.Join(append(failures, err)...)
		}
	}
	return len(acked), errors.Join(failures...)
}
