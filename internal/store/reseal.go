package store

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// A reseal brings every row the control plane holds under the caller's
// current key: rows older clients sealed without a key id, and rows an
// earlier key sealed. It runs in the background, since an account may
// hold thousands of rows, and the caller asks again to see where it
// stands.

const (
	// ResealBudget bounds one reseal; a caller whose rows take longer
	// sees a partial report and asks again.
	ResealBudget = 10 * time.Minute
	// ResealRetention is how long an ended reseal still answers for the
	// caller, so a client polling for its end sees it rather than
	// starting another.
	ResealRetention = 5 * time.Minute
	// resealPage is how many rows one listing from the control plane
	// names.
	resealPage = 200
	// resealAttempts bounds the reseals of a row another writer keeps
	// changing under them.
	resealAttempts = 3
)

// resealScopes is every scope the control plane holds rows of, in the
// order they are resealed.
var resealScopes = []seal.Scope{seal.ScopeProfile, seal.ScopeChat, seal.ScopeProject, seal.ScopeProjectDocument}

// ResealReport is where a reseal stands: the rows resealed so far, the rows
// still to try, and the rows no given key opens.
type ResealReport struct {
	Status    jobs.State `json:"status"`
	Migrated  int        `json:"migrated"`
	Remaining int        `json:"remaining"`
	Blocked   int        `json:"blocked"`
	Partial   bool       `json:"partial"` // the budget or the caller's credential ran out first
}

// Reseal seals every row an older key sealed under target, which must be
// the caller's registered current key, in the background; the report is
// where the job stands when the call returns.
func (s *Store) Reseal(ctx context.Context, p *auth.Principal, keys []auth.ContentKey, target auth.ContentKey) (ResealReport, error) {
	defer clear(target[:])
	defer func() {
		for i := range keys {
			clear(keys[i][:])
		}
	}()
	if len(keys) == 0 {
		return ResealReport{}, api.Err(400, "BAD_REQUEST", "At least one key is required")
	}
	// The control plane accepts a reseal only under the registered key,
	// so any other target would be refused row by row.
	current, err := s.Controlplane.GetCurrentKey(ctx, string(p.JWT), p.ID)
	if err != nil {
		return ResealReport{}, FromControlplane(err)
	}
	targetKey := seal.Key{Bytes: bytes.Clone(target[:]), KeyIDHex: keyID(target)}
	if current == nil || current.KeyID != targetKey.KeyIDHex {
		seal.Zero(targetKey.Bytes)
		e := api.Err(409, "KEY_MISMATCH", "The target is not the registered content key")
		if current != nil {
			e.KeyID = current.KeyID
		}
		return ResealReport{}, e
	}
	// The job outlives the request, so it takes copies of the keys and
	// of who the caller is.
	candidates := make([]seal.Key, 0, len(keys))
	for _, key := range keys {
		candidates = append(candidates, seal.Key{Bytes: bytes.Clone(key[:]), KeyIDHex: keyID(key)})
	}
	zero := func() {
		for _, candidate := range candidates {
			seal.Zero(candidate.Bytes)
		}
		seal.Zero(targetKey.Bytes)
	}
	caller := &auth.Principal{ID: p.ID, JWT: p.JWT}
	job, started := s.Reseals.Start(ctx, p.ID, func(ctx context.Context, report func(ResealReport)) (ResealReport, error) {
		defer zero()
		return s.reseal(ctx, caller, candidates, targetKey, report)
	})
	if !started {
		zero()
	}
	status := job.Status()
	status.Report.Status = status.State
	return status.Report, nil
}

// reseal walks each scope's rows left to reseal, a page at a time, until
// none remain or a page reseals nothing, and reports after each page. The
// counts of what remains and what is blocked are the control plane's,
// adjusted for the page just done.
func (s *Store) reseal(ctx context.Context, p *auth.Principal, keys []seal.Key, target seal.Key, report func(ResealReport)) (ResealReport, error) {
	rows := &Rows{Controlplane: s.Controlplane, Buckets: s.Buckets}
	var out ResealReport
	for _, scope := range resealScopes {
		remaining, blocked := 0, 0
		// A walk cut short keeps what the scope's last page counted.
		cutShort := func() (ResealReport, error) {
			out.Remaining, out.Blocked, out.Partial = out.Remaining+remaining, out.Blocked+blocked, true
			return out, nil
		}
		for {
			if ctx.Err() != nil {
				return cutShort()
			}
			list, err := s.Controlplane.ListNeedsMigration(ctx, string(scope), resealPage, string(p.JWT), p.ID)
			if err != nil {
				if ctx.Err() != nil || credentialRefused(err) {
					return cutShort()
				}
				return out, FromControlplane(err)
			}
			page := s.resealPage(ctx, rows, p, scope, list.IDs, keys, target)
			out.Migrated += page.migrated
			remaining = max(0, list.RetryableRemaining-page.migrated-page.blocked)
			blocked = list.BlockedUnmigrated + page.blocked
			report(ResealReport{Migrated: out.Migrated, Remaining: out.Remaining + remaining, Blocked: out.Blocked + blocked})
			if page.stopped {
				return cutShort()
			}
			if page.migrated == 0 || remaining == 0 {
				break
			}
		}
		out.Remaining += remaining
		out.Blocked += blocked
	}
	return out, nil
}

// resealedPage is what one page of rows came to: the rows resealed, the
// rows no key opens, and whether the page was cut short.
type resealedPage struct {
	migrated, blocked int
	stopped           bool
}

// resealPage reseals each row of a page. A row no key opens, or whose
// reseal fails, is recorded as blocked, which keeps it out of the next
// pages for a while; an ended context or a refused credential stops the
// page without recording the row it was on.
func (s *Store) resealPage(ctx context.Context, rows *Rows, p *auth.Principal, scope seal.Scope, ids []string, keys []seal.Key, target seal.Key) resealedPage {
	var page resealedPage
	for _, id := range ids {
		if ctx.Err() != nil {
			page.stopped = true
			return page
		}
		switch s.resealOne(ctx, rows, p, scope, id, keys, target) {
		case resealed:
			page.migrated++
		case resealBlocked:
			if ctx.Err() != nil {
				page.stopped = true
				return page
			}
			page.blocked++
			_ = s.Controlplane.RecordMigrationFailure(ctx, string(scope), id, string(p.JWT), p.ID)
		case resealStopped:
			page.stopped = true
			return page
		}
	}
	return page
}

// resealOutcome is what became of one row.
type resealOutcome int

const (
	resealed resealOutcome = iota
	resealBlocked
	resealStopped
)

// resealOne reads a row as it is now, opens it with whichever key seals
// it, and reseals it under target at the revision it read. A row that
// changed in between is read again; one already under target is done.
func (s *Store) resealOne(ctx context.Context, rows *Rows, p *auth.Principal, scope seal.Scope, id string, keys []seal.Key, target seal.Key) resealOutcome {
	for attempt := 0; attempt < resealAttempts; attempt++ {
		blob, err := s.Controlplane.GetBlob(ctx, string(scope), id, string(p.JWT), p.ID)
		if err != nil {
			return resealFailure(ctx, err)
		}
		if seal.Detect(blob.Ciphertext) == seal.VersionV2 && blob.KeyID == target.KeyIDHex {
			return resealed
		}
		plaintext, ok := openRow(blob.Ciphertext, keys, scope, id, p.ID)
		if !ok {
			return resealBlocked
		}
		_, err = rows.rewrapRow(ctx, p, string(scope), id, plaintext, blob.ETag, target)
		seal.Zero(plaintext)
		switch {
		case err == nil:
			return resealed
		case controlplane.IsCode(err, controlplane.StatusStaleBlob):
			continue
		default:
			return resealFailure(ctx, err)
		}
	}
	return resealBlocked
}

// resealFailure is whether a row's failed read or write blocks the row,
// or stops the job: the context ended, or the caller's credential was
// refused, which every later call would be too.
func resealFailure(ctx context.Context, err error) resealOutcome {
	if ctx.Err() != nil || credentialRefused(err) {
		return resealStopped
	}
	return resealBlocked
}

// openRow opens a row with whichever of keys seals it: a row with a key
// id by that key, a legacy row by trying each.
func openRow(ciphertext []byte, keys []seal.Key, scope seal.Scope, id, owner string) ([]byte, bool) {
	var dec seal.DecryptResult
	var err error
	switch seal.Detect(ciphertext) {
	case seal.VersionV2:
		dec, err = seal.DecryptV2(ciphertext, keys, seal.AAD{Scope: scope, ID: aadID(string(scope), id), ClerkUserID: owner})
	case seal.VersionV0, seal.VersionV1:
		dec, err = seal.DecryptLegacy(ciphertext, keys)
	default:
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	return dec.Plaintext, true
}

// credentialRefused is whether the control plane refused the caller's
// credential.
func credentialRefused(err error) bool {
	var cpe *controlplane.Error
	return errors.As(err, &cpe) && (cpe.StatusCode == http.StatusUnauthorized || cpe.StatusCode == http.StatusForbidden)
}
