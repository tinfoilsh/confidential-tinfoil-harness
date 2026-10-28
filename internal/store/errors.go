package store

import (
	"errors"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

var (
	// errRunScope refuses an operation outside what a grant covers. The
	// harness only asks within its grants, so this names a bug, not a
	// condition a client can act on.
	errRunScope = errors.New("store: the operation is outside the grant's storage scope")
	// errRowNotFetched is a row the control plane could not serve twice
	// running. A later request may fetch it.
	errRowNotFetched = api.Err(502, "UPSTREAM_REFUSED", "The sync service could not fetch the data in time")
	// errRowUnreadable is a row the control plane holds in no format this
	// harness opens: an envelope it does not know, or one that fails its
	// authentication.
	errRowUnreadable = api.Err(502, "UPSTREAM_REFUSED", "The stored row cannot be opened")
)

// refusedByControlplane is whether the control plane answered the request
// with a refusal of its own, which proves it wrote nothing for it.
func refusedByControlplane(err error) bool {
	var cpe *controlplane.Error
	return errors.As(err, &cpe) && cpe.StatusCode/100 == 4
}

// fromControlplane is the harness's error for what the control plane, or
// the sealing of a row, refused.
func fromControlplane(err error) *api.Error {
	var known *api.Error
	if errors.As(err, &known) {
		return known
	}
	if errors.Is(err, seal.ErrPlaintextTooLarge) {
		return api.Err(413, "THREAD_TOO_LARGE", "The chat is too large to store")
	}
	var unknown *controlplane.PutBlobOutcomeUnknownError
	if errors.As(err, &unknown) {
		// The write may have been stored. Retrying reapplies the change
		// to a freshly pulled row, so retrying is safe.
		return api.Err(503, "SYNC_OUTCOME_UNKNOWN", "The sync service may not have stored the change")
	}
	var cpe *controlplane.Error
	if !errors.As(err, &cpe) {
		return api.Err(502, "UPSTREAM_REFUSED", "A downstream request failed")
	}
	switch cpe.Code {
	case controlplane.StatusStaleKey, controlplane.StatusExistingDataUnderOtherKey, "UNKNOWN_KEY", "KEY_MISMATCH":
		e := api.Err(409, "KEY_MISMATCH", "The stored data uses another content key")
		e.KeyID = cpe.CurrentKeyID
		return e
	case controlplane.StatusStaleBlob, "SYNC_CONFLICT":
		return api.Err(409, "REVISION_CONFLICT", "The stored row changed")
	case controlplane.StatusMissingAttachment:
		e := api.Err(409, "MISSING_ATTACHMENT", "The chat references attachments that are not stored with it")
		e.Missing = cpe.MissingAttachments
		return e
	case "PAYLOAD_TOO_LARGE":
		return api.Err(413, "THREAD_TOO_LARGE", "The chat is too large to store")
	}
	switch cpe.StatusCode {
	case http.StatusUnauthorized:
		return api.Err(401, "UNAUTHENTICATED", "Authentication is required")
	case http.StatusNotFound:
		return api.Err(404, "THREAD_NOT_FOUND", "The requested data was not found")
	case http.StatusPreconditionFailed:
		return api.Err(409, "REVISION_CONFLICT", "The stored row changed")
	case http.StatusTooManyRequests:
		e := api.Err(429, "QUOTA_EXHAUSTED", "The request quota is exhausted")
		e.RetryAfter = int(cpe.RetryAfterSeconds)
		return e
	}
	e := api.Err(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	e.Status = cpe.StatusCode
	api.RefusedAsSent(e, cpe.StatusCode)
	return e
}
