package archives

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
)

// An export reads each attachment a stored chat keeps from the storage
// services. A read they fail to answer is tried again a few times; one that
// still fails leaves the attachment out, as one they no longer hold is, so a
// passing fault costs an export an attachment rather than the whole export.

const (
	// attachmentReadAttempts bounds the reads of a stored attachment the
	// storage services fail to answer; each wait is a step longer than the
	// last.
	attachmentReadAttempts = 3
	attachmentRetryStep    = 500 * time.Millisecond
)

// errAttachmentUnavailable is a stored attachment the storage services did
// not serve, however often they were asked.
var errAttachmentUnavailable = api.Err(502, "UPSTREAM_REFUSED", "The attachment could not be read")

// passing is whether a storage service's failure may pass: a refusal for
// load, or a failure or timeout of the service.
func passing(err error) bool {
	var e *api.Error
	return errors.As(err, &e) && (e.HTTP >= http.StatusInternalServerError || e.HTTP == http.StatusTooManyRequests)
}

// readStored reads a stored attachment, again after a failure that may pass.
// One the storage services no longer hold, or that cannot be read as its
// kind, is missing; one they keep failing to serve is unavailable.
func readStored[T any](ctx context.Context, read func() (T, error)) (T, error) {
	for attempt := 1; ; attempt++ {
		v, err := read()
		switch {
		case err == nil:
			return v, nil
		case attach.Unreachable(err):
			return v, errAttachmentMissing
		case ctx.Err() != nil || !passing(err):
			return v, err
		case attempt == attachmentReadAttempts:
			return v, errAttachmentUnavailable
		}
		wait := time.NewTimer(time.Duration(attempt) * attachmentRetryStep)
		select {
		case <-wait.C:
		case <-ctx.Done():
			wait.Stop()
			return v, ctx.Err()
		}
	}
}
