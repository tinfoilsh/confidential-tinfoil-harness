package main

import (
	"encoding/json"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// shareRequest shares a stored chat by its key, or a device chat by the
// thread its client sends.
type shareRequest struct {
	api.KeyRequest
	ThreadID string          `json:"threadId"`
	Local    bool            `json:"local"`
	Thread   json.RawMessage `json:"thread"`
}

func (in *shareRequest) Validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !in.Local {
		return nil
	}
	if !api.IsObject(in.Thread) {
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	return domain.ValidateLocalThread(in.Thread)
}

type openShareRequest struct {
	ShareID  string `json:"shareId"`
	Fragment string `json:"fragment"`
}

func (in *openShareRequest) Validate() error { return api.RequireID("shareId", in.ShareID) }

type importStatusRequest struct {
	JobID string `json:"jobId"`
}

func (in *importStatusRequest) Validate() error { return api.RequireID("jobId", in.JobID) }
