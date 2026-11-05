package api

// IDRequest names a row by id.
type IDRequest struct {
	ID string `json:"id"`
}

func (in *IDRequest) Validate() error { return RequireID("id", in.ID) }

// ThreadRequest names a chat by id.
type ThreadRequest struct {
	ThreadID string `json:"threadId"`
}

func (in *ThreadRequest) Validate() error { return RequireID("threadId", in.ThreadID) }

// DeleteRequest names a row to delete, at the revision the caller read, or
// a device chat its client deleted.
type DeleteRequest struct {
	IDRequest
	Revision string `json:"revision,omitempty"`
	Local    bool   `json:"local,omitempty"`
}
