package auth

import "time"

// StorageGrant is a run's or an operation's right to the stored rows for
// a while: the chat a run is for and the project its context reads, or
// the operation, the content key it was given for, and when it ends.
type StorageGrant struct {
	Operation string // "" for a chat's run; "import", "export" or "project-upload"
	ChatID    string
	ProjectID string
	KeyID     string // hex id of the content key, as seal.KeyIDHex gives it
	ExpiresAt time.Time
}
