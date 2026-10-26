package domain

import (
	"encoding/json"
	"slices"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// ThreadSource is where the chat an operation changes is kept: in the cloud,
// named by id with the content key, or on the device that sends it with
// the settings its prompt reads.
type ThreadSource struct {
	Local   bool            `json:"local"`
	Thread  json.RawMessage `json:"thread"`
	Profile json.RawMessage `json:"profile"`
}

// Validate refuses a source that names a chat in both places, or a device
// chat whose copy cannot be read.
func (s *ThreadSource) Validate() error {
	if !s.Local {
		if s.Thread != nil || s.Profile != nil {
			return api.Err(400, "BAD_REQUEST", "thread and profile are only accepted for local threads")
		}
		return nil
	}
	if !api.IsObject(s.Thread) {
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	if s.Profile != nil && !api.IsObject(s.Profile) {
		return api.Err(400, "BAD_REQUEST", "profile must be an object")
	}
	return ValidateLocalThread(s.Thread)
}

// threadInput is what a client's copy of a thread must hold before the
// legacy reader reads the rest.
type threadInput struct {
	Title            *string        `json:"title"`
	TitleState       *string        `json:"titleState"`
	Model            *string        `json:"model"`
	LastRequestID    *string        `json:"lastRequestId"`
	Pinned           *bool          `json:"pinned"`
	WebSearchEnabled *bool          `json:"webSearchEnabled"`
	PresetID         *string        `json:"presetId"`
	Messages         []messageInput `json:"messages"`
}

type messageInput struct {
	ID          string            `json:"id"`
	Role        string            `json:"role"`
	Content     *string           `json:"content"`
	Attachments []json.RawMessage `json:"attachments"`
	Timeline    []json.RawMessage `json:"timeline"`
}

// maxLocalMessages bounds the messages a client's thread may hold.
const maxLocalMessages = 10000

// MessageRoles are the roles a client's message may have.
var MessageRoles = []string{"user", "assistant"}

func ValidateLocalThread(data []byte) error {
	var thread threadInput
	if err := api.DecodeRequest(data, &thread); err != nil {
		return err
	}
	if len(thread.Messages) > maxLocalMessages {
		return api.Err(413, "CONTEXT_TOO_LARGE", "The thread has too many messages")
	}
	for _, message := range thread.Messages {
		if !slices.Contains(MessageRoles, message.Role) {
			return api.Err(400, "BAD_REQUEST", "A message role must be user or assistant")
		}
		if message.ID != "" && !api.IsIdentifier(message.ID) {
			return api.Err(400, "BAD_REQUEST", "A message id must be a valid identifier")
		}
	}
	return nil
}
