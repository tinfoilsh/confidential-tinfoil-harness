package engine

import (
	"cmp"
	"encoding/json"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// RunRequest names a run, or a turn queued behind one, of a chat.
type RunRequest struct {
	api.KeyRequest
	ThreadID string `json:"threadId"`
	RunID    string `json:"runId"`
	QueueID  string `json:"queueId"`
}

// ThreadImportRequest stores a device chat in the cloud under its id.
type ThreadImportRequest struct {
	api.KeyRequest
	ThreadID string          `json:"threadId"`
	Thread   json.RawMessage `json:"thread"`
}

func (in *ThreadImportRequest) Validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !api.IsObject(in.Thread) {
		return api.Err(400, "BAD_REQUEST", "thread must be an object")
	}
	return domain.ValidateLocalThread(in.Thread)
}

// TurnRequest is one user action on a chat, which its run carries out.
type TurnRequest struct {
	api.KeyRequest
	ClientRequestID string               `json:"clientRequestId"`
	ThreadID        string               `json:"threadId"`
	Kind            string               `json:"kind"`
	Ephemeral       bool                 `json:"ephemeral,omitempty"`
	Local           bool                 `json:"local,omitempty"`
	Thread          json.RawMessage      `json:"thread,omitempty"`
	Profile         json.RawMessage      `json:"profile,omitempty"`
	ProjectID       *string              `json:"projectId,omitempty"`
	PresetID        api.Nullable[string] `json:"presetId,omitzero"`
	Content         *string              `json:"content,omitempty"`
	Quote           *string              `json:"quote,omitempty"`
	MessageID       string               `json:"messageId,omitempty"`
	ToolCallID      string               `json:"toolCallId,omitempty"`
	Attachments     []attach.Sent        `json:"attachments,omitzero"`
	Widgets         []string             `json:"widgets,omitzero"`
	Options         turnOptions          `json:"options"`
}

type turnOptions struct {
	Model            string `json:"model,omitempty"`
	AutoIntelligence string `json:"autoIntelligence,omitempty"`
	ReasoningEffort  string `json:"reasoningEffort,omitempty"`
	Timezone         string `json:"timezone,omitempty"`
	Locale           string `json:"locale,omitempty"`
	Thinking         *bool  `json:"thinking,omitempty"`
	WebSearch        *bool  `json:"webSearch,omitempty"`
	PIICheck         *bool  `json:"piiCheck,omitempty"`
	GenUI            *bool  `json:"genUI,omitempty"`
	// CacheSecret scopes the prompt cache to the client that holds it, as
	// the Tinfoil SDKs scope theirs: 256 random bits, hex encoded.
	CacheSecret string `json:"cacheSecret,omitempty"`
}

var cacheSecretPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

var (
	turnKinds = []string{"send", "edit", "regenerate", "continue", "retryToolCall", "ask", "retry"}
)

// Validate refuses a turn its run could not carry out. Who may send it is
// checked with the caller.
func (in *TurnRequest) Validate() error {
	if _, err := uuid.Parse(in.ClientRequestID); err != nil {
		return api.Err(400, "BAD_REQUEST", "clientRequestId must be a UUID")
	}
	in.Kind = cmp.Or(in.Kind, "send")
	if !slices.Contains(turnKinds, in.Kind) {
		return api.Err(400, "BAD_REQUEST", "Unknown turn kind")
	}
	// An ask is never stored, so it may still say so.
	if in.Ephemeral && in.Kind != "ask" {
		return api.Err(400, "BAD_REQUEST", "Temporary chats are kept on the device")
	}
	if secret := in.Options.CacheSecret; secret != "" && !cacheSecretPattern.MatchString(secret) {
		return api.Err(400, "BAD_REQUEST", "cacheSecret must be 64 hex characters")
	}
	return in.PresetID.Check("presetId")
}

// validateContent refuses a turn whose content cannot make its message.
func (in *TurnRequest) validateContent() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if in.Local && in.ProjectID != nil {
		return api.Err(400, "BAD_REQUEST", "Projects require cloud storage")
	}
	for _, field := range []struct {
		name  string
		value json.RawMessage
	}{{"thread", in.Thread}, {"profile", in.Profile}} {
		if field.value == nil {
			continue
		}
		if !in.Local {
			return api.Err(400, "BAD_REQUEST", field.name+" is only accepted for local threads")
		}
		if !api.IsObject(field.value) {
			return api.Err(400, "BAD_REQUEST", field.name+" must be an object")
		}
	}
	if in.Thread != nil {
		if err := domain.ValidateLocalThread(in.Thread); err != nil {
			return err
		}
	} else if in.Local && in.Kind == "ask" {
		// An ask about a device chat reads the conversation its client sends.
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	pixels := int64(0)
	for i := range in.Attachments {
		if err := in.Attachments[i].Validate(); err != nil {
			return err
		}
		pixels += in.Attachments[i].ResizePixels()
	}
	if pixels > attach.MaxTurnResizePixels {
		return api.Err(413, "ATTACHMENT_TOO_LARGE", "The turn's images hold more pixels than one turn can resize")
	}
	if in.Kind == "send" && strings.TrimSpace(domain.ValueOr(in.Content, "")) == "" && strings.TrimSpace(domain.ValueOr(in.Quote, "")) == "" && len(in.Attachments) == 0 {
		return api.Err(400, "BAD_REQUEST", "A message or attachment is required")
	}
	return nil
}

// digest identifies a turn's content, so a retry of the request is told from
// another request under the same id.
func (in *TurnRequest) digest() string {
	copy := *in
	copy.Key = nil
	return api.HashHex(api.Raw(copy))
}
