package main

import (
	"bytes"
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

// Each route decodes its JSON body into one of these. A field of the wrong
// type refuses the request; a field the route does not read is ignored,
// except in a profile patch.

type idRequest struct {
	ID string `json:"id"`
}

type deleteThreadRequest struct {
	idRequest
	Revision string `json:"revision,omitempty"`
}

func (in *idRequest) Validate() error { return api.RequireID("id", in.ID) }

type threadRequest struct {
	ThreadID string `json:"threadId"`
}

func (in *threadRequest) Validate() error { return api.RequireID("threadId", in.ThreadID) }

type listRequest struct {
	Cursor    string `json:"cursor"`
	ProjectID string `json:"projectId"`
	Limit     int    `json:"limit"`
}

type windowRequest struct {
	ID     string `json:"id"`
	Before string `json:"before"`
	Limit  int    `json:"limit"`
}

func (in *windowRequest) Validate() error { return api.RequireID("id", in.ID) }

type updateThreadRequest struct {
	domain.ThreadSource
	ID               string               `json:"id"`
	Title            *string              `json:"title"`
	Model            *string              `json:"model"`
	WebSearchEnabled *bool                `json:"webSearchEnabled"`
	Pinned           *bool                `json:"pinned"`
	ProjectID        api.Nullable[string] `json:"projectId"`
	PresetID         api.Nullable[string] `json:"presetId"`
}

func (in *updateThreadRequest) Validate() error {
	if in.Local && (in.Pinned != nil || in.ProjectID.Set) {
		return api.Err(400, "BAD_REQUEST", "Pins and projects require cloud storage")
	}
	return cmp.Or(in.ProjectID.Check("projectId"), in.PresetID.Check("presetId"), api.RequireID("id", in.ID), in.ThreadSource.Validate())
}

// apply makes the settings change a chat's client asks for. Choosing another
// preset takes the settings it chooses, except those the request sets.
func (in *updateThreadRequest) apply(t *domain.Thread, c *domain.Catalog, profile *domain.Profile) {
	if in.Title != nil {
		t.Title, t.TitleState = *in.Title, "manual"
	}
	if in.PresetID.Set && domain.ValueOr(in.PresetID.Value, "") != domain.ValueOr(t.PresetID, "") {
		c.ApplyPreset(t, profile, in.PresetID.Value)
	}
	t.Model = domain.ValueOr(in.Model, t.Model)
	t.WebSearchEnabled = domain.ValueOr(in.WebSearchEnabled, t.WebSearchEnabled)
}

// messageRequest names a message of a chat; an edit sends its text.
type messageRequest struct {
	domain.ThreadSource
	ThreadID  string  `json:"threadId"`
	MessageID string  `json:"messageId"`
	Content   *string `json:"content"`
}

func (in *messageRequest) Validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if err := api.RequireID("messageId", in.MessageID); err != nil {
		return err
	}
	return in.ThreadSource.Validate()
}

type searchRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

type deleteAllRequest struct {
	ProjectID *string `json:"projectId"`
}

// runRequest names a run, or a turn queued behind one, of a chat.
type runRequest struct {
	api.KeyRequest
	ThreadID string `json:"threadId"`
	RunID    string `json:"runId"`
	QueueID  string `json:"queueId"`
}

type profileUpdateRequest struct {
	Patch json.RawMessage `json:"patch"`
}

// patch is the profile fields a request sets, and whether it clears the
// default prompt by setting it to null.
func (in *profileUpdateRequest) patch() (domain.Profile, bool, error) {
	if !api.IsObject(in.Patch) {
		return domain.Profile{}, false, api.Err(400, "BAD_REQUEST", "patch must be an object")
	}
	var patch domain.Profile
	decoder := json.NewDecoder(bytes.NewReader(in.Patch))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&patch); err != nil {
		if field, unknown := strings.CutPrefix(err.Error(), "json: unknown field "); unknown {
			return domain.Profile{}, false, api.Err(400, "BAD_REQUEST", "Unknown profile field: "+strings.Trim(field, `"`))
		}
		return domain.Profile{}, false, api.RequestError(err)
	}
	if err := patch.Validate(); err != nil {
		return domain.Profile{}, false, err
	}
	var preset struct {
		DefaultPromptPresetID api.Nullable[string] `json:"defaultPromptPresetId"`
	}
	json.Unmarshal(in.Patch, &preset)
	return patch, preset.DefaultPromptPresetID.Set && preset.DefaultPromptPresetID.Value == nil, nil
}

// profileUpgradeRequest names the settings a client keeps on its device.
type profileUpgradeRequest struct {
	Profile json.RawMessage `json:"profile"`
}

type profileMergeRequest struct {
	profileUpgradeRequest
	ChangedFields []string `json:"changedFields"`
}

func (in *profileUpgradeRequest) Validate() error {
	if !api.IsObject(in.Profile) {
		return api.Err(400, "BAD_REQUEST", "profile must be an object")
	}
	return nil
}

type projectRequest struct {
	ID                 string               `json:"id"`
	Name               *string              `json:"name"`
	Description        *string              `json:"description"`
	SystemInstructions *string              `json:"systemInstructions"`
	Color              api.Nullable[string] `json:"color"`
}

func (in *projectRequest) Validate() error { return in.Color.Check("color") }

type projectDocumentRequest struct {
	ProjectID  string `json:"projectId"`
	DocumentID string `json:"documentId"`
}

func (in *projectDocumentRequest) Validate() error {
	if err := api.RequireID("projectId", in.ProjectID); err != nil {
		return err
	}
	return api.RequireID("documentId", in.DocumentID)
}

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

// threadImportRequest stores a device chat in the cloud under its id.
type threadImportRequest struct {
	api.KeyRequest
	ThreadID string          `json:"threadId"`
	Thread   json.RawMessage `json:"thread"`
}

func (in *threadImportRequest) Validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !api.IsObject(in.Thread) {
		return api.Err(400, "BAD_REQUEST", "thread must be an object")
	}
	return domain.ValidateLocalThread(in.Thread)
}

type upgradeRequest struct {
	Threads *[]json.RawMessage `json:"threads"`
}

// turnRequest is one user action on a chat, which its run carries out.
type turnRequest struct {
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
func (in *turnRequest) Validate() error {
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
func (in *turnRequest) validateContent() error {
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
func (in *turnRequest) digest() string {
	copy := *in
	copy.Key = nil
	return api.HashHex(api.Raw(copy))
}
