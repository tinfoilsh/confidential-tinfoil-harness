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
)

// Each route decodes its JSON body into one of these. A field of the wrong
// type refuses the request; a field the route does not read is ignored,
// except in a profile patch.

// A missing key is checked against the run: keyless threads accept it, and
// every other thread compares the key's fingerprint.
func requestKey(key []byte) (contentKey, error) {
	if len(key) == 0 {
		return contentKey{}, nil
	}
	return decodeContentKey(key)
}

type idRequest struct {
	ID string `json:"id"`
}

type deleteThreadRequest struct {
	idRequest
	Revision string `json:"revision,omitempty"`
}

func (in *idRequest) validate() error { return api.RequireID("id", in.ID) }

type threadRequest struct {
	ThreadID string `json:"threadId"`
}

func (in *threadRequest) validate() error { return api.RequireID("threadId", in.ThreadID) }

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

func (in *windowRequest) validate() error { return api.RequireID("id", in.ID) }

// threadSource is where the chat an operation changes is kept: in the cloud,
// named by id with the content key, or on the device that sends it with
// the settings its prompt reads.
type threadSource struct {
	Local   bool            `json:"local"`
	Thread  json.RawMessage `json:"thread"`
	Profile json.RawMessage `json:"profile"`
}

func (s *threadSource) validate() error {
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
	return validateLocalThread(s.Thread)
}

type updateThreadRequest struct {
	threadSource
	ID               string               `json:"id"`
	Title            *string              `json:"title"`
	Model            *string              `json:"model"`
	WebSearchEnabled *bool                `json:"webSearchEnabled"`
	Pinned           *bool                `json:"pinned"`
	ProjectID        api.Nullable[string] `json:"projectId"`
	PresetID         api.Nullable[string] `json:"presetId"`
}

func (in *updateThreadRequest) validate() error {
	if in.Local && (in.Pinned != nil || in.ProjectID.Set) {
		return api.Err(400, "BAD_REQUEST", "Pins and projects require cloud storage")
	}
	return cmp.Or(in.ProjectID.Check("projectId"), in.PresetID.Check("presetId"), api.RequireID("id", in.ID), in.threadSource.validate())
}

// apply makes the settings change a chat's client asks for. Choosing another
// preset takes the settings it chooses, except those the request sets.
func (in *updateThreadRequest) apply(t *Thread, c *chatCatalog, profile *Profile) {
	if in.Title != nil {
		t.Title, t.TitleState = *in.Title, "manual"
	}
	if in.PresetID.Set && valueOr(in.PresetID.Value, "") != valueOr(t.PresetID, "") {
		c.applyPreset(t, profile, in.PresetID.Value)
	}
	t.Model = valueOr(in.Model, t.Model)
	t.WebSearchEnabled = valueOr(in.WebSearchEnabled, t.WebSearchEnabled)
}

// messageRequest names a message of a chat; an edit sends its text.
type messageRequest struct {
	threadSource
	ThreadID  string  `json:"threadId"`
	MessageID string  `json:"messageId"`
	Content   *string `json:"content"`
}

func (in *messageRequest) validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if err := api.RequireID("messageId", in.MessageID); err != nil {
		return err
	}
	return in.threadSource.validate()
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
func (in *profileUpdateRequest) patch() (Profile, bool, error) {
	if !api.IsObject(in.Patch) {
		return Profile{}, false, api.Err(400, "BAD_REQUEST", "patch must be an object")
	}
	var patch Profile
	decoder := json.NewDecoder(bytes.NewReader(in.Patch))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&patch); err != nil {
		if field, unknown := strings.CutPrefix(err.Error(), "json: unknown field "); unknown {
			return Profile{}, false, api.Err(400, "BAD_REQUEST", "Unknown profile field: "+strings.Trim(field, `"`))
		}
		return Profile{}, false, api.RequestError(err)
	}
	if err := patch.validate(); err != nil {
		return Profile{}, false, err
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

func (in *profileUpgradeRequest) validate() error {
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

func (in *projectRequest) validate() error { return in.Color.Check("color") }

type projectDocumentRequest struct {
	ProjectID  string `json:"projectId"`
	DocumentID string `json:"documentId"`
}

func (in *projectDocumentRequest) validate() error {
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

func (in *shareRequest) validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !in.Local {
		return nil
	}
	if !api.IsObject(in.Thread) {
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	return validateLocalThread(in.Thread)
}

type openShareRequest struct {
	ShareID  string `json:"shareId"`
	Fragment string `json:"fragment"`
}

func (in *openShareRequest) validate() error { return api.RequireID("shareId", in.ShareID) }

type importStatusRequest struct {
	JobID string `json:"jobId"`
}

func (in *importStatusRequest) validate() error { return api.RequireID("jobId", in.JobID) }

// pageRequest names a page whose details the metadata service reads.
type pageRequest struct {
	URL string `json:"url"`
}

func (in *pageRequest) validate() error {
	if strings.TrimSpace(in.URL) == "" {
		return api.Err(400, "BAD_REQUEST", "A page URL is required")
	}
	return nil
}

// speechRequest names a message to read aloud: one of a stored chat, named
// with the content key, or of a device chat its client sends. Text a client
// selected, or a shared chat's message, is sent as it is.
type speechRequest struct {
	threadSource
	api.KeyRequest
	ThreadID  string  `json:"threadId"`
	MessageID string  `json:"messageId"`
	Text      *string `json:"text"`
}

func (in *speechRequest) validate() error {
	if in.MessageID == "" {
		if strings.TrimSpace(valueOr(in.Text, "")) == "" {
			return api.Err(400, "BAD_REQUEST", "A message or text is required")
		}
		return nil
	}
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if err := api.RequireID("messageId", in.MessageID); err != nil {
		return err
	}
	return in.threadSource.validate()
}

// threadImportRequest stores a device chat in the cloud under its id.
type threadImportRequest struct {
	api.KeyRequest
	ThreadID string          `json:"threadId"`
	Thread   json.RawMessage `json:"thread"`
}

func (in *threadImportRequest) validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !api.IsObject(in.Thread) {
		return api.Err(400, "BAD_REQUEST", "thread must be an object")
	}
	return validateLocalThread(in.Thread)
}

type upgradeRequest struct {
	Threads *[]json.RawMessage `json:"threads"`
}

// attachmentRequest downloads an attachment of a stored chat, or one a share
// names by the key its link carries.
type attachmentRequest struct {
	api.KeyRequest
	ID          string         `json:"id"`
	ThreadID    string         `json:"threadId"`
	AttKey      string         `json:"attKey"`
	Type        string         `json:"type"`
	Kind        string         `json:"kind"`
	FileName    string         `json:"fileName"`
	MimeType    string         `json:"mimeType"`
	TextContent string         `json:"textContent"`
	Pages       []DocumentPage `json:"pages"`
}

func (in *attachmentRequest) validate() error {
	if err := api.RequireID("id", in.ID); err != nil {
		return err
	}
	if in.ThreadID != "" {
		return api.RequireID("threadId", in.ThreadID)
	}
	return nil
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
	Attachments     []sentAttachment     `json:"attachments,omitzero"`
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

// sentAttachment is an upload's descriptor as a turn sends it back, or the
// id alone of an attachment the thread already holds.
type sentAttachment struct {
	ID          string         `json:"id"`
	Kind        string         `json:"kind,omitempty"`
	FileName    string         `json:"fileName,omitempty"`
	MimeType    string         `json:"mimeType,omitempty"`
	Size        *int64         `json:"size,omitempty"`
	Thumbnail   *string        `json:"thumbnail,omitempty"`
	TextContent string         `json:"textContent,omitempty"`
	Description string         `json:"description,omitempty"`
	Pages       []DocumentPage `json:"pages,omitempty"`
	Base64      string         `json:"base64,omitempty"`
}

var (
	turnKinds       = []string{"send", "edit", "regenerate", "continue", "retryToolCall", "ask", "retry"}
	attachmentKinds = []string{"image", "document", "audio"}
)

// validate refuses a turn its run could not carry out. Who may send it is
// checked with the caller.
func (in *turnRequest) validate() error {
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
		if err := validateLocalThread(in.Thread); err != nil {
			return err
		}
	} else if in.Local && in.Kind == "ask" {
		// An ask about a device chat reads the conversation its client sends.
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	pixels := int64(0)
	for i := range in.Attachments {
		if err := in.Attachments[i].validate(); err != nil {
			return err
		}
		pixels += in.Attachments[i].resizePixels()
	}
	if pixels > maxTurnResizePixels {
		return api.Err(413, "ATTACHMENT_TOO_LARGE", "The turn's images hold more pixels than one turn can resize")
	}
	if in.Kind == "send" && strings.TrimSpace(valueOr(in.Content, "")) == "" && strings.TrimSpace(valueOr(in.Quote, "")) == "" && len(in.Attachments) == 0 {
		return api.Err(400, "BAD_REQUEST", "A message or attachment is required")
	}
	return nil
}

func (a *sentAttachment) validate() error {
	if err := api.RequireID("id", a.ID); err != nil {
		return err
	}
	if a.Kind != "" && !slices.Contains(attachmentKinds, a.Kind) {
		return api.Err(400, "BAD_REQUEST", "An attachment kind must be image, document, or audio")
	}
	if a.whole() {
		return a.fitsUpload()
	}
	return nil
}

// whole reports whether the turn sends the attachment's descriptor rather
// than naming one its thread holds.
func (a *sentAttachment) whole() bool { return a.Kind != "" }

// digest identifies a turn's content, so a retry of the request is told from
// another request under the same id.
func (in *turnRequest) digest() string {
	copy := *in
	copy.Key = nil
	return hashHex(api.Raw(copy))
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

var messageRoles = []string{"user", "assistant"}

func validateLocalThread(data []byte) error {
	var thread threadInput
	if err := api.DecodeRequest(data, &thread); err != nil {
		return err
	}
	if len(thread.Messages) > maxLocalMessages {
		return api.Err(413, "CONTEXT_TOO_LARGE", "The thread has too many messages")
	}
	for _, message := range thread.Messages {
		if !slices.Contains(messageRoles, message.Role) {
			return api.Err(400, "BAD_REQUEST", "A message role must be user or assistant")
		}
		if message.ID != "" && !api.IsIdentifier(message.ID) {
			return api.Err(400, "BAD_REQUEST", "A message id must be a valid identifier")
		}
	}
	return nil
}
