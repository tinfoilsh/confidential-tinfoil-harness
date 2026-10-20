package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Each route decodes its JSON body into one of these. A field of the wrong
// type refuses the request; a field the route does not read is ignored,
// except in a profile patch.

// readBody reads a request body of at most limit bytes that holds one JSON
// object.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) (json.RawMessage, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	decoder := json.NewDecoder(r.Body)
	var body json.RawMessage
	if err := decoder.Decode(&body); err != nil {
		return nil, bodyError(err, "Expected a JSON object")
	}
	if !isObject(body) {
		return nil, apiErr(400, "BAD_REQUEST", "Expected a JSON object")
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return nil, bodyError(err, "Expected one JSON object")
	}
	return body, nil
}

// bodyError tells a body over the size limit, or one that stopped
// arriving, from a malformed one.
func bodyError(err error, malformed string) *apiError {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return apiErr(http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "The request exceeds the size limit")
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return apiErr(http.StatusRequestTimeout, "REQUEST_TIMEOUT", "The request stopped sending its body")
	}
	return apiErr(400, "BAD_REQUEST", malformed)
}

func isObject(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// decodeRequest decodes a JSON object into a request, naming the first
// field of the wrong type.
func decodeRequest(body []byte, in any) error {
	return requestError(json.Unmarshal(body, in))
}

func requestError(err error) error {
	var mistyped *json.UnmarshalTypeError
	if err == nil || !errors.As(err, &mistyped) || mistyped.Field == "" {
		if err != nil {
			return apiErr(400, "BAD_REQUEST", "Expected a JSON object")
		}
		return nil
	}
	// A field path names list items by index; the field is the last name.
	path := strings.Split(mistyped.Field, ".")
	for len(path) > 1 {
		if _, err := strconv.Atoi(path[len(path)-1]); err != nil {
			break
		}
		path = path[:len(path)-1]
	}
	return apiErr(400, "BAD_REQUEST", path[len(path)-1]+" must be "+jsonKind(mistyped.Type))
}

func jsonKind(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Bool:
		return "a boolean"
	case reflect.Slice, reflect.Array:
		return "an array"
	case reflect.Struct, reflect.Map:
		return "an object"
	}
	return "a number"
}

// nullable is a field whose absence differs from null: null clears what
// absence leaves as it is.
type nullable[T any] struct {
	set, invalid bool
	value        *T
}

func (n *nullable[T]) UnmarshalJSON(b []byte) error {
	n.set = true
	if string(b) == "null" {
		return nil
	}
	n.value = new(T)
	if json.Unmarshal(b, n.value) != nil {
		n.value, n.invalid = nil, true
	}
	return nil
}

func (n nullable[T]) IsZero() bool                 { return !n.set }
func (n nullable[T]) MarshalJSON() ([]byte, error) { return json.Marshal(n.value) }

// check refuses a value of the wrong type, as decoding refuses other fields.
func (n nullable[T]) check(name string) error {
	if n.invalid {
		return apiErr(400, "BAD_REQUEST", name+" must be "+jsonKind(reflect.TypeFor[T]()))
	}
	return nil
}

// maxIDLength bounds the ids a caller names its rows with. A thread id is
// echoed into every frame of its run's log, so an unbounded one is an
// unbounded frame.
const maxIDLength = 128

// identifier holds a caller's id to printable ASCII, short of the length at
// which it stops being a name.
func identifier(s string) bool {
	if len(s) > maxIDLength {
		return false
	}
	for _, r := range s {
		if r < ' ' || r > '~' {
			return false
		}
	}
	return true
}

// requireID refuses an id that cannot name a row.
func requireID(field, id string) error {
	if id == "" || !identifier(id) || strings.ContainsAny(id, "/\\?#") {
		return apiErr(400, "BAD_REQUEST", field+" must be a valid identifier")
	}
	return nil
}

// A missing key is checked against the run: keyless threads accept it, and
// every other thread compares the key's fingerprint.
func requestKey(key string) (contentKey, error) {
	if key == "" {
		return contentKey{}, nil
	}
	return decodeContentKey(key)
}

type idRequest struct {
	ID string `json:"id"`
}

func (in *idRequest) validate() error { return requireID("id", in.ID) }

type threadRequest struct {
	ThreadID string `json:"threadId"`
}

func (in *threadRequest) validate() error { return requireID("threadId", in.ThreadID) }

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

func (in *windowRequest) validate() error { return requireID("id", in.ID) }

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
			return apiErr(400, "BAD_REQUEST", "thread and profile are only accepted for local threads")
		}
		return nil
	}
	if !isObject(s.Thread) {
		return apiErr(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	if s.Profile != nil && !isObject(s.Profile) {
		return apiErr(400, "BAD_REQUEST", "profile must be an object")
	}
	return validateLocalThread(s.Thread)
}

type updateThreadRequest struct {
	threadSource
	ID               string           `json:"id"`
	Title            *string          `json:"title"`
	Model            *string          `json:"model"`
	WebSearchEnabled *bool            `json:"webSearchEnabled"`
	Pinned           *bool            `json:"pinned"`
	ProjectID        nullable[string] `json:"projectId"`
	PresetID         nullable[string] `json:"presetId"`
}

func (in *updateThreadRequest) validate() error {
	if in.Local && (in.Pinned != nil || in.ProjectID.set) {
		return apiErr(400, "BAD_REQUEST", "Pins and projects require cloud storage")
	}
	return cmp.Or(in.ProjectID.check("projectId"), in.PresetID.check("presetId"), requireID("id", in.ID), in.threadSource.validate())
}

// apply makes the settings change a chat's client asks for. Choosing another
// preset takes the settings it chooses, except those the request sets.
func (in *updateThreadRequest) apply(t *Thread, c *chatCatalog, profile *Profile) {
	if in.Title != nil {
		t.Title, t.TitleState = *in.Title, "manual"
	}
	if in.PresetID.set && valueOr(in.PresetID.value, "") != valueOr(t.PresetID, "") {
		c.applyPreset(t, profile, in.PresetID.value)
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
	if err := requireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if err := requireID("messageId", in.MessageID); err != nil {
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
	Key      string `json:"key"`
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
	if !isObject(in.Patch) {
		return Profile{}, false, apiErr(400, "BAD_REQUEST", "patch must be an object")
	}
	var patch Profile
	decoder := json.NewDecoder(bytes.NewReader(in.Patch))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&patch); err != nil {
		if field, unknown := strings.CutPrefix(err.Error(), "json: unknown field "); unknown {
			return Profile{}, false, apiErr(400, "BAD_REQUEST", "Unknown profile field: "+strings.Trim(field, `"`))
		}
		return Profile{}, false, requestError(err)
	}
	if err := patch.validate(); err != nil {
		return Profile{}, false, err
	}
	var preset struct {
		DefaultPromptPresetID nullable[string] `json:"defaultPromptPresetId"`
	}
	json.Unmarshal(in.Patch, &preset)
	return patch, preset.DefaultPromptPresetID.set && preset.DefaultPromptPresetID.value == nil, nil
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
	if !isObject(in.Profile) {
		return apiErr(400, "BAD_REQUEST", "profile must be an object")
	}
	return nil
}

type projectRequest struct {
	ID                 string           `json:"id"`
	Name               *string          `json:"name"`
	Description        *string          `json:"description"`
	SystemInstructions *string          `json:"systemInstructions"`
	Color              nullable[string] `json:"color"`
}

func (in *projectRequest) validate() error { return in.Color.check("color") }

type projectDocumentRequest struct {
	ProjectID  string `json:"projectId"`
	DocumentID string `json:"documentId"`
}

func (in *projectDocumentRequest) validate() error {
	if err := requireID("projectId", in.ProjectID); err != nil {
		return err
	}
	return requireID("documentId", in.DocumentID)
}

// shareRequest shares a stored chat by its key, or a device chat by the
// thread its client sends.
type shareRequest struct {
	Key      string          `json:"key"`
	ThreadID string          `json:"threadId"`
	Local    bool            `json:"local"`
	Thread   json.RawMessage `json:"thread"`
}

func (in *shareRequest) validate() error {
	if err := requireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !in.Local {
		return nil
	}
	if !isObject(in.Thread) {
		return apiErr(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	return validateLocalThread(in.Thread)
}

type openShareRequest struct {
	ShareID  string `json:"shareId"`
	Fragment string `json:"fragment"`
}

func (in *openShareRequest) validate() error { return requireID("shareId", in.ShareID) }

type importStatusRequest struct {
	JobID string `json:"jobId"`
}

func (in *importStatusRequest) validate() error { return requireID("jobId", in.JobID) }

type faviconRequest struct {
	URL string `json:"url"`
}

// speechRequest names a message to read aloud: one of a stored chat, named
// with the content key, or of a device chat its client sends. Text a client
// selected, or a shared chat's message, is sent as it is.
type speechRequest struct {
	threadSource
	Key       string  `json:"key"`
	ThreadID  string  `json:"threadId"`
	MessageID string  `json:"messageId"`
	Text      *string `json:"text"`
}

func (in *speechRequest) validate() error {
	if in.MessageID == "" {
		if strings.TrimSpace(valueOr(in.Text, "")) == "" {
			return apiErr(400, "BAD_REQUEST", "A message or text is required")
		}
		return nil
	}
	if err := requireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if err := requireID("messageId", in.MessageID); err != nil {
		return err
	}
	return in.threadSource.validate()
}

// threadImportRequest stores a device chat in the cloud under its id.
type threadImportRequest struct {
	Key      string          `json:"key"`
	ThreadID string          `json:"threadId"`
	Thread   json.RawMessage `json:"thread"`
}

func (in *threadImportRequest) validate() error {
	if err := requireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !isObject(in.Thread) {
		return apiErr(400, "BAD_REQUEST", "thread must be an object")
	}
	return validateLocalThread(in.Thread)
}

type upgradeRequest struct {
	Threads *[]json.RawMessage `json:"threads"`
}

// attachmentRequest downloads an attachment of a stored chat, or one a share
// names by the key its link carries.
type attachmentRequest struct {
	Key         string         `json:"key"`
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
	if err := requireID("id", in.ID); err != nil {
		return err
	}
	if in.ThreadID != "" {
		return requireID("threadId", in.ThreadID)
	}
	return nil
}

// turnRequest is one user action on a chat, which its run carries out.
type turnRequest struct {
	Key             string           `json:"key,omitempty"`
	ClientRequestID string           `json:"clientRequestId"`
	ThreadID        string           `json:"threadId"`
	Kind            string           `json:"kind"`
	Ephemeral       bool             `json:"ephemeral,omitempty"`
	Local           bool             `json:"local,omitempty"`
	Thread          json.RawMessage  `json:"thread,omitempty"`
	Profile         json.RawMessage  `json:"profile,omitempty"`
	ProjectID       *string          `json:"projectId,omitempty"`
	PresetID        nullable[string] `json:"presetId,omitzero"`
	Content         *string          `json:"content,omitempty"`
	Quote           *string          `json:"quote,omitempty"`
	MessageID       string           `json:"messageId,omitempty"`
	ToolCallID      string           `json:"toolCallId,omitempty"`
	Attachments     []sentAttachment `json:"attachments,omitzero"`
	Widgets         []string         `json:"widgets,omitzero"`
	Options         turnOptions      `json:"options"`
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
}

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
		return apiErr(400, "BAD_REQUEST", "clientRequestId must be a UUID")
	}
	in.Kind = cmp.Or(in.Kind, "send")
	if !slices.Contains(turnKinds, in.Kind) {
		return apiErr(400, "BAD_REQUEST", "Unknown turn kind")
	}
	// An ask is never stored, so it may still say so.
	if in.Ephemeral && in.Kind != "ask" {
		return apiErr(400, "BAD_REQUEST", "Temporary chats are kept on the device")
	}
	return in.PresetID.check("presetId")
}

// validateContent refuses a turn whose content cannot make its message.
func (in *turnRequest) validateContent() error {
	if err := requireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if in.Local && in.ProjectID != nil {
		return apiErr(400, "BAD_REQUEST", "Projects require cloud storage")
	}
	for _, field := range []struct {
		name  string
		value json.RawMessage
	}{{"thread", in.Thread}, {"profile", in.Profile}} {
		if field.value == nil {
			continue
		}
		if !in.Local {
			return apiErr(400, "BAD_REQUEST", field.name+" is only accepted for local threads")
		}
		if !isObject(field.value) {
			return apiErr(400, "BAD_REQUEST", field.name+" must be an object")
		}
	}
	if in.Thread != nil {
		if err := validateLocalThread(in.Thread); err != nil {
			return err
		}
	} else if in.Local && in.Kind == "ask" {
		// An ask about a device chat reads the conversation its client sends.
		return apiErr(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	pixels := int64(0)
	for i := range in.Attachments {
		if err := in.Attachments[i].validate(); err != nil {
			return err
		}
		pixels += in.Attachments[i].resizePixels()
	}
	if pixels > maxTurnResizePixels {
		return apiErr(413, "ATTACHMENT_TOO_LARGE", "The turn's images hold more pixels than one turn can resize")
	}
	if in.Kind == "send" && strings.TrimSpace(valueOr(in.Content, "")) == "" && strings.TrimSpace(valueOr(in.Quote, "")) == "" && len(in.Attachments) == 0 {
		return apiErr(400, "BAD_REQUEST", "A message or attachment is required")
	}
	return nil
}

func (a *sentAttachment) validate() error {
	if err := requireID("id", a.ID); err != nil {
		return err
	}
	if a.Kind != "" && !slices.Contains(attachmentKinds, a.Kind) {
		return apiErr(400, "BAD_REQUEST", "An attachment kind must be image, document, or audio")
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
	copy.Key = ""
	return hashHex(raw(copy))
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
	if err := decodeRequest(data, &thread); err != nil {
		return err
	}
	if len(thread.Messages) > maxLocalMessages {
		return apiErr(413, "CONTEXT_TOO_LARGE", "The thread has too many messages")
	}
	for _, message := range thread.Messages {
		if !slices.Contains(messageRoles, message.Role) {
			return apiErr(400, "BAD_REQUEST", "A message role must be user or assistant")
		}
		if message.ID != "" && !identifier(message.ID) {
			return apiErr(400, "BAD_REQUEST", "A message id must be a valid identifier")
		}
	}
	return nil
}
