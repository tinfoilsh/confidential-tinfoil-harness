package server

import (
	"bytes"
	"cmp"
	"encoding/json"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Each route decodes its JSON body into one of these. A field of the wrong
// type refuses the request; a field the route does not read is ignored,
// except in a profile patch.

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

type upgradeRequest struct {
	Threads *[]json.RawMessage `json:"threads"`
}
