package main

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Profile is a user's settings: what the clients show and a prompt reads.
// Every field is optional, so a patch sets exactly the fields it names.
type Profile struct {
	ThemeMode                        *string         `json:"themeMode,omitempty"`
	ChatFont                         *string         `json:"chatFont,omitempty"`
	Language                         *string         `json:"language,omitempty"`
	Nickname                         *string         `json:"nickname,omitempty"`
	Profession                       *string         `json:"profession,omitempty"`
	Traits                           []string        `json:"traits,omitzero"`
	AdditionalContext                *string         `json:"additionalContext,omitempty"`
	IsUsingPersonalization           *bool           `json:"isUsingPersonalization,omitempty"`
	IsUsingCustomPrompt              *bool           `json:"isUsingCustomPrompt,omitempty"`
	CustomSystemPrompt               *string         `json:"customSystemPrompt,omitempty"`
	CustomPromptPresets              []CustomPreset  `json:"customPromptPresets,omitzero"`
	FavoritePromptPresetIDs          []string        `json:"favoritePromptPresetIds,omitzero"`
	DefaultPromptPresetID            *string         `json:"defaultPromptPresetId,omitempty"`
	SelectedModel                    *string         `json:"selectedModel,omitempty"`
	AutoIntelligence                 *string         `json:"autoIntelligence,omitempty"`
	ReasoningEffort                  *string         `json:"reasoningEffort,omitempty"`
	ThinkingEnabled                  *bool           `json:"thinkingEnabled,omitempty"`
	WebSearchEnabled                 *bool           `json:"webSearchEnabled,omitempty"`
	WebSearchAvailable               *bool           `json:"webSearchAvailable,omitempty"`
	PIICheckEnabled                  *bool           `json:"piiCheckEnabled,omitempty"`
	GenUIEnabled                     *bool           `json:"genUIEnabled,omitempty"`
	PixelateSidebarChatTitlesEnabled *bool           `json:"pixelateSidebarChatTitlesEnabled,omitempty"`
	BrowserTabChatTitleEnabled       *bool           `json:"browserTabChatTitleEnabled,omitempty"`
	EnterToNewlineEnabled            *bool           `json:"enterToNewlineEnabled,omitempty"`
	HasSeenOnboarding                *bool           `json:"hasSeenOnboarding,omitempty"`
	HasSeenWebSearchIntro            *bool           `json:"hasSeenWebSearchIntro,omitempty"`
	Dismissed                        map[string]bool `json:"dismissed,omitzero"`
}

// CustomPreset is a prompt the user saved. Its timestamps are kept as the
// client wrote them.
type CustomPreset struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	Description      *string         `json:"description,omitempty"`
	SystemPrompt     *string         `json:"systemPrompt,omitempty"`
	Model            *string         `json:"model,omitempty"`
	WebSearchEnabled *bool           `json:"webSearchEnabled,omitempty"`
	CreatedAt        json.RawMessage `json:"createdAt,omitempty"`
	UpdatedAt        json.RawMessage `json:"updatedAt,omitempty"`
}

// storedProfile is the profile row.
type storedProfile struct {
	Profile
	UpdatedAt     string `json:"updatedAt,omitempty"`
	RecordVersion int    `json:"recordVersion,omitempty"`
	// PinnedChatIDs makes favorites available outside the loaded history pages.
	PinnedChatIDs []string `json:"pinnedChatIds,omitzero"`
}

// profileView is the profile a client reads.
type profileView struct {
	Profile
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// maxProfileList bounds each list a profile holds.
const maxProfileList = 500

var (
	themeModes = []string{"light", "dark", "system"}
	chatFonts  = []string{"system", "serif", "mono", "dyslexic"}
)

// The defaults of profile fields that hold a choice.
const (
	defaultLanguage     = "System"
	defaultIntelligence = "high"
	defaultEffort       = "medium"
	defaultTheme        = "system"
	defaultFont         = "system"
)

// defaultProfile holds the value of every profile field a profile leaves
// unset. An empty default prompt preset is none. Thinking has no default:
// left unset, it follows each model's own.
func defaultProfile() Profile {
	return Profile{ThemeMode: new(defaultTheme), ChatFont: new(defaultFont), Language: new(defaultLanguage), Nickname: new(""), Profession: new(""), Traits: []string{}, AdditionalContext: new(""),
		IsUsingPersonalization: new(true), IsUsingCustomPrompt: new(false), CustomSystemPrompt: new(""), CustomPromptPresets: []CustomPreset{}, FavoritePromptPresetIDs: []string{}, DefaultPromptPresetID: new(""),
		SelectedModel: new(autoModelName), AutoIntelligence: new(defaultIntelligence), ReasoningEffort: new(defaultEffort), WebSearchEnabled: new(true),
		WebSearchAvailable: new(true), PIICheckEnabled: new(true), GenUIEnabled: new(true), PixelateSidebarChatTitlesEnabled: new(false), BrowserTabChatTitleEnabled: new(true), EnterToNewlineEnabled: new(false),
		HasSeenOnboarding: new(false), HasSeenWebSearchIntro: new(false), Dismissed: map[string]bool{}}
}

// withDefaults is the profile with every field it leaves unset at its default.
func (p Profile) withDefaults() Profile {
	filled := defaultProfile()
	filled.apply(p)
	return filled
}

func (p *Profile) validate() error {
	checked := *p
	if invalid := checked.dropInvalid(); invalid != nil {
		return invalid
	}
	return nil
}

// dropInvalid clears each field whose value a client may not set, and
// reports the first.
func (p *Profile) dropInvalid() *api.Error {
	var first *api.Error
	refuse := func(message string) {
		if first == nil {
			first = api.Err(400, "BAD_REQUEST", message)
		}
	}
	for _, choice := range []struct {
		name    string
		value   **string
		choices []string
	}{{"themeMode", &p.ThemeMode, themeModes}, {"chatFont", &p.ChatFont, chatFonts}, {"reasoningEffort", &p.ReasoningEffort, reasoningEfforts}, {"autoIntelligence", &p.AutoIntelligence, intelligenceIDs()}} {
		if *choice.value != nil && !slices.Contains(choice.choices, **choice.value) {
			refuse("Invalid " + choice.name)
			*choice.value = nil
		}
	}
	if len(p.Traits) > maxProfileList {
		refuse("traits has too many entries")
		p.Traits = nil
	}
	if len(p.FavoritePromptPresetIDs) > maxProfileList {
		refuse("favoritePromptPresetIds has too many entries")
		p.FavoritePromptPresetIDs = nil
	}
	if len(p.CustomPromptPresets) > maxProfileList {
		refuse("customPromptPresets has too many entries")
		p.CustomPromptPresets = nil
	}
	if slices.ContainsFunc(p.CustomPromptPresets, unnamedPreset) {
		refuse("A custom preset requires an id and name")
		p.CustomPromptPresets = slices.DeleteFunc(slices.Clone(p.CustomPromptPresets), unnamedPreset)
	}
	return first
}

func unnamedPreset(preset CustomPreset) bool { return preset.ID == "" || preset.Name == "" }

// apply sets every field the patch sets.
func (p *Profile) apply(patch Profile) {
	to, from := reflect.ValueOf(p).Elem(), reflect.ValueOf(patch)
	for i := range from.NumField() {
		if !from.Field(i).IsZero() {
			to.Field(i).Set(from.Field(i))
		}
	}
}

// mergeDeviceProfile imports missing settings and prompts, with explicit
// device edits taking precedence. Conflicting prompt text gets its own copy.
func mergeDeviceProfile(remote, local Profile, changed []string) Profile {
	merged := local
	merged.apply(remote)
	if slices.Contains(changed, "defaultPromptPresetId") {
		merged.DefaultPromptPresetID = local.DefaultPromptPresetID
	}
	to, from := reflect.ValueOf(&merged).Elem(), reflect.ValueOf(local)
	for i := range from.NumField() {
		name, _, _ := strings.Cut(from.Type().Field(i).Tag.Get("json"), ",")
		if slices.Contains(changed, name) && !from.Field(i).IsZero() {
			to.Field(i).Set(from.Field(i))
		}
	}
	presets := slices.Clone(remote.CustomPromptPresets)
	remapped := map[string]string{}
	for _, preset := range local.CustomPromptPresets {
		index := slices.IndexFunc(presets, func(p CustomPreset) bool { return p.ID == preset.ID })
		if index >= 0 {
			kept, incoming := presets[index], preset
			kept.CreatedAt, kept.UpdatedAt, incoming.CreatedAt, incoming.UpdatedAt = nil, nil, nil, nil
			if reflect.DeepEqual(kept, incoming) {
				continue
			}
			preset.ID = "user:" + uuid.NewSHA1(uuid.NameSpaceURL, api.Raw(incoming)).String()
			preset.Name += " (from this device)"
			remapped[incoming.ID] = preset.ID
			if slices.ContainsFunc(presets, func(p CustomPreset) bool { return p.ID == preset.ID }) {
				continue
			}
		}
		presets = append(presets, preset)
	}
	merged.CustomPromptPresets = presets
	merged.FavoritePromptPresetIDs = slices.Clone(remote.FavoritePromptPresetIDs)
	if slices.Contains(changed, "favoritePromptPresetIds") {
		merged.FavoritePromptPresetIDs = []string{}
	}
	for _, id := range local.FavoritePromptPresetIDs {
		if replacement := remapped[id]; replacement != "" {
			id = replacement
		}
		if !slices.Contains(merged.FavoritePromptPresetIDs, id) {
			merged.FavoritePromptPresetIDs = append(merged.FavoritePromptPresetIDs, id)
		}
	}
	if (remote.DefaultPromptPresetID == nil || slices.Contains(changed, "defaultPromptPresetId")) && local.DefaultPromptPresetID != nil {
		if replacement := remapped[*local.DefaultPromptPresetID]; replacement != "" {
			merged.DefaultPromptPresetID = &replacement
		}
	}
	return merged
}

// personalized reports whether the prompt carries the user's preferences.
func (p *Profile) personalized() bool {
	return p.IsUsingPersonalization == nil || *p.IsUsingPersonalization
}

// preset is a saved prompt by id.
func (p *Profile) preset(id string) *CustomPreset {
	for i := range p.CustomPromptPresets {
		if p.CustomPromptPresets[i].ID == id {
			return &p.CustomPromptPresets[i]
		}
	}
	return nil
}

// localProfile is the settings a device chat's client sends with a request,
// without the fields it may not set, which never refuse the request.
func localProfile(data []byte) *Profile {
	var profile Profile
	readLenient(data, &profile)
	upgradeProfile(&profile, api.Now())
	profile.dropInvalid()
	return &profile
}

// effectiveProfile is a profile as its client reads it: a saved prompt
// keeps a model only while the control plane lists it, the selected model
// is the one new chats start on, and every field the profile leaves unset
// reads as its default.
func (c *chatCatalog) effectiveProfile(p Profile) Profile {
	p.CustomPromptPresets = slices.Clone(p.CustomPromptPresets)
	for i := range p.CustomPromptPresets {
		if model := p.CustomPromptPresets[i].Model; model != nil && c.retired(*model) {
			p.CustomPromptPresets[i].Model = nil
		}
	}
	p.SelectedModel = new(c.selectedModel(&p))
	return p.withDefaults()
}

// selectedModel is the model new chats start on: the profile's while it is
// offered, and Auto otherwise.
func (c *chatCatalog) selectedModel(p *Profile) string {
	if model := valueOr(p.SelectedModel, ""); c.offers(model) {
		return model
	}
	return autoModelName
}
