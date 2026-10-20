package main

import (
	"encoding/json"
	"reflect"
	"slices"
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
	CodeExecutionEnabled             *bool           `json:"codeExecutionEnabled,omitempty"`
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
	UpdatedAt string `json:"updatedAt,omitempty"`
	// PinnedChatIDs are the favorites iOS kept here before each chat stored
	// its own pin. A chat this lists reads as pinned.
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

func (p *Profile) validate() error {
	for _, choice := range []struct {
		name    string
		value   *string
		choices []string
	}{{"themeMode", p.ThemeMode, themeModes}, {"chatFont", p.ChatFont, chatFonts}, {"reasoningEffort", p.ReasoningEffort, reasoningEfforts}, {"autoIntelligence", p.AutoIntelligence, intelligenceIDs()}} {
		if choice.value != nil && !slices.Contains(choice.choices, *choice.value) {
			return apiErr(400, "BAD_REQUEST", "Invalid "+choice.name)
		}
	}
	for _, list := range []struct {
		name   string
		length int
	}{{"traits", len(p.Traits)}, {"favoritePromptPresetIds", len(p.FavoritePromptPresetIDs)}, {"customPromptPresets", len(p.CustomPromptPresets)}} {
		if list.length > maxProfileList {
			return apiErr(400, "BAD_REQUEST", list.name+" has too many entries")
		}
	}
	for _, preset := range p.CustomPromptPresets {
		if preset.ID == "" || preset.Name == "" {
			return apiErr(400, "BAD_REQUEST", "A custom preset requires an id and name")
		}
	}
	return nil
}

// apply sets every field the patch sets.
func (p *Profile) apply(patch Profile) {
	to, from := reflect.ValueOf(p).Elem(), reflect.ValueOf(patch)
	for i := range from.NumField() {
		if !from.Field(i).IsZero() {
			to.Field(i).Set(from.Field(i))
		}
	}
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

// localProfile is the settings a device chat's client sends with its turn.
func localProfile(data []byte) *Profile {
	var profile Profile
	readLenient(data, &profile)
	profile.SelectedModel = new(normalizeModel(valueOr(profile.SelectedModel, "")))
	return &profile
}
