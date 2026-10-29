package domain

import (
	"encoding/json"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

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
// client wrote them; one stored without a number for either is dated then.
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

// StoredProfile is the profile row.
type StoredProfile struct {
	Profile
	UpdatedAt     string `json:"updatedAt,omitempty"`
	RecordVersion int    `json:"recordVersion,omitempty"`
	// PinnedChatIDs makes favorites available outside the loaded history pages.
	PinnedChatIDs []string `json:"pinnedChatIds,omitzero"`
	// Others are the row's members this harness does not name, such as the
	// edit clocks the apps' own profile sync keeps. A write keeps them.
	Others map[string]json.RawMessage `json:"-"`
}

// storedProfileFields is every member of the row a StoredProfile names.
var storedProfileFields = jsonFields(reflect.TypeFor[StoredProfile]())

func jsonFields(t reflect.Type) map[string]bool {
	fields := map[string]bool{}
	for i := range t.NumField() {
		field := t.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		switch {
		case field.Anonymous && name == "":
			maps.Copy(fields, jsonFields(field.Type))
		case name != "" && name != "-":
			fields[name] = true
		}
	}
	return fields
}

// otherMembers are the members of a profile row a StoredProfile does not name.
func otherMembers(data []byte) map[string]json.RawMessage {
	var members map[string]json.RawMessage
	json.Unmarshal(data, &members)
	maps.DeleteFunc(members, func(name string, _ json.RawMessage) bool { return storedProfileFields[name] })
	if len(members) == 0 {
		return nil
	}
	return members
}

// MarshalJSON writes the row with the members it does not name kept.
func (p StoredProfile) MarshalJSON() ([]byte, error) {
	type row StoredProfile
	data, err := json.Marshal(row(p))
	if err != nil || len(p.Others) == 0 {
		return data, err
	}
	members := map[string]json.RawMessage{}
	json.Unmarshal(data, &members)
	for name, value := range p.Others {
		if !storedProfileFields[name] {
			members[name] = value
		}
	}
	return json.Marshal(members)
}

// The apps' own profile sync merges the row field by field. It keeps an edit
// clock {v, w} for each field in fieldClocks, and trusts the clocks only while
// clockVersion is the row's revision. A write that leaves them stale makes
// those apps refuse to merge any field both they and the write changed.
const (
	fieldClocksMember  = "fieldClocks"
	clockVersionMember = "clockVersion"
	versionMember      = "version"
	clockWriter        = "harness"
	// maxClock is the largest counter those apps keep, the largest integer a
	// JavaScript number holds exactly.
	maxClock = 1<<53 - 1
)

// PrepareWrite readies a changed profile to be stored over data, the row it
// was read from, as revision, the revision the write creates or 0 when it is
// not known. Its saved prompts get every member the apps' own profile sync
// requires, and each field the write changes takes an edit clock past every
// one the row holds, so that sync can merge it.
func (p *StoredProfile) PrepareWrite(data json.RawMessage, revision int, now time.Time) {
	p.completePresets(now)
	p.UpdatedAt = now.Format(time.RFC3339Nano)
	p.RecordVersion = RecordVersion
	p.stampEdits(data, revision)
}

func (p *StoredProfile) stampEdits(data json.RawMessage, revision int) {
	type row StoredProfile
	var was, is map[string]any
	json.Unmarshal(data, &was)
	json.Unmarshal(api.Raw(row(*p)), &is)
	var changed []string
	for name := range storedProfileFields {
		if name != "updatedAt" && name != "recordVersion" && !reflect.DeepEqual(was[name], is[name]) {
			changed = append(changed, name)
		}
	}
	if p.Others == nil {
		p.Others = map[string]json.RawMessage{}
	}
	if len(changed) > 0 {
		clocks := map[string]json.RawMessage{}
		json.Unmarshal(p.Others[fieldClocksMember], &clocks)
		latest := int64(0)
		for _, raw := range clocks {
			var clock struct {
				V float64 `json:"v"`
			}
			if json.Unmarshal(raw, &clock) == nil {
				latest = max(latest, int64(clock.V))
			}
		}
		tick := api.Raw(api.Object{"v": min(latest+1, maxClock), "w": clockWriter})
		for _, name := range changed {
			clocks[name] = tick
		}
		p.Others[fieldClocksMember] = api.Raw(clocks)
	}
	if revision > 0 {
		p.Others[clockVersionMember], p.Others[versionMember] = api.Raw(revision), api.Raw(revision)
	}
}

// completePresets gives each saved prompt the members the apps' own profile
// sync requires of one, without which it refuses the whole profile: its text,
// empty when unset, and its dates as Unix milliseconds, now for one without.
func (p *Profile) completePresets(now time.Time) {
	for i := range p.CustomPromptPresets {
		preset := &p.CustomPromptPresets[i]
		preset.Description = new(ValueOr(preset.Description, ""))
		preset.SystemPrompt = new(ValueOr(preset.SystemPrompt, ""))
		if !isNumber(preset.CreatedAt) {
			preset.CreatedAt = api.Raw(now.UnixMilli())
		}
		if !isNumber(preset.UpdatedAt) {
			preset.UpdatedAt = preset.CreatedAt
		}
	}
}

func isNumber(raw json.RawMessage) bool {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	_, number := value.(float64)
	return number
}

// samePrompt reports whether two saved prompts read the same, text left unset
// reading as empty, as a stored prompt's does.
func samePrompt(a, b CustomPreset) bool {
	for _, preset := range []*CustomPreset{&a, &b} {
		preset.Description = new(ValueOr(preset.Description, ""))
		preset.SystemPrompt = new(ValueOr(preset.SystemPrompt, ""))
	}
	return reflect.DeepEqual(a, b)
}

// ProfileView is the profile a client reads.
type ProfileView struct {
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
	DefaultLanguage     = "System"
	DefaultIntelligence = "high"
	defaultEffort       = "medium"
	defaultTheme        = "system"
	defaultFont         = "system"
)

// DefaultProfile holds the value of every profile field a profile leaves
// unset. An empty default prompt preset is none. Thinking has no default:
// left unset, it follows each model's own.
func DefaultProfile() Profile {
	return Profile{ThemeMode: new(defaultTheme), ChatFont: new(defaultFont), Language: new(DefaultLanguage), Nickname: new(""), Profession: new(""), Traits: []string{}, AdditionalContext: new(""),
		IsUsingPersonalization: new(true), IsUsingCustomPrompt: new(false), CustomSystemPrompt: new(""), CustomPromptPresets: []CustomPreset{}, FavoritePromptPresetIDs: []string{}, DefaultPromptPresetID: new(""),
		SelectedModel: new(AutoModelName), AutoIntelligence: new(DefaultIntelligence), ReasoningEffort: new(defaultEffort), WebSearchEnabled: new(true),
		WebSearchAvailable: new(true), PIICheckEnabled: new(true), GenUIEnabled: new(true), PixelateSidebarChatTitlesEnabled: new(false), BrowserTabChatTitleEnabled: new(true), EnterToNewlineEnabled: new(false),
		HasSeenOnboarding: new(false), HasSeenWebSearchIntro: new(false), Dismissed: map[string]bool{}}
}

// withDefaults is the profile with every field it leaves unset at its default.
func (p Profile) withDefaults() Profile {
	filled := DefaultProfile()
	filled.Apply(p)
	return filled
}

// Validate refuses a profile with a value a client may not set.
func (p *Profile) Validate() error {
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
	}{{"themeMode", &p.ThemeMode, themeModes}, {"chatFont", &p.ChatFont, chatFonts}, {"reasoningEffort", &p.ReasoningEffort, ReasoningEfforts}, {"autoIntelligence", &p.AutoIntelligence, intelligenceIDs()}} {
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

// Apply sets every field the patch sets.
func (p *Profile) Apply(patch Profile) {
	to, from := reflect.ValueOf(p).Elem(), reflect.ValueOf(patch)
	for i := range from.NumField() {
		if !from.Field(i).IsZero() {
			to.Field(i).Set(from.Field(i))
		}
	}
}

// MergeDeviceProfile imports missing settings and prompts, with explicit
// device edits taking precedence. Conflicting prompt text gets its own copy.
func MergeDeviceProfile(remote, local Profile, changed []string) Profile {
	merged := local
	merged.Apply(remote)
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
			if samePrompt(kept, incoming) {
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

// Personalized reports whether the prompt carries the user's preferences.
func (p *Profile) Personalized() bool {
	return p.IsUsingPersonalization == nil || *p.IsUsingPersonalization
}

// Preset is a saved prompt by id.
func (p *Profile) Preset(id string) *CustomPreset {
	for i := range p.CustomPromptPresets {
		if p.CustomPromptPresets[i].ID == id {
			return &p.CustomPromptPresets[i]
		}
	}
	return nil
}

// LocalProfile is the settings a device chat's client sends with a request,
// without the fields it may not set, which never refuse the request.
func LocalProfile(data []byte) *Profile {
	var profile Profile
	readLenient(data, &profile)
	upgradeProfile(&profile, api.Now())
	profile.dropInvalid()
	return &profile
}

// EffectiveProfile is a profile as its client reads it: a saved prompt
// keeps a model only while the control plane lists it, the selected model
// is the one new chats start on, and every field the profile leaves unset
// reads as its default.
func (c *Catalog) EffectiveProfile(p Profile) Profile {
	p.CustomPromptPresets = slices.Clone(p.CustomPromptPresets)
	for i := range p.CustomPromptPresets {
		if model := p.CustomPromptPresets[i].Model; model != nil && c.retired(*model) {
			p.CustomPromptPresets[i].Model = nil
		}
	}
	p.SelectedModel = new(c.SelectedModel(&p))
	return p.withDefaults()
}

// SelectedModel is the model new chats start on: the profile's while it is
// offered, and Auto otherwise.
func (c *Catalog) SelectedModel(p *Profile) string {
	if model := ValueOr(p.SelectedModel, ""); c.Offers(model) {
		return model
	}
	return AutoModelName
}
