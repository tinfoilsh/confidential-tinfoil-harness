package main

import (
	"slices"
	"strings"
)

func validateFields(in object, stringsList, booleans, nullable string) error {
	for _, name := range strings.Fields(stringsList + " " + nullable) {
		value, exists := in[name]
		if !exists || value == nil && slices.Contains(strings.Fields(nullable), name) {
			continue
		}
		if _, ok := value.(string); !ok {
			return apiErr(400, "BAD_REQUEST", name+" must be a string")
		}
	}
	for _, name := range strings.Fields(booleans) {
		if value, exists := in[name]; exists {
			if _, ok := value.(bool); !ok {
				return apiErr(400, "BAD_REQUEST", name+" must be a boolean")
			}
		}
	}
	return nil
}
func validateProfile(patch object) error {
	if err := validateFields(patch, "themeMode chatFont language nickname profession additionalContext customSystemPrompt selectedModel autoIntelligence reasoningEffort", "isUsingPersonalization isUsingCustomPrompt thinkingEnabled webSearchEnabled codeExecutionEnabled sandboxEnabled piiCheckEnabled genUIEnabled pixelateSidebarChatTitlesEnabled browserTabChatTitleEnabled enterToNewlineEnabled hasSeenOnboarding hasSeenWebSearchIntro", ""); err != nil {
		return err
	}
	for field, choices := range map[string][]string{"themeMode": {"light", "dark", "system"}, "chatFont": {"system", "serif", "mono", "dyslexic"}, "reasoningEffort": {"low", "medium", "high"}, "autoIntelligence": {"instant", "low", "medium", "high", "extra", "max"}} {
		if value, ok := patch[field]; ok && !slices.Contains(choices, str(value)) {
			return apiErr(400, "BAD_REQUEST", "Invalid "+field)
		}
	}
	for _, field := range []string{"traits", "favoritePromptPresetIds", "customPromptPresets"} {
		if values, ok := patch[field]; ok {
			if _, ok := values.([]any); !ok {
				return apiErr(400, "BAD_REQUEST", field+" must be an array")
			}
			if len(arr(values)) > 500 {
				return apiErr(400, "BAD_REQUEST", field+" has too many entries")
			}
			for _, value := range arr(values) {
				if field == "customPromptPresets" {
					preset := obj(value)
					if str(preset["id"]) == "" || str(preset["name"]) == "" {
						return apiErr(400, "BAD_REQUEST", "A custom preset requires an id and name")
					}
					if err := validateFields(preset, "id name description systemPrompt", "", ""); err != nil {
						return err
					}
				} else if _, ok := value.(string); !ok {
					return apiErr(400, "BAD_REQUEST", field+" must contain strings")
				}
			}
		}
	}
	if value, ok := patch["dismissed"]; ok {
		if _, ok := value.(map[string]any); !ok {
			if _, ok := value.(object); !ok {
				return apiErr(400, "BAD_REQUEST", "dismissed must be an object")
			}
		}
		for name, value := range obj(value) {
			if _, ok := value.(bool); !ok {
				return apiErr(400, "BAD_REQUEST", "dismissed."+name+" must be a boolean")
			}
		}
	}
	return nil
}

func validObject(value any) bool {
	switch value.(type) {
	case object, map[string]any:
		return true
	}
	return false
}
