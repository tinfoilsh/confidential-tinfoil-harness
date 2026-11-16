package domain

import "encoding/json"

// ProjectArtifact is an artifact a reply in a project's chat showed: where it
// is, and what its card shows, without its source.
type ProjectArtifact struct {
	ThreadID    string `json:"threadId"`
	ThreadTitle string `json:"threadTitle"`
	MessageID   string `json:"messageId"`
	ToolCallID  string `json:"toolCallId"`
	Title       string `json:"title"`
	SourceType  string `json:"sourceType"`
	CreatedAt   string `json:"createdAt"`
}

// Artifacts are the artifacts the replies a chat shows drew, oldest first.
func (t *Thread) Artifacts() []ProjectArtifact {
	artifacts := []ProjectArtifact{}
	t.eachArtifact(func(artifact ProjectArtifact, _ string) bool {
		artifacts = append(artifacts, artifact)
		return true
	})
	return artifacts
}

// Artifact is the artifact a chat's reply drew in the call toolCallID, and
// the arguments it was drawn with.
func (t *Thread) Artifact(toolCallID string) (ProjectArtifact, string, bool) {
	var found ProjectArtifact
	var arguments string
	t.eachArtifact(func(artifact ProjectArtifact, args string) bool {
		if artifact.ToolCallID != toolCallID {
			return true
		}
		found, arguments = artifact, args
		return false
	})
	return found, arguments, arguments != ""
}

// eachArtifact visits each artifact call of the replies a chat shows whose
// arguments the widget draws, as its client would draw it, until visit
// returns false.
func (t *Thread) eachArtifact(visit func(artifact ProjectArtifact, arguments string) bool) {
	var widget *Widget
	for _, w := range WidgetCatalog() {
		if w.Name == ArtifactWidget {
			widget = &w
		}
	}
	if widget == nil {
		return
	}
	for _, m := range t.Messages[min(len(t.Messages), t.HiddenCount):] {
		if m.Role != "assistant" {
			continue
		}
		for _, b := range m.Timeline {
			if b.Type != "tool_call" || b.Name != ArtifactWidget || b.ToolCallID == "" || WidgetFailure(*widget, b.Arguments) != nil {
				continue
			}
			var args struct {
				Title  string `json:"title"`
				Source struct {
					Type string `json:"type"`
				} `json:"source"`
			}
			json.Unmarshal([]byte(b.Arguments), &args)
			artifact := ProjectArtifact{ThreadID: t.ID, ThreadTitle: t.Title, MessageID: m.ID, ToolCallID: b.ToolCallID, Title: args.Title, SourceType: args.Source.Type, CreatedAt: m.CreatedAt}
			if !visit(artifact, b.Arguments) {
				return
			}
		}
	}
}
