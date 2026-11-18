package domain

import (
	"encoding/json"
	"slices"
)

// Artifact is an artifact a reply in a chat showed: where it is, and what
// its card shows, without its source.
type Artifact struct {
	ThreadID    string  `json:"threadId"`
	ThreadTitle string  `json:"threadTitle"`
	ProjectID   *string `json:"projectId"`
	MessageID   string  `json:"messageId"`
	ToolCallID  string  `json:"toolCallId"`
	Title       string  `json:"title"`
	SourceType  string  `json:"sourceType"`
	CreatedAt   string  `json:"createdAt"`
}

// Artifacts are the artifacts the replies a chat shows drew, newest first.
func (t *Thread) Artifacts() []Artifact {
	artifacts := []Artifact{}
	t.eachArtifact(func(artifact Artifact, _ string) bool {
		artifacts = append(artifacts, artifact)
		return true
	})
	slices.Reverse(artifacts)
	return artifacts
}

// Artifact is the artifact the chat's reply messageID drew in the call
// toolCallID, and the arguments it was drawn with. A model may name calls
// in different replies alike, so the reply names it too.
func (t *Thread) Artifact(messageID, toolCallID string) (Artifact, string, bool) {
	var found Artifact
	var arguments string
	t.eachArtifact(func(artifact Artifact, args string) bool {
		if artifact.MessageID != messageID || artifact.ToolCallID != toolCallID {
			return true
		}
		found, arguments = artifact, args
		return false
	})
	return found, arguments, arguments != ""
}

// eachArtifact visits each artifact call of the replies a chat shows whose
// arguments the widget draws, as its client would draw it, oldest first,
// until visit returns false.
func (t *Thread) eachArtifact(visit func(artifact Artifact, arguments string) bool) {
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
			artifact := Artifact{ThreadID: t.ID, ThreadTitle: t.Title, ProjectID: t.ProjectID, MessageID: m.ID, ToolCallID: b.ToolCallID, Title: args.Title, SourceType: args.Source.Type, CreatedAt: m.CreatedAt}
			if !visit(artifact, b.Arguments) {
				return
			}
		}
	}
}
