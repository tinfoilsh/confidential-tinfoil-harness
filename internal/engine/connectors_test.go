package engine

import (
	"context"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A client answers an approval as soon as it reads it, so the approval
// takes an answer by the time it is published.
func TestAnApprovalTakesItsAnswerOnceItIsPublished(t *testing.T) {
	gate := &approvalGate{pending: map[string]chan bool{}, answered: map[string]bool{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := NewLog("00")
	defer log.Frames.Detach()
	out := &Stream{Run: log}
	published := false
	var refused error
	out.Observe = func(event domain.Event, _ string) {
		delta, ok := event.(domain.ActivityDelta)
		if !ok || delta.Display.Connector.Approval.Status != domain.ApprovalPending {
			return
		}
		published = true
		refused = gate.answer(delta.Display.Connector.Approval.ID, false)
		cancel()
	}
	app := domain.Connector{ID: "app", Name: "App", URL: "https://mcp.example/mcp", Enabled: true, Tools: []domain.ConnectorTool{{Name: "write", Permission: domain.ToolAsk}}}
	set := &Toolset{approvals: gate, currentApp: func(context.Context, string) (domain.Connector, error) { return app, nil }}
	callApp(ctx, out, set, &appTool{connector: &app, remote: "write"}, ToolCall{ID: "call", Name: "app__write", Args: "{}"})
	if !published {
		t.Fatal("the approval was never published")
	}
	if refused != nil {
		t.Fatalf("the approval refused an answer given as it was published: %v", refused)
	}
}
