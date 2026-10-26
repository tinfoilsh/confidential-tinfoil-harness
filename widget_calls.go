package main

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
)

func (eng *engine) widgetResult(ctx context.Context, call toolCall) (any, error) {
	if call.name != "render_link_preview" {
		return "[rendered for the user]", nil
	}
	var args api.Object
	if json.Unmarshal([]byte(call.args), &args) != nil {
		return nil, api.Err(400, "BAD_REQUEST", "Invalid widget arguments")
	}
	return eng.attachments.LinkDetails(ctx, api.Str(args["url"]))
}

func (eng *engine) retryWidget(ctx context.Context, r *chatRun, req *request, m *inference.Model) error {
	var declaration domain.Widget
	for _, w := range domain.WidgetCatalog() {
		if w.Name == r.retryTarget.block.Name {
			declaration = w
			break
		}
	}
	conversation, err := budgetConversation(req, false, nil)
	if err != nil {
		return err
	}
	conversation = append(conversation, api.Raw(api.Object{"role": "user", "content": "Regenerate the arguments for the " + declaration.Name + " widget. Preserve the user's intent and return only JSON matching its schema."}))
	payload := api.Object{"model": req.model, "messages": conversation, "user_cache_secret": req.cacheScope, "response_format": api.Object{"type": "json_schema", "json_schema": api.Object{"name": "widget_arguments", "strict": true, "schema": declaration.Schema}}}
	for k, v := range req.params {
		payload[k] = v
	}
	usage := usageForRun(r)
	usage.BillCustomerRequest = true
	content, err := eng.models.Completion(auth.WithUsage(ctx, usage), m, payload)
	if err != nil {
		return err
	}
	content = domain.WidgetArguments(strings.TrimSpace(content))
	var value any
	if json.Unmarshal([]byte(content), &value) != nil || declaration.Resolved.Validate(value) != nil {
		return api.Err(502, "UPSTREAM_REFUSED", "The replacement widget arguments do not match the schema")
	}
	id := r.input.ToolCallID
	r.out.callStart(id, declaration.Name, r.retryTarget.parentID)
	r.out.callArgs(id, content)
	r.out.callEnd(id)
	// The widget's result is the one a call with these arguments returns.
	result, err := eng.widgetResult(ctx, toolCall{id: id, name: declaration.Name, args: content})
	if err != nil {
		result = api.Object{"error": api.AsError(err).Message}
	}
	r.replacement = domain.ReplacedWidget{Arguments: content, Result: api.Raw(result)}
	r.out.emit(domain.ToolCallResult{MessageID: "msg_" + api.Token(), ToolCallID: id, Role: "tool", Content: r.replacement.Result})
	return nil
}

// replacedWidget is what a retry gives its widget call.
