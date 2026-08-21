package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This file is the tool enclave's half of the connection. MCP is the tool
// interface: nothing here re-describes what a tool is, and a tool that wants to
// run in this process should be an in-process MCP server, not a second kind of
// thing. Confidentiality is the caller's doing -- this speaks to the
// http.Client it was handed and nothing more.

// exposed maps the enclave's tool names to what the model is offered; a tool
// this map does not name is never advertised.
var exposed = map[string]string{"search": "web_search", "fetch": "web_fetch"}

// toolset is one request's tool surface: a session that holds the calls it will
// serve, defs as the model is told about them, and the enclave's own name for
// each tool the model may ask for.
type toolset struct {
	session *mcp.ClientSession
	defs    json.RawMessage
	remote  map[string]string // web_search -> search
}

// openTools gives one request its own session and asks the enclave what it
// serves rather than guessing.
func openTools(ctx context.Context, endpoint string, client *http.Client, progress func(token string, detail json.RawMessage)) (*toolset, error) {
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "confidential-tinfoil-harness", Version: "2"}, &mcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			token, _ := req.Params.ProgressToken.(string)
			if detail, err := json.Marshal(req.Params); err == nil {
				progress(token, detail)
			}
		},
	})
	session, err := mcpClient.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: client}, nil)
	if err != nil {
		return nil, err
	}
	found, err := session.ListTools(ctx, nil)
	if err != nil {
		session.Close()
		return nil, err
	}
	defs, remote, err := advertise(found.Tools)
	if err != nil {
		session.Close()
		return nil, err
	}
	return &toolset{session: session, defs: defs, remote: remote}, nil
}

// advertise renders the enclave's tools as the model is told about them, in the
// order the enclave listed them so that one caller's turns keep hitting the
// same prompt cache.
func advertise(found []*mcp.Tool) (json.RawMessage, map[string]string, error) {
	defs := []any{}
	remote := map[string]string{}
	for _, tool := range found {
		as, ok := exposed[tool.Name]
		if !ok {
			continue
		}
		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			return nil, nil, fmt.Errorf("tool %q published an unreadable schema: %w", tool.Name, err)
		}
		remote[as] = tool.Name
		defs = append(defs, map[string]any{"type": "function", "function": map[string]any{
			"name": as, "description": tool.Description, "parameters": schema}})
	}
	if len(defs) == 0 {
		return nil, nil, errors.New("enclave serves neither search nor fetch")
	}
	encoded, err := json.Marshal(defs)
	return encoded, remote, err
}

// call runs one advertised tool; token names it in the enclave's progress
// notifications. It returns what the model reads, plus Meta for the caller.
func (t *toolset) call(ctx context.Context, name, token string, args json.RawMessage) (string, json.RawMessage, error) {
	remote, ok := t.remote[name]
	if !ok {
		return "", nil, fmt.Errorf("no such tool %q", name)
	}
	arguments := map[string]any{}
	if len(bytes.TrimSpace(args)) > 0 {
		if err := json.Unmarshal(args, &arguments); err != nil {
			return "", nil, fmt.Errorf("arguments are not a JSON object: %w", err)
		}
	}
	params := &mcp.CallToolParams{Name: remote, Arguments: arguments}
	params.SetProgressToken(token)
	result, err := t.session.CallTool(ctx, params)
	if err != nil {
		return "", nil, err
	}
	out := output(result)
	if result.IsError {
		if out == "" {
			out = "tool call failed"
		}
		return "", nil, errors.New(out)
	}
	var meta json.RawMessage
	if len(result.Meta) > 0 {
		meta, _ = json.Marshal(result.Meta)
	}
	return out, meta, nil
}

func (t *toolset) close() { t.session.Close() }

// output renders a result as the model reads it: structured if any, else text.
func output(result *mcp.CallToolResult) string {
	if result.StructuredContent != nil {
		if encoded, err := json.Marshal(result.StructuredContent); err == nil {
			return string(encoded)
		}
	}
	var parts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			parts = append(parts, text.Text)
		}
	}
	return strings.Join(parts, "\n")
}
