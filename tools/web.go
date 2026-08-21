package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// exposed maps the enclave's tool names to what the model is offered; a tool
// this map does not name is never advertised.
var exposed = map[string]string{"search": "web_search", "fetch": "web_fetch"}

// Web is a Source reached over MCP. Confidentiality is the caller's doing: it
// calls the http.Client it was handed and nothing more.
type Web struct {
	endpoint string
	client   *http.Client

	// The listing describes the enclave, not a caller, so it is cached across
	// requests; only Call, which needs a request's connection, is left unset.
	mu     sync.Mutex
	listed []listing
}

// listing is one enclave tool: what the model is offered, plus the name the
// enclave knows it by.
type listing struct {
	Tool
	remote string
}

func NewWeb(endpoint string, client *http.Client) *Web {
	return &Web{endpoint: endpoint, client: client}
}

// Open gives one request its own connection, opened lazily and closed by release.
func (w *Web) Open(ctx context.Context, progress Progress) ([]Tool, func(), error) {
	c := &conn{web: w, progress: progress}
	listed, err := w.describe(ctx, c)
	if err != nil {
		c.close()
		return nil, nil, err
	}
	tools := make([]Tool, len(listed))
	for i, tool := range listed {
		tools[i] = tool.Tool
		tools[i].Call = func(ctx context.Context, token string, args json.RawMessage) (Result, error) {
			return c.call(ctx, tool.remote, token, args)
		}
	}
	return tools, c.close, nil
}

// describe asks the enclave what it serves rather than guessing.
func (w *Web) describe(ctx context.Context, c *conn) ([]listing, error) {
	w.mu.Lock()
	cached := w.listed
	w.mu.Unlock()
	if cached != nil {
		return cached, nil
	}

	session, err := c.open(ctx)
	if err != nil {
		return nil, err
	}
	found, err := session.ListTools(ctx, nil)
	if err != nil {
		return nil, err
	}
	var serves []listing
	for _, tool := range found.Tools {
		as, ok := exposed[tool.Name]
		if !ok {
			continue
		}
		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("tool %q published an unreadable schema: %w", tool.Name, err)
		}
		serves = append(serves, listing{Tool{Name: as, Description: tool.Description, Schema: schema}, tool.Name})
	}
	if len(serves) == 0 {
		return nil, errors.New("enclave serves neither search nor fetch")
	}

	w.mu.Lock()
	w.listed = serves
	w.mu.Unlock()
	return serves, nil
}

// conn is one request's connection, shared by every tool this source gave it.
type conn struct {
	web      *Web
	progress Progress
	mu       sync.Mutex
	session  *mcp.ClientSession
}

func (c *conn) open(ctx context.Context) (*mcp.ClientSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		return c.session, nil
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "confidential-tinfoil-harness", Version: "2"}, &mcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			token, _ := req.Params.ProgressToken.(string)
			if detail, err := json.Marshal(req.Params); err == nil {
				c.progress(token, detail)
			}
		},
	})
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: c.web.endpoint, HTTPClient: c.web.client}, nil)
	if err != nil {
		return nil, err
	}
	c.session = session
	return session, nil
}

func (c *conn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		c.session.Close()
		c.session = nil
	}
}

func (c *conn) call(ctx context.Context, name, token string, args json.RawMessage) (Result, error) {
	arguments := map[string]any{}
	if len(bytes.TrimSpace(args)) > 0 {
		if err := json.Unmarshal(args, &arguments); err != nil {
			return Result{}, fmt.Errorf("arguments are not a JSON object: %w", err)
		}
	}
	session, err := c.open(ctx)
	if err != nil {
		return Result{}, err
	}
	params := &mcp.CallToolParams{Name: name, Arguments: arguments}
	params.SetProgressToken(token)
	result, err := session.CallTool(ctx, params)
	if err != nil {
		return Result{}, err
	}
	out := output(result)
	if result.IsError {
		if out == "" {
			out = "tool call failed"
		}
		return Result{}, errors.New(out)
	}
	served := Result{Output: out}
	if len(result.Meta) > 0 {
		served.Meta, _ = json.Marshal(result.Meta)
	}
	return served, nil
}

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
