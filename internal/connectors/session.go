package connectors

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"golang.org/x/oauth2"
)

const (
	// maxTools bounds the tools a connector offers.
	maxTools = 200
	// The longest title and description of a tool the settings show.
	maxToolTitle       = 200
	maxToolDescription = 1024
	// openTimeout bounds connecting to a server and listing its tools.
	openTimeout = 30 * time.Second
)

// Session is a connection to a connector's server, and the tools the server
// offers on it.
type Session struct {
	mcp   *mcp.ClientSession
	Tools []*mcp.Tool
}

// Progress reports a call's progress, under the token the call named.
type Progress func(token, message string, done float64)

// Open connects to the connector's server, signing in with its tokens and
// refreshing them through save, and lists the server's tools. The session
// lasts until it is closed or ctx ends.
func (c *Client) Open(ctx context.Context, conn *domain.Connector, save Saver, progress Progress) (*Session, error) {
	client := c.http.direct
	if conn.Auth != nil {
		source := &keptSource{ctx: ctx, client: c.http.direct, config: c.config(conn.Auth), auth: *conn.Auth, save: save}
		if _, err := source.Token(); err != nil {
			return nil, err
		}
		client = &http.Client{Transport: &oauth2.Transport{Source: source, Base: c.http.direct.Transport}, CheckRedirect: c.http.direct.CheckRedirect}
	}
	options := &mcp.ClientOptions{}
	if progress != nil {
		options.ProgressNotificationHandler = func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			token, _ := req.Params.ProgressToken.(string)
			progress(token, req.Params.Message, req.Params.Progress)
		}
	}
	opening, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	opened, err := mcp.NewClient(&mcp.Implementation{Name: clientName, Version: clientVersion}, options).Connect(opening,
		&mcp.StreamableClientTransport{Endpoint: conn.URL, HTTPClient: client, DisableStandaloneSSE: true}, nil)
	if err != nil {
		if errors.Is(err, ErrSignedOut) {
			return nil, ErrSignedOut
		}
		return nil, errUnreachable()
	}
	s := &Session{mcp: opened}
	for tool, err := range opened.Tools(opening, nil) {
		if err != nil {
			opened.Close()
			return nil, errUnreachable()
		}
		if len(s.Tools) == maxTools {
			break
		}
		s.Tools = append(s.Tools, tool)
	}
	return s, nil
}

func (s *Session) Close() { s.mcp.Close() }

// Tools lists the tools the connector's server offers, as the settings show
// them.
func (c *Client) Tools(ctx context.Context, conn *domain.Connector, save Saver) ([]domain.ConnectorTool, error) {
	s, err := c.Open(ctx, conn, save, nil)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	tools := make([]domain.ConnectorTool, 0, len(s.Tools))
	for _, t := range s.Tools {
		readOnly := ReadOnly(t)
		tools = append(tools, domain.ConnectorTool{
			Name:        t.Name,
			Title:       clipRunes(Title(t), maxToolTitle),
			Description: clipRunes(t.Description, maxToolDescription),
			ReadOnly:    readOnly,
			Permission:  domain.DefaultPermission(readOnly),
		})
	}
	return tools, nil
}

// ReadOnly is whether the server says the tool changes nothing.
func ReadOnly(t *mcp.Tool) bool { return t.Annotations != nil && t.Annotations.ReadOnlyHint }

// Title is the tool's name for people.
func Title(t *mcp.Tool) string {
	title := t.Title
	if title == "" && t.Annotations != nil {
		title = t.Annotations.Title
	}
	return title
}

// Call calls a tool, reporting progress under the progress token, and
// returns its output as text. A call the tool reports failed returns its
// output as the error.
func (s *Session) Call(ctx context.Context, name string, arguments map[string]any, progress string) (string, error) {
	params := &mcp.CallToolParams{Name: name, Arguments: arguments}
	if progress != "" {
		params.SetProgressToken(progress)
	}
	result, err := s.mcp.CallTool(ctx, params)
	if err != nil {
		return "", err
	}
	out := output(result)
	if result.IsError {
		return "", errors.New(cmp.Or(out, "the tool call failed"))
	}
	return out, nil
}

// output is what a call returned, as the model reads it: its text, what its
// resources say, and its structured content when it has no text.
func output(result *mcp.CallToolResult) string {
	var parts []string
	for _, content := range result.Content {
		switch c := content.(type) {
		case *mcp.TextContent:
			parts = append(parts, c.Text)
		case *mcp.ResourceLink:
			parts = append(parts, "["+cmp.Or(c.Title, c.Name, c.URI)+"]("+c.URI+")")
		case *mcp.EmbeddedResource:
			if c.Resource != nil && c.Resource.Text != "" {
				parts = append(parts, c.Resource.Text)
			}
		case *mcp.ImageContent:
			parts = append(parts, "[image]")
		case *mcp.AudioContent:
			parts = append(parts, "[audio]")
		}
	}
	if len(parts) == 0 && result.StructuredContent != nil {
		if encoded, err := json.Marshal(result.StructuredContent); err == nil {
			return string(encoded)
		}
	}
	return strings.Join(parts, "\n")
}

func clipRunes(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit])
}
