package connectors

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"golang.org/x/oauth2"
)

const (
	// MaxTools bounds the tools a connector offers, and lists.
	MaxTools = 200
	// The longest title and description of a tool the settings show.
	maxToolTitle       = 200
	maxToolDescription = 1024
	// openTimeout bounds connecting to a server and listing its tools.
	openTimeout = 30 * time.Second
)

// Session is a connection to a connector's server, and the tools the server
// offers on it.
type Session struct {
	mcp    *mcp.ClientSession
	source *keptSource // nil for a server that asks for no sign-in
	Tools  []*mcp.Tool
}

// Progress reports a call's progress, under the token the call named.
type Progress func(token, message string, done float64)

// refusals notes whether the server refused a request's credentials.
type refusals struct {
	inner   http.RoundTripper
	refused atomic.Bool
}

func (t *refusals) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(r)
	if err == nil && resp.StatusCode == http.StatusUnauthorized {
		t.refused.Store(true)
	}
	return resp, err
}

// Open connects to the connector's server, signing in with its tokens, which
// it refreshes and keeps through creds, and lists the server's tools. A
// server that refuses a token that has not expired, as one its user revoked,
// is asked again with a new one; a connector whose token it cannot renew is
// signed out. The session lasts until it is closed or ctx ends.
func (c *Client) Open(ctx context.Context, conn *domain.Connector, creds Credentials, progress Progress) (*Session, error) {
	s := &Session{}
	client := c.http.direct
	seen := &refusals{inner: c.http.direct.Transport}
	if conn.Auth != nil {
		s.source = &keptSource{ctx: ctx, client: c.http.direct, config: c.config, auth: *conn.Auth, creds: creds}
		if _, err := s.source.Token(); err != nil {
			if errors.Is(err, ErrSignedOut) {
				s.source.signOut()
			}
			s.source.close()
			return nil, err
		}
		client = &http.Client{Transport: &oauth2.Transport{Source: s.source, Base: seen}, CheckRedirect: c.http.direct.CheckRedirect}
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
	connect := func() error {
		opened, err := mcp.NewClient(&mcp.Implementation{Name: clientName, Version: clientVersion}, options).Connect(opening,
			&mcp.StreamableClientTransport{Endpoint: conn.URL, HTTPClient: client, DisableStandaloneSSE: true}, nil)
		if err != nil {
			return err
		}
		s.mcp, s.Tools = opened, nil
		for tool, err := range opened.Tools(opening, nil) {
			if err != nil {
				opened.Close()
				return err
			}
			if len(s.Tools) == MaxTools {
				break
			}
			s.Tools = append(s.Tools, tool)
		}
		return nil
	}
	err := connect()
	if err != nil && s.source != nil && seen.refused.Load() {
		if err = s.source.renew(); err == nil {
			seen.refused.Store(false)
			err = connect()
		} else if errors.Is(err, ErrSignedOut) {
			s.source.signOut()
		}
	}
	if err != nil {
		s.source.close()
		if errors.Is(err, ErrSignedOut) {
			return nil, ErrSignedOut
		}
		return nil, errUnreachable()
	}
	return s, nil
}

// Close ends the session, once it has kept any tokens it refreshed.
func (s *Session) Close() {
	s.source.close()
	s.mcp.Close()
}

// Tools lists the tools the connector's server offers, as the settings show
// them.
func (c *Client) Tools(ctx context.Context, conn *domain.Connector, creds Credentials) ([]domain.ConnectorTool, error) {
	s, err := c.Open(ctx, conn, creds, nil)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	return s.Listed(), nil
}

// Listed are the tools the server offers on the session, as a connector
// lists them, each with its permission until the user chooses one.
func (s *Session) Listed() []domain.ConnectorTool {
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
	return tools
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
