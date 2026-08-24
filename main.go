// Command confidential-tinfoil-harness serves a streaming AG-UI endpoint that
// seals every model turn and tool call to enclaves it attests before it starts
// serving. It holds no state between runs and no credentials.
//
// The file reads as one request's lifecycle: startup and attestation, the
// handler and request parsing, the tool loop, one gateway turn, the MCP
// toolset, and the event stream back to the caller.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	tinfoil "github.com/tinfoilsh/tinfoil-go"
)

const (
	maxRequestBody = 8 << 20
	maxTurns       = 8
	maxToolOutput  = 30000
	maxSSEEvent    = 1 << 20
)

// --- Startup and attestation ---

// repo is the trust anchor -- the gateway may reroute, but a host that does not
// measure up is never sealed to. enclave is only where attestation starts.
type model struct {
	name    string
	repo    string
	enclave string
	vision  bool
	context int // usable prompt budget, in tokens

	client *http.Client // the sealing client, once the enclave has measured up
}

var catalog = []*model{
	{name: "kimi-k3", repo: "tinfoilsh/confidential-kimi-k3", enclave: "kimi-k3.inf6.tinfoil.sh", context: 256_000},
	{name: "deepseek-v4-flash", repo: "tinfoilsh/confidential-deepseek-v4-flash", enclave: "deepseek-v4-flash.inf6.tinfoil.sh", context: 128_000},
	{name: "gemma4-31b", repo: "tinfoilsh/confidential-gemma4-31b", enclave: "gemma4-31b.inf6.tinfoil.sh", context: 128_000, vision: true},
	{name: "gpt-oss-120b", repo: "tinfoilsh/confidential-gpt-oss-120b", enclave: "gpt-oss-120b.inf6.tinfoil.sh", context: 128_000},
	{name: "llama3-3-70b", repo: "tinfoilsh/confidential-llama3-3-70b", enclave: "llama3-3-70b.inf6.tinfoil.sh", context: 128_000},
}

const (
	toolsRepo           = "tinfoilsh/confidential-websearch"
	defaultToolsEnclave = "websearch.inf6.tinfoil.sh"
)

type harness struct {
	gateway       string
	models        []*model
	toolsEndpoint string
	toolsClient   *http.Client
}

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	gateway := flag.String("gateway", env("TINFOIL_GATEWAY_URL", "https://gateway.tinfoil.sh"), "tinfoil-gateway base URL")
	toolsHost := flag.String("tools-enclave", env("TINFOIL_TOOLS_ENCLAVE", defaultToolsEnclave), "host of the enclave serving web_search and web_fetch")
	flag.Parse()

	gw := strings.TrimRight(*gateway, "/")
	toolsClient, err := attest(*toolsHost, toolsRepo, "")
	if err != nil {
		slog.Error("verify tools enclave", "enclave", *toolsHost, "error", err)
		os.Exit(1)
	}
	models, err := attestCatalog(gw)
	if err != nil {
		slog.Error("verify model enclaves", "gateway", gw, "error", err)
		os.Exit(1)
	}
	h := &harness{gateway: gw, models: models, toolsEndpoint: "https://" + *toolsHost + "/mcp", toolsClient: toolsClient}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("POST /agui", h.agui)

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	slog.Info("listening", "addr", *addr, "gateway", gw, "tools", *toolsHost, "models", served(models))
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func attest(enclave, repo, baseURL string) (*http.Client, error) {
	opts := []tinfoil.ClientOption{tinfoil.WithEnclave(enclave), tinfoil.WithRepo(repo)}
	if baseURL != "" { // the tools enclave is dialled directly, not via the gateway
		opts = append(opts, tinfoil.WithBaseURL(baseURL))
	}
	verified, err := tinfoil.NewClientWithOptions(opts...)
	if err != nil {
		return nil, err
	}
	client := verified.HTTPClient()
	client.Transport = &callerAuth{inner: client.Transport}
	return client, nil
}

func attestCatalog(gateway string) ([]*model, error) {
	var wg sync.WaitGroup
	for _, m := range catalog {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := attest(m.enclave, m.repo, gateway+"/v1/")
			if err != nil {
				slog.Warn("model enclave did not verify", "model", m.name, "enclave", m.enclave, "error", err)
				return
			}
			m.client = client
		}()
	}
	wg.Wait()

	var live []*model
	for _, m := range catalog {
		if m.client != nil {
			live = append(live, m)
		}
	}
	if len(live) == 0 {
		return nil, errors.New("no pinned model enclave verified")
	}
	return live, nil
}

func served(models []*model) string {
	names := make([]string, len(models))
	for i, m := range models {
		names[i] = m.name
	}
	return strings.Join(names, ", ")
}

type apiKeyKey struct{}

// callerAuth signs every attested request -- model turn or tool call -- with
// the caller's key, carried down on the context. Everything is metered against
// the caller: the harness has no key of its own.
type callerAuth struct{ inner http.RoundTripper }

func (t *callerAuth) RoundTrip(r *http.Request) (*http.Response, error) {
	if key, ok := r.Context().Value(apiKeyKey{}).(string); ok && key != "" {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer "+key)
	}
	return t.inner.RoundTrip(r)
}

// --- Handler and request parsing ---

func (h *harness) agui(w http.ResponseWriter, r *http.Request) {
	apiKey := bearer(r.Header.Get("Authorization"))
	if apiKey == "" {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read request body")
		return
	}
	req, err := parseRequest(body, apiKey)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	m := h.pick(req)
	if m == nil {
		writeError(w, http.StatusNotFound, h.unserved(req))
		return
	}
	out := newStream(w, req.threadID, req.runID)
	defer out.done()

	// The key rides the context down to the transports that sign every model
	// turn and tool call.
	ctx := context.WithValue(r.Context(), apiKeyKey{}, apiKey)
	start := time.Now()
	if err := h.run(ctx, out, req, m); err != nil {
		slog.Error("run", "model", m.name, "error", err)
		out.fail(err)
		return
	}
	slog.Info("served", "model", m.name, "elapsed", time.Since(start).Round(time.Millisecond))
}

type request struct {
	threadID   string
	runID      string
	model      string
	cacheScope string // partitions the enclave's prompt cache per caller; a namespace, not a credential
	prompt
}

// prompt is the converted conversation and what it costs a model to read.
type prompt struct {
	messages []json.RawMessage
	images   bool
	tokens   int
}

type runAgentInput struct {
	ThreadID       string            `json:"threadId"`
	RunID          string            `json:"runId"`
	Messages       []inMessage       `json:"messages"`
	Tools          []json.RawMessage `json:"tools"`
	ForwardedProps json.RawMessage   `json:"forwardedProps"`
}

type inMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	Name       string          `json:"name"`
	ToolCalls  []toolCallJSON  `json:"toolCalls"`
	ToolCallID string          `json:"toolCallId"`
}

type toolCallJSON struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type outMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content,omitempty"`
	Name       string          `json:"name,omitempty"`
	ToolCalls  []toolCallJSON  `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}

func parseRequest(body []byte, apiKey string) (*request, error) {
	var in runAgentInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	if len(in.Messages) == 0 {
		return nil, errors.New(`"messages" must be a non-empty array`)
	}
	// The loop executes only the tools it can attest.
	if len(in.Tools) > 0 {
		return nil, errors.New("this agent runs its own tools (web_search, web_fetch) and cannot execute caller-supplied ones")
	}
	p, err := convert(in.Messages)
	if err != nil {
		return nil, err
	}
	var props struct {
		Model string `json:"model"`
	}
	json.Unmarshal(in.ForwardedProps, &props)
	runID := in.RunID
	if runID == "" {
		runID = "run_" + token()
	}
	return &request{threadID: in.ThreadID, runID: runID, model: props.Model,
		cacheScope: cacheScope(apiKey), prompt: p}, nil
}

func cacheScope(apiKey string) string {
	sum := sha256.Sum256([]byte("confidential-tinfoil-harness cache scope\x00" + apiKey))
	return hex.EncodeToString(sum[:])
}

func convert(messages []inMessage) (prompt, error) {
	var p prompt
	for _, msg := range messages {
		// Activity never travels back to the agent, and reasoning is the client's.
		if msg.Role == "activity" || msg.Role == "reasoning" {
			continue
		}
		content, err := p.content(msg.Content)
		if err != nil {
			return prompt{}, err
		}
		for i := range msg.ToolCalls {
			msg.ToolCalls[i].Type = "function"
			p.tokens += len(msg.ToolCalls[i].Function.Arguments) / 4
		}
		encoded, err := json.Marshal(outMessage{Role: msg.Role, Content: content,
			Name: msg.Name, ToolCalls: msg.ToolCalls, ToolCallID: msg.ToolCallID})
		if err != nil {
			return prompt{}, err
		}
		p.messages = append(p.messages, encoded)
	}
	if len(p.messages) == 0 {
		return prompt{}, errors.New("no messages the model can read")
	}
	return p, nil
}

// imageTokens is what one image is charged against a model's budget; its
// base64 is not text and sizing it as text fits nothing.
const imageTokens = 1500

// content renders one message's content for the gateway, charging its text and
// images against the prompt's budget.
func (p *prompt) content(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var plain string
	if json.Unmarshal(raw, &plain) == nil {
		p.tokens += len(plain) / 4
		return raw, nil
	}
	var parts []inPart
	if json.Unmarshal(raw, &parts) != nil {
		return nil, errors.New(`"content" must be a string or an array of content parts`)
	}
	rendered := make([]any, 0, len(parts))
	for _, part := range parts {
		switch part.Type {
		case "text":
			p.tokens += len(part.Text) / 4
			rendered = append(rendered, map[string]string{"type": "text", "text": part.Text})
		case "image":
			url, err := inlined(part.Source)
			if err != nil {
				return nil, err
			}
			p.images, p.tokens = true, p.tokens+imageTokens
			rendered = append(rendered, map[string]any{"type": "image_url",
				"image_url": map[string]string{"url": url}})
		default:
			return nil, fmt.Errorf("content part %q is not served: this agent reads text and images", part.Type)
		}
	}
	return json.Marshal(rendered)
}

type inPart struct {
	Type   string   `json:"type"`
	Text   string   `json:"text"`
	Source inSource `json:"source"`
}

type inSource struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	MimeType string `json:"mimeType"`
}

// inlined is why images cross as bytes: a remote URL would be dereferenced by
// the model enclave, off the attested path and outside its egress allowlist.
func inlined(source inSource) (string, error) {
	switch {
	case source.Type == "data" && source.MimeType != "":
		return "data:" + source.MimeType + ";base64," + source.Value, nil
	case source.Type == "url" && strings.HasPrefix(source.Value, "data:"):
		return source.Value, nil
	case source.Type == "url":
		return "", errors.New("image sources must be inline: a remote URL would be fetched outside the attested path")
	}
	return "", errors.New(`image source must be {"type":"data","value":...,"mimeType":...}`)
}

func (h *harness) pick(req *request) *model {
	named := req.model != "" && req.model != "auto"
	for _, m := range h.models {
		if named {
			if m.name == req.model {
				return m
			}
			continue
		}
		if (req.images && !m.vision) || req.tokens > m.context {
			continue
		}
		return m
	}
	return nil
}

func (h *harness) unserved(req *request) string {
	if req.model != "" && req.model != "auto" {
		return fmt.Sprintf("unknown model %q: this agent serves %s and \"auto\"", req.model, served(h.models))
	}
	return fmt.Sprintf("no pinned model fits this run (about %d tokens, images: %t)", req.tokens, req.images)
}

func bearer(header string) string {
	const scheme = "bearer "
	if len(header) < len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return ""
	}
	return strings.TrimSpace(header[len(scheme):])
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"message": message})
}

// --- The tool loop ---

func (h *harness) run(ctx context.Context, out *stream, req *request, m *model) error {
	set, err := openTools(ctx, h.toolsEndpoint, h.toolsClient, out.progress)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	defer set.close()

	conversation := slices.Clone(req.messages)
	for turn := 1; ; turn++ {
		payload, err := body(req, m, conversation, set.defs)
		if err != nil {
			return err
		}
		calls, answer, err := h.turn(ctx, out, m, payload)
		if err != nil {
			return err
		}
		if len(calls) == 0 {
			return nil
		}
		if turn == maxTurns {
			return fmt.Errorf("model called tools for %d turns without answering", maxTurns)
		}
		conversation = append(conversation, requested(answer, calls))
		conversation = append(conversation, execute(ctx, out, set, calls)...)
	}
}

func body(req *request, m *model, conversation []json.RawMessage, defs json.RawMessage) ([]byte, error) {
	return json.Marshal(map[string]any{
		"model":             m.name,
		"stream":            true,
		"messages":          conversation,
		"tools":             defs,
		"user_cache_secret": req.cacheScope,
	})
}

func execute(ctx context.Context, out *stream, set *toolset, calls []toolCall) []json.RawMessage {
	messages := make([]json.RawMessage, len(calls))
	var wg sync.WaitGroup
	for i, call := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			messages[i] = invoke(ctx, out, set, call)
		}()
	}
	wg.Wait()
	return messages
}

func invoke(ctx context.Context, out *stream, set *toolset, call toolCall) json.RawMessage {
	out.activity(call)
	content, meta, err := set.call(ctx, call.name, call.id, json.RawMessage(call.args))
	if err != nil {
		// The model reads the failure and can retry rather than the turn dying.
		failure, _ := json.Marshal(map[string]string{"error": err.Error()})
		content, meta = string(failure), nil
	}
	content = clip(content)
	out.emit(event{Type: "TOOL_CALL_RESULT", MessageID: "msg_" + token(),
		ToolCallID: call.id, Role: "tool", Content: quote(content), Metadata: meta})
	return answered(call.id, content)
}

func clip(out string) string {
	if len(out) > maxToolOutput {
		return out[:maxToolOutput] + "\n[truncated]"
	}
	if out == "" {
		return "[no output]"
	}
	return out
}

// --- One gateway turn ---

func (h *harness) turn(ctx context.Context, out *stream, m *model, payload []byte) ([]toolCall, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.gateway+"/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("gateway: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, "", &refusal{status: resp.StatusCode, detail: string(bytes.TrimSpace(detail))}
	}

	var calls []toolCall
	var answer strings.Builder
	message := "msg_" + token()
	events := bufio.NewScanner(resp.Body)
	events.Buffer(nil, maxSSEEvent)
	for events.Scan() {
		data, ok := strings.CutPrefix(events.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "" || data == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   string          `json:"content"`
					Reasoning string          `json:"reasoning_content"`
					ToolCalls []toolCallDelta `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
			Usage json.RawMessage `json:"usage"`
		}
		if json.Unmarshal([]byte(data), &chunk) != nil {
			continue
		}
		if len(chunk.Usage) > 0 {
			out.meter(chunk.Usage)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		delta := chunk.Choices[0].Delta
		if delta.Content != "" {
			answer.WriteString(delta.Content)
			out.emit(event{Type: "TEXT_MESSAGE_CHUNK", MessageID: message, Role: "assistant", Delta: delta.Content})
		}
		if delta.Reasoning != "" {
			out.emit(event{Type: "REASONING_MESSAGE_CHUNK", MessageID: message + "-reasoning", Delta: delta.Reasoning})
		}
		for _, called := range delta.ToolCalls {
			calls = announce(out, parent(message, &answer), calls, called)
		}
	}
	for i := range calls {
		if !calls[i].open {
			out.emit(event{Type: "TOOL_CALL_START", ToolCallID: calls[i].id,
				ToolName: calls[i].name, ParentID: parent(message, &answer)})
		}
		// The enclave reads absent arguments as none; the caller should too.
		if calls[i].args == "" {
			out.emit(event{Type: "TOOL_CALL_ARGS", ToolCallID: calls[i].id, Delta: "{}"})
		}
		out.emit(event{Type: "TOOL_CALL_END", ToolCallID: calls[i].id})
	}
	return calls, answer.String(), events.Err()
}

type toolCall struct {
	id, name, args string
	open           bool
}

type toolCallDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// parent names the message a tool call belongs to, and only once the caller
// has seen one: a turn that opens with a call has no message to hang it on.
func parent(message string, answer *strings.Builder) string {
	if answer.Len() == 0 {
		return ""
	}
	return message
}

// announce accumulates one tool-call delta and streams it on. Calls are
// addressed by id rather than by position, so parallel ones may interleave. A
// call the model left unnamed still gets an id, so its result correlates.
func announce(out *stream, message string, calls []toolCall, delta toolCallDelta) []toolCall {
	for len(calls) <= delta.Index {
		calls = append(calls, toolCall{id: "call_" + token()})
	}
	call := &calls[delta.Index]
	if delta.ID != "" {
		call.id = delta.ID
	}
	call.name += delta.Function.Name
	call.args += delta.Function.Arguments
	if !call.open && call.name != "" {
		call.open = true
		out.emit(event{Type: "TOOL_CALL_START", ToolCallID: call.id, ToolName: call.name, ParentID: message})
	}
	if delta.Function.Arguments != "" {
		out.emit(event{Type: "TOOL_CALL_ARGS", ToolCallID: call.id, Delta: delta.Function.Arguments})
	}
	return calls
}

func requested(answer string, calls []toolCall) json.RawMessage {
	made := make([]toolCallJSON, len(calls))
	for i, call := range calls {
		made[i].ID, made[i].Type = call.id, "function"
		made[i].Function.Name, made[i].Function.Arguments = call.name, call.args
	}
	message, _ := json.Marshal(outMessage{Role: "assistant", Content: quote(answer), ToolCalls: made})
	return message
}

func answered(id, content string) json.RawMessage {
	message, _ := json.Marshal(outMessage{Role: "tool", ToolCallID: id, Content: quote(content)})
	return message
}

type refusal struct {
	status int
	detail string
}

func (r *refusal) Error() string { return fmt.Sprintf("gateway returned %d: %s", r.status, r.detail) }

// --- The MCP toolset ---

// exposed maps the enclave's tool names to what the model is offered; a tool
// this map does not name is never advertised.
var exposed = map[string]string{"search": "web_search", "fetch": "web_fetch"}

type toolset struct {
	session *mcp.ClientSession
	defs    json.RawMessage
}

func openTools(ctx context.Context, endpoint string, client *http.Client, progress func(string, string, float64)) (*toolset, error) {
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "confidential-tinfoil-harness", Version: "2"}, &mcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			call, _ := req.Params.ProgressToken.(string)
			progress(call, req.Params.Message, req.Params.Progress)
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
	defs, err := advertise(found.Tools)
	if err != nil {
		session.Close()
		return nil, err
	}
	return &toolset{session: session, defs: defs}, nil
}

// Order follows the enclave's, so one caller's turns keep hitting the same
// prompt cache.
func advertise(found []*mcp.Tool) (json.RawMessage, error) {
	defs := []any{}
	for _, tool := range found {
		as, ok := exposed[tool.Name]
		if !ok {
			continue
		}
		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("tool %q published an unreadable schema: %w", tool.Name, err)
		}
		defs = append(defs, map[string]any{"type": "function", "function": map[string]any{
			"name": as, "description": tool.Description, "parameters": schema}})
	}
	if len(defs) == 0 {
		return nil, errors.New("enclave serves neither search nor fetch")
	}
	return json.Marshal(defs)
}

func (t *toolset) call(ctx context.Context, name, token string, args json.RawMessage) (string, json.RawMessage, error) {
	var remote string
	for enclaves, as := range exposed {
		if as == name {
			remote = enclaves
		}
	}
	if remote == "" {
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

// --- The event stream back to the caller ---

type event struct {
	Type       string          `json:"type"`
	ThreadID   string          `json:"threadId,omitempty"`
	RunID      string          `json:"runId,omitempty"`
	MessageID  string          `json:"messageId,omitempty"`
	ParentID   string          `json:"parentMessageId,omitempty"`
	ToolCallID string          `json:"toolCallId,omitempty"`
	ToolName   string          `json:"toolCallName,omitempty"`
	Role       string          `json:"role,omitempty"`
	Delta      string          `json:"delta,omitempty"`
	Content    json.RawMessage `json:"content,omitempty"`
	Activity   string          `json:"activityType,omitempty"`
	Patch      json.RawMessage `json:"patch,omitempty"`
	Message    string          `json:"message,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

type usage struct {
	Prompt     int64 `json:"prompt_tokens"`
	Completion int64 `json:"completion_tokens"`
	Total      int64 `json:"total_tokens"`
}

const (
	streamIdle = iota // no event yet; a failure still owns the HTTP status
	streamOpen        // RUN_STARTED sent
	streamShut        // RUN_FINISHED or RUN_ERROR sent; late events are dropped
)

type stream struct {
	w        http.ResponseWriter
	flush    *http.ResponseController
	threadID string
	runID    string

	// mu serializes writes against the tool goroutines.
	mu    sync.Mutex
	spend usage
	state int
}

func newStream(w http.ResponseWriter, threadID, runID string) *stream {
	return &stream{w: w, flush: http.NewResponseController(w), threadID: threadID, runID: runID}
}

func (s *stream) emit(e event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.send(e)
}

// activity opens one tool call's output, which progress then grows. Its id is
// derived from the call, so a notification needs no state to find it.
func (s *stream) activity(call toolCall) {
	content, err := json.Marshal(map[string]any{
		"toolCallId": call.id, "tool": call.name, "progress": 0, "output": []string{}})
	if err != nil {
		return
	}
	s.emit(event{Type: "ACTIVITY_SNAPSHOT", MessageID: "act_" + call.id, Activity: activityTool, Content: content})
}

// progress appends what a tool has emitted so far. RFC 6902 cannot grow a
// string, so output is a list and each note is one more entry.
func (s *stream) progress(call, note string, done float64) {
	ops := []map[string]any{{"op": "replace", "path": "/progress", "value": done}}
	if note != "" {
		ops = append(ops, map[string]any{"op": "add", "path": "/output/-", "value": note})
	}
	patch, err := json.Marshal(ops)
	if err != nil {
		return
	}
	s.emit(event{Type: "ACTIVITY_DELTA", MessageID: "act_" + call, Activity: activityTool, Patch: patch})
}

const activityTool = "TOOL"

func (s *stream) meter(raw json.RawMessage) {
	var turn usage
	if json.Unmarshal(raw, &turn) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spend.Prompt += turn.Prompt
	s.spend.Completion += turn.Completion
	s.spend.Total += turn.Total
}

func (s *stream) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case streamShut:
	case streamOpen:
		s.send(event{Type: "RUN_ERROR", ThreadID: s.threadID, RunID: s.runID, Message: err.Error()})
		s.state = streamShut
	default: // a failure before the run opens reaches the caller as itself
		status, detail := http.StatusBadGateway, err.Error()
		var refused *refusal
		if errors.As(err, &refused) {
			status, detail = refused.status, refused.detail
		}
		s.state = streamShut
		writeError(s.w, status, detail)
	}
}

func (s *stream) done() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == streamShut {
		return
	}
	metadata, _ := json.Marshal(map[string]usage{"usage": s.spend})
	s.send(event{Type: "RUN_FINISHED", ThreadID: s.threadID, RunID: s.runID, Metadata: metadata})
	s.state = streamShut
}

func (s *stream) send(e event) {
	if s.state == streamShut {
		return
	}
	if s.state == streamIdle {
		s.state = streamOpen
		s.w.Header().Set("Content-Type", "text/event-stream")
		s.w.Header().Set("Cache-Control", "no-store")
		s.w.Header().Set("X-Accel-Buffering", "no")
		s.frame(event{Type: "RUN_STARTED", ThreadID: s.threadID, RunID: s.runID})
	}
	s.frame(e)
}

func (s *stream) frame(e event) {
	encoded, err := json.Marshal(e)
	if err != nil {
		return
	}
	fmt.Fprintf(s.w, "data: %s\n\n", encoded)
	s.flush.Flush()
}

func quote(s string) json.RawMessage {
	if s == "" {
		return nil
	}
	encoded, _ := json.Marshal(s)
	return encoded
}

func token() string {
	var raw [16]byte
	rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}
