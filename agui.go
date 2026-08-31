package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const maxRequestBody = 8 << 20

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
	req.from = resumeFrom(r.Header.Get("Last-Event-ID"))

	// The key rides the context down to the transports that sign every request.
	ctx := context.WithValue(r.Context(), apiKeyKey{}, apiKey)
	if req.storageID != "" {
		live, err := h.lookup(req.storageID, req.secret)
		switch {
		case err != nil:
			writeError(w, http.StatusForbidden, errNotYours.Error())
			return
		case live != nil:
			follow(w, r, live, req.from)
			return
		case req.resume:
			h.cold(w, r.WithContext(ctx), req)
			return
		}
	}
	m := h.pick(req)
	if m == nil {
		writeError(w, http.StatusNotFound, h.unserved(req))
		return
	}
	rn, err := h.start(ctx, req, m)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	follow(w, r, rn, 0)
}

func (h *harness) drop(w http.ResponseWriter, r *http.Request) {
	apiKey := bearer(r.Header.Get("Authorization"))
	if apiKey == "" {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<12))
	var in runAgentInput
	if err != nil || json.Unmarshal(body, &in) != nil || !hexID(in.SessionID) || !hexID(string(in.RecoveryToken)) {
		writeError(w, http.StatusBadRequest, `"sessionId" and "recoveryToken" must be 32 hex characters`)
		return
	}
	ctx := context.WithValue(r.Context(), apiKeyKey{}, apiKey)
	// Authorized by opening the log; a live run is stopped so its spill cannot race the delete.
	switch live, err := h.lookup(in.SessionID, in.RecoveryToken); {
	case err != nil:
		writeError(w, http.StatusForbidden, errNotYours.Error())
		return
	case live != nil:
		live.abandon()
	default:
		if _, err := h.stored(ctx, in.SessionID, in.RecoveryToken); err != nil {
			writeError(w, http.StatusForbidden, errNotYours.Error())
			return
		}
	}
	if err := h.dispose(ctx, in.SessionID); err != nil {
		writeError(w, http.StatusBadGateway, "the store did not drop the log: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *harness) cold(w http.ResponseWriter, r *http.Request, req *request) {
	rn, err := h.stored(r.Context(), req.storageID, req.secret)
	if err != nil {
		writeError(w, http.StatusForbidden, errNotYours.Error())
		return
	}
	follow(w, r, rn, req.from)
}

type request struct {
	threadID   string
	runID      string
	model      string
	cacheScope string              // partitions the enclave's prompt cache per caller; a namespace, not a credential
	rendered   []json.RawMessage   // widgets the caller draws: advertised, never dialled
	families   map[string]mcp.Meta // tool enclaves this run dials, and what its calls carry
	piiCheck   bool
	storageID  string
	secret     secret
	resume     bool
	from       int
	prompt
}

type secret string

func (secret) LogValue() slog.Value { return slog.StringValue("REDACTED") }

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
	SessionID      string            `json:"sessionId"`
	RecoveryToken  secret            `json:"recoveryToken"`
	Resume         bool              `json:"resume"`
}

type inMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	Name       string          `json:"name"`
	ToolCalls  []toolCallJSON  `json:"toolCalls"`
	ToolCallID string          `json:"toolCallId"`
}

func parseRequest(body []byte, apiKey string) (*request, error) {
	var in runAgentInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	if len(in.Messages) == 0 {
		return nil, errors.New(`"messages" must be a non-empty array`)
	}
	if in.SessionID != "" && !(hexID(in.SessionID) && hexID(string(in.RecoveryToken))) {
		return nil, errors.New(`"sessionId" and "recoveryToken" must be 32 hex characters`)
	}
	if in.Resume && in.SessionID == "" {
		return nil, errors.New(`"resume" needs a "sessionId"`)
	}
	p, err := convert(in.Messages)
	if err != nil {
		return nil, err
	}
	var props struct {
		Model    string `json:"model"`
		PIICheck *bool  `json:"piiCheck"`
	}
	json.Unmarshal(in.ForwardedProps, &props)
	dialled := map[string]mcp.Meta{}
	for _, f := range families {
		on, meta, err := f.live(in.ForwardedProps)
		if err != nil {
			return nil, err
		}
		if !on {
			continue
		}
		// Counted here: the loop prepends this prompt after h.pick has sized the run.
		dialled[f.name], p.tokens = meta, p.tokens+len(f.prompt)/4
	}
	// After the families, since which are dialled is what a caller tool may not shadow.
	for _, tool := range in.Tools {
		if err := declaration(tool, dialled); err != nil {
			return nil, err
		}
	}
	runID := in.RunID
	if runID == "" {
		runID = "run_" + token()
	}
	return &request{threadID: in.ThreadID, runID: runID, model: props.Model,
		cacheScope: cacheScope(apiKey), rendered: in.Tools, families: dialled,
		piiCheck:  props.PIICheck != nil && *props.PIICheck,
		storageID: in.SessionID, secret: in.RecoveryToken, resume: in.Resume,
		prompt: p}, nil
}

func hexID(s string) bool {
	raw, err := hex.DecodeString(s)
	return err == nil && len(raw) == 16
}

func resumeFrom(header string) int {
	n, err := strconv.Atoi(header)
	if err != nil || n < 0 {
		return 0
	}
	return n + 1
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

// imageTokens is what one image costs; sizing its base64 as text fits nothing.
const imageTokens = 1500

// content renders one message for the gateway, charging it against the budget.
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

// Images cross as bytes: the model enclave would fetch a URL off the attested path.
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

func terminal(frame []byte) bool {
	var e struct {
		Type string `json:"type"`
	}
	json.Unmarshal(frame, &e)
	return e.Type == "RUN_FINISHED" || e.Type == "RUN_ERROR"
}

type stream struct {
	run      *run
	threadID string
	runID    string

	// mu serializes writes against the tool goroutines.
	mu    sync.Mutex
	spend usage
}

// A run has started the moment it is accepted, not when the first upstream
// turn yields: the caller gets a thinking state instead of an open connection
// saying nothing, and an enlisted run always has the frame that authorizes it.
func newStream(rn *run, threadID, runID string) *stream {
	s := &stream{run: rn, threadID: threadID, runID: runID}
	s.frame(event{Type: "RUN_STARTED", ThreadID: threadID, RunID: runID})
	return s
}

func (s *stream) emit(e event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frame(e)
}

func (s *stream) text(message, delta string) {
	s.emit(event{Type: "TEXT_MESSAGE_CHUNK", MessageID: message, Role: "assistant", Delta: delta})
}

func (s *stream) reasoning(message, delta string) {
	s.emit(event{Type: "REASONING_MESSAGE_CHUNK", MessageID: message + "-reasoning", Delta: delta})
}

func (s *stream) callStart(id, name, parent string) {
	s.emit(event{Type: "TOOL_CALL_START", ToolCallID: id, ToolName: name, ParentID: parent})
}

func (s *stream) callArgs(id, delta string) {
	s.emit(event{Type: "TOOL_CALL_ARGS", ToolCallID: id, Delta: delta})
}

func (s *stream) callEnd(id string) {
	s.emit(event{Type: "TOOL_CALL_END", ToolCallID: id})
}

func (s *stream) result(id, content string, meta json.RawMessage) {
	s.emit(event{Type: "TOOL_CALL_RESULT", MessageID: "msg_" + token(),
		ToolCallID: id, Role: "tool", Content: quote(content), Metadata: meta})
}

// activity opens one call's output under an id derived from the call itself.
func (s *stream) activity(call toolCall) {
	content, err := json.Marshal(map[string]any{
		"toolCallId": call.id, "tool": call.name, "progress": 0, "output": []string{}})
	if err != nil {
		return
	}
	s.emit(event{Type: "ACTIVITY_SNAPSHOT", MessageID: "act_" + call.id, Activity: activityTool, Content: content})
}

// output is a list because RFC 6902 cannot grow a string.
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
	s.frame(event{Type: "RUN_ERROR", ThreadID: s.threadID, RunID: s.runID, Message: err.Error()})
	s.run.log.close(errDone)
}

func (s *stream) done() {
	s.mu.Lock()
	defer s.mu.Unlock()
	metadata, _ := json.Marshal(map[string]usage{"usage": s.spend})
	s.frame(event{Type: "RUN_FINISHED", ThreadID: s.threadID, RunID: s.runID, Metadata: metadata})
	s.run.log.close(errDone)
}

func (s *stream) frame(e event) {
	encoded, err := json.Marshal(e)
	if err != nil {
		return
	}
	// Nothing can read a closed log, so stop the run rather than bill turns into it.
	if s.run.log.grow(func(id int) []byte { return s.run.seal(id, encoded) }) != nil {
		s.run.stop()
	}
}

func follow(w http.ResponseWriter, r *http.Request, rn *run, from int) {
	flush := http.NewResponseController(w)
	opened := false
	open := func() {
		if opened {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		opened = true
	}
	for {
		frames, wake, closed := rn.log.read(from)
		for i, frame := range frames {
			plain, err := rn.open(from+i, frame)
			if err != nil {
				return
			}
			open()
			fmt.Fprintf(w, "id: %d\ndata: %s\n\n", from+i, plain)
		}
		from += len(frames)
		if len(frames) > 0 {
			flush.Flush()
		}
		if closed != nil {
			open()
			if closed != errDone {
				encoded, _ := json.Marshal(event{Type: "RUN_ERROR", Message: closed.Error()})
				fmt.Fprintf(w, "id: %d\ndata: %s\n\n", from, encoded)
				flush.Flush()
			}
			return
		}
		select {
		case <-wake:
		case <-r.Context().Done():
			rn.begin()
			return
		}
	}
}
