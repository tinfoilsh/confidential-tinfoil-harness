package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// Completed first-party chat conversations are submitted to the safeguards
// sidecar for acceptable-use classification, as the model router submits the
// chats it serves. Ported from the router's safeguards package.

const (
	safeguardsTimeout    = 5 * time.Second
	maxSubmissionBytes   = 1 << 20
	messageJSONOverhead  = len(`{"role":"","content":""},`)
	maxConversationIDLen = 128
)

type safeguardMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type submission struct {
	Credential     string             `json:"credential"`
	ConversationID string             `json:"conversation_id,omitempty"`
	Messages       []safeguardMessage `json:"messages"`
}

// submitter delivers a conversation from the run that produced it, after the
// run's terminal frame and within safeguardsTimeout, so it never delays or
// fails the run.
type submitter struct {
	endpoint string
	client   *http.Client
}

// newSubmitter is nil without a sidecar; a nil submitter does nothing.
func newSubmitter(baseURL string) *submitter {
	if baseURL == "" {
		return nil
	}
	return &submitter{
		endpoint: strings.TrimRight(baseURL, "/") + "/ingest",
		client:   &http.Client{Timeout: safeguardsTimeout},
	}
}

// submit delivers a conversation that ends in an assistant reply; without one
// there is nothing to judge.
func (s *submitter) submit(credential, conversationID string, messages []safeguardMessage) {
	if s == nil || len(messages) == 0 || messages[len(messages)-1].Role != "assistant" || messages[len(messages)-1].Content == "" {
		return
	}
	if len(conversationID) > maxConversationIDLen {
		conversationID = ""
	}
	body, ok := encodeSubmission(credential, conversationID, messages)
	if !ok {
		return
	}
	s.deliver(body)
}

// encodeSubmission serializes within maxSubmissionBytes, trimming the oldest
// history when all of it does not fit. It fails only when not even part of the
// latest turn fits.
func encodeSubmission(credential, conversationID string, messages []safeguardMessage) ([]byte, bool) {
	if fitsSubmission(credential, conversationID, messages) {
		body, _ := json.Marshal(submission{Credential: credential, ConversationID: conversationID, Messages: messages})
		if len(body) <= maxSubmissionBytes {
			return body, true
		}
	}
	messages = trimToFit(credential, conversationID, messages)
	if len(messages) == 0 {
		return nil, false
	}
	body, _ := json.Marshal(submission{Credential: credential, ConversationID: conversationID, Messages: messages})
	return body, len(body) <= maxSubmissionBytes
}

// trimToFit keeps the newest messages that fit, cutting the oldest one kept
// from the front, so the latest turns always reach the classifier.
func trimToFit(credential, conversationID string, messages []safeguardMessage) []safeguardMessage {
	envelope, _ := json.Marshal(submission{Credential: credential, ConversationID: conversationID, Messages: []safeguardMessage{}})
	remaining := maxSubmissionBytes - len(envelope)
	var kept []safeguardMessage
	for i := len(messages) - 1; i >= 0; i-- {
		if len(kept) > 0 {
			remaining -= len(",")
		}
		m, size, ok := fitMessage(messages[i], remaining)
		if !ok {
			break
		}
		kept = append(kept, m)
		remaining -= size
		if len(m.Content) < len(messages[i].Content) {
			break
		}
	}
	slices.Reverse(kept)
	return kept
}

// fitMessage returns m, or m with its content cut from the front, encoding
// within budget bytes, with that encoded size; ok is false when none fits.
func fitMessage(m safeguardMessage, budget int) (safeguardMessage, int, bool) {
	if len(m.Role)+len(m.Content)+messageJSONOverhead <= budget {
		if b, _ := json.Marshal(m); len(b) <= budget {
			return m, len(b), true
		}
	}
	empty, _ := json.Marshal(safeguardMessage{Role: m.Role})
	// JSON encodes a byte as at most 6 (\u00XX, \ufffd), so a sixth of the
	// room fits however it escapes. Cloned so the tail does not pin the original.
	cut := safeguardMessage{Role: m.Role, Content: strings.Clone(tail(m.Content, (budget-len(empty))/6))}
	b, _ := json.Marshal(cut)
	if len(b) > budget || cut.Content == "" {
		return safeguardMessage{}, 0, false
	}
	return cut, len(b), true
}

// tail is at most n trailing bytes of s, without splitting a UTF-8 sequence.
func tail(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	start := len(s) - n
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[start:]
}

// fitsSubmission bounds the input before encoding; escaping can grow the
// encoding, so its length is checked separately.
func fitsSubmission(credential, conversationID string, messages []safeguardMessage) bool {
	remaining := maxSubmissionBytes
	for _, value := range []string{credential, conversationID} {
		if len(value) > remaining {
			return false
		}
		remaining -= len(value)
	}
	for _, message := range messages {
		if messageJSONOverhead > remaining {
			return false
		}
		remaining -= messageJSONOverhead
		for _, value := range []string{message.Role, message.Content} {
			if len(value) > remaining {
				return false
			}
			remaining -= len(value)
		}
	}
	return true
}

func (s *submitter) deliver(body []byte) {
	defer func() { rescued("safeguards submission", recover()) }()
	req, err := http.NewRequest(http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		slog.Warn("safeguards submission failed", "error", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		slog.Warn("safeguards sidecar rejected submission", "status", resp.StatusCode)
	}
}

// flattenContent renders string or part-array content as text, replacing
// each non-text part with a placeholder so no binary leaves the enclave.
func flattenContent(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var b strings.Builder
		for i, part := range c {
			p, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if i > 0 {
				b.WriteByte('\n')
			}
			if kind := str(p["type"]); kind != "text" {
				fmt.Fprintf(&b, "[%s]", kind)
			} else if text, ok := p["text"].(string); ok {
				b.WriteString(text)
			} else {
				b.WriteString(str(p["refusal"]))
			}
		}
		return b.String()
	}
	return ""
}

// isFirstPartyChatAccessJWT recognises Tinfoil Chat's own access tokens, the
// only credentials whose conversations are classified.
func isFirstPartyChatAccessJWT(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return false
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var metadata struct {
		Type string `json:"typ"`
	}
	if json.Unmarshal(header, &metadata) != nil || strings.TrimPrefix(strings.ToLower(metadata.Type), "application/") != "at+jwt" {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		ClientID string `json:"client_id"`
		Product  string `json:"product"`
	}
	return json.Unmarshal(payload, &claims) == nil && claims.ClientID == "tinfoil-chat" && claims.Product == "chat"
}

// screening is what a run submits for classification once it completes.
type screening struct {
	conversation string
	messages     []safeguardMessage
}

// screening captures the prompt and history a run was asked, without tool
// results. Turns through auto are screened here too: the router is not told
// their chat, so the control plane ignores the router's report of them.
func (h *harness) screening(req *request) *screening {
	if h.safeguards == nil {
		return nil
	}
	var messages []safeguardMessage
	if req.system != "" {
		messages = append(messages, safeguardMessage{Role: "system", Content: req.system})
	}
	for _, group := range req.groups {
		for _, encoded := range group {
			var message struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			}
			if json.Unmarshal(encoded, &message) == nil && message.Role != "" && message.Role != "tool" {
				messages = append(messages, safeguardMessage{Role: message.Role, Content: flattenContent(message.Content)})
			}
		}
	}
	if !fitsSubmission("", "", messages) {
		messages = trimToFit("", "", messages)
	}
	return &screening{conversation: req.conversation, messages: messages}
}

// screen submits a run that completed with a reply, under a first-party chat
// credential, for classification.
func (h *harness) screen(r *chatRun, reply []object, runErr error) {
	h.live.mu.Lock()
	deleted := r.thread.deleted
	h.live.mu.Unlock()
	if r.screening == nil || runErr != nil || r.cancelled || deleted || r.retryTarget != nil || r.principal.Anonymous || !isFirstPartyChatAccessJWT(string(r.credential.Key)) {
		return
	}
	var text []string
	for _, m := range reply {
		if content := str(m["content"]); content != "" {
			text = append(text, content)
		}
	}
	history := r.screening.messages
	// A continued reply, whole, takes the place of its earlier text.
	if r.input["kind"] == "continue" && len(history) > 0 && history[len(history)-1].Role == "assistant" {
		history = history[:len(history)-1]
	}
	h.safeguards.submit(string(r.credential.Key), r.screening.conversation, append(history, safeguardMessage{Role: "assistant", Content: strings.Join(text, "\n")}))
}
