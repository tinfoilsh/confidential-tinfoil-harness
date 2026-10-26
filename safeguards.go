package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Completed first-party chat conversations are submitted to the safeguards
// sidecar for acceptable-use classification, as the model router submits the
// chats it serves. Ported from the router's safeguards package.

const (
	safeguardsTimeout = 5 * time.Second
	// maxSubmissionBytes is the sidecar's request cap, MAX_REQUEST_BYTES in
	// tinfoil-config.yml. The sidecar reads only a transcript's tail.
	maxSubmissionBytes = 4 << 20
	// jsonEscapeGrowth is the most one byte grows when encoded, as \u00XX.
	jsonEscapeGrowth = 6
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
// run's terminal frame, so the reply is already delivered. The run waits up
// to safeguardsTimeout for the delivery before it releases what it holds,
// and a failed delivery never fails it.
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
	s.deliver(encodeSubmission(credential, conversationID, messages))
}

// encodeSubmission keeps the newest messages that fit the sidecar's cap, and
// cuts the newest from the front when it alone does not fit.
func encodeSubmission(credential, conversationID string, messages []safeguardMessage) []byte {
	envelope, _ := json.Marshal(submission{Credential: credential, ConversationID: conversationID, Messages: []safeguardMessage{}})
	room, start := maxSubmissionBytes-len(envelope), len(messages)
	for ; start > 0; start-- {
		encoded, _ := json.Marshal(messages[start-1])
		if len(encoded)+len(",") > room {
			break
		}
		room -= len(encoded) + len(",")
	}
	kept := messages[start:]
	if len(kept) == 0 {
		last := messages[len(messages)-1]
		empty, _ := json.Marshal(safeguardMessage{Role: last.Role})
		keep := min(len(last.Content), max(0, room-len(empty))/jsonEscapeGrowth)
		last.Content = strings.ToValidUTF8(last.Content[len(last.Content)-keep:], "")
		kept = []safeguardMessage{last}
	}
	body, _ := json.Marshal(submission{Credential: credential, ConversationID: conversationID, Messages: kept})
	return body
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
			if kind := api.Str(p["type"]); kind != "text" {
				fmt.Fprintf(&b, "[%s]", kind)
			} else if text, ok := p["text"].(string); ok {
				b.WriteString(text)
			} else {
				b.WriteString(api.Str(p["refusal"]))
			}
		}
		return b.String()
	}
	return ""
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
	return &screening{conversation: req.conversation, messages: messages}
}

// screen submits a run that completed with a reply, under a first-party chat
// credential, for classification.
func (h *harness) screen(r *chatRun, reply []Message, runErr error) {
	h.live.mu.Lock()
	deleted := r.thread.deleted
	h.live.mu.Unlock()
	if r.screening == nil || runErr != nil || r.cancelled || deleted || r.retryTarget != nil || r.principal.Anonymous || !r.credential.FirstParty {
		return
	}
	var text []string
	for _, m := range reply {
		if m.Content != "" {
			text = append(text, m.Content)
		}
	}
	history := r.screening.messages
	// A continued reply, whole, takes the place of its earlier text.
	if r.turn.Kind == "continue" && len(history) > 0 && history[len(history)-1].Role == "assistant" {
		history = history[:len(history)-1]
	}
	h.safeguards.submit(string(r.credential.Key), r.screening.conversation, append(history, safeguardMessage{Role: "assistant", Content: strings.Join(text, "\n")}))
}
