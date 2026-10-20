package main

import (
	"cmp"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The legacy reader is the one place that reads rows of any age: chats the
// web and iOS apps stored in sync, device chats the old web client kept, old
// shares, profiles and projects old clients wrote, and records in the
// current format. It returns them in the current format. A field of the
// wrong type reads as absent, so one odd field never hides a chat.

// readLenient decodes data into v, leaving out each field of the wrong
// type. Rows reach it as valid JSON objects.
func readLenient(data []byte, v any) { json.Unmarshal(data, v) }

// looseTime is a time old clients stored as RFC 3339 text or as Unix
// milliseconds, read as RFC 3339.
type looseTime string

func (t *looseTime) UnmarshalJSON(b []byte) error {
	var value any
	json.Unmarshal(b, &value)
	*t = looseTime(isoTime(value))
	return nil
}

func isoTime(value any) string {
	switch v := value.(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return parsed.UTC().Format(time.RFC3339Nano)
		}
		return v
	case float64:
		if v > 0 {
			return time.UnixMilli(int64(v)).UTC().Format(time.RFC3339Nano)
		}
	}
	return ""
}

// legacyThread is a chat row as any client stored it.
type legacyThread struct {
	Thread
	CreatedAt        looseTime       `json:"createdAt"`
	UpdatedAt        looseTime       `json:"updatedAt"`
	WebSearchEnabled *bool           `json:"webSearchEnabled"`
	Messages         []legacyMessage `json:"messages"`
}

// legacyMessage is a message with the flat fields the apps stored before
// replies had timelines and attachments had their own records.
type legacyMessage struct {
	Message
	Timestamp               looseTime        `json:"timestamp"`
	Timeline                []legacyBlock    `json:"timeline"`
	Thoughts                string           `json:"thoughts"`
	WebSearch               *legacySearch    `json:"webSearch"`
	WebSearchBeforeThinking bool             `json:"webSearchBeforeThinking"`
	URLFetches              []legacyFetch    `json:"urlFetches"`
	CodeExecCalls           []legacyCodeCall `json:"codeExecCalls"`
	ToolCalls               []Block          `json:"toolCalls"`
	DocumentContent         string           `json:"documentContent"`
	MultimodalText          string           `json:"multimodalText"`
	ImageData               []legacyImage    `json:"imageData"`
}

// legacyBlock is a timeline block, including the kinds the web client wrote
// before every tool step was a tool call.
type legacyBlock struct {
	Block
	State   *legacySearch    `json:"state"`
	Fetches []legacyFetch    `json:"fetches"`
	Calls   []legacyCodeCall `json:"calls"`
}

type legacySearch struct {
	Query   string `json:"query"`
	Sources []struct {
		URL         string `json:"url"`
		Title       string `json:"title"`
		Snippet     string `json:"snippet"`
		Description string `json:"description"`
	} `json:"sources"`
}

type legacyFetch struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

type legacyCodeCall struct {
	ID        string          `json:"id"`
	ToolName  string          `json:"toolName"`
	Arguments json.RawMessage `json:"arguments"`
	Output    string          `json:"output"`
}

type legacyImage struct {
	Base64   string `json:"base64"`
	MimeType string `json:"mimeType"`
}

// readThread reads a chat row into the thread format.
func readThread(id string, data []byte) Thread { return upgradeThread(id, data, "") }

// upgradeThread reads a chat, dated created when it holds no date.
func upgradeThread(id string, data []byte, created string) Thread {
	var stored legacyThread
	readLenient(data, &stored)
	t := stored.Thread
	t.ID, t.CreatedAt, t.UpdatedAt = id, cmp.Or(string(stored.CreatedAt), created), string(stored.UpdatedAt)
	t.Title = cmp.Or(t.Title, "New Chat")
	if t.TitleState == "" {
		t.TitleState = "generated"
		if t.Title == "New Chat" {
			t.TitleState = "placeholder"
		}
	}
	t.Model = normalizeModel(t.Model)
	t.WebSearchEnabled = valueOr(stored.WebSearchEnabled, true)
	t.Messages = make([]Message, len(stored.Messages))
	for i, item := range stored.Messages {
		m := item.Message
		m.ID = cmp.Or(m.ID, id+"-message-"+strconv.Itoa(i))
		m.CreatedAt = cmp.Or(m.CreatedAt, string(item.Timestamp), t.CreatedAt)
		m.Timeline = item.timeline(m)
		m.Attachments = item.attachments(m.ID)
		t.Messages[i] = m
	}
	return t
}

// readThreadRow reads a stored chat. Sync files a chat under its project
// apart from the sealed row, and that filing is the chat's project.
func readThreadRow(row *storedRow) *threadRow {
	t := readThread(row.ID, row.Data)
	if row.projectSet {
		t.ProjectID = row.project
	}
	return &threadRow{Thread: t, etag: row.ETag}
}

// readDeviceThread reads a thread a client keeps on its device. The fields
// the harness owns are never taken from a client's copy.
func readDeviceThread(id string, data []byte) Thread {
	t := upgradeThread(id, data, timestamp())
	t.ActiveRun, t.HiddenCount, t.ProjectID, t.ContextUsage = nil, 0, nil, nil
	return t
}

// timeline is a reply's timeline, built from its old flat fields when it
// has none and with every old block kind made a tool call.
func (m *legacyMessage) timeline(message Message) []Block {
	if message.Role != "assistant" {
		if m.Timeline == nil {
			return nil
		}
		blocks := make([]Block, len(m.Timeline))
		for i, b := range m.Timeline {
			blocks[i] = b.Block
		}
		return blocks
	}
	id, source := message.ID, m.Timeline
	if len(source) == 0 {
		addSearch := func() {
			if m.WebSearch != nil {
				source = append(source, legacyBlock{Block: Block{Type: "web_search", ID: id + "-search"}, State: m.WebSearch})
			}
		}
		if m.WebSearchBeforeThinking {
			addSearch()
		}
		if m.Thoughts != "" {
			source = append(source, legacyBlock{Block: Block{Type: "thinking", ID: id + "-thinking", Content: m.Thoughts}})
		}
		if !m.WebSearchBeforeThinking {
			addSearch()
		}
		if len(m.URLFetches) > 0 {
			source = append(source, legacyBlock{Block: Block{Type: "url_fetches", ID: id + "-fetches"}, Fetches: m.URLFetches})
		}
		if len(m.CodeExecCalls) > 0 {
			source = append(source, legacyBlock{Block: Block{Type: "code_exec", ID: id + "-code"}, Calls: m.CodeExecCalls})
		}
		for _, call := range m.ToolCalls {
			call.Type, call.ToolCallID = "tool_call", call.ID
			source = append(source, legacyBlock{Block: call})
		}
		if message.Content != "" {
			source = append(source, legacyBlock{Block: Block{Type: "content", ID: id + "-content", Content: message.Content}})
		}
	}
	blocks := []Block{}
	call := func(id, name, args string, result any) {
		blocks = append(blocks, Block{Type: "tool_call", ID: id, ToolCallID: id, Name: name, Arguments: args, Result: raw(result), Complete: true})
	}
	for index, block := range source {
		blockID := cmp.Or(block.ID, id+"-block-"+strconv.Itoa(index))
		switch block.Type {
		case "web_search":
			state := legacySearch{}
			if block.State != nil {
				state = *block.State
			}
			results := []map[string]string{}
			for _, source := range state.Sources {
				results = append(results, map[string]string{"url": source.URL, "title": source.Title, "snippet": cmp.Or(source.Snippet, source.Description)})
			}
			call(blockID, "web_search", string(raw(map[string]string{"query": state.Query})), map[string]any{"query": state.Query, "results": results})
		case "url_fetches":
			for i, fetch := range block.Fetches {
				call(cmp.Or(fetch.ID, blockID+"-"+strconv.Itoa(i)), "web_fetch", string(raw(map[string]string{"url": fetch.URL})), map[string]string{"url": fetch.URL, "title": fetch.Title, "text": fetch.Text})
			}
		case "code_exec":
			for i, c := range block.Calls {
				args := c.arguments()
				call(cmp.Or(c.ID, blockID+"-"+strconv.Itoa(i)), c.ToolName, args, normalizeToolResult(toolCall{name: c.ToolName, args: args}, c.Output))
			}
		default:
			block.ID = blockID
			blocks = append(blocks, block.Block)
		}
	}
	return blocks
}

// arguments is a code call's arguments as JSON text. The apps stored them
// as text or as an object, which is written out with sorted keys.
func (c legacyCodeCall) arguments() string {
	var text string
	if json.Unmarshal(c.Arguments, &text) == nil && text != "" {
		return text
	}
	fields := map[string]any{}
	json.Unmarshal(c.Arguments, &fields)
	return string(raw(fields))
}

// attachments are a message's attachments, made from the document text and
// image bytes the apps once kept on the message itself. Each has an id.
func (m *legacyMessage) attachments(messageID string) []Attachment {
	attachments := m.Attachments
	if len(attachments) == 0 {
		if content := cmp.Or(m.DocumentContent, m.MultimodalText); content != "" {
			attachments = append(attachments, Attachment{Type: "document", FileName: "Attached document", TextContent: content})
		}
		for i, image := range m.ImageData {
			if image.Base64 != "" {
				attachments = append(attachments, Attachment{Type: "image", FileName: "image-" + strconv.Itoa(i), MimeType: cmp.Or(image.MimeType, "image/png"), Base64: image.Base64})
			}
		}
	}
	for i := range attachments {
		attachments[i].ID = cmp.Or(attachments[i].ID, messageID+"-attachment-"+strconv.Itoa(i))
	}
	return attachments
}

// readProfile reads a profile row. A missing row is an empty profile.
func readProfile(data []byte) storedProfile {
	var stored struct {
		storedProfile
		UpdatedAt looseTime `json:"updatedAt"`
	}
	readLenient(data, &stored)
	profile := stored.storedProfile
	profile.UpdatedAt = string(stored.UpdatedAt)
	return profile
}

// readProject reads a project row.
func readProject(row *storedRow) Project {
	var stored struct {
		Project
		CreatedAt looseTime `json:"createdAt"`
		UpdatedAt looseTime `json:"updatedAt"`
	}
	readLenient(row.Data, &stored)
	project := stored.Project
	project.ID, project.CreatedAt, project.UpdatedAt = row.ID, string(stored.CreatedAt), string(stored.UpdatedAt)
	return project
}

// readProjectDocument reads a project document row, which sync files under
// the project's id.
func readProjectDocument(row *storedRow, projectID string) ProjectDocument {
	var stored struct {
		ProjectDocument
		CreatedAt looseTime `json:"createdAt"`
		UpdatedAt looseTime `json:"updatedAt"`
	}
	readLenient(row.Data, &stored)
	doc := stored.ProjectDocument
	doc.ID, doc.ProjectID = strings.TrimPrefix(row.ID, projectID+"/"), projectID
	doc.CreatedAt, doc.UpdatedAt = string(stored.CreatedAt), string(stored.UpdatedAt)
	return doc
}

// handleUpgrade reads chats the previous web client kept on a device, or in
// an export, into the thread format, in order. It stores nothing. A batch
// holding a chat it cannot read is refused whole, so its client can tell
// which chat that is.
func (h *harness) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	if _, err := h.auth.authenticate(r, true); err != nil {
		respond(w, nil, err)
		return
	}
	in, err := decodeJSONWithin(w, r, maxTurnBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	rows, ok := in["threads"].([]any)
	if !ok {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "threads must be an array"))
		return
	}
	threads := make([]Thread, 0, len(rows))
	for _, item := range rows {
		if !validObject(item) {
			respond(w, nil, apiErr(400, "BAD_REQUEST", "Each thread must be an object"))
			return
		}
		id, err := requiredID(obj(item), "id")
		if err != nil {
			respond(w, nil, err)
			return
		}
		if err := validateLocalThread(obj(item)); err != nil {
			respond(w, nil, err)
			return
		}
		// The thread is one its client can send back with a turn.
		threads = append(threads, readDeviceThread(id, raw(item)))
	}
	respond(w, map[string]any{"threads": threads}, nil)
}
