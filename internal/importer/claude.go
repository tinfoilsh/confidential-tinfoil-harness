package importer

import (
	"encoding/json"
	"path"
	"strings"
)

// Claude's conversations and Tinfoil's export share one shape. Tinfoil's
// attachments name their kind and the archive entry of an image's bytes;
// Claude's name a document's extracted text, and its images are files.

type claudeConversation struct {
	UUID         string          `json:"uuid"`
	Name         string          `json:"name"`
	CreatedAt    string          `json:"created_at"`
	UpdatedAt    string          `json:"updated_at"`
	ChatMessages []claudeMessage `json:"chat_messages"`
}

type claudeMessage struct {
	Text      string `json:"text"`
	Sender    string `json:"sender"`
	CreatedAt string `json:"created_at"`
	Content   []struct {
		Type     string `json:"type"`
		Thinking string `json:"thinking"`
	} `json:"content"`
	Attachments []claudeAttachment `json:"attachments"`
	Files       []claudeFile       `json:"files"`
	FilesV2     []claudeFile       `json:"files_v2"`
}

type claudeAttachment struct {
	// Claude's document
	FileName         string `json:"file_name"`
	FileSize         int64  `json:"file_size"`
	ExtractedContent string `json:"extracted_content"`
	// Tinfoil's attachment
	ID          string `json:"id"`
	Type        string `json:"type"`
	Name        string `json:"fileName"`
	MimeType    string `json:"mimeType"`
	Size        int64  `json:"fileSize"`
	ExportPath  string `json:"exportPath"`
	TextContent string `json:"textContent"`
}

type claudeFile struct {
	FileName string `json:"file_name"`
	FileUUID string `json:"file_uuid"`
}

// ReadClaude reads a Claude or Tinfoil conversation, or nil when it has no
// message to show. Images resolve through index.
func ReadClaude(record json.RawMessage, index *Index) (*Conversation, error) {
	var conv claudeConversation
	if err := json.Unmarshal(record, &conv); err != nil {
		return nil, invalid("claude conversation: %v", err)
	}
	var messages []Message
	for i := range conv.ChatMessages {
		msg := &conv.ChatMessages[i]
		role := map[string]string{"human": "user", "assistant": "assistant"}[msg.Sender]
		text := strings.TrimSpace(msg.Text)
		attachments := claudeAttachments(msg, index)
		if role == "" || text == "" && len(attachments) == 0 {
			continue
		}
		out := Message{Role: role, Content: text, CreatedAt: parseISOTime(msg.CreatedAt), Attachments: attachments}
		if role == "assistant" {
			var thoughts []string
			for _, c := range msg.Content {
				if c.Type == "thinking" && c.Thinking != "" {
					thoughts = append(thoughts, c.Thinking)
				}
			}
			out.Thoughts = strings.Join(thoughts, "\n\n")
		}
		messages = append(messages, out)
	}
	if len(messages) == 0 {
		return nil, nil
	}
	key := conv.UUID
	if key == "" {
		key = "claude:" + conv.Name + ":" + conv.CreatedAt
	}
	return &Conversation{Key: key, Title: conv.Name, CreatedAt: parseISOTime(conv.CreatedAt), UpdatedAt: parseISOTime(conv.UpdatedAt), Messages: messages}, nil
}

func claudeAttachments(msg *claudeMessage, index *Index) []Attachment {
	var out []Attachment
	for _, a := range msg.Attachments {
		switch {
		case a.Type == "image":
			att := Attachment{ID: a.ID, Image: true, FileName: a.Name, MimeType: a.MimeType, Size: a.Size}
			if name, ok := index.Exact(a.ExportPath); ok && a.ExportPath != "" {
				att.Entry = name
				if att.FileName == "" {
					att.FileName = path.Base(name)
				}
			}
			out = append(out, att)
		case a.Type != "" || a.TextContent != "":
			out = append(out, Attachment{ID: a.ID, FileName: a.Name, MimeType: a.MimeType, Size: a.Size, TextContent: a.TextContent})
		case a.ExtractedContent != "":
			out = append(out, Attachment{FileName: a.FileName, Size: a.FileSize, TextContent: a.ExtractedContent})
		}
	}
	seen := map[string]bool{}
	for _, f := range append(append([]claudeFile{}, msg.Files...), msg.FilesV2...) {
		key := f.FileUUID
		if key == "" {
			key = f.FileName
		}
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		// A file's id names it alone; its name may be one any file has.
		name, ok := index.ByIDPrefix(f.FileUUID)
		if !ok {
			name, ok = index.Exact(f.FileName)
		}
		if !ok {
			name, ok = index.Basename(f.FileName)
		}
		if ok && isImage("", name) {
			out = append(out, Attachment{Image: true, FileName: path.Base(name), MimeType: imageType(name), Entry: name})
		}
	}
	return out
}

type claudeProject struct {
	UUID           string `json:"uuid"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	PromptTemplate string `json:"prompt_template"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
	Docs           []struct {
		UUID      string `json:"uuid"`
		Filename  string `json:"filename"`
		Content   string `json:"content"`
		CreatedAt string `json:"created_at"`
	} `json:"docs"`
}

// ReadClaudeProject reads a Claude project, or nil for the empty starter
// project Claude gives every account.
func ReadClaudeProject(record json.RawMessage) (*Project, error) {
	var p claudeProject
	if err := json.Unmarshal(record, &p); err != nil {
		return nil, invalid("claude project: %v", err)
	}
	project := &Project{Key: p.UUID, Name: p.Name, Description: p.Description, Instructions: p.PromptTemplate, CreatedAt: parseISOTime(p.CreatedAt), UpdatedAt: parseISOTime(p.UpdatedAt)}
	for _, d := range p.Docs {
		if d.Content != "" && d.Filename != "" {
			project.Documents = append(project.Documents, Document{Key: d.UUID, FileName: d.Filename, Content: d.Content, CreatedAt: parseISOTime(d.CreatedAt)})
		}
	}
	if project.Name == "" && project.Instructions == "" && len(project.Documents) == 0 {
		return nil, nil
	}
	if project.Key == "" {
		project.Key = "claude-project:" + p.Name + ":" + p.CreatedAt
	}
	return project, nil
}
