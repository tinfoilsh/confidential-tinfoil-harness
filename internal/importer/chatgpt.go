package importer

import (
	"encoding/json"
	"path"
	"sort"
	"strings"
	"time"
)

type chatgptConversation struct {
	ID             string                 `json:"id"`
	ConversationID string                 `json:"conversation_id"`
	Title          string                 `json:"title"`
	CreateTime     float64                `json:"create_time"`
	UpdateTime     float64                `json:"update_time"`
	CurrentNode    string                 `json:"current_node"`
	Mapping        map[string]chatgptNode `json:"mapping"`
}

type chatgptNode struct {
	Message  *chatgptMessage `json:"message"`
	Parent   string          `json:"parent"`
	Children []string        `json:"children"`
}

type chatgptMessage struct {
	Author struct {
		Role string `json:"role"`
	} `json:"author"`
	Content struct {
		ContentType string            `json:"content_type"`
		Parts       []json.RawMessage `json:"parts"`
		Thoughts    []struct {
			Content string `json:"content"`
			Summary string `json:"summary"`
		} `json:"thoughts"`
	} `json:"content"`
	CreateTime float64 `json:"create_time"`
	Metadata   struct {
		Attachments []struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			MimeType string `json:"mime_type"`
		} `json:"attachments"`
	} `json:"metadata"`
}

type chatgptAssetPointer struct {
	AssetPointer string `json:"asset_pointer"`
}

// chatgptRoot is the parent ChatGPT gives a conversation's first nodes.
const chatgptRoot = "client-created-root"

// ReadChatGPT reads a ChatGPT conversation along its current branch, or
// nil when it has no message to show. Images resolve through index.
func ReadChatGPT(record json.RawMessage, index *Index) (*Conversation, error) {
	var conv chatgptConversation
	if err := json.Unmarshal(record, &conv); err != nil {
		return nil, invalid("chatgpt conversation: %v", err)
	}
	visited := map[string]bool{}
	var messages []Message
	visit := func(nodeID string) {
		if visited[nodeID] {
			return
		}
		visited[nodeID] = true
		node, ok := conv.Mapping[nodeID]
		if !ok || node.Message == nil || !chatgptRenderable(node.Message) {
			return
		}
		if m, ok := chatgptMessageOf(nodeID, &conv, index); ok {
			messages = append(messages, m)
		}
	}
	if branch := chatgptActivePath(&conv); len(branch) > 0 {
		for _, nodeID := range branch {
			visit(nodeID)
		}
	} else {
		// Without a current node, the first child of each node is followed.
		followed := map[string]bool{}
		var follow func(string)
		follow = func(nodeID string) {
			if followed[nodeID] {
				return
			}
			followed[nodeID] = true
			visit(nodeID)
			if node, ok := conv.Mapping[nodeID]; ok && len(node.Children) > 0 {
				follow(node.Children[0])
			}
		}
		for _, nodeID := range chatgptRoots(conv.Mapping) {
			follow(nodeID)
		}
	}
	if len(messages) == 0 {
		return nil, nil
	}
	created := unixSeconds(conv.CreateTime)
	key := conv.ID
	if key == "" {
		key = conv.ConversationID
	}
	if key == "" {
		key = "chatgpt:" + conv.Title + ":" + created.Format(time.RFC3339Nano)
	}
	return &Conversation{Key: key, Title: conv.Title, CreatedAt: created, UpdatedAt: unixSeconds(conv.UpdateTime), Messages: messages}, nil
}

func chatgptRenderable(msg *chatgptMessage) bool {
	role, kind := msg.Author.Role, msg.Content.ContentType
	return (role == "user" || role == "assistant") && (kind == "text" || kind == "multimodal_text") &&
		(len(msg.Content.Parts) > 0 || len(msg.Metadata.Attachments) > 0)
}

// chatgptActivePath is the branch from the root to the current node, or
// nil when the tree does not lead there.
func chatgptActivePath(conv *chatgptConversation) []string {
	var reversed []string
	seen := map[string]bool{}
	for current := conv.CurrentNode; current != "" && current != chatgptRoot; {
		node, ok := conv.Mapping[current]
		if seen[current] || !ok {
			return nil
		}
		seen[current] = true
		reversed = append(reversed, current)
		current = node.Parent
	}
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	return reversed
}

func chatgptRoots(mapping map[string]chatgptNode) []string {
	var roots []string
	for nodeID, node := range mapping {
		if node.Parent == "" || node.Parent == chatgptRoot {
			roots = append(roots, nodeID)
		}
	}
	sort.Strings(roots)
	return roots
}

func chatgptMessageOf(nodeID string, conv *chatgptConversation, index *Index) (Message, bool) {
	msg := conv.Mapping[nodeID].Message
	var texts []string
	var pointers []string
	for _, part := range msg.Content.Parts {
		var text string
		if json.Unmarshal(part, &text) == nil {
			texts = append(texts, text)
			continue
		}
		var pointer chatgptAssetPointer
		if json.Unmarshal(part, &pointer) == nil && pointer.AssetPointer != "" {
			pointers = append(pointers, pointer.AssetPointer)
		}
	}
	content := strings.TrimSpace(strings.Join(texts, "\n"))
	attachments := chatgptAttachments(msg, pointers, index)
	if content == "" && len(attachments) == 0 {
		return Message{}, false
	}
	created := unixSeconds(conv.CreateTime)
	if msg.CreateTime != 0 {
		created = unixSeconds(msg.CreateTime)
	}
	out := Message{Role: msg.Author.Role, Content: content, CreatedAt: created, Attachments: attachments}
	if out.Role == "assistant" {
		out.Thoughts = chatgptThoughts(nodeID, conv.Mapping)
	}
	return out, true
}

// trimFileID strips the schemes ChatGPT names its files with, so a
// reference can be matched against entry names.
func trimFileID(ref string) string {
	ref = strings.TrimSpace(ref)
	ref = strings.TrimPrefix(ref, "file-service://")
	return strings.TrimPrefix(ref, "sediment://")
}

func chatgptAttachments(msg *chatgptMessage, pointers []string, index *Index) []Attachment {
	var out []Attachment
	seen := map[string]bool{}
	add := func(a Attachment, key string) {
		if key != "" && seen[key] {
			return
		}
		seen[key] = true
		out = append(out, a)
	}
	for _, a := range msg.Metadata.Attachments {
		id := trimFileID(a.ID)
		att := Attachment{FileName: a.Name, MimeType: a.MimeType, Image: isImage(a.MimeType, a.Name)}
		if name, ok := index.ByIDPrefix(id); ok {
			att.Image = att.Image || isImage(a.MimeType, name)
			att.Entry = name
			if att.FileName == "" {
				att.FileName = path.Base(name)
			}
			if att.MimeType == "" {
				att.MimeType = imageType(name)
			}
		}
		add(att, id)
	}
	for _, pointer := range pointers {
		id := trimFileID(pointer)
		if name, ok := index.ByIDPrefix(id); ok {
			add(Attachment{Image: true, FileName: path.Base(name), MimeType: imageType(name), Entry: name}, id)
		}
	}
	return out
}

// chatgptThoughts is the reasoning ChatGPT recorded on the way to a reply.
func chatgptThoughts(nodeID string, mapping map[string]chatgptNode) string {
	seen := map[string]bool{}
	thoughts := ""
	for current := nodeID; current != "" && !seen[current]; {
		seen[current] = true
		node, ok := mapping[current]
		if !ok {
			break
		}
		if msg := node.Message; msg != nil && msg.Content.ContentType == "thoughts" {
			var texts []string
			for _, t := range msg.Content.Thoughts {
				if t.Content != "" {
					texts = append(texts, t.Content)
				} else if t.Summary != "" {
					texts = append(texts, t.Summary)
				}
			}
			if len(texts) > 0 && thoughts == "" {
				thoughts = strings.Join(texts, "\n\n")
			}
		}
		current = node.Parent
	}
	return thoughts
}
