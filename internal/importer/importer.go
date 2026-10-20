// Package importer reads the chat exports of ChatGPT and Claude, and the
// conversations export Tinfoil writes in Claude's format, one record at a
// time. Attachment bytes are named by archive entry; reading them is the
// caller's.
package importer

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// ErrInvalid wraps every error that comes from the export itself, so a
// caller can tell a malformed upload from its own failures.
var ErrInvalid = errors.New("importer: invalid export")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Format is the kind of record an export holds.
type Format string

const (
	// ChatGPT conversations, each a tree of message nodes.
	ChatGPT Format = "chatgpt"
	// Claude conversations, and the Tinfoil export that shares their shape.
	Claude Format = "claude"
	// ClaudeProjects are Claude projects with the text of their documents.
	ClaudeProjects Format = "claude-projects"
)

// Conversation is one chat of an export.
type Conversation struct {
	// Key identifies the conversation across exports of it.
	Key       string
	Title     string
	CreatedAt time.Time
	UpdatedAt time.Time
	Messages  []Message
}

// Message is one turn of a conversation.
type Message struct {
	Role        string // user or assistant
	Content     string
	CreatedAt   time.Time
	Thoughts    string
	Attachments []Attachment
}

// Attachment is a file a message carries: an image held by an archive
// entry, or a document's text.
type Attachment struct {
	// ID is the export's own id for the attachment, when it has one.
	ID          string
	Image       bool
	FileName    string
	MimeType    string
	Size        int64
	TextContent string
	// Entry is the archive entry that holds an image's bytes.
	Entry string
}

// Project is a Claude project: its instructions and its documents' text.
type Project struct {
	Key          string
	Name         string
	Description  string
	Instructions string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Documents    []Document
}

// Document is the extracted text of a project's file.
type Document struct {
	Key       string
	FileName  string
	Content   string
	CreatedAt time.Time
}

// Records calls visit with each object of a JSON array, or with the one
// object a document holds, decoding one at a time so an export is never
// held whole.
func Records(r io.Reader, visit func(json.RawMessage) error) error {
	br := bufio.NewReader(r)
	first, err := firstByte(br)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(br)
	record := func() error {
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return invalid("decode record: %v", err)
		}
		if trimmed := bytes.TrimSpace(value); len(trimmed) == 0 || trimmed[0] != '{' {
			return invalid("a record is not an object")
		}
		return visit(value)
	}
	switch first {
	case '{':
		if err := record(); err != nil {
			return err
		}
	case '[':
		if _, err := dec.Token(); err != nil {
			return invalid("open array: %v", err)
		}
		for dec.More() {
			if err := record(); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return invalid("close array: %v", err)
		}
	default:
		return invalid("the export is not a JSON array or object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return invalid("data after the export")
	}
	return nil
}

// firstByte peeks at the first byte of a document past whitespace and a
// byte order mark.
func firstByte(br *bufio.Reader) (byte, error) {
	if bom, err := br.Peek(3); err == nil && string(bom) == "\xef\xbb\xbf" {
		br.Discard(3)
	}
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, invalid("the export is empty")
		}
		if !strings.ContainsRune(" \t\r\n", rune(b)) {
			return b, br.UnreadByte()
		}
	}
}

// Classify names the format of one record, or none it reads.
func Classify(record json.RawMessage) Format {
	var keys struct {
		Mapping        json.RawMessage `json:"mapping"`
		ChatMessages   json.RawMessage `json:"chat_messages"`
		PromptTemplate json.RawMessage `json:"prompt_template"`
		Docs           json.RawMessage `json:"docs"`
	}
	json.Unmarshal(record, &keys)
	switch {
	case marks(keys.Mapping):
		return ChatGPT
	case marks(keys.ChatMessages):
		return Claude
	case marks(keys.PromptTemplate) || marks(keys.Docs):
		return ClaudeProjects
	}
	return ""
}

// marks reports whether a record holds a key that names its format, with
// a value other than null.
func marks(value json.RawMessage) bool {
	return value != nil && !bytes.Equal(value, []byte("null"))
}

func parseISOTime(s string) time.Time {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func unixSeconds(sec float64) time.Time {
	if sec <= 0 {
		return time.Time{}
	}
	whole := int64(sec)
	return time.Unix(whole, int64((sec-float64(whole))*1e9)).UTC()
}
