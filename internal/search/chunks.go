package search

import (
	"encoding/json"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

const (
	// The embedding model family separates documents from queries: each
	// is tagged with its task so the two embed into their own subspaces.
	docPrefix   = "search_document: "
	queryPrefix = "search_query: "

	// chunkChars is the size a chunk of chat text is cut to, on message
	// boundaries where possible: about 500 tokens, small enough that one
	// topic dominates the chunk's vector.
	chunkChars = 2000
)

// chatText is what of a stored chat is indexed: its title and the text of
// its messages. Other members are left alone, so a client may add to the
// shape without breaking indexing.
type chatText struct {
	Title    string `json:"title"`
	Messages []struct {
		Content string `json:"content"`
	} `json:"messages"`
}

// chatChunks is a chat's text in embedding-sized chunks: the title, then
// each message, packed up to chunkChars and cut on message boundaries
// where possible; a message larger than a chunk is split. At most
// searchindex.MaxChunksPerChat chunks are kept. A chat that is not chat
// shaped, or has no text, has none.
func chatChunks(plaintext []byte) []string {
	var chat chatText
	if err := json.Unmarshal(plaintext, &chat); err != nil {
		return nil
	}
	parts := make([]string, 0, len(chat.Messages)+1)
	if title := strings.TrimSpace(chat.Title); title != "" {
		parts = append(parts, title)
	}
	for _, m := range chat.Messages {
		if content := strings.TrimSpace(m.Content); content != "" {
			parts = append(parts, content)
		}
	}
	var chunks []string
	var current strings.Builder
	// flush closes the chunk being built, and is whether there is room
	// for another.
	flush := func() bool {
		if current.Len() == 0 {
			return true
		}
		chunks = append(chunks, current.String())
		current.Reset()
		return len(chunks) < searchindex.MaxChunksPerChat
	}
	for _, part := range parts {
		for len(part) > 0 {
			if current.Len() > 0 && len(part)+1 <= chunkChars-current.Len() {
				current.WriteByte('\n')
				current.WriteString(part)
				break
			}
			if current.Len() > 0 && len(part) <= chunkChars {
				if !flush() {
					return chunks
				}
				continue
			}
			available := chunkChars - current.Len()
			if current.Len() > 0 {
				available--
			}
			piece := truncateUTF8(part, available)
			if piece == "" {
				// Less room left than the next rune needs: close the
				// chunk and try again with a fresh one.
				if !flush() {
					return chunks
				}
				continue
			}
			if current.Len() > 0 {
				current.WriteByte('\n')
			}
			current.WriteString(piece)
			part = part[len(piece):]
			if len(part) > 0 && !flush() {
				return chunks
			}
		}
	}
	flush()
	return chunks
}

// truncateUTF8 cuts s to at most n bytes without splitting a rune. A
// non-positive n is the empty string, which the chunker reads as no room
// left.
func truncateUTF8(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut]
}
