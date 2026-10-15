package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
)

// Repairs for known bugs in the tool-call decoders of specific models and
// inference stacks, ported from the model router (toolruntime/quirks.go).

// controlTokenPattern matches Kimi's <|...|> and Gemma's <|"| framing tokens.
// The inner class cannot span two tokens and is capped so a pathological
// argument cannot stall the stripper.
var controlTokenPattern = regexp.MustCompile(`<\|[^|<>]{0,64}\|>?`)

// urlFields hold URLs and nothing else, so stripping them cannot touch user text.
var urlFields = map[string]bool{"url": true, "urls": true, "fetch_url": true, "page_url": true, "href": true, "link": true}

// sanitizeToolCallArguments strips leaked control tokens from URL arguments in
// place and reports whether anything changed.
func sanitizeToolCallArguments(args map[string]any) bool {
	return len(args) > 0 && deepStripURLTokens(args)
}

func stripModelControlTokens(s string) string {
	if !strings.Contains(s, "<|") {
		return s
	}
	return controlTokenPattern.ReplaceAllString(s, "")
}

// looksLikeLeakedURL admits a string outside urlFields only when it is a URL
// once its tokens are gone.
func looksLikeLeakedURL(s string) bool {
	if !strings.Contains(s, "<|") {
		return false
	}
	stripped := stripModelControlTokens(s)
	return strings.HasPrefix(stripped, "http://") || strings.HasPrefix(stripped, "https://")
}

func deepStripURLTokens(v any) bool {
	strip := func(s string, url bool) (string, bool) {
		if url || looksLikeLeakedURL(s) {
			stripped := stripModelControlTokens(s)
			return stripped, stripped != s
		}
		return s, false
	}
	changed := false
	switch val := v.(type) {
	case map[string]any:
		for key, child := range val {
			url := urlFields[strings.ToLower(key)]
			switch c := child.(type) {
			case string:
				if s, ok := strip(c, url); ok {
					val[key], changed = s, true
				}
			case []any:
				for i, item := range c {
					if s, ok := item.(string); ok {
						if s, ok := strip(s, url); ok {
							c[i], changed = s, true
						}
					} else if deepStripURLTokens(item) {
						changed = true
					}
				}
			case map[string]any:
				changed = deepStripURLTokens(c) || changed
			}
		}
	case []any:
		for _, item := range val {
			changed = deepStripURLTokens(item) || changed
		}
	}
	return changed
}

// sanitizeToolCallArgumentsJSON cuts a payload such as Kimi's
// `{"a":1}Now I will...` down to its leading object. Valid JSON, and anything
// that does not open with a complete object, is returned untouched.
func sanitizeToolCallArgumentsJSON(raw []byte) ([]byte, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' || json.Valid(trimmed) {
		return raw, false
	}
	object, ok := extractFirstJSONObject(string(trimmed))
	if !ok {
		return raw, false
	}
	return []byte(object), true
}

// extractFirstJSONObject returns the first balanced top-level object in s,
// honouring strings and their escapes.
func extractFirstJSONObject(s string) (string, bool) {
	if len(s) == 0 || s[0] != '{' {
		return "", false
	}
	depth, inString, escaped := 0, false, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case escaped:
			escaped = false
		case inString:
			escaped, inString = c == '\\', c != '"'
		case c == '"':
			inString = true
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth == 0 {
				return s[:i+1], true
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	return "", false
}

// historyArgs is what a replayed tool call carries: one call whose arguments
// do not parse would otherwise fail every later request that repeats it.
func historyArgs(s string) string {
	if json.Valid([]byte(s)) {
		return s
	}
	if repaired, ok := sanitizeToolCallArgumentsJSON([]byte(s)); ok && json.Valid(repaired) {
		return string(repaired)
	}
	return "{}"
}
