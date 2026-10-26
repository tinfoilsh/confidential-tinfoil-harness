package domain

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// MaxWidgetRetries bounds how often one run tells the model a widget's
// arguments failed validation; two cover fixing one field surfacing another.
const MaxWidgetRetries = 2

// WidgetArguments is what a widget call is drawn and validated with: repaired,
// then canonical.
func WidgetArguments(raw string) string {
	if repaired, ok := SanitizeToolCallArgumentsJSON([]byte(raw)); ok && json.Valid(repaired) {
		raw = string(repaired)
	}
	return CanonicalizeWidgetArguments(raw)
}

// CanonicalizeWidgetArguments unwraps nested arrays and objects sent as JSON
// strings, such as DeepSeek's {"stats":"[{...}]"}, which widget schemas
// reject. Arguments that need no unwrapping come back byte-for-byte, and
// anything that does not parse comes back untouched.
func CanonicalizeWidgetArguments(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed[0] != '{' && trimmed[0] != '[' {
		return raw
	}
	decoded, err := decodeJSONValue(trimmed)
	if err != nil {
		return raw
	}
	unwrapped, changed := deepUnwrapJSONStrings(decoded)
	if !changed {
		return raw
	}
	encoded, err := json.Marshal(unwrapped)
	if err != nil {
		return raw
	}
	return string(encoded)
}

// deepUnwrapJSONStrings replaces every string holding a JSON array or object
// with its decoded value, reporting whether any was replaced.
func deepUnwrapJSONStrings(value any) (any, bool) {
	switch v := value.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" || trimmed[0] != '{' && trimmed[0] != '[' {
			return v, false
		}
		nested, err := decodeJSONValue(trimmed)
		if err != nil {
			return v, false
		}
		unwrapped, _ := deepUnwrapJSONStrings(nested)
		return unwrapped, true
	case []any:
		out, changed := make([]any, len(v)), false
		for i, item := range v {
			var itemChanged bool
			out[i], itemChanged = deepUnwrapJSONStrings(item)
			changed = changed || itemChanged
		}
		return out, changed
	case map[string]any:
		out, changed := make(map[string]any, len(v)), false
		for k, item := range v {
			var itemChanged bool
			out[k], itemChanged = deepUnwrapJSONStrings(item)
			changed = changed || itemChanged
		}
		return out, changed
	}
	return value, false
}

// decodeJSONValue keeps numbers exact and refuses trailing data.
func decodeJSONValue(raw string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return decoded, nil
}

// WidgetFailure is what a widget call whose arguments cannot be drawn
// returns, or nil when they can.
func WidgetFailure(w Widget, args string) json.RawMessage {
	var value any
	problem := "arguments are not valid JSON"
	if json.Unmarshal([]byte(cmp.Or(args, "{}")), &value) == nil {
		err := w.Resolved.Validate(value)
		if err == nil {
			return nil
		}
		problem = strings.TrimPrefix(err.Error(), "validating root: ")
	}
	return api.Raw(api.Object{"status": "error", "error": fmt.Sprintf("The arguments for %s did not match the tool's parameter schema and the component was not displayed: %s. Call %s again with corrected arguments that match the schema exactly.", w.Name, problem, w.Name)})
}

// CheckedWidgets is a copy of a timeline for its client, in which each widget
// call is marked when its stored arguments fail the check new ones pass
// before they are sent.
func CheckedWidgets(timeline []Block) []Block {
	checked := clonedSlice(timeline)
	for i := range checked {
		b := &checked[i]
		b.InvalidArguments = false
		if b.Type != "tool_call" {
			continue
		}
		for _, w := range WidgetCatalog() {
			if w.Name == b.Name {
				b.InvalidArguments = WidgetFailure(w, b.Arguments) != nil
			}
		}
	}
	return checked
}

// CheckWidgets marks the widget calls of a thread its client is sent.
func (t *Thread) CheckWidgets() {
	for i := range t.Messages {
		t.Messages[i].Timeline = CheckedWidgets(t.Messages[i].Timeline)
	}
}

func resolveSchema(raw json.RawMessage) (*jsonschema.Resolved, error) {
	var schema jsonschema.Schema
	// zod-to-json-schema's OpenAPI target uses draft-4 exclusive bounds.
	// Keep the advertised schema identical to the client reference and convert
	// those bounds for the draft-2020 validator.
	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return nil, err
	}
	normalizeSchema(normalized)
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(encoded, &schema); err != nil {
		return nil, err
	}
	return schema.Resolve(nil)
}

func normalizeSchema(value any) {
	switch node := value.(type) {
	case map[string]any:
		for _, bound := range []string{"Minimum", "Maximum"} {
			exclusive, ok := node["exclusive"+bound].(bool)
			if ok {
				delete(node, "exclusive"+bound)
				field := strings.ToLower(bound)
				if exclusive {
					node["exclusive"+bound] = node[field]
					delete(node, field)
				}
			}
		}
		// As the client's schemas did, a field no property declares is
		// ignored rather than refused.
		if open, ok := node["additionalProperties"].(bool); ok && !open {
			delete(node, "additionalProperties")
		}
		if nullable, _ := node["nullable"].(bool); nullable {
			if kind, ok := node["type"].(string); ok {
				node["type"] = []any{kind, "null"}
			}
			delete(node, "nullable")
		}
		for _, child := range node {
			normalizeSchema(child)
		}
	case []any:
		for _, child := range node {
			normalizeSchema(child)
		}
	}
}

// NormalizeToolResult is the caller's copy of a tool result. Its text shares
// one budget, spent after parsing so that a large result still parses.
func NormalizeToolResult(name, arguments, content string) any {
	var result any
	if json.Unmarshal([]byte(content), &result) != nil {
		result = api.Object{"output": content}
	}
	value := api.Obj(result)
	args := api.Obj(api.Parsed([]byte(HistoryArgs(arguments))))
	if strings.HasPrefix(name, "render_") {
		return result
	}
	budget := MaxShownOutput
	take := func(s string) string {
		if len(s) > budget {
			s = s[:budget] + "\n[truncated]"
		}
		budget -= min(budget, len(s))
		return s
	}
	switch name {
	case "web_search":
		rows, seen := []api.Object{}, map[string]bool{}
		for _, r := range api.Arr(value["results"]) {
			row := api.Obj(r)
			url := strings.TrimSpace(api.Str(row["url"]))
			if url == "" || seen[url] {
				continue
			}
			seen[url] = true
			source := api.Object{"url": url, "title": strings.TrimSpace(api.Str(row["title"])), "snippet": take(api.FirstString(row["snippet"], row["content"], row["text"]))}
			if published := strings.TrimSpace(api.Str(row["published_date"])); published != "" {
				source["publishedDate"] = published
			}
			if author := strings.TrimSpace(api.Str(row["author"])); author != "" {
				source["author"] = author
			}
			rows = append(rows, source)
		}
		shown := api.Object{"query": args["query"], "results": rows}
		if api.Bool(value["pii_checked"], false) {
			shown["pii"] = piiReport(value)
		}
		return shown
	case "web_fetch":
		shown := api.Object{"url": api.FirstString(api.Arr(args["urls"])...), "title": "Fetched page", "text": ""}
		pages, failed := []api.Object{}, []api.Object{}
		for _, p := range api.Arr(value["pages"]) {
			page := api.Obj(p)
			url := strings.TrimSpace(api.Str(page["url"]))
			if url == "" {
				continue
			}
			text := take(api.FirstString(page["content"], page["text"]))
			if len(pages) == 0 {
				// Shown twice, as the result and as its first page, so spent twice.
				shown["url"], shown["text"] = url, take(text)
			}
			pages = append(pages, api.Object{"url": url, "title": "Fetched page", "text": text})
		}
		for _, r := range api.Arr(value["results"]) {
			if result := api.Obj(r); api.Str(result["status"]) != "completed" {
				failed = append(failed, api.Object{"url": result["url"], "error": result["error"]})
			}
		}
		shown["pages"], shown["failed"] = pages, failed
		return shown
	case "present":
		if _, ok := value["filename"]; ok {
			return value
		}
		name := path.Base(api.Str(args["path"]))
		language, body := "", content
		if header, rest, ok := strings.Cut(content, "\n"); ok {
			fence := header[:len(header)-len(strings.TrimLeft(header, "`"))]
			if len(fence) >= 3 {
				language = strings.TrimPrefix(header, fence)
				body = strings.TrimSuffix(rest, "\n"+fence)
			}
		}
		return api.Object{"filename": name, "language": language, "content": take(body)}
	default:
		return api.Object{"output": take(cmp.Or(api.FirstString(value["output"], value["stdout"]), content))}
	}
}

// piiReport is what the privacy filter masked from a search's query, in the
// model router's shape. Offsets are code points into the query the model
// sent; the removed text itself never leaves the enclave.
func piiReport(result api.Object) api.Object {
	offset := func(v any) (int64, bool) {
		f, ok := v.(float64)
		if !ok {
			return 0, false
		}
		return floatToBoundedInt64(f)
	}
	redactions := []api.Object{}
	for _, item := range api.Arr(result["pii_redactions"]) {
		span := api.Obj(item)
		start, startOK := offset(span["start"])
		end, endOK := offset(span["end"])
		if startOK && endOK {
			redactions = append(redactions, api.Object{"type": api.Str(span["type"]), "start": start, "end": end})
		}
	}
	return api.Object{"masked": api.Bool(result["pii_masked"], false), "redacted_query": api.Str(result["redacted_query"]), "redactions": redactions}
}

type ReplacedWidget struct {
	Arguments string          `json:"-"`
	Result    json.RawMessage `json:"-"`
}

// A retry must not revive a deleted widget or overwrite a concurrent writer.
// expected is the widget's arguments when the retry started.
func ReplaceWidget(t *Thread, messageID, callID, expected string, replacement ReplacedWidget) error {
	for i := range t.Messages {
		m := &t.Messages[i]
		if m.ID != messageID {
			continue
		}
		for j := range m.Timeline {
			block := &m.Timeline[j]
			if block.CallID() != callID {
				continue
			}
			if block.Arguments != expected && block.Arguments != replacement.Arguments {
				return api.Err(409, "REVISION_CONFLICT", "The widget changed during its retry")
			}
			block.Arguments, block.Result = replacement.Arguments, replacement.Result
			return nil
		}
	}
	return api.Err(409, "REVISION_CONFLICT", "The widget was removed during its retry")
}

const (
	// MaxShownOutput bounds a tool result as the caller sees it.
	MaxShownOutput = 1 << 20
	// ToolError is the result of a tool call that failed.
	ToolError = `{"error":"tool_error"}`
)

// HistoryArgs is what a replayed tool call carries: one call whose arguments
// do not parse would otherwise fail every later request that repeats it.
func HistoryArgs(s string) string {
	if json.Valid([]byte(s)) {
		return s
	}
	if repaired, ok := SanitizeToolCallArgumentsJSON([]byte(s)); ok && json.Valid(repaired) {
		return string(repaired)
	}
	return "{}"
}
