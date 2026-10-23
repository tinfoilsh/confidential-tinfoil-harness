package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
)

// maxWidgetRetries bounds how often one run tells the model a widget's
// arguments failed validation; two cover fixing one field surfacing another.
const maxWidgetRetries = 2

// widgetArguments is what a widget call is drawn and validated with: repaired,
// then canonical.
func widgetArguments(raw string) string {
	if repaired, ok := sanitizeToolCallArgumentsJSON([]byte(raw)); ok && json.Valid(repaired) {
		raw = string(repaired)
	}
	return canonicalizeWidgetArguments(raw)
}

// canonicalizeWidgetArguments unwraps nested arrays and objects sent as JSON
// strings, such as DeepSeek's {"stats":"[{...}]"}, which widget schemas
// reject. Arguments that need no unwrapping come back byte-for-byte, and
// anything that does not parse comes back untouched.
func canonicalizeWidgetArguments(raw string) string {
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

// widgetFailure is what a widget call whose arguments cannot be drawn
// returns, or nil when they can.
func widgetFailure(w widget, args string) json.RawMessage {
	var value any
	problem := "arguments are not valid JSON"
	if json.Unmarshal([]byte(cmp.Or(args, "{}")), &value) == nil {
		err := w.schema.Validate(value)
		if err == nil {
			return nil
		}
		problem = strings.TrimPrefix(err.Error(), "validating root: ")
	}
	return raw(object{"status": "error", "error": fmt.Sprintf("The arguments for %s did not match the tool's parameter schema and the component was not displayed: %s. Call %s again with corrected arguments that match the schema exactly.", w.Name, problem, w.Name)})
}

// checkedWidgets is a copy of a timeline for its client, in which each widget
// call is marked when its stored arguments fail the check new ones pass
// before they are sent.
func checkedWidgets(timeline []Block) []Block {
	checked := clonedSlice(timeline)
	for i := range checked {
		b := &checked[i]
		b.InvalidArguments = false
		if b.Type != "tool_call" {
			continue
		}
		for _, w := range widgetCatalog() {
			if w.Name == b.Name {
				b.InvalidArguments = widgetFailure(w, b.Arguments) != nil
			}
		}
	}
	return checked
}

// checkWidgets marks the widget calls of a thread its client is sent.
func (t *Thread) checkWidgets() {
	for i := range t.Messages {
		t.Messages[i].Timeline = checkedWidgets(t.Messages[i].Timeline)
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
func (h *harness) widgetResult(ctx context.Context, call toolCall) (any, error) {
	if call.name != "render_link_preview" {
		return "[rendered for the user]", nil
	}
	var args object
	if json.Unmarshal([]byte(call.args), &args) != nil {
		return nil, apiErr(400, "BAD_REQUEST", "Invalid widget arguments")
	}
	return h.linkDetails(ctx, str(args["url"]))
}

// normalizeToolResult is the caller's copy of a tool result. Its text shares
// one budget, spent after parsing so that a large result still parses.
func normalizeToolResult(call toolCall, content string) any {
	var result any
	if json.Unmarshal([]byte(content), &result) != nil {
		result = object{"output": content}
	}
	value := obj(result)
	args := obj(decodeValue([]byte(historyArgs(call.args))))
	if strings.HasPrefix(call.name, "render_") {
		return result
	}
	budget := maxShownOutput
	take := func(s string) string {
		if len(s) > budget {
			s = s[:budget] + "\n[truncated]"
		}
		budget -= min(budget, len(s))
		return s
	}
	switch call.name {
	case "web_search":
		rows, seen := []object{}, map[string]bool{}
		for _, r := range arr(value["results"]) {
			row := obj(r)
			url := strings.TrimSpace(str(row["url"]))
			if url == "" || seen[url] {
				continue
			}
			seen[url] = true
			source := object{"url": url, "title": strings.TrimSpace(str(row["title"])), "snippet": take(firstString(row["snippet"], row["content"], row["text"]))}
			if published := strings.TrimSpace(str(row["published_date"])); published != "" {
				source["publishedDate"] = published
			}
			if author := strings.TrimSpace(str(row["author"])); author != "" {
				source["author"] = author
			}
			rows = append(rows, source)
		}
		shown := object{"query": args["query"], "results": rows}
		if boolean(value["pii_checked"], false) {
			shown["pii"] = piiReport(value)
		}
		return shown
	case "web_fetch":
		shown := object{"url": firstString(arr(args["urls"])...), "title": "Fetched page", "text": ""}
		pages, failed := []object{}, []object{}
		for _, p := range arr(value["pages"]) {
			page := obj(p)
			url := strings.TrimSpace(str(page["url"]))
			if url == "" {
				continue
			}
			text := take(firstString(page["content"], page["text"]))
			if len(pages) == 0 {
				// Shown twice, as the result and as its first page, so spent twice.
				shown["url"], shown["text"] = url, take(text)
			}
			pages = append(pages, object{"url": url, "title": "Fetched page", "text": text})
		}
		for _, r := range arr(value["results"]) {
			if result := obj(r); str(result["status"]) != "completed" {
				failed = append(failed, object{"url": result["url"], "error": result["error"]})
			}
		}
		shown["pages"], shown["failed"] = pages, failed
		return shown
	case "present":
		if _, ok := value["filename"]; ok {
			return value
		}
		name := path.Base(str(args["path"]))
		language, body := "", content
		if header, rest, ok := strings.Cut(content, "\n"); ok {
			fence := header[:len(header)-len(strings.TrimLeft(header, "`"))]
			if len(fence) >= 3 {
				language = strings.TrimPrefix(header, fence)
				body = strings.TrimSuffix(rest, "\n"+fence)
			}
		}
		return object{"filename": name, "language": language, "content": take(body)}
	default:
		return object{"output": take(cmp.Or(firstString(value["output"], value["stdout"]), content))}
	}
}

// piiReport is what the privacy filter masked from a search's query, in the
// model router's shape. Offsets are code points into the query the model
// sent; the removed text itself never leaves the enclave.
func piiReport(result object) object {
	offset := func(v any) (int64, bool) {
		f, ok := v.(float64)
		if !ok {
			return 0, false
		}
		return floatToBoundedInt64(f)
	}
	redactions := []object{}
	for _, item := range arr(result["pii_redactions"]) {
		span := obj(item)
		start, startOK := offset(span["start"])
		end, endOK := offset(span["end"])
		if startOK && endOK {
			redactions = append(redactions, object{"type": str(span["type"]), "start": start, "end": end})
		}
	}
	return object{"masked": boolean(result["pii_masked"], false), "redacted_query": str(result["redacted_query"]), "redactions": redactions}
}

func (h *harness) retryWidget(ctx context.Context, r *chatRun, req *request, m *model) error {
	var declaration widget
	for _, w := range widgetCatalog() {
		if w.Name == r.retryTarget.block.Name {
			declaration = w
			break
		}
	}
	conversation, err := budgetConversation(req, false, nil)
	if err != nil {
		return err
	}
	conversation = append(conversation, raw(object{"role": "user", "content": "Regenerate the arguments for the " + declaration.Name + " widget. Preserve the user's intent and return only JSON matching its schema."}))
	payload := object{"model": req.model, "messages": conversation, "user_cache_secret": req.cacheScope, "response_format": object{"type": "json_schema", "json_schema": object{"name": "widget_arguments", "strict": true, "schema": declaration.Schema}}}
	for k, v := range req.params {
		payload[k] = v
	}
	usage := usageForRun(r)
	usage.BillCustomerRequest = true
	content, err := h.completion(context.WithValue(ctx, usageContextKey{}, usage), m, payload)
	if err != nil {
		return err
	}
	content = widgetArguments(strings.TrimSpace(content))
	var value any
	if json.Unmarshal([]byte(content), &value) != nil || declaration.schema.Validate(value) != nil {
		return apiErr(502, "UPSTREAM_REFUSED", "The replacement widget arguments do not match the schema")
	}
	id := r.input.ToolCallID
	r.out.callStart(id, declaration.Name, r.retryTarget.parentID)
	r.out.callArgs(id, content)
	r.out.callEnd(id)
	// The widget's result is the one a call with these arguments returns.
	result, err := h.widgetResult(ctx, toolCall{id: id, name: declaration.Name, args: content})
	if err != nil {
		result = object{"error": asAPIError(err).Message}
	}
	r.replacement = replacedWidget{arguments: content, result: raw(result)}
	r.out.emit(event{Type: "TOOL_CALL_RESULT", MessageID: "msg_" + token(), ToolCallID: id, Role: "tool", Content: r.replacement.result})
	return nil
}

// replacedWidget is what a retry gives its widget call.
type replacedWidget struct {
	arguments string
	result    json.RawMessage
}

// A retry must not revive a deleted widget or overwrite a concurrent writer.
// expected is the widget's arguments when the retry started.
func replaceWidget(t *Thread, messageID, callID, expected string, replacement replacedWidget) error {
	for i := range t.Messages {
		m := &t.Messages[i]
		if m.ID != messageID {
			continue
		}
		for j := range m.Timeline {
			block := &m.Timeline[j]
			if block.callID() != callID {
				continue
			}
			if block.Arguments != expected && block.Arguments != replacement.arguments {
				return apiErr(409, "REVISION_CONFLICT", "The widget changed during its retry")
			}
			block.Arguments, block.Result = replacement.arguments, replacement.result
			return nil
		}
	}
	return apiErr(409, "REVISION_CONFLICT", "The widget was removed during its retry")
}
