package domain

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Models routinely send tool arguments whose JSON types differ from the tool's
// published schema (Llama stringifies every number), and the enclave's
// validator then rejects the call with an error the model cannot act on.
// CoerceArgumentsToSchema converts such values before the call leaves, but
// only when the result clearly satisfies the schema; anything else is left for
// the enclave to reject. Ported from the model router (toolruntime/coerce.go).

// coerceMaxIntFromFloat keeps "1e100" from saturating into an int64.
const coerceMaxIntFromFloat = float64(1 << 53)

// SchemaLookup parses each published InputSchema by tool name, omitting any that do not parse.
func SchemaLookup(tools []*mcp.Tool) map[string]*jsonschema.Schema {
	if len(tools) == 0 {
		return nil
	}
	out := make(map[string]*jsonschema.Schema, len(tools))
	for _, tool := range tools {
		if tool == nil || tool.InputSchema == nil {
			continue
		}
		if schema, err := parseInputSchema(tool.InputSchema); err == nil && schema != nil {
			out[tool.Name] = schema
		}
	}
	return out
}

func parseInputSchema(raw any) (*jsonschema.Schema, error) {
	switch v := raw.(type) {
	case *jsonschema.Schema:
		return v, nil
	case jsonschema.Schema:
		return &v, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(encoded, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

// CoerceArgumentsToSchema rewrites arguments in place so each field matches its
// declared type, leaving any field whose coerced value would break the schema.
func CoerceArgumentsToSchema(toolName string, arguments map[string]any, schemas map[string]*jsonschema.Schema) {
	if len(arguments) == 0 || len(schemas) == 0 {
		return
	}
	if schema := schemas[toolName]; schema != nil {
		coerceObject(arguments, schema)
	}
}

func coerceObject(obj map[string]any, schema *jsonschema.Schema) {
	if obj == nil || schema == nil {
		return
	}
	for key, value := range obj {
		if sub := propertySchema(schema, key); sub != nil {
			if coerced, changed := coerceValue(value, sub); changed {
				obj[key] = coerced
			}
		}
	}
}

// propertySchema finds a property's schema directly or through allOf, anyOf and oneOf.
func propertySchema(parent *jsonschema.Schema, key string) *jsonschema.Schema {
	if parent == nil {
		return nil
	}
	if sub, ok := parent.Properties[key]; ok && sub != nil {
		return sub
	}
	for _, branches := range [][]*jsonschema.Schema{parent.AllOf, parent.AnyOf, parent.OneOf} {
		for _, branch := range branches {
			if sub := propertySchema(branch, key); sub != nil {
				return sub
			}
		}
	}
	return nil
}

func declaredTypes(schema *jsonschema.Schema) []string {
	if schema == nil {
		return nil
	}
	if schema.Type != "" {
		return []string{schema.Type}
	}
	var out []string
	seen := map[string]bool{}
	for _, t := range schema.Types {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// coerceValue returns value converted to the schema's declared type, and
// whether it was converted. Values that already match, or whose conversion
// would violate enum or range constraints, come back unchanged.
func coerceValue(value any, schema *jsonschema.Schema) (any, bool) {
	if schema == nil {
		return value, false
	}
	types := declaredTypes(schema)
	if len(types) == 0 || valueMatchesAnyType(value, types) {
		return descendIntoContainers(value, schema)
	}
	for _, t := range types {
		var coerced any
		ok := false
		switch t {
		case "integer":
			coerced, ok = coerceToInteger(value, schema)
		case "number":
			coerced, ok = coerceToNumber(value, schema)
		case "boolean":
			coerced, ok = coerceToBoolean(value)
		case "string":
			coerced, ok = coerceToString(value)
		case "array":
			coerced, ok = coerceToArray(value, schema)
		case "object":
			coerced, ok = coerceToObject(value, schema)
		case "null":
			coerced, ok = coerceToNull(value)
		}
		if ok {
			return coerced, true
		}
	}
	return descendIntoContainers(value, schema)
}

// descendIntoContainers still coerces the leaves of a correctly typed container.
func descendIntoContainers(value any, schema *jsonschema.Schema) (any, bool) {
	switch v := value.(type) {
	case map[string]any:
		coerceObject(v, schema)
	case []any:
		coerceItems(v, schema)
	}
	return value, false
}

func coerceItems(items []any, schema *jsonschema.Schema) {
	if schema.Items == nil {
		return
	}
	for i, item := range items {
		if coerced, changed := coerceValue(item, schema.Items); changed {
			items[i] = coerced
		}
	}
}

func valueMatchesAnyType(value any, types []string) bool {
	for _, t := range types {
		if valueMatchesType(value, t) {
			return true
		}
	}
	return false
}

func valueMatchesType(value any, t string) bool {
	switch t {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "integer":
		return isIntegerValue(value)
	case "number":
		return isNumericValue(value)
	case "array":
		_, ok := value.([]any)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "null":
		return value == nil
	}
	return false
}

func isNumericValue(value any) bool {
	switch value.(type) {
	case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return true
	}
	return false
}

// isIntegerValue excludes whole floats, so 5.0 still becomes an int64 for
// validators that check the Go type rather than the value.
func isIntegerValue(value any) bool {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case json.Number:
		_, err := v.Int64()
		return err == nil
	}
	return false
}

func coerceToInteger(value any, schema *jsonschema.Schema) (int64, bool) {
	switch v := value.(type) {
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return 0, false
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, integerSatisfiesSchema(n, schema)
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			if n, ok := floatToBoundedInt64(f); ok && integerSatisfiesSchema(n, schema) {
				return n, true
			}
		}
	case float32:
		if n, ok := floatToBoundedInt64(float64(v)); ok && integerSatisfiesSchema(n, schema) {
			return n, true
		}
	case float64:
		if n, ok := floatToBoundedInt64(v); ok && integerSatisfiesSchema(n, schema) {
			return n, true
		}
	case json.Number:
		if n, err := v.Int64(); err == nil && integerSatisfiesSchema(n, schema) {
			return n, true
		}
	}
	return 0, false
}

// floatToBoundedInt64 accepts only finite whole numbers within coerceMaxIntFromFloat.
func floatToBoundedInt64(f float64) (int64, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f || math.Abs(f) > coerceMaxIntFromFloat {
		return 0, false
	}
	return int64(f), true
}

func coerceToNumber(value any, schema *jsonschema.Schema) (float64, bool) {
	switch v := value.(type) {
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil && numberSatisfiesSchema(f, schema) {
			return f, true
		}
	case json.Number:
		if f, err := v.Float64(); err == nil && numberSatisfiesSchema(f, schema) {
			return f, true
		}
	}
	return 0, false
}

func coerceToBoolean(value any) (bool, bool) {
	switch v := value.(type) {
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "1", "yes", "on", "y", "t":
			return true, true
		case "false", "0", "no", "off", "n", "f":
			return false, true
		}
	case float64:
		if v == 1 || v == 0 {
			return v == 1, true
		}
	case int:
		if v == 1 || v == 0 {
			return v == 1, true
		}
	}
	return false, false
}

func coerceToString(value any) (string, bool) {
	switch v := value.(type) {
	case float64:
		if v == math.Trunc(v) {
			return strconv.FormatInt(int64(v), 10), true
		}
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(v), true
	case json.Number:
		return v.String(), true
	}
	return "", false
}

// coerceToArray accepts a JSON array literal or a comma-separated list.
func coerceToArray(value any, schema *jsonschema.Schema) ([]any, bool) {
	switch v := value.(type) {
	case []any:
		coerceItems(v, schema)
		return v, false
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return nil, false
		}
		var parsed []any
		if json.Unmarshal([]byte(s), &parsed) == nil {
			coerceItems(parsed, schema)
			return parsed, true
		}
		if looksLikeCSV(s) {
			var list []any
			for _, part := range splitCSV(s) {
				list = append(list, part)
			}
			coerceItems(list, schema)
			return list, true
		}
	}
	return nil, false
}

func coerceToObject(value any, schema *jsonschema.Schema) (map[string]any, bool) {
	switch v := value.(type) {
	case map[string]any:
		coerceObject(v, schema)
		return v, false
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return nil, false
		}
		var parsed map[string]any
		if json.Unmarshal([]byte(s), &parsed) == nil {
			coerceObject(parsed, schema)
			return parsed, true
		}
	}
	return nil, false
}

func coerceToNull(value any) (any, bool) {
	s, ok := value.(string)
	return nil, ok && strings.EqualFold(strings.TrimSpace(s), "null")
}

func integerSatisfiesSchema(n int64, schema *jsonschema.Schema) bool {
	return numberSatisfiesSchema(float64(n), schema)
}

func numberSatisfiesSchema(f float64, schema *jsonschema.Schema) bool {
	switch {
	case schema.Minimum != nil && f < *schema.Minimum,
		schema.Maximum != nil && f > *schema.Maximum,
		schema.ExclusiveMinimum != nil && f <= *schema.ExclusiveMinimum,
		schema.ExclusiveMaximum != nil && f >= *schema.ExclusiveMaximum,
		schema.MultipleOf != nil && *schema.MultipleOf != 0 && math.Mod(f, *schema.MultipleOf) != 0:
		return false
	}
	return enumContainsNumeric(schema.Enum, f)
}

// enumContainsNumeric is true without an enum, or when one entry equals candidate.
func enumContainsNumeric(enum []any, candidate float64) bool {
	if len(enum) == 0 {
		return true
	}
	for _, entry := range enum {
		switch e := entry.(type) {
		case float64:
			if e == candidate {
				return true
			}
		case int:
			if float64(e) == candidate {
				return true
			}
		case int64:
			if float64(e) == candidate {
				return true
			}
		case json.Number:
			if f, err := e.Float64(); err == nil && f == candidate {
				return true
			}
		}
	}
	return false
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// looksLikeCSV keeps a single token such as "python.org" from becoming an array.
func looksLikeCSV(s string) bool {
	return strings.Contains(s, ",") && len(splitCSV(s)) >= 2
}

// HumanizeToolArgError turns a validator error into one sentence naming the
// field, which a model can correct on its next turn.
func HumanizeToolArgError(toolName string, err error, arguments map[string]any) string {
	if err == nil {
		return ""
	}
	raw := err.Error()
	summary := extractValidatorSummary(raw)
	if summary == "" {
		return fmt.Sprintf("Tool %q rejected the arguments: %s", toolName, raw)
	}
	args, _ := json.Marshal(arguments)
	return fmt.Sprintf("Tool %q rejected the arguments. %s Original args: %s", toolName, summary, args)
}

// extractValidatorSummary reads jsonschema-go's
// `validating /properties/<field>: type: X has type "Y", want "Z"`.
func extractValidatorSummary(raw string) string {
	fieldIdx := strings.Index(raw, "/properties/")
	if fieldIdx == -1 {
		return ""
	}
	tail := raw[fieldIdx+len("/properties/"):]
	colon := strings.Index(tail, ":")
	if colon == -1 {
		return ""
	}
	field := strings.TrimSpace(tail[:colon])
	rest := tail[colon+1:]
	typeIdx := strings.Index(rest, "type:")
	if typeIdx == -1 {
		return fmt.Sprintf("Invalid field %q.", field)
	}
	expected, actual := parseTypeMismatch(rest[typeIdx+len("type:"):])
	if expected == "" || actual == "" {
		return fmt.Sprintf("Invalid field %q.", field)
	}
	return fmt.Sprintf("Field %q must be %s (received %s). Retry with the corrected type.", field, expected, actual)
}

func parseTypeMismatch(s string) (expected, actualQuoted string) {
	wantIdx := strings.Index(s, "want ")
	if wantIdx == -1 {
		return "", ""
	}
	want := strings.Trim(strings.TrimSpace(s[wantIdx+len("want "):]), "\"' .,")
	has := strings.Index(s[:wantIdx], "has type ")
	if has == -1 {
		return want, ""
	}
	return want, strings.TrimRight(strings.TrimSpace(s[has+len("has type "):wantIdx]), " ,")
}
