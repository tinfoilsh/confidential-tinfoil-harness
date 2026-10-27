package server

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// searchSchemaJSON pins the fields that regressed with Llama: stringified
// integers, an enum-backed string, a string array and nested objects.
const searchSchemaJSON = `{
	"type": "object",
	"properties": {
		"query":             {"type": "string"},
		"max_results":       {"type": "integer", "minimum": 1, "maximum": 50},
		"max_content_chars": {"type": "integer"},
		"content_mode":      {"type": "string", "enum": ["highlights","text"]},
		"allowed_domains":   {"type": "array", "items": {"type": "string"}},
		"max_age_hours":     {"type": "integer"},
		"enable_pii_check":  {"type": "boolean"},
		"user_location": {
			"type": "object",
			"properties": {
				"approximate": {"type": "object", "properties": {"city": {"type": "string"}, "country": {"type": "string"}}}
			}
		}
	}
}`

func searchSchemas(t *testing.T) map[string]*jsonschema.Schema {
	t.Helper()
	var s jsonschema.Schema
	if err := json.Unmarshal([]byte(searchSchemaJSON), &s); err != nil {
		t.Fatal(err)
	}
	return map[string]*jsonschema.Schema{"search": &s}
}

func TestCoerce_IntegerOutOfRangeLeftUntouched(t *testing.T) {
	args := map[string]any{"max_results": "9999"}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if args["max_results"] != "9999" {
		t.Fatalf("out-of-range value coerced anyway: %v", args["max_results"])
	}
}

func TestCoerce_BooleanStringBecomesBool(t *testing.T) {
	args := map[string]any{"enable_pii_check": "true"}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if args["enable_pii_check"] != true {
		t.Fatalf("enable_pii_check: got %v, want true", args["enable_pii_check"])
	}
}

func TestCoerce_ArrayFromJSONString(t *testing.T) {
	args := map[string]any{"allowed_domains": `["python.org","docs.python.org"]`}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if want := []any{"python.org", "docs.python.org"}; !reflect.DeepEqual(args["allowed_domains"], want) {
		t.Fatalf("allowed_domains: got %[1]T(%[1]v), want %v", args["allowed_domains"], want)
	}
}

func TestCoerce_ArrayFromCSVString(t *testing.T) {
	args := map[string]any{"allowed_domains": "python.org, docs.python.org"}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if want := []any{"python.org", "docs.python.org"}; !reflect.DeepEqual(args["allowed_domains"], want) {
		t.Fatalf("allowed_domains: got %[1]T(%[1]v), want %v", args["allowed_domains"], want)
	}
}

func TestCoerce_SingleTokenNotSplitAsCSV(t *testing.T) {
	args := map[string]any{"allowed_domains": "python.org"}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if got := args["allowed_domains"]; got != "python.org" {
		t.Fatalf("single-token CSV mis-coerced to %[1]T(%[1]v)", got)
	}
}

func TestCoerce_NestedObjectFieldsCoerced(t *testing.T) {
	args := map[string]any{"user_location": map[string]any{"approximate": map[string]any{"city": "London", "country": 44.0}}}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if want := map[string]any{"user_location": map[string]any{"approximate": map[string]any{"city": "London", "country": "44"}}}; !reflect.DeepEqual(args, want) {
		t.Fatalf("nested fields: got %#v, want %#v", args, want)
	}
}

func TestCoerce_NestedObjectFromJSONStringExpands(t *testing.T) {
	args := map[string]any{"user_location": `{"approximate":{"city":"London","country":"GB"}}`}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	loc, ok := args["user_location"].(map[string]any)
	if !ok {
		t.Fatalf("user_location: got %T, want map[string]any", args["user_location"])
	}
	approx, ok := loc["approximate"].(map[string]any)
	if !ok || approx["city"] != "London" || approx["country"] != "GB" {
		t.Fatalf("expanded nested values wrong: %v", loc)
	}
}

func TestCoerce_FloatWholeNumberBecomesInt(t *testing.T) {
	args := map[string]any{"max_results": 5.0}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if got, ok := args["max_results"].(int64); !ok || got != 5 {
		t.Fatalf("max_results: got %[1]T(%[1]v), want int64(5)", args["max_results"])
	}
}

func TestCoerce_FloatOverflowLeftUntouched(t *testing.T) {
	for name, value := range map[string]any{"float64_1e100": 1e100, "float64_neg_1e100": -1e100, "float32_huge": float32(1e30)} {
		t.Run(name, func(t *testing.T) {
			args := map[string]any{"max_content_chars": value}
			domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
			if !reflect.DeepEqual(args["max_content_chars"], value) {
				t.Fatalf("value mutated: got %[1]T(%[1]v), want %[2]T(%[2]v)", args["max_content_chars"], value)
			}
		})
	}
}

func TestCoerce_FloatNonFiniteLeftUntouched(t *testing.T) {
	args := map[string]any{"max_content_chars": math.NaN()}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if _, converted := args["max_content_chars"].(int64); converted {
		t.Fatalf("non-finite value silently coerced to int64: %v", args["max_content_chars"])
	}
}

func TestCoerce_StringScientificOverflowLeftUntouched(t *testing.T) {
	args := map[string]any{"max_content_chars": "1e100"}
	domain.CoerceArgumentsToSchema("search", args, searchSchemas(t))
	if args["max_content_chars"] != "1e100" {
		t.Fatalf("string overflow mutated to %v", args["max_content_chars"])
	}
}
