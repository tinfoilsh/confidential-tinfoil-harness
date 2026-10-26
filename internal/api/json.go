package api

import (
	"encoding/json"
	"time"
)

// Object is JSON whose shape the harness does not own: tool output, model
// messages, downstream errors, and the model catalog.
type Object map[string]any

func Raw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

func Str(v any) string { s, _ := v.(string); return s }
func Obj(v any) Object {
	switch m := v.(type) {
	case Object:
		return m
	case map[string]any:
		return Object(m)
	}
	return Object{}
}
func Arr(v any) []any {
	a, _ := v.([]any)
	if values, ok := v.([]Object); ok {
		for _, value := range values {
			a = append(a, value)
		}
	}
	if a == nil {
		return []any{}
	}
	return a
}
func Bool(v any, fallback bool) bool {
	b, ok := v.(bool)
	if ok {
		return b
	}
	return fallback
}
func Int(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

func Now() time.Time    { return time.Now().UTC() }
func Timestamp() string { return Now().Format(time.RFC3339Nano) }
func FirstString(values ...any) string {
	for _, v := range values {
		if s := Str(v); s != "" {
			return s
		}
	}
	return ""
}
