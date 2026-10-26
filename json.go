package main

import (
	"encoding/json"
	"time"
)

// object is JSON whose shape the harness does not own: tool output, model
// messages, downstream errors, and the model catalog.
type object map[string]any

func raw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

func str(v any) string { s, _ := v.(string); return s }
func obj(v any) object {
	switch m := v.(type) {
	case object:
		return m
	case map[string]any:
		return object(m)
	}
	return object{}
}
func arr(v any) []any {
	a, _ := v.([]any)
	if values, ok := v.([]object); ok {
		for _, value := range values {
			a = append(a, value)
		}
	}
	if a == nil {
		return []any{}
	}
	return a
}
func boolean(v any, fallback bool) bool {
	b, ok := v.(bool)
	if ok {
		return b
	}
	return fallback
}
func integer(v any) int {
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

func nowUTC() time.Time { return time.Now().UTC() }
func timestamp() string { return nowUTC().Format(time.RFC3339Nano) }
func firstString(values ...any) string {
	for _, v := range values {
		if s := str(v); s != "" {
			return s
		}
	}
	return ""
}
