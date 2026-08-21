// Package tools is the harness's tool surface. A Tool is the same to the loop
// whether it runs here or in an enclave; nothing here verifies an enclave.
package tools

import (
	"context"
	"encoding/json"
	"fmt"
)

// Progress reports that the call named by token advanced; detail is uninterpreted.
type Progress func(token string, detail json.RawMessage)

// Result is one call's outcome: what the model reads, plus Meta for the caller.
type Result struct {
	Output string
	Meta   json.RawMessage
}

// A Tool is one capability the model can call; token identifies it in progress.
type Tool struct {
	Name        string
	Description string
	Schema      json.RawMessage
	Call        func(ctx context.Context, token string, args json.RawMessage) (Result, error)
}

// A Source contributes tools to one request, opened per request because a
// remote source holds a session for the calls it will serve.
type Source interface {
	Open(ctx context.Context, progress Progress) (tools []Tool, release func(), err error)
}

// Set is one request's tool surface, whatever it was assembled from; Defs is
// that surface as the model is told about it, in the order the sources gave it
// so that one caller's turns keep hitting the same prompt cache.
type Set struct {
	Defs    json.RawMessage
	tools   map[string]Tool
	release []func()
}

// Open assembles one request's tools; a duplicate name fails rather than resolving.
func Open(ctx context.Context, progress Progress, sources ...Source) (*Set, error) {
	set := &Set{tools: map[string]Tool{}}
	defs := []any{}
	for _, source := range sources {
		opened, release, err := source.Open(ctx, progress)
		if err != nil {
			set.Close()
			return nil, err
		}
		set.release = append(set.release, release)
		for _, tool := range opened {
			if _, taken := set.tools[tool.Name]; taken {
				set.Close()
				return nil, fmt.Errorf("two sources offer a tool named %q", tool.Name)
			}
			set.tools[tool.Name] = tool
			schema := tool.Schema
			if len(schema) == 0 { // a schemaless tool takes an object
				schema = json.RawMessage(`{"type":"object"}`)
			}
			defs = append(defs, map[string]any{"type": "function", "function": map[string]any{
				"name": tool.Name, "description": tool.Description, "parameters": schema}})
		}
	}
	encoded, err := json.Marshal(defs)
	if err != nil {
		set.Close()
		return nil, err
	}
	set.Defs = encoded
	return set, nil
}

func (s *Set) Call(ctx context.Context, name, token string, args json.RawMessage) (Result, error) {
	tool, ok := s.tools[name]
	if !ok {
		return Result{}, fmt.Errorf("no such tool %q", name)
	}
	return tool.Call(ctx, token, args)
}

func (s *Set) Close() {
	for _, release := range s.release {
		release()
	}
	s.release = nil
}
