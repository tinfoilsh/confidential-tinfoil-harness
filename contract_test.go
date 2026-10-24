package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"go.yaml.in/yaml/v3"
)

// Everything the harness says in the tests is checked against the protocol
// it publishes: each request a route accepts, each response, each event of a
// run, and each refusal. The published schemas leave objects open, since a
// client must tolerate members a newer harness adds; the check closes them,
// so the harness cannot send a member the protocol does not name.

const protocolFile = "protocol/openapi.yaml"

type contract struct {
	doc  map[string]any
	defs map[string]any

	mu       sync.Mutex
	resolved map[string]*jsonschema.Resolved
}

func loadContract() (*contract, error) {
	data, err := os.ReadFile(protocolFile)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", protocolFile, err)
	}
	// Schemas refer to each other as OpenAPI places them; a validator reads
	// them as the definitions of one JSON Schema document.
	doc = rewriteRefs(doc, "#/components/schemas/", "#/$defs/").(map[string]any)
	components, _ := doc["components"].(map[string]any)
	defs, _ := components["schemas"].(map[string]any)
	return &contract{doc: doc, defs: defs, resolved: map[string]*jsonschema.Resolved{}}, nil
}

func rewriteRefs(v any, from, to string) any {
	switch v := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, child := range v {
			if ref, ok := child.(string); ok && k == "$ref" && strings.HasPrefix(ref, from) {
				out[k] = to + strings.TrimPrefix(ref, from)
			} else {
				out[k] = rewriteRefs(child, from, to)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			out[i] = rewriteRefs(child, from, to)
		}
		return out
	}
	return v
}

// closed is schema with every object a value is checked against on its own
// admitting no member its schema does not name. The parts of an allOf, and
// definitions, are not checked on their own: the schema that names them
// closes over all their members together. A schema that names no members,
// or declares the rest open, stays open.
func (c *contract) closed(schema any, whole bool) any {
	switch s := schema.(type) {
	case []any:
		out := make([]any, len(s))
		for i, child := range s {
			out[i] = c.closed(child, whole)
		}
		return out
	case map[string]any:
		out := maps.Clone(s)
		for _, k := range []string{"properties", "patternProperties", "dependentSchemas"} {
			if children, ok := s[k].(map[string]any); ok {
				shut := make(map[string]any, len(children))
				for name, child := range children {
					shut[name] = c.closed(child, true)
				}
				out[k] = shut
			}
		}
		if children, ok := s["$defs"].(map[string]any); ok {
			shut := make(map[string]any, len(children))
			for name, child := range children {
				shut[name] = c.closed(child, false)
			}
			out["$defs"] = shut
		}
		for _, k := range []string{"items", "additionalProperties", "unevaluatedProperties", "unevaluatedItems", "contains", "not", "if", "then", "else", "contentSchema", "prefixItems", "oneOf", "anyOf"} {
			if child, ok := s[k]; ok {
				out[k] = c.closed(child, true)
			}
		}
		if child, ok := s["allOf"]; ok {
			out["allOf"] = c.closed(child, false)
		}
		if whole && c.namesMembers(s) {
			out["unevaluatedProperties"] = false
		}
		return out
	}
	return schema
}

// namesMembers reports whether a schema, or the definition it refers to,
// names an object's members without leaving the rest open.
func (c *contract) namesMembers(schema any) bool {
	s, _ := schema.(map[string]any)
	for _, k := range []string{"additionalProperties", "unevaluatedProperties"} {
		if _, open := s[k]; open {
			return false
		}
	}
	for _, k := range []string{"properties", "allOf", "oneOf", "anyOf"} {
		if _, named := s[k]; named {
			return true
		}
	}
	if ref, ok := s["$ref"].(string); ok {
		return c.namesMembers(c.defs[strings.TrimPrefix(ref, "#/$defs/")])
	}
	return false
}

// at is the node of the protocol a path of member names leads to, following
// references to shared responses.
func (c *contract) at(path ...string) any {
	var node any = c.doc
	for _, name := range path {
		m, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		node = m[name]
		if m, ok := node.(map[string]any); ok {
			if ref, ok := m["$ref"].(string); ok && strings.HasPrefix(ref, "#/components/responses/") {
				node = c.at("components", "responses", strings.TrimPrefix(ref, "#/components/responses/"))
			}
		}
	}
	return node
}

// validator checks values against the schema at a path of the protocol.
func (c *contract) validator(path ...string) (*jsonschema.Resolved, error) {
	key := strings.Join(path, "\x00")
	c.mu.Lock()
	defer c.mu.Unlock()
	if resolved, ok := c.resolved[key]; ok {
		return resolved, nil
	}
	target, ok := c.at(path...).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the protocol has no schema at %s", strings.Join(path, "/"))
	}
	root := c.closed(maps.Clone(target), true).(map[string]any)
	root["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	root["$defs"] = c.closed(map[string]any{"$defs": c.defs}, false).(map[string]any)["$defs"]
	encoded, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(encoded, &schema); err != nil {
		return nil, fmt.Errorf("%s: %w", strings.Join(path, "/"), err)
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", strings.Join(path, "/"), err)
	}
	c.resolved[key] = resolved
	return resolved, nil
}

func (c *contract) validate(data []byte, path ...string) error {
	resolved, err := c.validator(path...)
	if err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("not JSON: %w", err)
	}
	if err := resolved.Validate(value); err != nil {
		return c.explain(value, c.at(path...), err)
	}
	return nil
}

// explain names what a validation failure is about: the members the
// protocol does not name, or the schema a discriminated value missed.
func (c *contract) explain(value, schema any, err error) error {
	if extra := c.undeclared(value, schema, ""); len(extra) > 0 {
		return fmt.Errorf("members the protocol does not name: %s", strings.Join(extra, ", "))
	}
	if branch, name := c.discriminated(value, schema); branch != nil {
		resolved, rerr := c.validator("components", "schemas", name)
		if rerr != nil {
			return rerr
		}
		if berr := resolved.Validate(value); berr != nil {
			return fmt.Errorf("%s: %v", name, berr)
		}
	} else if name != "" {
		return fmt.Errorf("%s is not a value the protocol names", name)
	}
	return err
}

// deref follows a schema's reference to a definition.
func (c *contract) deref(schema any) map[string]any {
	s, _ := schema.(map[string]any)
	for s != nil {
		ref, ok := s["$ref"].(string)
		if !ok || !strings.HasPrefix(ref, "#/$defs/") {
			return s
		}
		s, _ = c.defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	}
	return nil
}

// discriminated is the branch of a discriminated union a value names, and
// that branch's definition. A value naming no branch returns its name alone.
func (c *contract) discriminated(value, schema any) (map[string]any, string) {
	s := c.deref(schema)
	d, _ := s["discriminator"].(map[string]any)
	object, _ := value.(map[string]any)
	if d == nil || object == nil {
		return nil, ""
	}
	tag, _ := object[d["propertyName"].(string)].(string)
	mapping, _ := d["mapping"].(map[string]any)
	ref, ok := mapping[tag].(string)
	if !ok {
		return nil, fmt.Sprintf("%s %q", d["propertyName"], tag)
	}
	name := ref[strings.LastIndex(ref, "/")+1:]
	return c.deref(map[string]any{"$ref": "#/$defs/" + name}), name
}

// undeclared lists, as JSON pointers, the members of value its schema does
// not name.
func (c *contract) undeclared(value, schema any, at string) []string {
	s := c.deref(schema)
	if s == nil {
		return nil
	}
	if branch, _ := c.discriminated(value, s); branch != nil {
		return c.undeclared(value, branch, at)
	}
	for _, union := range []string{"oneOf", "anyOf"} {
		if branches, ok := s[union].([]any); ok {
			var best []string
			for i, branch := range branches {
				if !c.admitsType(value, branch) {
					continue
				}
				extra := c.undeclared(value, branch, at)
				if i == 0 || best == nil || len(extra) < len(best) {
					best = extra
				}
				if len(extra) == 0 {
					return nil
				}
			}
			return best
		}
	}
	switch v := value.(type) {
	case []any:
		var extra []string
		for i, item := range v {
			extra = append(extra, c.undeclared(item, s["items"], fmt.Sprintf("%s/%d", at, i))...)
		}
		return extra
	case map[string]any:
		properties := c.properties(s)
		rest := s["additionalProperties"]
		open := rest != false && (rest != nil || !c.namesMembers(s))
		var extra []string
		for _, name := range slices.Sorted(maps.Keys(v)) {
			switch property, named := properties[name]; {
			case named:
				extra = append(extra, c.undeclared(v[name], property, at+"/"+name)...)
			case open:
				extra = append(extra, c.undeclared(v[name], rest, at+"/"+name)...)
			default:
				extra = append(extra, at+"/"+name)
			}
		}
		return extra
	}
	return nil
}

// properties are the members a schema names, with those of its allOf parts.
func (c *contract) properties(s map[string]any) map[string]any {
	out := map[string]any{}
	for _, part := range func() []any { parts, _ := s["allOf"].([]any); return parts }() {
		maps.Copy(out, c.properties(c.deref(part)))
	}
	if named, ok := s["properties"].(map[string]any); ok {
		maps.Copy(out, named)
	}
	return out
}

// admitsType reports whether a branch of a union could hold the value's
// JSON type.
func (c *contract) admitsType(value, branch any) bool {
	s := c.deref(branch)
	types, declared := s["type"]
	if !declared {
		return true
	}
	kind := map[bool]string{true: "object"}[func() bool { _, ok := value.(map[string]any); return ok }()]
	switch value.(type) {
	case nil:
		kind = "null"
	case []any:
		kind = "array"
	case string:
		kind = "string"
	case bool:
		kind = "boolean"
	case float64:
		kind = "number"
	}
	for _, t := range append([]any{}, types) {
		if list, ok := t.([]any); ok {
			for _, each := range list {
				if each == kind || (each == "integer" && kind == "number") {
					return true
				}
			}
			return false
		}
		return t == kind || (t == "integer" && kind == "number")
	}
	return false
}

// check serves next, then reports each way its exchange broke the protocol.
func (c *contract) check(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sent bytes.Buffer
		if r.Body != nil {
			r.Body = teeBody{r.Body, &sent}
		}
		answer := &teeResponse{ResponseWriter: w}
		next.ServeHTTP(answer, r)
		for _, problem := range c.problems(r, sent.Bytes(), answer) {
			contractFailure(fmt.Sprintf("%s %s: %s", r.Method, r.URL.Path, problem))
		}
	})
}

func (c *contract) problems(r *http.Request, sent []byte, answer *teeResponse) []string {
	method := strings.ToLower(r.Method)
	operation := []string{"paths", r.URL.Path, method}
	if c.at(operation...) == nil {
		return []string{"the route is not in the protocol"}
	}
	status := cmp0(answer.status, http.StatusOK)
	var problems []string
	note := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	// A request is held to its schema only when the harness accepted it;
	// the tests also send requests the harness must refuse.
	if status < 400 && len(sent) == 0 && c.at(append(operation, "requestBody", "required")...) == true {
		note("the harness accepted a request without the body the protocol requires")
	}
	if requestType := mediaType(r.Header.Get("Content-Type"), sent); status < 400 && len(sent) > 0 {
		body := append(operation, "requestBody", "content", requestType)
		switch {
		case c.at(body...) == nil:
			note("the harness accepted a %s body the protocol does not declare", requestType)
		case requestType == "application/json":
			if err := c.validate(sent, append(body, "schema")...); err != nil {
				note("accepted request: %v", err)
			}
		}
	}

	responseType := mediaType(answer.Header().Get("Content-Type"), nil)
	if status >= 400 {
		if responseType != "application/json" {
			note("a %d refusal is %s, not the error envelope", status, cmpString(responseType, "empty"))
		} else if err := c.validate(answer.body.Bytes(), "components", "schemas", "ErrorResponse"); err != nil {
			note("%d refusal: %v", status, err)
		}
		return problems
	}
	response := append(operation, "responses", strconv.Itoa(status))
	if c.at(response...) == nil {
		note("status %d is not in the protocol", status)
		return problems
	}
	content := append(response, "content", responseType)
	if c.at(content...) == nil {
		note("a %d answer is %s, which the protocol does not declare", status, cmpString(responseType, "empty"))
		return problems
	}
	switch {
	case responseType == "text/event-stream":
		problems = append(problems, c.eventProblems(answer.body.Bytes(), append(content, "x-item-schema"))...)
	case c.at(append(content, "x-item-schema")...) != nil:
		for line := range strings.SplitSeq(answer.body.String(), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			if err := c.validate([]byte(line), append(content, "x-item-schema")...); err != nil {
				note("line %s: %v", truncated(line), err)
			}
		}
	case responseType == "application/json":
		if err := c.validate(answer.body.Bytes(), append(content, "schema")...); err != nil {
			note("%d answer: %v", status, err)
		}
	}
	return problems
}

// eventProblems checks a stream's whole frames: each carries the next
// cursor and one event.
func (c *contract) eventProblems(stream []byte, schema []string) []string {
	var problems []string
	frames := strings.Split(string(stream), "\n\n")
	cursor := -1
	for _, frame := range frames[:len(frames)-1] {
		var data []string
		id := ""
		for line := range strings.SplitSeq(frame, "\n") {
			switch {
			case strings.HasPrefix(line, ":"):
			case strings.HasPrefix(line, "data: "):
				data = append(data, strings.TrimPrefix(line, "data: "))
			case strings.HasPrefix(line, "id: "):
				id = strings.TrimPrefix(line, "id: ")
			default:
				problems = append(problems, "a frame holds "+truncated(line))
			}
		}
		if len(data) == 0 {
			continue
		}
		event := strings.Join(data, "\n")
		if next, err := strconv.Atoi(id); err != nil || (cursor >= 0 && next != cursor+1) {
			problems = append(problems, fmt.Sprintf("event %s has cursor %q after %d", truncated(event), id, cursor))
		} else {
			cursor = next
		}
		if err := c.validate([]byte(event), schema...); err != nil {
			problems = append(problems, fmt.Sprintf("event %s: %v", truncated(event), err))
		}
	}
	return problems
}

// mediaType is a Content-Type without its parameters. A JSON body sent
// without one is read as JSON, as the harness reads it.
func mediaType(header string, body []byte) string {
	if header == "" {
		if json.Valid(body) && len(body) > 0 {
			return "application/json"
		}
		return ""
	}
	parsed, _, err := mime.ParseMediaType(header)
	if err != nil {
		return header
	}
	return parsed
}

func truncated(s string) string {
	if len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}

func cmp0(v, fallback int) int {
	if v == 0 {
		return fallback
	}
	return v
}

func cmpString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

type teeBody struct {
	io.ReadCloser
	copy *bytes.Buffer
}

func (t teeBody) Read(p []byte) (int, error) {
	n, err := t.ReadCloser.Read(p)
	t.copy.Write(p[:n])
	return n, err
}

type teeResponse struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (t *teeResponse) WriteHeader(code int) {
	if t.status == 0 {
		t.status = code
	}
	t.ResponseWriter.WriteHeader(code)
}

func (t *teeResponse) Write(b []byte) (int, error) {
	if t.status == 0 {
		t.status = http.StatusOK
	}
	t.body.Write(b)
	return t.ResponseWriter.Write(b)
}

func (t *teeResponse) Unwrap() http.ResponseWriter { return t.ResponseWriter }

// The running test a protocol failure fails, and the failures no running
// test was there to claim.
var contractScope struct {
	sync.Mutex
	test      *testing.T
	unclaimed []string
}

// watchContract has the protocol failures of the requests a test sends fail
// it. Tests run one at a time, so the latest test to watch is the one
// running.
func watchContract(t *testing.T) {
	contractScope.Lock()
	previous := contractScope.test
	contractScope.test = t
	contractScope.Unlock()
	t.Cleanup(func() {
		contractScope.Lock()
		contractScope.test = previous
		contractScope.Unlock()
	})
}

func contractFailure(problem string) {
	contractScope.Lock()
	defer contractScope.Unlock()
	if contractScope.test != nil {
		contractScope.test.Errorf("protocol: %s", problem)
		return
	}
	contractScope.unclaimed = append(contractScope.unclaimed, problem)
}

func TestMain(m *testing.M) {
	c, err := loadContract()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	contractCheck = c.check
	code := m.Run()
	if unclaimed := slices.Compact(slices.Sorted(slices.Values(contractScope.unclaimed))); len(unclaimed) > 0 {
		fmt.Fprintln(os.Stderr, "Exchanges outside the protocol:")
		for _, problem := range unclaimed {
			fmt.Fprintln(os.Stderr, "  "+problem)
		}
		code = cmp0(code, 1)
	}
	os.Exit(code)
}

// The protocol names every route the harness serves, and the harness serves
// every route the protocol names.
func TestTheProtocolNamesEveryRoute(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	c, err := loadContract()
	if err != nil {
		t.Fatal(err)
	}
	served := map[string]bool{}
	for pattern := range h.handlers() {
		served[pattern] = true
		method, path, _ := strings.Cut(pattern, " ")
		if c.at("paths", path, strings.ToLower(method)) == nil {
			t.Errorf("%s is served but not in the protocol", pattern)
		}
	}
	for path, item := range c.at("paths").(map[string]any) {
		for method := range item.(map[string]any) {
			if pattern := strings.ToUpper(method) + " " + path; !served[pattern] {
				t.Errorf("%s is in the protocol but not served", pattern)
			}
		}
	}
}
