package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
)

func respond(w http.ResponseWriter, result any, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		e := asAPIError(err)
		if e.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(e.RetryAfter))
		}
		w.WriteHeader(e.HTTP)
		result = object{"error": e}
	}
	if result == nil {
		result = object{}
	}
	json.NewEncoder(w).Encode(result)
}

const maxRequestBody = 8 << 20

// maxResponseBody bounds a downstream answer: a pull of sync's largest batch,
// or an attachment, base64 encoded.
const maxResponseBody = 96 << 20

// readBody reads a request body of at most limit bytes that holds one JSON
// object.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) (json.RawMessage, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	body, err := readWipeable(r.Body)
	if err != nil {
		clear(body)
		return nil, bodyError(err, "Expected a JSON object")
	}
	if !isObject(body) || !json.Valid(body) {
		clear(body)
		return nil, apiErr(400, "BAD_REQUEST", "Expected a JSON object")
	}
	return body, nil
}

const bodyReadChunk = 4 << 10

// readWipeable clears both the read scratch space and replaced growth buffers.
// The caller owns and clears the returned bytes.
func readWipeable(r io.Reader) ([]byte, error) {
	chunk := make([]byte, bodyReadChunk)
	defer clear(chunk)
	var buffer wipingBuffer
	_, err := io.CopyBuffer(&buffer, r, chunk)
	return buffer.data, err
}

type wipingBuffer struct{ data []byte }

func (b *wipingBuffer) Write(p []byte) (int, error) {
	if size := len(b.data) + len(p); size > cap(b.data) {
		next := make([]byte, len(b.data), max(size, 2*cap(b.data)))
		copy(next, b.data)
		clear(b.data)
		b.data = next
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

// bodyError tells a body over the size limit, or one that stopped
// arriving, from a malformed one.
func bodyError(err error, malformed string) *apiError {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return apiErr(http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "The request exceeds the size limit")
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return apiErr(http.StatusRequestTimeout, "REQUEST_TIMEOUT", "The request stopped sending its body")
	}
	return apiErr(400, "BAD_REQUEST", malformed)
}

func isObject(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// decodeRequest decodes a JSON object into a request, naming the first
// field of the wrong type.
func decodeRequest(body []byte, in any) error {
	return requestError(json.Unmarshal(body, in))
}

// decodeBody consumes the owned HTTP buffer after copying its fields.
func decodeBody(body []byte, in any) error {
	defer clear(body)
	return decodeRequest(body, in)
}

type keyRequest struct {
	Key keyBytes `json:"key,omitempty"`
}

type keyBytes []byte

func (key *keyBytes) UnmarshalJSON(data []byte) error {
	clear(*key)
	*key = nil
	if bytes.Equal(data, []byte("null")) {
		return nil
	}
	if len(data) == 0 || data[0] != '"' {
		return apiErr(400, "BAD_REQUEST", "key must be a base64 string")
	}
	if err := json.Unmarshal(data, (*[]byte)(key)); err != nil {
		return apiErr(400, "BAD_REQUEST", "key must be a base64 string")
	}
	return nil
}

func (in *keyRequest) clearKey() {
	clear(in.Key)
	in.Key = nil
}

func requestError(err error) error {
	var refused *apiError
	if errors.As(err, &refused) {
		return refused
	}
	var mistyped *json.UnmarshalTypeError
	if err == nil || !errors.As(err, &mistyped) || mistyped.Field == "" {
		if err != nil {
			return apiErr(400, "BAD_REQUEST", "Expected a JSON object")
		}
		return nil
	}
	// A field path names list items by index; the field is the last name.
	path := strings.Split(mistyped.Field, ".")
	for len(path) > 1 {
		if _, err := strconv.Atoi(path[len(path)-1]); err != nil {
			break
		}
		path = path[:len(path)-1]
	}
	return apiErr(400, "BAD_REQUEST", path[len(path)-1]+" must be "+jsonKind(mistyped.Type))
}

func jsonKind(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Bool:
		return "a boolean"
	case reflect.Slice, reflect.Array:
		return "an array"
	case reflect.Struct, reflect.Map:
		return "an object"
	}
	return "a number"
}

// nullable is a field whose absence differs from null: null clears what
// absence leaves as it is.
type nullable[T any] struct {
	set, invalid bool
	value        *T
}

func (n *nullable[T]) UnmarshalJSON(b []byte) error {
	n.set = true
	if string(b) == "null" {
		return nil
	}
	n.value = new(T)
	if json.Unmarshal(b, n.value) != nil {
		n.value, n.invalid = nil, true
	}
	return nil
}

func (n nullable[T]) IsZero() bool                 { return !n.set }
func (n nullable[T]) MarshalJSON() ([]byte, error) { return json.Marshal(n.value) }

// check refuses a value of the wrong type, as decoding refuses other fields.
func (n nullable[T]) check(name string) error {
	if n.invalid {
		return apiErr(400, "BAD_REQUEST", name+" must be "+jsonKind(reflect.TypeFor[T]()))
	}
	return nil
}

// maxIDLength bounds the ids a caller names its rows with. A thread id is
// echoed into every frame of its run's log, so an unbounded one is an
// unbounded frame.
const maxIDLength = 128

// identifier holds a caller's id to printable ASCII, short of the length at
// which it stops being a name.
func identifier(s string) bool {
	if len(s) > maxIDLength {
		return false
	}
	for _, r := range s {
		if r < ' ' || r > '~' {
			return false
		}
	}
	return true
}

// requireID refuses an id that cannot name a row. Ids become path segments
// of other services' URLs, so the dot segments are refused, as sync does.
func requireID(field, id string) error {
	if id == "" || id == "." || id == ".." || !identifier(id) || strings.ContainsAny(id, "/\\?#") {
		return apiErr(400, "BAD_REQUEST", field+" must be a valid identifier")
	}
	return nil
}
