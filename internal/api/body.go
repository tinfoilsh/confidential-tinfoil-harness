package api

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

func Respond(w http.ResponseWriter, result any, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		e := AsError(err)
		if e.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(e.RetryAfter))
		}
		w.WriteHeader(e.HTTP)
		result = Object{"error": e}
	}
	if result == nil {
		result = Object{}
	}
	json.NewEncoder(w).Encode(result)
}

const MaxRequestBody = 8 << 20

// MaxResponseBody bounds a downstream answer: a pull of sync's largest batch,
// or an attachment, base64 encoded.
const MaxResponseBody = 96 << 20

// ReadBody reads a request body of at most limit bytes that holds one JSON
// Object.
func ReadBody(w http.ResponseWriter, r *http.Request, limit int64) (json.RawMessage, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	body, err := ReadWipeable(r.Body)
	if err != nil {
		clear(body)
		return nil, BodyError(err, "Expected a JSON object")
	}
	if !IsObject(body) || !json.Valid(body) {
		clear(body)
		return nil, Err(400, "BAD_REQUEST", "Expected a JSON object")
	}
	return body, nil
}

const bodyReadChunk = 4 << 10

// ReadWipeable clears both the read scratch space and replaced growth buffers.
// The caller owns and clears the returned bytes.
func ReadWipeable(r io.Reader) ([]byte, error) {
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

// BodyError tells a body over the size limit, or one that stopped
// arriving, from a malformed one.
func BodyError(err error, malformed string) *Error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return Err(http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "The request exceeds the size limit")
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return Err(http.StatusRequestTimeout, "REQUEST_TIMEOUT", "The request stopped sending its body")
	}
	return Err(400, "BAD_REQUEST", malformed)
}

func IsObject(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// DecodeRequest decodes a JSON object into a request, naming the first
// field of the wrong type.
func DecodeRequest(body []byte, in any) error {
	return RequestError(json.Unmarshal(body, in))
}

// DecodeBody consumes the owned HTTP buffer after copying its fields.
func DecodeBody(body []byte, in any) error {
	defer clear(body)
	return DecodeRequest(body, in)
}

type KeyRequest struct {
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
		return Err(400, "BAD_REQUEST", "key must be a base64 string")
	}
	if err := json.Unmarshal(data, (*[]byte)(key)); err != nil {
		return Err(400, "BAD_REQUEST", "key must be a base64 string")
	}
	return nil
}

func (in *KeyRequest) ClearKey() {
	clear(in.Key)
	in.Key = nil
}

func RequestError(err error) error {
	var refused *Error
	if errors.As(err, &refused) {
		return refused
	}
	var mistyped *json.UnmarshalTypeError
	if err == nil || !errors.As(err, &mistyped) || mistyped.Field == "" {
		if err != nil {
			return Err(400, "BAD_REQUEST", "Expected a JSON object")
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
	return Err(400, "BAD_REQUEST", path[len(path)-1]+" must be "+jsonKind(mistyped.Type))
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

// Nullable is a field whose absence differs from null: null clears what
// absence leaves as it is.
type Nullable[T any] struct {
	Set, invalid bool
	Value        *T
}

func (n *Nullable[T]) UnmarshalJSON(b []byte) error {
	n.Set = true
	if string(b) == "null" {
		return nil
	}
	n.Value = new(T)
	if json.Unmarshal(b, n.Value) != nil {
		n.Value, n.invalid = nil, true
	}
	return nil
}

func (n Nullable[T]) IsZero() bool                 { return !n.Set }
func (n Nullable[T]) MarshalJSON() ([]byte, error) { return json.Marshal(n.Value) }

// check refuses a value of the wrong type, as decoding refuses other fields.
func (n Nullable[T]) Check(name string) error {
	if n.invalid {
		return Err(400, "BAD_REQUEST", name+" must be "+jsonKind(reflect.TypeFor[T]()))
	}
	return nil
}

// maxIDLength bounds the ids a caller names its rows with. A thread id is
// echoed into every frame of its run's log, so an unbounded one is an
// unbounded frame.
const maxIDLength = 128

// IsIdentifier holds a caller's id to printable ASCII, short of the length at
// which it stops being a name.
func IsIdentifier(s string) bool {
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

// RequireID refuses an id that cannot name a row. Ids become path segments
// of other services' URLs, so the dot segments are refused, as sync does.
func RequireID(field, id string) error {
	if id == "" || id == "." || id == ".." || !IsIdentifier(id) || strings.ContainsAny(id, "/\\?#") {
		return Err(400, "BAD_REQUEST", field+" must be a valid identifier")
	}
	return nil
}
