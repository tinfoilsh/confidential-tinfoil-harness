package api

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestRetryAfterAcceptsOneDelayOrDate(t *testing.T) {
	for _, tc := range []struct {
		values []string
		want   int
	}{
		{nil, 0}, {[]string{"0"}, 1}, {[]string{" 42 "}, 42},
		{[]string{"Wed, 21 Oct 2015 07:28:00 GMT"}, 1}, {[]string{"99999999999"}, maxRetryAfter},
		{[]string{"-1"}, 0}, {[]string{"1.5"}, 0}, {[]string{"10", "20"}, 0}, {[]string{"10, 20"}, 0},
	} {
		if got := RetryAfter(http.Header{"Retry-After": tc.values}); got != tc.want {
			t.Errorf("Retry-After %q = %d, want %d", tc.values, got, tc.want)
		}
	}
	date := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if got := RetryAfter(http.Header{"Retry-After": {date}}); got < 89 || got > 91 {
		t.Errorf("Retry-After %q = %d, want about 90", date, got)
	}
}

func TestUpstreamRefusalsKeepTheirMeaning(t *testing.T) {
	long := "a" + strings.Repeat("é", 400)
	for _, tc := range []struct {
		name, body                  string
		status, http                int
		code, message, upstreamCode string
		action                      string
	}{
		{"exhausted free key", `{"error":{"message":"Daily request limit reached.","type":"insufficient_quota","code":"insufficient_quota"}}`, 429, 429, "QUOTA_EXHAUSTED", "Daily request limit reached.", "", "wait"},
		{"rate limited", `{"error":{"message":"Rate limit reached for requests. Retry after 5 seconds.","type":"rate_limit_error","code":"rate_limit_exceeded"}}`, 429, 429, "RATE_LIMITED", "Rate limit reached for requests. Retry after 5 seconds.", "", "retry"},
		{"queue full behind the router", `{"error":{"message":"The server had an error while processing your request.","type":"server_error","param":null,"code":"upstream_error"}}`, 429, 429, "RATE_LIMITED", "The server had an error while processing your request.", "", "retry"},
		{"model unavailable", `{"error":{"message":"The model 'kimi-k3' is temporarily unavailable. Please try again later.","type":"service_unavailable_error","code":"model_unavailable"}}`, 503, 503, "MODEL_UNAVAILABLE", "The model 'kimi-k3' is temporarily unavailable. Please try again later.", "", "retry"},
		{"overloaded", `{"error":{"message":"The model 'kimi-k3' is currently overloaded. Retry after 3 seconds.","type":"service_unavailable_error","code":"server_is_overloaded"}}`, 503, 503, "UPSTREAM_REFUSED", "The model 'kimi-k3' is currently overloaded. Retry after 3 seconds.", "", "retry"},
		{"flat context length", `{"object":"error","message":"This model's maximum context length is 8192 tokens.","type":"BadRequestError","param":null,"code":400}`, 400, 413, "CONTEXT_TOO_LARGE", "This model's maximum context length is 8192 tokens.", "", "revise"},
		{"invalid request", `{"error":{"message":"Invalid parameter: 'tools' must be an array.","type":"invalid_request_error","param":"tools"}}`, 400, 502, "UPSTREAM_REFUSED", "Invalid parameter: 'tools' must be an array.", "", "stop"},
		{"unknown gateway model", `{"error":{"message":"unknown model: X-Tinfoil-Model must name a configured model","type":"invalid_request_error"}}`, 404, 502, "UPSTREAM_REFUSED", "unknown model: X-Tinfoil-Model must name a configured model", "", "stop"},
		{"unsealable request", `{"error":{"message":"payload is sealed to a.example, which is not where this request routes","type":"invalid_request_error"}}`, 412, 502, "UPSTREAM_REFUSED", "payload is sealed to a.example, which is not where this request routes", "", "stop"},
		{"long message", `{"error":{"message":"` + long + `","type":"invalid_request_error"}}`, 400, 502, "UPSTREAM_REFUSED", long[:511], "", "stop"},
		{"document failure", `{"error":"Document processing exceeded the time limit. Try splitting the document into smaller files.","object":"error","message":"Document processing exceeded the time limit. Try splitting the document into smaller files.","type":"server_error","code":"document_parser_timeout","param":null}`, 502, 502, "UPSTREAM_REFUSED", "Document processing exceeded the time limit. Try splitting the document into smaller files.", "document_parser_timeout", "revise"},
		{"server internals", `{"error":{"message":"Traceback: secret internals","type":"server_error"}}`, 500, 502, "UPSTREAM_REFUSED", "A downstream service refused the request", "", "retry"},
		{"rejected credential", `{"error":{"message":"Incorrect API key provided.","type":"invalid_request_error","code":"invalid_api_key"}}`, 401, 401, "UNAUTHENTICATED", "Authentication is required", "", "reauthenticate"},
		{"sync row missing", `{"ok":false,"code":"NOT_FOUND"}`, 404, 404, "THREAD_NOT_FOUND", "The requested data was not found", "", "stop"},
		{"sync revision", `{"ok":false,"code":"STALE_BLOB"}`, 412, 409, "REVISION_CONFLICT", "The stored row changed", "", "reload"},
		{"refusal without a message", `{"error":{"type":"invalid_request_error"}}`, 400, 502, "UPSTREAM_REFUSED", "A downstream service refused the request", "", "stop"},
		{"sync refusal", `{"ok":false,"code":"BAD_REQUEST"}`, 400, 502, "UPSTREAM_REFUSED", "A downstream service refused the request", "", "stop"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := DownstreamError(tc.status, []byte(tc.body))
			if e.HTTP != tc.http || e.Code != tc.code || e.Message != tc.message || e.Upstream != tc.upstreamCode || e.Action != tc.action {
				t.Fatalf("got HTTP %d %s %q upstream %q action %s", e.HTTP, e.Code, e.Message, e.Upstream, e.Action)
			}
			if !utf8.ValidString(e.Message) {
				t.Fatal("clipped message is not UTF-8")
			}
		})
	}
}

func TestARefusalWithoutAHeaderKeepsItsBodyDelay(t *testing.T) {
	e := DownstreamError(429, []byte(`{"error":{"message":"Slow down.","type":"rate_limit_error","code":"rate_limit_exceeded","retryAfter":9}}`))
	if e.Code != "RATE_LIMITED" || e.RetryAfter != 9 {
		t.Fatalf("got %s retry %d", e.Code, e.RetryAfter)
	}
}

func TestKeyBufferGrowthErasesPreviousAllocation(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	var buffer wipingBuffer
	buffer.Write(key[:])
	previous := buffer.data
	buffer.Write(bytes.Repeat([]byte("x"), cap(previous)+1))
	if !bytes.Equal(previous, make([]byte, len(previous))) || !bytes.Equal(buffer.data[:len(key)], key[:]) {
		t.Fatal("growing the key buffer retained the old allocation or lost the key")
	}
}
