package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
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
		if got := retryAfter(http.Header{"Retry-After": tc.values}); got != tc.want {
			t.Errorf("Retry-After %q = %d, want %d", tc.values, got, tc.want)
		}
	}
	date := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if got := retryAfter(http.Header{"Retry-After": {date}}); got < 89 || got > 91 {
		t.Errorf("Retry-After %q = %d, want about 90", date, got)
	}
}

func TestUpstreamRefusalsKeepTheirMeaning(t *testing.T) {
	long := "a" + strings.Repeat("é", 400)
	for _, tc := range []struct {
		name, body                  string
		status, http                int
		code, message, upstreamCode string
	}{
		{"exhausted free key", `{"error":{"message":"Daily request limit reached.","type":"insufficient_quota","code":"insufficient_quota"}}`, 429, 429, "QUOTA_EXHAUSTED", "Daily request limit reached.", ""},
		{"rate limited", `{"error":{"message":"Rate limit reached for requests. Retry after 5 seconds.","type":"rate_limit_error","code":"rate_limit_exceeded"}}`, 429, 429, "RATE_LIMITED", "Rate limit reached for requests. Retry after 5 seconds.", ""},
		{"queue full behind the router", `{"error":{"message":"The server had an error while processing your request.","type":"server_error","param":null,"code":"upstream_error"}}`, 429, 429, "RATE_LIMITED", "The server had an error while processing your request.", ""},
		{"model unavailable", `{"error":{"message":"The model 'kimi-k3' is temporarily unavailable. Please try again later.","type":"service_unavailable_error","code":"model_unavailable"}}`, 503, 503, "MODEL_UNAVAILABLE", "The model 'kimi-k3' is temporarily unavailable. Please try again later.", ""},
		{"overloaded", `{"error":{"message":"The model 'kimi-k3' is currently overloaded. Retry after 3 seconds.","type":"service_unavailable_error","code":"server_is_overloaded"}}`, 503, 503, "UPSTREAM_REFUSED", "The model 'kimi-k3' is currently overloaded. Retry after 3 seconds.", ""},
		{"flat context length", `{"object":"error","message":"This model's maximum context length is 8192 tokens.","type":"BadRequestError","param":null,"code":400}`, 400, 413, "CONTEXT_TOO_LARGE", "This model's maximum context length is 8192 tokens.", ""},
		{"invalid request", `{"error":{"message":"Invalid parameter: 'tools' must be an array.","type":"invalid_request_error","param":"tools"}}`, 400, 502, "UPSTREAM_REFUSED", "Invalid parameter: 'tools' must be an array.", ""},
		{"unknown gateway model", `{"error":{"message":"unknown model: X-Tinfoil-Model must name a configured model","type":"invalid_request_error"}}`, 404, 502, "UPSTREAM_REFUSED", "unknown model: X-Tinfoil-Model must name a configured model", ""},
		{"unsealable request", `{"error":{"message":"payload is sealed to a.example, which is not where this request routes","type":"invalid_request_error"}}`, 412, 502, "UPSTREAM_REFUSED", "payload is sealed to a.example, which is not where this request routes", ""},
		{"long message", `{"error":{"message":"` + long + `","type":"invalid_request_error"}}`, 400, 502, "UPSTREAM_REFUSED", long[:511], ""},
		{"document failure", `{"error":"Document processing exceeded the time limit. Try splitting the document into smaller files.","object":"error","message":"Document processing exceeded the time limit. Try splitting the document into smaller files.","type":"server_error","code":"document_parser_timeout","param":null}`, 502, 502, "UPSTREAM_REFUSED", "Document processing exceeded the time limit. Try splitting the document into smaller files.", "document_parser_timeout"},
		{"server internals", `{"error":{"message":"Traceback: secret internals","type":"server_error"}}`, 500, 502, "UPSTREAM_REFUSED", "A downstream service refused the request", ""},
		{"rejected credential", `{"error":{"message":"Incorrect API key provided.","type":"invalid_request_error","code":"invalid_api_key"}}`, 401, 401, "UNAUTHENTICATED", "Authentication is required", ""},
		{"sync row missing", `{"ok":false,"code":"NOT_FOUND"}`, 404, 404, "THREAD_NOT_FOUND", "The requested data was not found", ""},
		{"sync revision", `{"ok":false,"code":"STALE_BLOB"}`, 412, 409, "REVISION_CONFLICT", "The stored row changed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := downstreamError(tc.status, []byte(tc.body))
			if e.HTTP != tc.http || e.Code != tc.code || e.Message != tc.message || e.Upstream != tc.upstreamCode {
				t.Fatalf("got HTTP %d %s %q upstream %q", e.HTTP, e.Code, e.Message, e.Upstream)
			}
			if !utf8.ValidString(e.Message) {
				t.Fatal("clipped message is not UTF-8")
			}
		})
	}
}

func TestModelRefusalsReachTheRunWithTheirMeaning(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		retryAfter string
		body       string
		code       string
		message    string
		wait       int
	}{
		{"rate limited", 429, "7", `{"error":{"message":"Rate limit reached for requests. Retry after 7 seconds.","type":"rate_limit_error","code":"rate_limit_exceeded"}}`, "RATE_LIMITED", "Rate limit reached for requests. Retry after 7 seconds.", 7},
		{"overloaded mid-stream", 200, "", `{"error":{"message":"The model 'kimi-k3' is currently overloaded. Retry after 3 seconds.","type":"service_unavailable_error","param":null,"code":"server_is_overloaded"}}`, "UPSTREAM_REFUSED", "The model 'kimi-k3' is currently overloaded. Retry after 3 seconds.", 0},
		{"flat error mid-stream", 200, "", `{"object":"error","message":"This model's maximum context length is 8192 tokens.","type":"BadRequestError","param":null,"code":400}`, "CONTEXT_TOO_LARGE", "This model's maximum context length is 8192 tokens.", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.status != 200 {
					if tc.retryAfter != "" {
						w.Header().Set("Retry-After", tc.retryAfter)
					}
					w.WriteHeader(tc.status)
					io.WriteString(w, tc.body)
					return
				}
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\ndata: "+tc.body+"\n\n")
			})
			events := eventsIn(t, postRoute(h, "/v1/threads/turn", sendInput(key), "alice"))
			last := events[len(events)-1]
			if last.Type != "RUN_ERROR" || last.Code != tc.code || last.Message != tc.message || last.RetryAfter != tc.wait {
				t.Fatalf("run ended with %s %s %q retry %d", last.Type, last.Code, last.Message, last.RetryAfter)
			}
			if tc.status == 200 {
				row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", events[0].ThreadID)
				if reply := obj(arr(row.Data["messages"])[1]); reply["content"] != "partial" || reply["isError"] != true {
					t.Fatalf("the streamed answer was lost: %v", reply)
				}
			}
		})
	}
}

func TestServiceRefusalsKeepTheirRetryDelay(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"error":{"message":"The model 'voxtral-small-24b' is temporarily unavailable. Please try again later.","type":"service_unavailable_error","code":"model_unavailable"}}`)
	}))
	defer router.Close()
	h.services["router"] = &downstreamService{URL: router.URL, Client: router.Client()}
	response := uploadRoute(h, "/v1/transcriptions", "memo.mp3", []byte("ID3 audio"), nil, "")
	var body struct {
		Error apiError `json:"error"`
	}
	json.Unmarshal(response.Body.Bytes(), &body)
	if response.Code != 503 || response.Header().Get("Retry-After") != "30" || body.Error.Code != "MODEL_UNAVAILABLE" || body.Error.RetryAfter != 30 {
		t.Fatalf("HTTP %d Retry-After %q: %s", response.Code, response.Header().Get("Retry-After"), response.Body.String())
	}
}

func uploadRoute(h *harness, path, filename string, data []byte, fields map[string]string, user string) *httptest.ResponseRecorder {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for name, value := range fields {
		form.WriteField(name, value)
	}
	part, _ := form.CreateFormFile("file", filename)
	part.Write(data)
	form.Close()
	r := httptest.NewRequest("POST", path, &body)
	r.Header.Set("Content-Type", form.FormDataContentType())
	if user != "" {
		r.Header.Set("Authorization", "Bearer clerk-"+user)
	} else {
		r.Header.Set("Authorization", "Bearer free_anonymous-key")
	}
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	return w
}

func TestARefusalWithoutAHeaderKeepsItsBodyDelay(t *testing.T) {
	e := downstreamError(429, []byte(`{"error":{"message":"Slow down.","type":"rate_limit_error","code":"rate_limit_exceeded","retryAfter":9}}`))
	if e.Code != "RATE_LIMITED" || e.RetryAfter != 9 {
		t.Fatalf("got %s retry %d", e.Code, e.RetryAfter)
	}
}
