package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

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
				row, _ := rows.load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", events[0].ThreadID)
				if reply := mustRead(readThreadRow(row)).Messages[1]; reply.Content != "partial" || !valueOr(reply.IsError, false) {
					t.Fatalf("the streamed answer was lost: %+v", reply)
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
		Error api.Error `json:"error"`
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
	signIn(r, user)
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	return w
}
