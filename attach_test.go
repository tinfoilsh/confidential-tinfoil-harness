package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func routerHarness(t *testing.T, router http.HandlerFunc) *harness {
	t.Helper()
	h, _, _ := testChatHarness(t, nil)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	h.services["router"] = &downstreamService{URL: server.URL, Client: &http.Client{Transport: &callerAuth{inner: server.Client().Transport}}}
	return h
}

func TestDocumentsConvertThroughTheRouter(t *testing.T) {
	var reply atomic.Value
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		file, header, err := r.FormFile("files")
		data, _ := io.ReadAll(file)
		if err != nil || r.URL.Path != "/v1/convert/file" || r.URL.RawQuery != "mode=images" || header.Filename != "report.pdf" || string(data) != "%PDF-1.7 report" || r.Header.Get("Authorization") != "Bearer free_anonymous-key" {
			t.Errorf("router got %s %s?%s auth %q", r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"))
		}
		io.WriteString(w, reply.Load().(string))
	})
	ctx := context.WithValue(context.Background(), apiKeyKey{}, "free_anonymous-key")
	for _, tc := range []struct {
		name, reply string
		text        any
		pages       int
	}{
		{"text without pages", `{"document":{"md_content":"# Report","pages":null},"status":"success"}`, "# Report", 0},
		{"pages without text", `{"document":{"pages":[{"page":1,"image":"aW1hZ2U=","is_scanned":true}]},"status":"success"}`, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply.Store(tc.reply)
			descriptor, err := h.preprocess(ctx, "report.pdf", []byte("%PDF-1.7 report"))
			if err != nil {
				t.Fatal(err)
			}
			text, hasText := descriptor["textContent"]
			pages, hasPages := descriptor["pages"]
			if hasText != (tc.text != nil) || text != tc.text || hasPages != (tc.pages > 0) || len(arr(pages)) != tc.pages {
				t.Fatalf("descriptor kept absent fields or lost present ones: %v", descriptor)
			}
		})
	}
}

func TestDocumentConversionWaitsOutCapacityRejections(t *testing.T) {
	for _, tc := range []struct {
		name, retryAfter, body string
		status, calls          int
		code, upstream         string
	}{
		{"parser busy", "1", `{"error":{"message":"parser busy","type":"rate_limit_error","param":null,"code":"document_parser_busy"}}`, 429, 2, "", ""},
		{"no delay given", "", `{"error":{"message":"parser busy","type":"rate_limit_error","param":null,"code":"document_parser_busy"}}`, 429, 1, "RATE_LIMITED", "document_parser_busy"},
		{"delay past the deadline", "30", `{"error":{"message":"The model 'doc-upload' is temporarily unavailable.","type":"service_unavailable_error","code":"model_unavailable"}}`, 503, 1, "MODEL_UNAVAILABLE", ""},
		{"exhausted quota", "1", `{"error":{"message":"Daily request limit reached.","type":"insufficient_quota","code":"insufficient_quota"}}`, 429, 1, "QUOTA_EXHAUSTED", ""},
		{"conversion failure", "1", `{"error":{"message":"Could not extract content from the document. Try exporting it again or uploading a text version.","type":"server_error","param":null,"code":"document_extraction_failed"}}`, 502, 1, "UPSTREAM_REFUSED", "document_extraction_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadline := conversionDeadline
			conversionDeadline = 10 * time.Second
			t.Cleanup(func() { conversionDeadline = deadline })
			var calls atomic.Int32
			h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) > 1 {
					io.WriteString(w, `{"document":{"md_content":"converted"},"status":"success"}`)
					return
				}
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			response := uploadRoute(h, "/v1/attachments/upload", "report.pdf", []byte("%PDF-1.7 report"), nil, "")
			var body struct {
				Attachment struct {
					TextContent string `json:"textContent"`
				} `json:"attachment"`
				Error apiError `json:"error"`
			}
			json.Unmarshal(response.Body.Bytes(), &body)
			if int(calls.Load()) != tc.calls || body.Error.Code != tc.code || body.Error.Upstream != tc.upstream {
				t.Fatalf("calls=%d HTTP %d %s", calls.Load(), response.Code, response.Body.String())
			}
			if tc.code == "" && (response.Code != 200 || body.Attachment.TextContent != "converted") {
				t.Fatalf("retried conversion failed: HTTP %d %s", response.Code, response.Body.String())
			}
			if tc.code == "UPSTREAM_REFUSED" && body.Error.Message != "Could not extract content from the document. Try exporting it again or uploading a text version." {
				t.Fatalf("lost the document failure message: %s", response.Body.String())
			}
		})
	}
}

func TestDocumentConversionStopsWaitingWithItsCaller(t *testing.T) {
	var calls atomic.Int32
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"message":"parser busy","type":"rate_limit_error","param":null,"code":"document_parser_busy"}}`)
	})
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), apiKeyKey{}, "free_anonymous-key"))
	defer cancel()
	time.AfterFunc(100*time.Millisecond, cancel)
	began := time.Now()
	_, err := h.convert(ctx, "report.pdf", []byte("%PDF-1.7 report"))
	if asAPIError(err).Code != "RATE_LIMITED" || calls.Load() != 1 || time.Since(began) < 100*time.Millisecond || time.Since(began) > 2*time.Second {
		t.Fatalf("waited %v after its caller left: calls=%d err=%v", time.Since(began), calls.Load(), err)
	}
}

func TestDocumentConversionsTakeTwoSlots(t *testing.T) {
	var active, peak, calls atomic.Int32
	release := make(chan struct{})
	var released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		now := active.Add(1)
		for seen := peak.Load(); now > seen && !peak.CompareAndSwap(seen, now); seen = peak.Load() {
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		active.Add(-1)
		io.WriteString(w, `{"document":{"md_content":"converted"},"status":"success"}`)
	})
	// Registered after the router's cleanup, so it runs first: a failed
	// assertion must not leave the handlers holding the server open.
	t.Cleanup(unblock)
	ctx := context.WithValue(context.Background(), apiKeyKey{}, "free_anonymous-key")
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Go(func() {
			_, err := h.convert(ctx, "report.pdf", []byte("%PDF-1.7 report"))
			errs <- err
		})
	}
	for calls.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	canceled, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := h.convert(canceled, "report.pdf", []byte("%PDF-1.7 report")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a conversion waiting for a slot ignored its context: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("%d conversions reached the router at once", calls.Load())
	}
	unblock()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 3 || peak.Load() != 2 {
		t.Fatalf("calls=%d peak=%d, want 3 calls at most 2 at a time", calls.Load(), peak.Load())
	}
}

func TestRecordingsTranscribeThroughTheRouter(t *testing.T) {
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		file, header, err := r.FormFile("file")
		data, _ := io.ReadAll(file)
		if err != nil || r.URL.Path != "/v1/audio/transcriptions" || r.FormValue("model") != "voxtral-small-24b" || r.FormValue("response_format") != "text" || header.Filename != "memo.mp3" || string(data) != "ID3 audio" || r.Header.Get("Authorization") != "Bearer inference-alice" {
			t.Errorf("router got %s %s model %q auth %q", r.Method, r.URL.Path, r.FormValue("model"), r.Header.Get("Authorization"))
		}
		io.WriteString(w, "the transcript\n")
	})
	response := uploadRoute(h, "/v1/transcriptions", "memo.mp3", []byte("ID3 audio"), nil, "alice")
	if response.Code != 200 || response.Body.String() != "{\"text\":\"the transcript\\n\"}\n" {
		t.Fatalf("HTTP %d %s", response.Code, response.Body.String())
	}
}
