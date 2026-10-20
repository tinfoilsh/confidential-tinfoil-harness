package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
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
			fields := obj(decodeValue(raw(descriptor)))
			text, hasText := fields["textContent"]
			pages, hasPages := fields["pages"]
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

// Every file type the session's composer offers is one uploads convert.
func TestTheComposerOffersWhatUploadsAccept(t *testing.T) {
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })
	var session struct {
		Composer composerView `json:"composer"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", object{}, "").Body.Bytes(), &session)
	offered := session.Composer
	if offered.Attachments.MaxBytes != maxUploadBytes || offered.Attachments.MaxTextBytes != maxTextBytes || offered.PasteAsFileChars == 0 || offered.DictationMaxSeconds == 0 || len(offered.ProjectDocuments.Accept) == 0 {
		t.Fatalf("the composer offers %+v", offered)
	}
	refusedAsUnknown := func(name string) bool {
		w := uploadRoute(h, "/v1/attachments/upload", name, []byte("x"), nil, "alice")
		return strings.Contains(w.Body.String(), "Unsupported file type")
	}
	if !refusedAsUnknown("file.exe") {
		t.Fatal("an unknown file type was not refused as one")
	}
	for _, ext := range append(offered.Attachments.Accept, offered.ProjectDocuments.Accept...) {
		if refusedAsUnknown("file" + ext) {
			t.Errorf("uploads refuse %s, which the composer offers", ext)
		}
	}
}

// An image keeps one form from its upload to every place a chat holds or
// shows it: the upload's descriptor carries the copy models read, a stored
// chat keeps that copy in its blob, and views and device chats name the
// same fields without the bytes or the blob's key.
func TestAnImageKeepsOneFormFromUploadToView(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	var source bytes.Buffer
	png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 2000, 10)))
	upload := uploadRoute(h, "/v1/attachments/upload", "photo.png", source.Bytes(), nil, "alice")
	var uploaded struct {
		Attachment object `json:"attachment"`
	}
	json.Unmarshal(upload.Body.Bytes(), &uploaded)
	sent := uploaded.Attachment
	shown := []string{"fileName", "id", "kind", "mimeType", "size", "thumbnail"}
	if got := slices.Sorted(maps.Keys(sent)); upload.Code != 200 || !slices.Equal(got, slices.Sorted(slices.Values(append(shown, "base64")))) || sent["kind"] != "image" || integer(sent["size"]) != source.Len() {
		t.Fatalf("upload: HTTP %d %v", upload.Code, sent)
	}
	modelCopy, _ := base64.StdEncoding.DecodeString(str(sent["base64"]))
	if config, format, err := image.DecodeConfig(bytes.NewReader(modelCopy)); err != nil || format != "jpeg" || config.Width != 1536 {
		t.Fatalf("the upload's copy for models is %s %+v: %v", format, config, err)
	}
	fieldsOf := func(a any) []string { return slices.Sorted(maps.Keys(obj(a))) }
	in := sendInput(key)
	obj(in["options"])["model"] = "auto"
	in["attachments"] = []any{sent}
	events := finishedRun(t, h, "/v1/threads/turn", in)
	thread := events[0].ThreadID
	row, _ := rows.load(context.Background(), &principal{ID: "alice"}, key, "chat", thread)
	stored := obj(arr(obj(arr(rowJSON(row)["messages"])[0])["attachments"])[0])
	if got := fieldsOf(stored); !slices.Equal(got, []string{"encryptionKey", "fileName", "id", "kind", "mimeType", "size", "storageThreadId", "thumbnail"}) || stored["thumbnail"] != sent["thumbnail"] || !bytes.Equal(fake.blobs[str(stored["id"])].data, modelCopy) {
		t.Fatalf("the chat stored %v", stored)
	}
	var window struct {
		Messages []object `json:"messages"`
	}
	json.Unmarshal(postRoute(h, "/v1/threads/get", object{"key": key.base64(), "id": thread}, "alice").Body.Bytes(), &window)
	views := []any{arr(obj(arr(eventOfType(events, "MESSAGES_SNAPSHOT").Messages)[0])["attachments"])[0], arr(window.Messages[0]["attachments"])[0]}
	local := localInput("", nil)
	obj(local["options"])["model"] = "auto"
	local["attachments"] = []any{sent}
	views = append(views, arr(obj(arr(endedThread(t, h, local, "RUN_FINISHED")["messages"])[0])["attachments"])[0])
	for _, view := range views {
		if got := fieldsOf(view); !slices.Equal(got, shown) || obj(view)["thumbnail"] != sent["thumbnail"] || obj(view)["fileName"] != "photo.png" {
			t.Fatalf("an attachment is shown as %v", view)
		}
	}
	referenceViews(t, views[1:2], []any{object{"id": stored["id"], "type": "image", "fileName": "photo.png", "mimeType": "image/png", "fileSize": source.Len(), "thumbnailBase64": sent["thumbnail"]}})
}
