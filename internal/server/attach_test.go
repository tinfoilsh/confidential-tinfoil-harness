package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"image"
	"image/png"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
)

func routerHarness(t *testing.T, router http.HandlerFunc) *harness {
	t.Helper()
	h, _, _ := testChatHarness(t, nil)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	h.models.Services["router"] = &inference.Service{URL: server.URL, Client: &http.Client{Transport: &auth.CallerTransport{Inner: server.Client().Transport}}}
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
	ctx := auth.WithCallerKey(context.Background(), "free_anonymous-key")
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
			descriptor, err := h.attachments.Preprocess(ctx, "report.pdf", []byte("%PDF-1.7 report"))
			if err != nil {
				t.Fatal(err)
			}
			fields := api.Obj(api.Parsed(api.Raw(descriptor)))
			text, hasText := fields["textContent"]
			pages, hasPages := fields["pages"]
			if hasText != (tc.text != nil) || text != tc.text || hasPages != (tc.pages > 0) || len(api.Arr(pages)) != tc.pages {
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
			deadline := attach.ConversionDeadline
			attach.ConversionDeadline = 10 * time.Second
			t.Cleanup(func() { attach.ConversionDeadline = deadline })
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
				Error api.Error `json:"error"`
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
	ctx, cancel := context.WithCancel(auth.WithCallerKey(context.Background(), "free_anonymous-key"))
	defer cancel()
	time.AfterFunc(100*time.Millisecond, cancel)
	began := time.Now()
	_, err := h.attachments.Convert(ctx, "report.pdf", []byte("%PDF-1.7 report"))
	if api.AsError(err).Code != "RATE_LIMITED" || calls.Load() != 1 || time.Since(began) < 100*time.Millisecond || time.Since(began) > 2*time.Second {
		t.Fatalf("waited %v after its caller left: calls=%d err=%v", time.Since(began), calls.Load(), err)
	}
}

func TestRecordingsTranscribeThroughTheRouter(t *testing.T) {
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		file, header, err := r.FormFile("file")
		data, _ := io.ReadAll(file)
		if err != nil || r.URL.Path != "/v1/audio/transcriptions" || r.FormValue("model") != "voxtral-small-24b" || r.FormValue("response_format") != "text" || header.Filename != "memo.mp3" || string(data) != "ID3 audio" || r.Header.Get("Authorization") != "Bearer "+chatTokenOf("alice") {
			t.Errorf("router got %s %s model %q auth %q", r.Method, r.URL.Path, r.FormValue("model"), r.Header.Get("Authorization"))
		}
		io.WriteString(w, "the transcript\n")
	})
	response := uploadRoute(h, "/v1/transcriptions", "memo.mp3", []byte("ID3 audio"), nil, "alice")
	if response.Code != 200 || response.Body.String() != "{\"text\":\"the transcript\\n\"}\n" {
		t.Fatalf("HTTP %d %s", response.Code, response.Body.String())
	}
}

// Each group of files the session's composer offers is what uploads take,
// up to the group's size, and text, which is read whole, has its own.
func TestTheComposerOffersWhatUploadsAccept(t *testing.T) {
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })
	// Small limits stand in for the real ones, which take tens of MiB to reach.
	for i, g := range attach.AttachmentGroups {
		saved := g.MaxBytes
		g.MaxBytes = 16 * (i + 1)
		t.Cleanup(func() { g.MaxBytes = saved })
	}
	var session struct {
		Composer struct {
			Attachments, ProjectDocuments []attach.UploadGroup
			PasteAsFileChars              int
			DictationMaxSeconds           int
		} `json:"composer"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", api.Object{}, "").Body.Bytes(), &session)
	offered := session.Composer
	limitOf := func(groups []attach.UploadGroup, ext string) int {
		for _, g := range groups {
			if slices.Contains(g.Accept, ext) {
				return g.MaxBytes
			}
		}
		return 0
	}
	if slices.Contains([]int{limitOf(offered.Attachments, ".png"), limitOf(offered.Attachments, ".pdf"), limitOf(offered.Attachments, ".mp3"), limitOf(offered.ProjectDocuments, ".pdf"), limitOf(offered.ProjectDocuments, ".txt")}, 0) ||
		limitOf(offered.Attachments, ".txt") == limitOf(offered.Attachments, ".png") || limitOf(offered.ProjectDocuments, ".png") == 0 || offered.PasteAsFileChars == 0 || offered.DictationMaxSeconds == 0 {
		t.Fatalf("the composer offers %+v", offered)
	}
	refusal := func(name string, size int) api.Error {
		data := bytes.Repeat([]byte("x"), size)
		// A file of no type uploads take is refused unless it reads as text.
		if filepath.Ext(name) == ".exe" {
			data[0] = 0
		}
		w := uploadRoute(h, "/v1/attachments/upload", name, data, nil, "alice")
		var body struct {
			Error api.Error `json:"error"`
		}
		json.Unmarshal(w.Body.Bytes(), &body)
		return body.Error
	}
	const unknownType = "Unsupported file type"
	if refusal("file.exe", 1).Message != unknownType {
		t.Fatal("an unknown file type was not refused as one")
	}
	for _, groups := range [][]attach.UploadGroup{offered.Attachments, offered.ProjectDocuments} {
		for _, g := range groups {
			for _, ext := range g.Accept {
				if e := refusal("file"+ext, g.MaxBytes); e.Code == "ATTACHMENT_TOO_LARGE" || e.Message == unknownType {
					t.Errorf("uploads refuse a %s of %d bytes: %s", ext, g.MaxBytes, e.Message)
				}
				if e := refusal("file"+ext, g.MaxBytes+1); e.Code != "ATTACHMENT_TOO_LARGE" {
					t.Errorf("uploads take a %s over its %d bytes: %s", ext, g.MaxBytes, e.Code)
				}
			}
		}
	}
}

// A file of a type uploads do not take is read as text when it sniffs as
// text, and text that is not all UTF-8 is read with replacement characters,
// as the web client read both. A turn takes such a file back.
func TestFilesThatReadAsTextAreTakenAsText(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	mostlyText := append(bytes.Repeat([]byte("a"), 99), 0xff)
	for _, tc := range []struct {
		name string
		data []byte
		text string
		code string
	}{
		{"Dockerfile", []byte("FROM scratch\n"), "FROM scratch\n", ""},
		{"notes.custom", mostlyText, strings.Repeat("a", 99) + "\ufffd", ""},
		{"sales.csv", []byte("caf\xe9,3"), "caf\ufffd,3", ""},
		{"program.bin", []byte("MZ\x00\x01"), "", "ATTACHMENT_UNSUPPORTED"},
		{"noise.dat", bytes.Repeat([]byte{'a', 0xff}, 50), "", "ATTACHMENT_UNSUPPORTED"},
	} {
		w := uploadRoute(h, "/v1/attachments/upload", tc.name, tc.data, nil, "alice")
		var got struct {
			Attachment domain.Attachment `json:"attachment"`
			Error      api.Error         `json:"error"`
		}
		json.Unmarshal(w.Body.Bytes(), &got)
		if got.Error.Code != tc.code || got.Attachment.TextContent != tc.text || tc.code == "" && (got.Attachment.Kind != "document" || got.Attachment.FileName != tc.name) {
			t.Errorf("%s: HTTP %d %s", tc.name, w.Code, w.Body.String())
		}
	}
	// Text that replacement characters make longer than the limit is refused
	// at upload rather than by the turn that sends it.
	saved := attach.TextUploads.MaxBytes
	attach.TextUploads.MaxBytes = 8
	w := uploadRoute(h, "/v1/attachments/upload", "notes.txt", []byte("\xffa\xffa\xffa"), nil, "alice")
	attach.TextUploads.MaxBytes = saved
	if w.Code != http.StatusRequestEntityTooLarge || !strings.Contains(w.Body.String(), "ATTACHMENT_TOO_LARGE") {
		t.Errorf("text longer than its limit once read: HTTP %d %s", w.Code, w.Body.String())
	}
	in := sendInput(key)
	in["attachments"] = []any{api.Object{"id": "att_docker", "kind": "document", "fileName": "Dockerfile", "mimeType": "text/plain; charset=utf-8", "textContent": "FROM scratch\n"}}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" {
		t.Fatalf("a turn attaching a text file of no known type ended with %+v", last)
	}
	if held := storedChat(t, rows, key, events[0].ThreadID).Messages[0].Attachments; len(held) != 1 || held[0].FileName != "Dockerfile" {
		t.Fatalf("the chat holds %+v", held)
	}
}

// An upload a turn sends back is held to what uploads make: a file of a
// type they take, of a kind its type makes, no larger than they take, and an
// image with its bytes, no larger than the copy an upload makes for models. A
// copy already within it is kept as it is.
func TestATurnsUploadIsHeldToWhatUploadsMake(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	picture := func(width int) []byte {
		var b bytes.Buffer
		png.Encode(&b, image.NewRGBA(image.Rect(0, 0, width, 10)))
		return b.Bytes()
	}
	turn := func(attachments ...api.Object) *httptest.ResponseRecorder {
		in := sendInput(key)
		api.Obj(in["options"])["model"] = "auto"
		sent := []any{}
		for _, a := range attachments {
			sent = append(sent, a)
		}
		in["attachments"] = sent
		return postRoute(h, "/v1/threads/turn", in, "alice")
	}
	large, small := picture(2000), picture(100)
	events := eventsIn(t, turn(api.Object{"id": "att_large", "kind": "image", "fileName": "large.png", "mimeType": "image/png", "base64": base64.StdEncoding.EncodeToString(large), "thumbnail": "dGh1bWI="},
		api.Object{"id": "att_small", "kind": "image", "fileName": "small.png", "mimeType": "image/png", "base64": base64.StdEncoding.EncodeToString(small), "thumbnail": "dGh1bWI="}))
	chat := storedChat(t, rows, key, events[0].ThreadID)
	dimensions := func(encoded []byte) string {
		config, format, err := image.DecodeConfig(bytes.NewReader(encoded))
		return fmt.Sprintf("%s %dx%d %v", format, config.Width, config.Height, err)
	}
	stored := chat.Messages[0].Attachments
	thumbnail, _ := base64.StdEncoding.DecodeString(stored[0].Thumbnail)
	if copied := attachmentBytes(t, rows, stored[0]); dimensions(copied) != "jpeg 1536x7 <nil>" || dimensions(thumbnail) != "jpeg 256x1 <nil>" {
		t.Fatalf("a large image was kept as %s with a thumbnail of %s", dimensions(copied), dimensions(thumbnail))
	}
	if !bytes.Equal(attachmentBytes(t, rows, stored[1]), small) || stored[1].Thumbnail != "dGh1bWI=" {
		t.Fatalf("an image within the bounds was changed: %+v", stored[1])
	}
	// Small limits stand in for the real ones, which take tens of MiB to reach.
	for _, g := range []*attach.UploadGroup{&attach.TextUploads, &attach.ImageUploads} {
		saved := g.MaxBytes
		g.MaxBytes = 64
		t.Cleanup(func() { g.MaxBytes = saved })
	}
	for _, tc := range []struct {
		name       string
		attachment api.Object
		code       string
	}{
		{"unknown type", api.Object{"kind": "document", "fileName": "run.exe", "base64": base64.StdEncoding.EncodeToString([]byte{0, 1, 2})}, "ATTACHMENT_UNSUPPORTED"},
		{"wrong kind", api.Object{"kind": "image", "fileName": "notes.txt", "textContent": "text"}, "ATTACHMENT_UNSUPPORTED"},
		{"long text", api.Object{"kind": "document", "fileName": "notes.txt", "textContent": strings.Repeat("x", 65)}, "ATTACHMENT_TOO_LARGE"},
		{"large image", api.Object{"kind": "image", "fileName": "small.png", "base64": base64.StdEncoding.EncodeToString(small)}, "ATTACHMENT_TOO_LARGE"},
		{"not an image", api.Object{"kind": "image", "fileName": "photo.png", "base64": base64.StdEncoding.EncodeToString([]byte("not an image"))}, "ATTACHMENT_UNSUPPORTED"},
		{"image without its bytes", api.Object{"kind": "image", "fileName": "photo.png", "thumbnail": "dGh1bWI="}, "BAD_REQUEST"},
	} {
		tc.attachment["id"] = "att_refused"
		w := turn(tc.attachment)
		var refused struct {
			Error api.Error `json:"error"`
		}
		json.Unmarshal(w.Body.Bytes(), &refused)
		if refused.Error.Code != tc.code {
			t.Errorf("%s: HTTP %d %s", tc.name, w.Code, w.Body.String())
		}
	}
	events = eventsIn(t, turn(api.Object{"id": "att_text", "kind": "document", "fileName": "notes.txt", "textContent": strings.Repeat("x", 64)}))
	if events[len(events)-1].Type != "RUN_FINISHED" {
		t.Fatalf("text at the limit ended with %+v", events[len(events)-1])
	}
	held := storedChat(t, rows, key, events[0].ThreadID).Messages[0].Attachments
	if len(held) != 1 || held[0].FileName != "notes.txt" {
		t.Fatalf("the chat holds %+v", held)
	}
	if content, err := attach.DecodeDocument(attachmentBytes(t, rows, held[0])); err != nil || content.TextContent != strings.Repeat("x", 64) {
		t.Fatalf("the stored document holds %+v: %v", content, err)
	}
}

// declaredPNG is a PNG that declares its size but holds one pixel, so only
// decoding it whole fails.
func declaredPNG(width, height int) []byte {
	var b bytes.Buffer
	png.Encode(&b, image.NewGray(image.Rect(0, 0, 1, 1)))
	data := b.Bytes()
	binary.BigEndian.PutUint32(data[16:], uint32(width))
	binary.BigEndian.PutUint32(data[20:], uint32(height))
	binary.BigEndian.PutUint32(data[29:], crc32.ChecksumIEEE(data[12:29]))
	return data
}

// The images a turn sends to be resized hold together no more pixels than
// one image may, and a turn past that is refused before any is decoded.
func TestATurnResizesNoMorePixelsThanOneImageHolds(t *testing.T) {
	h, _, key := testChatHarness(t, nil)
	const width = 5000
	for _, tc := range []struct {
		height, status int
		code           string
	}{
		{attach.MaxTurnResizePixels / 2 / width, 415, "ATTACHMENT_UNSUPPORTED"},
		{attach.MaxTurnResizePixels/2/width + 1, 413, "ATTACHMENT_TOO_LARGE"},
	} {
		encoded := base64.StdEncoding.EncodeToString(declaredPNG(width, tc.height))
		in := sendInput(key)
		in["attachments"] = []any{
			api.Object{"id": "att_first", "kind": "image", "fileName": "first.png", "base64": encoded},
			api.Object{"id": "att_second", "kind": "image", "fileName": "second.png", "base64": encoded},
		}
		w := postRoute(h, "/v1/threads/turn", in, "alice")
		var refused struct {
			Error api.Error `json:"error"`
		}
		json.Unmarshal(w.Body.Bytes(), &refused)
		if w.Code != tc.status || refused.Error.Code != tc.code {
			t.Errorf("two images of %dx%d: HTTP %d %s", width, tc.height, w.Code, w.Body.String())
		}
	}
}

// An image keeps one form from its upload to every place a chat holds or
// shows it: the upload's descriptor carries the copy models read, a stored
// chat keeps that copy in its blob, and views and device chats name the
// same fields without the bytes or the blob's key.
func TestAnImageKeepsOneFormFromUploadToView(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	var source bytes.Buffer
	png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 2000, 10)))
	upload := uploadRoute(h, "/v1/attachments/upload", "photo.png", source.Bytes(), nil, "alice")
	var uploaded struct {
		Attachment api.Object `json:"attachment"`
	}
	json.Unmarshal(upload.Body.Bytes(), &uploaded)
	sent := uploaded.Attachment
	shown := []string{"fileName", "id", "kind", "mimeType", "size", "thumbnail"}
	if got := slices.Sorted(maps.Keys(sent)); upload.Code != 200 || !slices.Equal(got, slices.Sorted(slices.Values(append(shown, "base64")))) || sent["kind"] != "image" || api.Int(sent["size"]) != source.Len() {
		t.Fatalf("upload: HTTP %d %v", upload.Code, sent)
	}
	modelCopy, _ := base64.StdEncoding.DecodeString(api.Str(sent["base64"]))
	if config, format, err := image.DecodeConfig(bytes.NewReader(modelCopy)); err != nil || format != "jpeg" || config.Width != 1536 {
		t.Fatalf("the upload's copy for models is %s %+v: %v", format, config, err)
	}
	fieldsOf := func(a any) []string { return slices.Sorted(maps.Keys(api.Obj(a))) }
	in := sendInput(key)
	api.Obj(in["options"])["model"] = "auto"
	in["attachments"] = []any{sent}
	events := finishedRun(t, h, "/v1/threads/turn", in)
	thread := events[0].ThreadID
	row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", thread)
	stored := api.Obj(api.Arr(api.Obj(api.Arr(rowJSON(row)["messages"])[0])["attachments"])[0])
	if got := fieldsOf(stored); !slices.Equal(got, []string{"encryptionKey", "fileName", "id", "kind", "mimeType", "size", "storageThreadId", "thumbnail"}) || stored["thumbnail"] != sent["thumbnail"] || !bytes.Equal(attachmentBytes(t, rows, mustRead(domain.ReadThreadRow(row)).Messages[0].Attachments[0]), modelCopy) {
		t.Fatalf("the chat stored %v", stored)
	}
	var window struct {
		Messages []api.Object `json:"messages"`
	}
	json.Unmarshal(postRoute(h, "/v1/threads/get", api.Object{"key": keyBase64(key), "id": thread}, "alice").Body.Bytes(), &window)
	views := []any{api.Arr(api.Obj(api.Arr(eventOfType(events, "MESSAGES_SNAPSHOT").Messages)[0])["attachments"])[0], api.Arr(window.Messages[0]["attachments"])[0]}
	local := localInput("", nil)
	api.Obj(local["options"])["model"] = "auto"
	local["attachments"] = []any{sent}
	views = append(views, api.Arr(api.Obj(api.Arr(endedThread(t, h, local, "RUN_FINISHED")["messages"])[0])["attachments"])[0])
	for _, view := range views {
		if got := fieldsOf(view); !slices.Equal(got, shown) || api.Obj(view)["thumbnail"] != sent["thumbnail"] || api.Obj(view)["fileName"] != "photo.png" {
			t.Fatalf("an attachment is shown as %v", view)
		}
	}
}
