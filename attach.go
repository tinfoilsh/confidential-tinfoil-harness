package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	_ "golang.org/x/image/bmp"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

const maxUploadBytes = 33 << 20
const maxTextBytes = 50 << 20
const maxImagePixels = 40_000_000

var imageExtensions = strings.Fields(".jpg .jpeg .png .gif .webp .bmp .tiff .tif")
var documentExtensions = strings.Fields(".pdf .docx .pptx .xlsx")
var audioExtensions = strings.Fields(".mp3 .wav .ogg .m4a .aac .flac .webm .wma")
var textExtensions = strings.Fields(".txt .md .py .js .jsx .ts .tsx .css .json .xml .yaml .yml .toml .sh .bash .rb .java .cpp .c .h .hpp .go .rs .swift .kt .kts .r .sql .lua .pl .php .cs .csx .vb .fs .fsx .scala .dart .ex .exs .erl .hs .ml .mli .clj .cljs .groovy .gradle .m .mm .zig .nim .jl .ps1 .psm1 .bat .cmd .asm .s .proto .graphql .gql .tf .tfvars .dockerfile .makefile .cmake .tex .bib .svg .properties .lock .diff .patch .env .ini .cfg .conf .log .rtf .html .htm .xhtml .csv .tsv .qfx .qif .ofx .ifs .qbo .qbx .bai .bai2 .mt940 .sta .ics .vcf")

func randomBytes(n int) []byte { b := make([]byte, n); rand.Read(b); return b }

func publicAttachment(a object) object {
	out := object{"id": a["id"], "kind": firstString(a["kind"], a["type"]), "fileName": a["fileName"], "mimeType": a["mimeType"], "size": a["fileSize"], "thumbnail": a["thumbnailBase64"], "pages": nil, "status": "ready"}
	if text := str(a["textContent"]); text != "" {
		out["textContent"] = text
	}
	// A text-only model reads an image through this. Local clients keep it,
	// so later turns do not ask a vision model to describe the image again.
	if text := str(a["description"]); text != "" {
		out["description"] = text
	}
	if pages := arr(a["pages"]); len(pages) > 0 {
		out["pages"] = len(pages)
	} else if count := integer(a["pageCount"]); count > 0 {
		out["pages"] = count
	}
	return out
}
func (h *harness) handleUpload(w http.ResponseWriter, r *http.Request) {
	p, err := h.auth.authenticate(r, r.URL.Path == "/v1/transcriptions" || r.URL.Path == "/v1/attachments/upload")
	if err != nil {
		respond(w, nil, err)
		return
	}
	if r.URL.Path == "/v1/import" {
		boundedHandler(archiveSlots, func(w http.ResponseWriter, r *http.Request) { h.handleImport(w, r, p) })(w, r)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTextBytes+(1<<20))
	reader, err := r.MultipartReader()
	if err != nil {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "Expected multipart form data"))
		return
	}
	fields := object{}
	var file []byte
	filename := ""
	for count := 0; ; count++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= 12 {
			respond(w, nil, apiErr(400, "BAD_REQUEST", "Invalid multipart upload"))
			return
		}
		if part.FormName() == "file" {
			if filename != "" || part.FileName() == "" {
				respond(w, nil, apiErr(400, "BAD_REQUEST", "Expected one named file"))
				return
			}
			filename = filepath.Base(part.FileName())
			file, err = io.ReadAll(io.LimitReader(part, maxTextBytes+1))
			if err != nil || len(file) > maxTextBytes {
				respond(w, nil, apiErr(413, "ATTACHMENT_TOO_LARGE", "The upload exceeds the size limit"))
				return
			}
		} else {
			value, err := io.ReadAll(io.LimitReader(part, 8193))
			if err != nil || len(value) > 8192 {
				respond(w, nil, apiErr(400, "BAD_REQUEST", "Multipart field is too large"))
				return
			}
			if _, exists := fields[part.FormName()]; exists {
				respond(w, nil, apiErr(400, "BAD_REQUEST", "Duplicate multipart field"))
				return
			}
			fields[part.FormName()] = string(value)
		}
		part.Close()
	}
	defer clear(file)
	if filename == "" || len(file) == 0 {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "A nonempty file is required"))
		return
	}
	cred, err := h.auth.inference(r.Context(), p)
	if err != nil {
		respond(w, nil, err)
		return
	}
	ctx := context.WithValue(r.Context(), apiKeyKey{}, string(cred.Key))
	if r.URL.Path == "/v1/transcriptions" {
		text, err := h.transcribe(ctx, filename, file)
		respond(w, object{"text": text}, err)
		return
	}
	descriptor, err := h.preprocess(ctx, filename, file)
	if err != nil {
		respond(w, nil, err)
		return
	}
	// An upload is only converted. Its client sends the descriptor back with
	// the turn that attaches it, and that turn stores it with the chat.
	if r.URL.Path == "/v1/attachments/upload" {
		descriptor["id"] = "att_" + token()
		respond(w, object{"attachment": uploadView(descriptor)}, nil)
		return
	}
	key, err := decodeContentKey(str(fields["key"]))
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	if r.URL.Path == "/v1/projects/documents/upload" {
		projectID, err := requiredID(fields, "projectId")
		if err != nil {
			respond(w, nil, err)
			return
		}
		if _, err := h.storeRows.load(ctx, p, key, "project", projectID); err != nil {
			respond(w, nil, err)
			return
		}
		if str(descriptor["textContent"]) == "" {
			respond(w, nil, apiErr(415, "ATTACHMENT_UNSUPPORTED", "Project documents require extracted text"))
			return
		}
		id := rowID()
		row, err := h.mutate(ctx, p, key, "project_document", projectID+"/"+id, true, nil, func(data object) error {
			data["projectId"], data["filename"], data["contentType"], data["sizeBytes"], data["content"] = projectID, filename, descriptor["mimeType"], len(file), descriptor["textContent"]
			return nil
		})
		if err != nil {
			respond(w, nil, err)
			return
		}
		view := projectView(row)
		view["id"] = id
		respond(w, view, nil)
		return
	}
}

// uploadView is an upload's descriptor as its client holds it until a turn
// sends it back: page images included, image bytes not.
func uploadView(a object) object {
	out := object{"id": a["id"], "kind": firstString(a["kind"], a["type"]), "fileName": a["fileName"], "mimeType": a["mimeType"], "size": a["fileSize"]}
	for field, stored := range map[string]string{"thumbnail": "thumbnailBase64", "textContent": "textContent", "description": "description", "pages": "pages"} {
		if value, ok := a[stored]; ok {
			out[field] = value
		}
	}
	return out
}

// sentAttachment is the stored form of an attachment a turn sends whole:
// an upload's descriptor, and an image's bytes while its client holds them.
func sentAttachment(d object) object {
	a := object{"id": d["id"], "type": "document", "fileName": d["fileName"], "mimeType": d["mimeType"], "fileSize": d["size"]}
	switch d["kind"] {
	case "image":
		a["type"] = "image"
	case "audio":
		a["kind"] = "audio"
	}
	for field, sent := range map[string]string{"thumbnailBase64": "thumbnail", "textContent": "textContent", "description": "description", "pages": "pages", "base64": "base64"} {
		if value, ok := d[sent]; ok && value != nil {
			a[field] = value
		}
	}
	return a
}

func (h *harness) preprocess(ctx context.Context, filename string, data []byte) (object, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	detected := http.DetectContentType(data)
	descriptor := object{"type": "document", "fileName": filename, "mimeType": detected, "fileSize": len(data)}
	switch {
	case slices.Contains(textExtensions, ext) || strings.EqualFold(filename, "Dockerfile") || strings.EqualFold(filename, "Makefile"):
		if !utf8.Valid(data) || bytes.ContainsRune(data, 0) {
			return nil, apiErr(415, "ATTACHMENT_UNSUPPORTED", "Text uploads must contain UTF-8 text")
		}
		descriptor["textContent"] = string(data)
		if m := mime.TypeByExtension(ext); m != "" {
			descriptor["mimeType"] = m
		}
	case slices.Contains(imageExtensions, ext):
		if len(data) > maxUploadBytes {
			return nil, apiErr(413, "ATTACHMENT_TOO_LARGE", "The image exceeds the size limit")
		}
		_, thumb, err := resizeImage(data)
		if err != nil {
			return nil, err
		}
		descriptor["type"], descriptor["thumbnailBase64"] = "image", base64.StdEncoding.EncodeToString(thumb)
	case slices.Contains(documentExtensions, ext):
		if len(data) > maxUploadBytes {
			return nil, apiErr(413, "ATTACHMENT_TOO_LARGE", "The document exceeds the size limit")
		}
		if ext == ".pdf" && !bytes.HasPrefix(data, []byte("%PDF-")) || ext != ".pdf" && !bytes.HasPrefix(data, []byte("PK\x03\x04")) {
			return nil, apiErr(415, "ATTACHMENT_UNSUPPORTED", "The file contents do not match the document type")
		}
		result, err := h.convert(ctx, filename, data)
		if err != nil {
			return nil, err
		}
		var converted object
		if json.Unmarshal(result, &converted) != nil {
			return nil, apiErr(502, "UPSTREAM_REFUSED", "Invalid document response")
		}
		if results := arr(converted["results"]); len(results) > 0 {
			converted = obj(results[0])
		}
		if document := obj(converted["document"]); len(document) > 0 {
			converted = document
		}
		if text := str(converted["md_content"]); text != "" {
			descriptor["textContent"] = text
		}
		if pages := arr(converted["pages"]); len(pages) > 0 {
			descriptor["pages"] = pages
		}
	case slices.Contains(audioExtensions, ext):
		text, err := h.transcribe(ctx, filename, data)
		if err != nil {
			return nil, err
		}
		descriptor["type"], descriptor["kind"], descriptor["textContent"] = "document", "audio", text
	default:
		return nil, apiErr(415, "ATTACHMENT_UNSUPPORTED", "Unsupported file type")
	}
	return descriptor, nil
}
func resizeImage(data []byte) ([]byte, []byte, error) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width)*int64(cfg.Height) > maxImagePixels {
		return nil, nil, apiErr(415, "ATTACHMENT_UNSUPPORTED", "The image cannot be decoded within the pixel limit")
	}
	im, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, nil, apiErr(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
	}
	encode := func(maxSide int) ([]byte, error) {
		width, height := cfg.Width, cfg.Height
		if max(width, height) > maxSide {
			if width >= height {
				height = max(1, height*maxSide/width)
				width = maxSide
			} else {
				width = max(1, width*maxSide/height)
				height = maxSide
			}
		}
		dst := image.NewRGBA(image.Rect(0, 0, width, height))
		draw.Draw(dst, dst.Bounds(), image.White, image.Point{}, draw.Src)
		draw.CatmullRom.Scale(dst, dst.Bounds(), im, im.Bounds(), draw.Over, nil)
		var b bytes.Buffer
		err := jpeg.Encode(&b, dst, &jpeg.Options{Quality: 85})
		return b.Bytes(), err
	}
	resized, err := encode(1536)
	if err != nil {
		return nil, nil, err
	}
	thumb, err := encode(256)
	return resized, thumb, err
}

// The document service converts four files at once and queues sixteen for
// every user, so conversions take two slots and wait out capacity rejections
// while the retry still fits a deadline that starts once a slot is free.
var conversionSlots = make(chan struct{}, 2)
var conversionDeadline = 10 * time.Minute

func (h *harness) convert(ctx context.Context, filename string, data []byte) ([]byte, error) {
	router := h.services["router"]
	if router == nil {
		return nil, apiErr(503, "TOOL_UNAVAILABLE", "Document conversion is unavailable")
	}
	select {
	case conversionSlots <- struct{}{}:
		defer func() { <-conversionSlots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, conversionDeadline)
	defer cancel()
	deadline, _ := ctx.Deadline()
	for {
		result, err := multipartCall(ctx, router, "/v1/convert/file?mode=images", "files", filename, data, nil)
		delay := capacityDelay(err)
		if delay == 0 || !time.Now().Add(delay).Before(deadline) {
			return result, err
		}
		wait := time.NewTimer(delay)
		select {
		case <-wait.C:
		case <-ctx.Done():
			wait.Stop()
			return nil, err
		}
	}
}

// capacityDelay is how long a capacity rejection asks to wait, or zero when the
// refusal is final. An exhausted quota needs the user rather than a retry.
func capacityDelay(err error) time.Duration {
	var e *apiError
	if !errors.As(err, &e) || e.Code == "QUOTA_EXHAUSTED" || e.Status != http.StatusTooManyRequests && e.Status != http.StatusServiceUnavailable {
		return 0
	}
	return time.Duration(e.RetryAfter) * time.Second
}

func multipartCall(ctx context.Context, service *downstreamService, path, fileField, name string, data []byte, fields object) ([]byte, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	for key, value := range fields {
		if err := writer.WriteField(key, str(value)); err != nil {
			return nil, err
		}
	}
	part, err := writer.CreateFormFile(fileField, name)
	if err != nil {
		return nil, err
	}
	if _, err := part.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", service.URL+path, &buffer)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if key := callerKey(ctx); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := service.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxTextBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxTextBytes {
		return nil, apiErr(413, "ATTACHMENT_TOO_LARGE", "The converted document exceeds the text limit")
	}
	if resp.StatusCode/100 != 2 {
		return nil, withRetryAfter(downstreamError(resp.StatusCode, b), resp.Header)
	}
	return b, nil
}
func (h *harness) transcribe(ctx context.Context, filename string, data []byte) (string, error) {
	if len(data) > maxUploadBytes {
		return "", apiErr(413, "ATTACHMENT_TOO_LARGE", "The recording exceeds the size limit")
	}
	if !slices.Contains(audioExtensions, strings.ToLower(filepath.Ext(filename))) {
		return "", apiErr(415, "ATTACHMENT_UNSUPPORTED", "Unsupported recording type")
	}
	// The router reads the model from the form, as the web client's SDK sends it.
	router := h.services["router"]
	if router == nil {
		return "", apiErr(503, "TOOL_UNAVAILABLE", "Transcription is unavailable")
	}
	b, err := multipartCall(ctx, router, "/v1/audio/transcriptions", "file", filename, data, object{"model": "voxtral-small-24b", "response_format": "text"})
	if err != nil {
		return "", err
	}
	var response object
	if json.Unmarshal(b, &response) == nil {
		return str(response["text"]), nil
	}
	return string(b), nil
}

// attachment finds an attachment of the named thread for its client to download.
func (h *harness) attachment(ctx context.Context, p *principal, key contentKey, id, threadID string) (object, []byte, error) {
	if p.Anonymous {
		return nil, nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	if key == (contentKey{}) {
		return nil, nil, apiErr(401, "KEY_REQUIRED", "A content key is required")
	}
	var descriptor object
	if threadID != "" {
		row, err := h.storeRows.load(ctx, p, key, "chat", threadID)
		if err != nil {
			return nil, nil, err
		}
		descriptor = threadAttachment(normalizeThread(row), id)
	}
	if len(descriptor) == 0 {
		return nil, nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	b, err := h.attachmentBytes(ctx, p, descriptor, false)
	return descriptor, b, err
}

func threadAttachment(thread object, id string) object {
	for _, message := range arr(thread["messages"]) {
		for _, item := range arr(obj(message)["attachments"]) {
			if a := obj(item); a["id"] == id {
				return clone(a)
			}
		}
	}
	return nil
}

func (h *harness) attachmentBytes(ctx context.Context, p *principal, descriptor object, public bool) ([]byte, error) {
	if value := str(descriptor["base64"]); value != "" {
		return base64.StdEncoding.DecodeString(value)
	}
	id := str(descriptor["id"])
	if !public && str(descriptor["encryptionKey"]) == "" && str(descriptor["key"]) != "" {
		return h.legacyAttachmentBytes(ctx, id, str(descriptor["key"]))
	}
	path := "/v1/attachment/get"
	if public {
		path += "-public"
	}
	var result object
	err := h.syncAPI.call(ctx, p, path, object{"id": id, "att_key": firstString(descriptor["encryptionKey"], descriptor["attKey"], descriptor["key"])}, &result)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(str(result["plaintext"]))
}

var legacyAttachments = &http.Client{Timeout: 5 * time.Minute}

// Images the web and iOS apps stored before the sync enclave existed carry
// only a `key` and live encrypted at the control plane, as a 12-byte IV and
// AES-256-GCM ciphertext. The web client reads them the same way.
func (h *harness) legacyAttachmentBytes(ctx context.Context, id, encodedKey string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(key) != 32 {
		return nil, apiErr(422, "ATTACHMENT_UNSUPPORTED", "An old attachment key is invalid")
	}
	defer clear(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.controlplane+"/api/storage/attachment/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	resp, err := legacyAttachments.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*maxTextBytes))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, downstreamError(resp.StatusCode, body)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(body) < gcm.NonceSize()+gcm.Overhead() {
		return nil, apiErr(422, "ATTACHMENT_UNSUPPORTED", "An old attachment is truncated")
	}
	plain, err := gcm.Open(nil, body[:gcm.NonceSize()], body[gcm.NonceSize():], nil)
	if err != nil {
		return nil, apiErr(422, "ATTACHMENT_UNSUPPORTED", "An old attachment cannot be decrypted")
	}
	return plain, nil
}

func (h *harness) handleAttachmentGet(w http.ResponseWriter, r *http.Request) {
	in, err := decodeJSON(w, r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	public := r.URL.Path == "/v1/attachments/get-public"
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	id, err := requiredID(in, "id")
	if err != nil {
		respond(w, nil, err)
		return
	}
	threadID := str(in["threadId"])
	if threadID != "" {
		if _, err := requiredID(in, "threadId"); err != nil {
			respond(w, nil, err)
			return
		}
	}
	var descriptor object
	var data []byte
	if public {
		if _, err := decodeContentKey(str(in["attKey"])); err != nil {
			respond(w, nil, err)
			return
		}
		descriptor = in
		data, err = h.attachmentBytes(r.Context(), &principal{Anonymous: true}, descriptor, true)
	} else {
		key, keyErr := requestKey(in)
		if keyErr != nil {
			respond(w, nil, keyErr)
			return
		}
		defer clear(key[:])
		descriptor, data, err = h.attachment(r.Context(), p, key, id, threadID)
	}
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(data)
	contentType, fileName := firstString(descriptor["mimeType"], http.DetectContentType(data)), firstString(descriptor["fileName"], "attachment")
	if documentRequest(descriptor, data) {
		// A stored document keeps its extracted content, not the uploaded file.
		content := object{"textContent": descriptor["textContent"], "pages": descriptor["pages"]}
		if !documentContent(content) {
			if content, err = decodeDocument(data); err != nil {
				respond(w, nil, err)
				return
			}
		}
		clear(data)
		data = []byte(documentText(content))
		contentType, fileName = "text/markdown; charset=utf-8", strings.TrimSuffix(fileName, filepath.Ext(fileName))+".md"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": fileName}))
	w.Write(data)
}

// A shared chat's client may not say what an attachment is. A document blob is
// JSON, and no supported image format starts with a brace.
func documentRequest(descriptor object, data []byte) bool {
	switch firstString(descriptor["type"], descriptor["kind"]) {
	case "image":
		return false
	case "":
		trimmed := bytes.TrimSpace(data)
		return len(trimmed) > 0 && trimmed[0] == '{'
	}
	return true
}

// turnAttachments resolves the attachments a turn names: an upload sent
// whole, or by id one the thread already holds, as an edit sends it.
func turnAttachments(in, thread object) ([]any, error) {
	result := []any{}
	for _, value := range arr(in["attachments"]) {
		sent := obj(value)
		if _, whole := sent["kind"]; whole {
			result = append(result, sentAttachment(sent))
			continue
		}
		descriptor := threadAttachment(thread, str(sent["id"]))
		if descriptor == nil {
			return nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment was not found")
		}
		result = append(result, descriptor)
	}
	return result, nil
}

// A local transcript may omit older images' bytes to stay small. The model
// then reads such an image through its description, or not at all.
func (h *harness) hydrateContext(ctx context.Context, p *principal, thread object, temporary bool) error {
	name := str(thread["model"])
	c := h.catalog.get()
	if c == nil {
		return apiErr(503, "MODEL_UNAVAILABLE", "The catalog is unavailable")
	}
	vision := boolean(c.definition(name)["multimodal"], false)
	if name == "auto" {
		vision = true
		for _, candidate := range c.Models {
			vision = vision && boolean(candidate["multimodal"], false)
		}
	}
	for _, message := range arr(thread["messages"]) {
		for _, value := range arr(obj(message)["attachments"]) {
			a := obj(value)
			if !isImage(a) {
				continue
			}
			if !vision && str(a["description"]) != "" {
				continue
			}
			// Without its bytes, only its thumbnail was kept.
			if str(a["base64"]) == "" && (temporary || attachmentKey(a) == "") {
				continue
			}
			data, err := h.attachmentBytes(ctx, p, a, false)
			if unreachable(err) {
				continue
			}
			if err != nil {
				return err
			}
			resized, _, err := resizeImage(data)
			clear(data)
			if err != nil {
				return err
			}
			encoded := base64.StdEncoding.EncodeToString(resized)
			clear(resized)
			if vision {
				a["base64"], a["inferenceMimeType"] = encoded, "image/jpeg"
				continue
			}
			var model *model
			for _, m := range h.offer.get() {
				if m.vision {
					model = m
					break
				}
			}
			if model == nil {
				return apiErr(404, "MODEL_UNAVAILABLE", "No vision model can describe the image")
			}
			response, err := h.completion(ctx, model, object{"model": model.name, "messages": []object{{"role": "user", "content": []object{{"type": "text", "text": "Describe this image faithfully for a text-only assistant. Include visible text and details relevant to understanding it."}, {"type": "image_url", "image_url": object{"url": "data:image/jpeg;base64," + encoded}}}}}, "max_tokens": 2048})
			if err != nil {
				return err
			}
			a["description"] = response
		}
	}
	return nil
}
