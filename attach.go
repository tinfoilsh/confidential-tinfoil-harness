package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
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

func uploadAccept() []string {
	result := []string{}
	result = append(result, documentExtensions...)
	result = append(result, imageExtensions...)
	result = append(result, audioExtensions...)
	return append(result, textExtensions...)
}
func randomBytes(n int) []byte { b := make([]byte, n); rand.Read(b); return b }

type pendingAttachment struct {
	owner, keyHash string
	descriptor     object
	bytes          []byte
	expires        time.Time
}

func publicAttachment(a object) object {
	out := object{"id": a["id"], "kind": firstString(a["kind"], a["type"]), "fileName": a["fileName"], "mimeType": a["mimeType"], "size": a["fileSize"], "thumbnail": a["thumbnailBase64"], "pages": nil, "status": "ready"}
	if text := str(a["textContent"]); text != "" {
		out["textContent"] = text
	}
	if pages := arr(a["pages"]); len(pages) > 0 {
		out["pages"] = len(pages)
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
	ephemeral := str(fields["ephemeral"]) == "true" || p.Anonymous
	var key contentKey
	if !p.Anonymous {
		key, err = decodeContentKey(str(fields["key"]))
		if err != nil {
			respond(w, nil, err)
			return
		}
	}
	defer clear(key[:])
	descriptor, err := h.preprocess(ctx, filename, file)
	if err != nil {
		respond(w, nil, err)
		return
	}
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
	id := "att_" + token()
	if !ephemeral {
		chatID := str(fields["threadId"])
		if chatID == "" {
			chatID = "pending_" + token()
		} else {
			if _, err := requiredID(fields, "threadId"); err != nil {
				respond(w, nil, err)
				return
			}
			if _, err := h.storeRows.load(ctx, p, key, "chat", chatID); err != nil {
				respond(w, nil, err)
				return
			}
		}
		var stored object
		err := h.syncAPI.call(ctx, p, "/v1/attachment/put", object{"chat_id": chatID, "plaintext": base64.StdEncoding.EncodeToString(file), "idempotency_key": uuid.NewString()}, &stored)
		if err != nil {
			respond(w, nil, err)
			return
		}
		id = str(stored["id"])
		descriptor["encryptionKey"] = stored["att_key"]
		descriptor["id"], descriptor["storageThreadId"] = id, chatID
		if err := h.storePageImages(ctx, p, chatID, descriptor); err != nil {
			respond(w, nil, err)
			return
		}
		_, err = h.mutate(ctx, p, key, "profile", "profile", true, nil, func(data object) error {
			index := obj(data["harnessAttachments"])
			index[id] = clone(descriptor)
			data["harnessAttachments"] = index
			return nil
		})
		if err != nil {
			respond(w, nil, err)
			return
		}
	} else {
		descriptor["id"] = id
	}
	if !ephemeral {
		respond(w, publicAttachment(descriptor), nil)
		return
	}
	h.chatMu.Lock()
	if h.attachments == nil {
		h.attachments = map[string]*pendingAttachment{}
	}
	cacheBytes := len(file) + len(raw(descriptor))
	for _, cached := range h.attachments {
		cacheBytes += len(cached.bytes) + len(raw(cached.descriptor))
	}
	if len(h.attachments) >= 256 || cacheBytes > 128<<20 {
		h.chatMu.Unlock()
		respond(w, nil, apiErr(503, "UPSTREAM_REFUSED", "The upload cache is at capacity"))
		return
	}
	entry := &pendingAttachment{owner: p.scope(), keyHash: key.fingerprint(), descriptor: clone(descriptor), expires: nowUTC().Add(runTimeout)}
	if ephemeral {
		entry.bytes = bytes.Clone(file)
	}
	h.attachments[p.scope()+"/"+id] = entry
	h.chatMu.Unlock()
	respond(w, publicAttachment(descriptor), nil)
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
		service := h.services["document"]
		if service == nil {
			return nil, apiErr(503, "TOOL_UNAVAILABLE", "Document conversion is unavailable")
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		result, err := multipartCall(ctx, service, "/v1/convert/file?mode=images", "files", filename, data, nil)
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
		descriptor["textContent"], descriptor["pages"] = converted["md_content"], converted["pages"]
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
	if service.Role == "audio" {
		req.Header.Set("X-Tinfoil-Model", "voxtral-small-24b")
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
		return nil, downstreamError(resp.StatusCode, b)
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
	service := h.services["audio"]
	if service == nil {
		return "", apiErr(503, "TOOL_UNAVAILABLE", "Transcription is unavailable")
	}
	b, err := multipartCall(ctx, service, "/v1/audio/transcriptions", "file", filename, data, object{"model": "voxtral-small-24b", "response_format": "text"})
	if err != nil {
		return "", err
	}
	var response object
	if json.Unmarshal(b, &response) == nil {
		return str(response["text"]), nil
	}
	return string(b), nil
}

func (h *harness) attachment(ctx context.Context, p *principal, key contentKey, id string) (object, []byte, error) {
	h.chatMu.Lock()
	cached := h.attachments[p.scope()+"/"+id]
	if cached != nil && cached.expires.After(nowUTC()) {
		if !p.Anonymous && cached.keyHash != key.fingerprint() {
			h.chatMu.Unlock()
			return nil, nil, apiErr(409, "KEY_MISMATCH", "The attachment uses another key")
		}
		descriptor, data := clone(cached.descriptor), bytes.Clone(cached.bytes)
		h.chatMu.Unlock()
		if len(data) > 0 {
			return descriptor, data, nil
		}
		b, err := h.attachmentBytes(ctx, p, descriptor, false)
		return descriptor, b, err
	}
	h.chatMu.Unlock()
	if p.Anonymous {
		return nil, nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment expired")
	}
	profile, err := h.profile(ctx, p, key)
	if err != nil {
		return nil, nil, err
	}
	descriptor := clone(obj(obj(profile["harnessAttachments"])[id]))
	if len(descriptor) == 0 {
		cursor := ""
		for {
			page, err := h.storeRows.list(ctx, p, key, "chat", cursor, "", 100)
			if err != nil {
				return nil, nil, err
			}
			for _, row := range page.Rows {
				normalizeThread(&row)
				for _, message := range arr(row.Data["messages"]) {
					for _, item := range arr(obj(message)["attachments"]) {
						if obj(item)["id"] == id {
							descriptor = clone(obj(item))
						}
					}
				}
			}
			if len(descriptor) > 0 || page.Next == "" || page.Next == cursor {
				break
			}
			cursor = page.Next
		}
	}
	if len(descriptor) == 0 {
		return nil, nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	b, err := h.attachmentBytes(ctx, p, descriptor, false)
	return descriptor, b, err
}
func (h *harness) attachmentBytes(ctx context.Context, p *principal, descriptor object, public bool) ([]byte, error) {
	if value := str(descriptor["base64"]); value != "" {
		return base64.StdEncoding.DecodeString(value)
	}
	path := "/v1/attachment/get"
	if public {
		path += "-public"
	}
	var result object
	err := h.syncAPI.call(ctx, p, path, object{"id": firstString(descriptor["storageId"], descriptor["id"]), "att_key": firstString(descriptor["encryptionKey"], descriptor["attKey"], descriptor["key"])}, &result)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(str(result["plaintext"]))
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
		key, keyErr := requestKey(p, in)
		if keyErr != nil {
			respond(w, nil, keyErr)
			return
		}
		defer clear(key[:])
		descriptor, data, err = h.attachment(r.Context(), p, key, id)
	}
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(data)
	contentType := firstString(descriptor["mimeType"], http.DetectContentType(data))
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": firstString(descriptor["fileName"], "attachment")}))
	w.Write(data)
}
func (h *harness) deleteAttachment(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "id")
	if err != nil {
		return nil, err
	}
	_, data, err := h.attachment(ctx, p, key, id)
	clear(data)
	if err != nil {
		return nil, err
	}
	if err := h.syncAPI.call(ctx, p, "/v1/attachment/delete", object{"id": id}, nil); err != nil {
		return nil, err
	}
	_, err = h.mutate(ctx, p, key, "profile", "profile", true, nil, func(data object) error { delete(obj(data["harnessAttachments"]), id); return nil })
	h.chatMu.Lock()
	if cached := h.attachments[p.scope()+"/"+id]; cached != nil {
		clear(cached.bytes)
	}
	delete(h.attachments, p.scope()+"/"+id)
	h.chatMu.Unlock()
	return object{}, err
}
func (h *harness) turnAttachments(ctx context.Context, p *principal, key contentKey, in, thread object, ephemeral bool) ([]any, error) {
	result := []any{}
	for _, value := range arr(in["attachments"]) {
		var descriptor object
		var data []byte
		var err error
		if ephemeral {
			h.chatMu.Lock()
			cached := h.attachments[p.scope()+"/"+str(value)]
			if cached != nil && cached.expires.After(nowUTC()) && (p.Anonymous || cached.keyHash == key.fingerprint()) && len(cached.bytes) > 0 {
				descriptor = clone(cached.descriptor)
				cached.expires = nowUTC().Add(runTimeout)
			}
			h.chatMu.Unlock()
			if descriptor == nil {
				return nil, apiErr(404, "THREAD_NOT_FOUND", "Upload this attachment as temporary before using it in an ephemeral thread")
			}
		} else {
			descriptor, data, err = h.attachment(ctx, p, key, str(value))
		}

		if err != nil {
			return nil, err
		}
		clear(data)
		result = append(result, descriptor)
	}
	return result, nil
}
func (h *harness) expireAttachments() {
	h.chatMu.Lock()
	defer h.chatMu.Unlock()
	for id, item := range h.attachments {
		if !item.expires.After(nowUTC()) {
			clear(item.bytes)
			delete(h.attachments, id)
		}
	}
}

func stripInlineAttachments(thread object) {
	for _, message := range arr(thread["messages"]) {
		delete(obj(message), "imageData")
		for _, value := range arr(obj(message)["attachments"]) {
			a := obj(value)
			if str(a["encryptionKey"]) != "" {
				delete(a, "base64")
				delete(a, "inferenceMimeType")
				delete(a, "legacyId")
				for _, page := range arr(a["pages"]) {
					delete(obj(page), "image")
				}
			}
		}
	}
}

func (h *harness) storePageImages(ctx context.Context, p *principal, chatID string, attachment object) error {
	for _, item := range arr(attachment["pages"]) {
		page := obj(item)
		encoded := str(page["image"])
		if encoded == "" {
			continue
		}
		if strings.HasPrefix(encoded, "data:") {
			_, encoded, _ = strings.Cut(encoded, ",")
		}
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return apiErr(502, "UPSTREAM_REFUSED", "Invalid document page image")
		}
		var stored object
		err = h.syncAPI.call(ctx, p, "/v1/attachment/put", object{"chat_id": chatID, "plaintext": base64.StdEncoding.EncodeToString(data), "idempotency_key": uuid.NewString()}, &stored)
		clear(data)
		if err != nil {
			return err
		}
		page["imageAttachment"] = object{"id": stored["id"], "encryptionKey": stored["att_key"], "mimeType": "image/png"}
		delete(page, "image")
	}
	return nil
}

func copyAttachmentDerivatives(source, target object) {
	byID := map[string]object{}
	for _, item := range arr(source["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			a := obj(value)
			byID[firstString(a["legacyId"], a["id"])] = a
		}
	}
	for _, item := range arr(target["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			a := obj(value)
			if source := byID[str(a["id"])]; source != nil {
				for _, field := range []string{"id", "description", "encryptionKey", "pages"} {
					if v, ok := source[field]; ok {
						a[field] = v
					}
				}
			}
		}
	}
}

func (h *harness) hydrateContext(ctx context.Context, p *principal, key contentKey, thread object, ephemeral bool) error {
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
			if a["type"] != "image" {
				if vision && !ephemeral {
					for _, item := range arr(a["pages"]) {
						page := obj(item)
						ref := obj(page["imageAttachment"])
						if str(ref["id"]) != "" {
							data, err := h.attachmentBytes(ctx, p, ref, false)
							if err != nil {
								return err
							}
							page["image"] = base64.StdEncoding.EncodeToString(data)
							clear(data)
						}
					}
				}
				continue
			}
			if !vision && str(a["description"]) != "" {
				continue
			}
			var data []byte
			var err error
			if ephemeral && str(a["base64"]) != "" {
				data, err = base64.StdEncoding.DecodeString(str(a["base64"]))
				if err != nil {
					return err
				}
			} else if ephemeral {
				h.chatMu.Lock()
				cached := h.attachments[p.scope()+"/"+str(a["id"])]
				if cached != nil {
					data = bytes.Clone(cached.bytes)
					cached.expires = nowUTC().Add(runTimeout)
				}
				h.chatMu.Unlock()
				if len(data) == 0 {
					return apiErr(404, "THREAD_NOT_FOUND", "The temporary attachment expired")
				}
			} else {
				data, err = h.attachmentBytes(ctx, p, a, false)
				if err != nil {
					return err
				}
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
			for _, m := range h.models {
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
			if ephemeral && str(a["base64"]) != "" {
				data, err = base64.StdEncoding.DecodeString(str(a["base64"]))
				if err != nil {
					return err
				}
			} else if ephemeral {
				h.chatMu.Lock()
				if cached := h.attachments[p.scope()+"/"+str(a["id"])]; cached != nil {
					cached.descriptor["description"] = response
				}
				h.chatMu.Unlock()
			}
		}
	}
	return nil
}

func (h *harness) storeLegacyAttachments(ctx context.Context, p *principal, thread object) error {
	for _, item := range arr(thread["messages"]) {
		for _, value := range arr(obj(item)["attachments"]) {
			a := obj(value)
			if str(a["encryptionKey"]) == "" {
				var data []byte
				var err error
				if encoded := str(a["base64"]); encoded != "" {
					data, err = base64.StdEncoding.DecodeString(encoded)
				} else if text := str(a["textContent"]); text != "" {
					data = []byte(text)
				}
				if err != nil {
					return apiErr(400, "ATTACHMENT_UNSUPPORTED", "An old attachment is not valid base64")
				}
				if len(data) > 0 {
					var stored object
					err = h.syncAPI.call(ctx, p, "/v1/attachment/put", object{"chat_id": thread["id"], "plaintext": base64.StdEncoding.EncodeToString(data), "idempotency_key": uuid.NewString()}, &stored)
					clear(data)
					if err != nil {
						return err
					}
					a["legacyId"], a["id"], a["encryptionKey"] = a["id"], stored["id"], stored["att_key"]
				}
			}
			if err := h.storePageImages(ctx, p, str(thread["id"]), a); err != nil {
				return err
			}
		}
	}
	return nil
}
