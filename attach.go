package main

import (
	"bytes"
	"cmp"
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

// Text is read whole into a chat, so a text file may be larger than the
// files the harness converts. No upload is read past maxTextBytes.
const maxUploadBytes = 33 << 20
const maxTextBytes = 50 << 20
const maxImagePixels = 40_000_000

// An image is kept as the copy models read, no longer on either side than
// modelImageSide, with a thumbnail no longer than thumbnailSide.
const (
	modelImageSide = 1536
	thumbnailSide  = 256
)

// Resizing decodes an image whole, so the images a turn sends larger than
// the copy models read hold together no more pixels than one image may.
const maxTurnResizePixels = maxImagePixels

// An upload's form holds its file and a few short fields.
const (
	maxFormParts = 12
	maxFormField = 8 << 10
)

// uploadGroup is a kind of file uploads accept, named by extension, and the
// largest file of that kind. Clients pick a file's group by its extension.
type uploadGroup struct {
	Accept       []string `json:"accept"`
	MaxBytes     int      `json:"maxBytes"`
	ImageMaxSide int      `json:"imageMaxSide,omitempty"`
	kind         string   // the kind of attachment an upload of the group makes
}

var (
	textUploads     = uploadGroup{Accept: strings.Fields(".txt .md .py .js .jsx .ts .tsx .css .json .xml .yaml .yml .toml .sh .bash .rb .java .cpp .c .h .hpp .go .rs .swift .kt .kts .r .sql .lua .pl .php .cs .csx .vb .fs .fsx .scala .dart .ex .exs .erl .hs .ml .mli .clj .cljs .groovy .gradle .m .mm .zig .nim .jl .ps1 .psm1 .bat .cmd .asm .s .proto .graphql .gql .tf .tfvars .dockerfile .makefile .cmake .tex .bib .svg .properties .lock .diff .patch .env .ini .cfg .conf .log .rtf .html .htm .xhtml .csv .tsv .qfx .qif .ofx .ifs .qbo .qbx .bai .bai2 .mt940 .sta .ics .vcf"), MaxBytes: maxTextBytes, kind: "document"}
	imageUploads    = uploadGroup{Accept: strings.Fields(".jpg .jpeg .png .gif .webp .bmp .tiff .tif"), MaxBytes: maxUploadBytes, ImageMaxSide: modelImageSide, kind: "image"}
	documentUploads = uploadGroup{Accept: strings.Fields(".pdf .docx .pptx .xlsx"), MaxBytes: maxUploadBytes, kind: "document"}
	audioUploads    = uploadGroup{Accept: strings.Fields(".mp3 .wav .ogg .m4a .aac .flac .webm .wma"), MaxBytes: maxUploadBytes, kind: "audio"}
)

// A message attaches files of any group; a project keeps the text of its
// documents, which images do not have.
var (
	attachmentGroups      = []*uploadGroup{&textUploads, &imageUploads, &documentUploads, &audioUploads}
	projectDocumentGroups = []*uploadGroup{&textUploads, &documentUploads, &imageUploads, &audioUploads}
)

func (g *uploadGroup) accepts(filename string) bool {
	return slices.Contains(g.Accept, strings.ToLower(filepath.Ext(filename)))
}

// attachmentGroup is the group a message's file belongs to, if any.
func attachmentGroup(filename string) *uploadGroup {
	for _, g := range attachmentGroups {
		if g.accepts(filename) {
			return g
		}
	}
	return nil
}

// maxContent is the most an upload of the group holds: a text file's text
// and an image's bytes are the file, and a converted file's content is what
// the converter answers with.
func (g *uploadGroup) maxContent() int {
	if g == &textUploads || g == &imageUploads {
		return g.MaxBytes
	}
	return maxTextBytes
}

// What a client's composer does before anything reaches the harness: a
// paste longer than pasteAsFileChars becomes a text file, and dictation
// stops after dictationMaxSeconds.
const (
	pasteAsFileChars    = 3000
	dictationMaxSeconds = 10 * 60
)

// composerView is what a client's composer accepts: the groups of files
// uploads take, for a message and for a project.
type composerView struct {
	Attachments         []uploadGroup `json:"attachments"`
	ProjectDocuments    []uploadGroup `json:"projectDocuments"`
	PasteAsFileChars    int           `json:"pasteAsFileChars"`
	DictationMaxSeconds int           `json:"dictationMaxSeconds"`
}

func composer() composerView {
	shown := func(groups []*uploadGroup) []uploadGroup {
		out := []uploadGroup{}
		for _, g := range groups {
			out = append(out, *g)
		}
		return out
	}
	return composerView{
		Attachments:         shown(attachmentGroups),
		ProjectDocuments:    shown(projectDocumentGroups),
		PasteAsFileChars:    pasteAsFileChars,
		DictationMaxSeconds: dictationMaxSeconds,
	}
}

func randomBytes(n int) []byte { b := make([]byte, n); rand.Read(b); return b }

func (h *harness) handleUpload(w http.ResponseWriter, r *http.Request) {
	p, err := h.auth.authenticate(r, r.URL.Path == "/v1/transcriptions" || r.URL.Path == "/v1/attachments/upload")
	if err != nil {
		respond(w, nil, err)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTextBytes+(1<<20))
	reader, err := r.MultipartReader()
	if err != nil {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "Expected multipart form data"))
		return
	}
	fields := map[string]string{}
	var file []byte
	filename := ""
	for count := 0; ; count++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= maxFormParts {
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
			value, err := io.ReadAll(io.LimitReader(part, maxFormField+1))
			if err != nil || len(value) > maxFormField {
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
		respond(w, nil, h.withQuota(p, cred.Limit, err))
		return
	}
	ctx := context.WithValue(r.Context(), apiKeyKey{}, string(cred.Key))
	if r.URL.Path == "/v1/transcriptions" {
		text, err := h.transcribe(ctx, filename, file)
		respond(w, object{"text": text}, h.withQuota(p, cred.Limit, err))
		return
	}
	descriptor, err := h.preprocess(ctx, filename, file)
	if err != nil {
		respond(w, nil, h.withQuota(p, cred.Limit, err))
		return
	}
	// An upload is only converted. Its client sends the descriptor back with
	// the turn that attaches it, and that turn stores it with the chat.
	if r.URL.Path == "/v1/attachments/upload" {
		descriptor.ID = "att_" + token()
		respond(w, struct {
			Attachment Attachment `json:"attachment"`
		}{descriptor}, nil)
		return
	}
	key, err := decodeContentKey(fields["key"])
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	if r.URL.Path == "/v1/projects/documents/upload" {
		projectID := fields["projectId"]
		if err := requireID("projectId", projectID); err != nil {
			respond(w, nil, err)
			return
		}
		if _, err := h.loadProject(ctx, p, key, projectID); err != nil {
			respond(w, nil, err)
			return
		}
		if descriptor.isImage() {
			descriptor.TextContent, err = h.describeImage(ctx, descriptor.Base64)
			if err != nil {
				respond(w, nil, h.withQuota(p, cred.Limit, err))
				return
			}
		}
		if descriptor.TextContent == "" {
			respond(w, nil, apiErr(415, "ATTACHMENT_UNSUPPORTED", "Project documents require extracted text"))
			return
		}
		doc, err := h.createProjectDocument(ctx, p, key, ProjectDocument{ID: rowID(), ProjectID: projectID, Filename: filename, ContentType: descriptor.MimeType, SizeBytes: len(file), Content: descriptor.TextContent})
		respond(w, doc, err)
		return
	}
}

var errUnsupportedType = apiErr(415, "ATTACHMENT_UNSUPPORTED", "Unsupported file type")

// fitsUpload refuses a descriptor no upload makes: one of a file uploads do
// not take, an image without its bytes, or holding more than an upload of
// its file's group. A turn names an image its chat holds by id alone.
func (d *sentAttachment) fitsUpload() error {
	group := attachmentGroup(d.FileName)
	if group == nil || group.kind != d.Kind {
		return errUnsupportedType
	}
	if group == &imageUploads && d.Base64 == "" {
		return apiErr(400, "BAD_REQUEST", "An image attachment requires its bytes")
	}
	content := len(d.TextContent) + base64.StdEncoding.DecodedLen(len(d.Base64))
	for _, page := range d.Pages {
		content += len(page.Text) + len(page.Image)
	}
	if content > group.maxContent() {
		return apiErr(413, "ATTACHMENT_TOO_LARGE", "The attachment exceeds the size limit for its type")
	}
	return nil
}

// stored is the form of an attachment a turn sends whole that its chat
// stores: an upload's descriptor, and an image's bytes no larger than the
// copy an upload makes for models.
func (d *sentAttachment) stored() (Attachment, error) {
	a := Attachment{ID: d.ID, Kind: d.Kind, FileName: d.FileName, MimeType: d.MimeType, Size: d.Size, Thumbnail: valueOr(d.Thumbnail, ""), TextContent: d.TextContent, Description: d.Description, Pages: d.Pages, Base64: d.Base64}
	if !a.isImage() {
		return a, nil
	}
	data, err := base64.StdEncoding.DecodeString(a.Base64)
	if err != nil {
		return Attachment{}, apiErr(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
	}
	defer clear(data)
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return Attachment{}, apiErr(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
	}
	// An image within the bounds, as the copy an upload makes is, is kept.
	if max(cfg.Width, cfg.Height) <= modelImageSide {
		return a, nil
	}
	resized, thumb, err := resizeImage(data)
	if err != nil {
		return Attachment{}, err
	}
	a.Base64, a.Thumbnail = base64.StdEncoding.EncodeToString(resized), base64.StdEncoding.EncodeToString(thumb)
	return a, nil
}

// resizePixels is how many pixels storing the attachment decodes to resize
// it: an image's, when it is larger than the copy models read.
func (d *sentAttachment) resizePixels() int64 {
	if d.Kind != "image" || d.Base64 == "" {
		return 0
	}
	cfg, _, err := image.DecodeConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(d.Base64)))
	if err != nil || max(cfg.Width, cfg.Height) <= modelImageSide {
		return 0
	}
	return int64(cfg.Width) * int64(cfg.Height)
}

func (h *harness) preprocess(ctx context.Context, filename string, data []byte) (Attachment, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	group := attachmentGroup(filename)
	if group == nil {
		return Attachment{}, errUnsupportedType
	}
	descriptor := Attachment{Kind: group.kind, FileName: filename, MimeType: http.DetectContentType(data), Size: new(int64(len(data)))}
	if len(data) > group.MaxBytes {
		return Attachment{}, apiErr(413, "ATTACHMENT_TOO_LARGE", "The file exceeds the size limit for its type")
	}
	switch group {
	case &textUploads:
		if !utf8.Valid(data) || bytes.ContainsRune(data, 0) {
			return Attachment{}, apiErr(415, "ATTACHMENT_UNSUPPORTED", "Text uploads must contain UTF-8 text")
		}
		descriptor.TextContent = string(data)
		if m := mime.TypeByExtension(ext); m != "" {
			descriptor.MimeType = m
		}
	case &imageUploads:
		resized, thumb, err := resizeImage(data)
		if err != nil {
			return Attachment{}, err
		}
		descriptor.Base64, descriptor.Thumbnail = base64.StdEncoding.EncodeToString(resized), base64.StdEncoding.EncodeToString(thumb)
	case &documentUploads:
		if ext == ".pdf" && !bytes.HasPrefix(data, []byte("%PDF-")) || ext != ".pdf" && !bytes.HasPrefix(data, []byte("PK\x03\x04")) {
			return Attachment{}, apiErr(415, "ATTACHMENT_UNSUPPORTED", "The file contents do not match the document type")
		}
		result, err := h.convert(ctx, filename, data)
		if err != nil {
			return Attachment{}, err
		}
		// The converter answers with the document, or with it nested.
		type conversion struct {
			Markdown string         `json:"md_content"`
			Pages    []DocumentPage `json:"pages"`
		}
		var converted struct {
			conversion
			Document *conversion `json:"document"`
		}
		var mistyped *json.UnmarshalTypeError
		if err := json.Unmarshal(result, &converted); err != nil && !errors.As(err, &mistyped) {
			return Attachment{}, apiErr(502, "UPSTREAM_REFUSED", "Invalid document response")
		}
		document := converted.conversion
		if converted.Document != nil {
			document = *converted.Document
		}
		descriptor.TextContent = document.Markdown
		if len(document.Pages) > 0 {
			descriptor.Pages = document.Pages
		}
	case &audioUploads:
		text, err := h.transcribe(ctx, filename, data)
		if err != nil {
			return Attachment{}, err
		}
		descriptor.TextContent = text
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
	resized, err := encode(modelImageSide)
	if err != nil {
		return nil, nil, err
	}
	thumb, err := encode(thumbnailSide)
	return resized, thumb, err
}

// The document service queues conversions itself and answers a full queue
// with a delay, which a conversion waits out while it fits the deadline.
var conversionDeadline = 10 * time.Minute

func (h *harness) convert(ctx context.Context, filename string, data []byte) ([]byte, error) {
	router := h.services["router"]
	if router == nil {
		return nil, apiErr(503, "TOOL_UNAVAILABLE", "Document conversion is unavailable")
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

func multipartCall(ctx context.Context, service *downstreamService, path, fileField, name string, data []byte, fields map[string]string) ([]byte, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
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
	if len(data) > audioUploads.MaxBytes {
		return "", apiErr(413, "ATTACHMENT_TOO_LARGE", "The recording exceeds the size limit")
	}
	if !audioUploads.accepts(filename) {
		return "", apiErr(415, "ATTACHMENT_UNSUPPORTED", "Unsupported recording type")
	}
	// The router reads the model from the form, as the web client's SDK sends it.
	router := h.services["router"]
	if router == nil {
		return "", apiErr(503, "TOOL_UNAVAILABLE", "Transcription is unavailable")
	}
	b, err := multipartCall(ctx, router, "/v1/audio/transcriptions", "file", filename, data, map[string]string{"model": "voxtral-small-24b", "response_format": "text"})
	if err != nil {
		return "", err
	}
	var response struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(b, &response) == nil {
		return response.Text, nil
	}
	return string(b), nil
}

// attachment finds an attachment of the named thread for its client to download.
func (h *harness) attachment(ctx context.Context, p *principal, key contentKey, id, threadID string) (*Attachment, []byte, error) {
	if p.Anonymous {
		return nil, nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	if key == (contentKey{}) {
		return nil, nil, apiErr(401, "KEY_REQUIRED", "A content key is required")
	}
	var descriptor *Attachment
	if threadID != "" {
		row, err := h.loadThread(ctx, p, key, threadID)
		if err != nil {
			return nil, nil, err
		}
		descriptor = row.attachment(id)
	}
	if descriptor == nil {
		return nil, nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	b, err := h.attachmentBytes(ctx, p, descriptor, false)
	return descriptor, b, err
}

func (h *harness) attachmentBytes(ctx context.Context, p *principal, a *Attachment, public bool) ([]byte, error) {
	if a.Base64 != "" {
		return base64.StdEncoding.DecodeString(a.Base64)
	}
	data, err := h.getAttachment(ctx, p, a.ID, a.storageKey(), public)
	// Sync moves an image that only has a legacy key into its own storage
	// under the same id and key, after which the control plane refuses it.
	if err != nil && asAPIError(err).HTTP == http.StatusNotFound && !public && a.EncryptionKey == "" && a.Key != "" {
		return h.legacyAttachmentBytes(ctx, a.ID, a.Key)
	}
	return data, err
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
	body, err := readBody(w, r, maxRequestBody)
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
	var in attachmentRequest
	if err := decodeRequest(body, &in); err != nil {
		respond(w, nil, err)
		return
	}
	if err := in.validate(); err != nil {
		respond(w, nil, err)
		return
	}
	var descriptor *Attachment
	var data []byte
	if public {
		if _, err := decodeContentKey(in.AttKey); err != nil {
			respond(w, nil, err)
			return
		}
		// A share's client names the attachment and the key its link carries.
		descriptor = &Attachment{ID: in.ID, EncryptionKey: in.AttKey, Kind: cmp.Or(in.Kind, in.Type), FileName: in.FileName, MimeType: in.MimeType, TextContent: in.TextContent, Pages: in.Pages}
		data, err = h.attachmentBytes(r.Context(), &principal{Anonymous: true}, descriptor, true)
	} else {
		key, keyErr := requestKey(in.Key)
		if keyErr != nil {
			respond(w, nil, keyErr)
			return
		}
		defer clear(key[:])
		descriptor, data, err = h.attachment(r.Context(), p, key, in.ID, in.ThreadID)
	}
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(data)
	contentType, fileName := cmp.Or(descriptor.MimeType, http.DetectContentType(data)), cmp.Or(descriptor.FileName, "attachment")
	if documentRequest(descriptor, data) {
		// A stored document keeps its extracted content, not the uploaded file.
		content := descriptor.content()
		if content.empty() {
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
func documentRequest(a *Attachment, data []byte) bool {
	switch a.Kind {
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
func turnAttachments(in *turnRequest, t *Thread) ([]Attachment, error) {
	result := []Attachment{}
	for i := range in.Attachments {
		sent := &in.Attachments[i]
		if sent.whole() {
			stored, err := sent.stored()
			if err != nil {
				return nil, err
			}
			result = append(result, stored)
			continue
		}
		held := t.attachment(sent.ID)
		if held == nil {
			return nil, apiErr(404, "THREAD_NOT_FOUND", "The attachment was not found")
		}
		result = append(result, *held)
	}
	return result, nil
}

// A local transcript may omit older images' bytes to stay small. The model
// then reads such an image through its description, or not at all.
func (h *harness) hydrateContext(ctx context.Context, p *principal, t *Thread, keyless bool) error {
	vision := h.catalog.get().vision(t.Model)
	return t.eachAttachment(func(a *Attachment) error {
		if !a.isImage() || !vision && a.Description != "" {
			return nil
		}
		// Without its bytes, only its thumbnail was kept.
		if a.Base64 == "" && (keyless || a.storageKey() == "") {
			return nil
		}
		data, err := h.attachmentBytes(ctx, p, a, false)
		if unreachable(err) {
			return nil
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
			a.Base64, a.inferenceMimeType = encoded, "image/jpeg"
			return nil
		}
		response, err := h.describeImage(ctx, encoded)
		if err != nil {
			return err
		}
		a.Description = response
		return nil
	})
}

const imageDescriptionTokens = 2048

func (h *harness) describeImage(ctx context.Context, encoded string) (string, error) {
	for _, m := range h.offer.get() {
		if m.vision {
			return h.completion(ctx, m, object{"model": m.name, "messages": []object{{"role": "user", "content": []object{{"type": "text", "text": "Describe this image faithfully for a text-only assistant. Include visible text and details relevant to understanding it."}, {"type": "image_url", "image_url": object{"url": "data:image/jpeg;base64," + encoded}}}}}, "max_tokens": imageDescriptionTokens})
		}
	}
	return "", apiErr(404, "MODEL_UNAVAILABLE", "No vision model can describe the image")
}
