package attach

import (
	"bytes"
	"cmp"
	"context"
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
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
	_ "golang.org/x/image/bmp"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

type Service struct {
	Auth   *auth.Authenticator
	Models *inference.Models
	Store  *store.Store
	Pages  Pages
}

// Text is read whole into a chat, so a text file may be larger than the
// files the harness converts. No upload is read past maxTextBytes.
const MaxUploadBytes = 33 << 20
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
const MaxTurnResizePixels = maxImagePixels

// An upload's form holds its file and a few short fields.
const (
	MaxFormParts = 12
	MaxFormField = 8 << 10
)

// UploadGroup is a kind of file uploads accept, named by extension, and the
// largest file of that kind. Clients pick a file's group by its extension.
type UploadGroup struct {
	Accept       []string `json:"accept"`
	MaxBytes     int      `json:"maxBytes"`
	ImageMaxSide int      `json:"imageMaxSide,omitempty"`
	kind         string   // the kind of attachment an upload of the group makes
}

var (
	TextUploads     = UploadGroup{Accept: strings.Fields(".txt .md .py .js .jsx .ts .tsx .css .json .xml .yaml .yml .toml .sh .bash .rb .java .cpp .c .h .hpp .go .rs .swift .kt .kts .r .sql .lua .pl .php .cs .csx .vb .fs .fsx .scala .dart .ex .exs .erl .hs .ml .mli .clj .cljs .groovy .gradle .m .mm .zig .nim .jl .ps1 .psm1 .bat .cmd .asm .s .proto .graphql .gql .tf .tfvars .dockerfile .makefile .cmake .tex .bib .svg .properties .lock .diff .patch .env .ini .cfg .conf .log .rtf .html .htm .xhtml .csv .tsv .qfx .qif .ofx .ifs .qbo .qbx .bai .bai2 .mt940 .sta .ics .vcf"), MaxBytes: maxTextBytes, kind: "document"}
	ImageUploads    = UploadGroup{Accept: strings.Fields(".jpg .jpeg .png .gif .webp .bmp .tiff .tif"), MaxBytes: MaxUploadBytes, ImageMaxSide: modelImageSide, kind: "image"}
	documentUploads = UploadGroup{Accept: strings.Fields(".pdf .docx .pptx .xlsx"), MaxBytes: MaxUploadBytes, kind: "document"}
	audioUploads    = UploadGroup{Accept: strings.Fields(".mp3 .wav .ogg .m4a .aac .flac .webm .wma"), MaxBytes: MaxUploadBytes, kind: "audio"}
)

// A message attaches files of any group; a project keeps extracted text,
// image descriptions, and audio transcripts.
var (
	AttachmentGroups      = []*UploadGroup{&TextUploads, &ImageUploads, &documentUploads, &audioUploads}
	projectDocumentGroups = []*UploadGroup{&TextUploads, &documentUploads, &ImageUploads, &audioUploads}
)

// A file of no type uploads take is read as text, as the web client read
// it, when its first textSniffBytes hold no NUL byte and at most
// maxInvalidTextPercent of their characters are not UTF-8.
const (
	textSniffBytes        = 8192
	maxInvalidTextPercent = 5
)

func sniffsAsText(data []byte) bool {
	sample := data[:min(len(data), textSniffBytes)]
	if len(sample) == 0 || bytes.IndexByte(sample, 0) >= 0 {
		return false
	}
	chars, invalid := 0, 0
	for len(sample) > 0 {
		r, size := utf8.DecodeRune(sample)
		if r == utf8.RuneError && size == 1 {
			invalid++
		}
		chars++
		sample = sample[size:]
	}
	return invalid*100 <= chars*maxInvalidTextPercent
}

func (g *UploadGroup) accepts(filename string) bool {
	return slices.Contains(g.Accept, strings.ToLower(filepath.Ext(filename)))
}

// attachmentGroup is the group a message's file belongs to, if any.
func attachmentGroup(filename string) *UploadGroup {
	for _, g := range AttachmentGroups {
		if g.accepts(filename) {
			return g
		}
	}
	return nil
}

// maxContent is the most an upload of the group holds: a text file's text
// and an image's bytes are the file, and a converted file's content is what
// the converter answers with.
func (g *UploadGroup) maxContent() int {
	if g == &TextUploads || g == &ImageUploads {
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

// ComposerView is what a client's composer accepts: the groups of files
// uploads take, for a message and for a project.
type ComposerView struct {
	Attachments         []UploadGroup `json:"attachments"`
	ProjectDocuments    []UploadGroup `json:"projectDocuments"`
	PasteAsFileChars    int           `json:"pasteAsFileChars"`
	DictationMaxSeconds int           `json:"dictationMaxSeconds"`
}

func Composer() ComposerView {
	shown := func(groups []*UploadGroup) []UploadGroup {
		out := []UploadGroup{}
		for _, g := range groups {
			out = append(out, *g)
		}
		return out
	}
	return ComposerView{
		Attachments:         shown(AttachmentGroups),
		ProjectDocuments:    shown(projectDocumentGroups),
		PasteAsFileChars:    pasteAsFileChars,
		DictationMaxSeconds: dictationMaxSeconds,
	}
}

func (at *Service) HandleUpload(w http.ResponseWriter, r *http.Request) {
	p, err := at.Auth.Authenticate(r, r.URL.Path == "/v1/transcriptions" || r.URL.Path == "/v1/attachments/upload")
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	cred, err := at.Auth.Inference(r.Context(), p)
	if err != nil {
		api.Respond(w, nil, auth.WithQuota(p, cred.Limit, err))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTextBytes+(1<<20))
	reader, err := r.MultipartReader()
	if err != nil {
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Expected multipart form data"))
		return
	}
	fields := map[string]string{}
	var key auth.ContentKey
	defer clear(key[:])
	var keyField []byte
	defer func() { clear(keyField) }()
	var file []byte
	filename := ""
	for count := 0; ; count++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= MaxFormParts {
			api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Invalid multipart upload"))
			return
		}
		if part.FormName() == "file" {
			if filename != "" || part.FileName() == "" {
				api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Expected one named file"))
				return
			}
			if r.URL.Path == "/v1/projects/documents/upload" {
				key, err = auth.DecodeBase64Key(keyField)
				clear(keyField)
				keyField = nil
				if err != nil {
					api.Respond(w, nil, err)
					return
				}
				p.Grant, err = at.Store.Rows.AuthorizeStorage(r.Context(), p, key, "project-upload", fields["projectId"])
				if err != nil {
					api.Respond(w, nil, err)
					return
				}
			}
			filename = filepath.Base(part.FileName())
			file, err = io.ReadAll(io.LimitReader(part, maxTextBytes+1))
			if err != nil || len(file) > maxTextBytes {
				api.Respond(w, nil, api.Err(413, "ATTACHMENT_TOO_LARGE", "The upload exceeds the size limit"))
				return
			}
		} else {
			value, err := api.ReadWipeable(io.LimitReader(part, MaxFormField+1))
			if err != nil || len(value) > MaxFormField {
				clear(value)
				api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Multipart field is too large"))
				return
			}
			if _, exists := fields[part.FormName()]; exists {
				clear(value)
				api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Duplicate multipart field"))
				return
			}
			if part.FormName() == "key" {
				keyField = value
				fields["key"] = ""
			} else {
				fields[part.FormName()] = string(value)
				clear(value)
			}
		}
		part.Close()
	}
	defer clear(file)
	if filename == "" || len(file) == 0 {
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "A nonempty file is required"))
		return
	}
	ctx := auth.WithCallerKey(r.Context(), string(cred.Key))
	if r.URL.Path == "/v1/transcriptions" {
		text, err := at.transcribe(ctx, filename, file)
		api.Respond(w, api.Object{"text": text}, auth.WithQuota(p, cred.Limit, err))
		return
	}
	descriptor, err := at.Preprocess(ctx, filename, file)
	if err != nil {
		api.Respond(w, nil, auth.WithQuota(p, cred.Limit, err))
		return
	}
	// An upload is only converted. Its client sends the descriptor back with
	// the turn that attaches it, and that turn stores it with the chat.
	if r.URL.Path == "/v1/attachments/upload" {
		descriptor.ID = "att_" + api.Token()
		api.Respond(w, struct {
			Attachment domain.Attachment `json:"attachment"`
		}{descriptor}, nil)
		return
	}
	if r.URL.Path == "/v1/projects/documents/upload" {
		projectID := fields["projectId"]
		if err := api.RequireID("projectId", projectID); err != nil {
			api.Respond(w, nil, err)
			return
		}
		if _, err := at.Store.LoadProject(ctx, p, key, projectID); err != nil {
			api.Respond(w, nil, err)
			return
		}
		if descriptor.IsImage() {
			descriptor.TextContent, err = at.describeImage(ctx, descriptor.Base64)
			if err != nil {
				api.Respond(w, nil, auth.WithQuota(p, cred.Limit, err))
				return
			}
		}
		if descriptor.TextContent == "" {
			api.Respond(w, nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "Project documents require extracted text"))
			return
		}
		doc, err := at.Store.CreateProjectDocument(ctx, p, key, domain.ProjectDocument{ID: domain.RowID(), ProjectID: projectID, Filename: filename, ContentType: descriptor.MimeType, SizeBytes: len(file), Content: descriptor.TextContent})
		api.Respond(w, doc, err)
		return
	}
}

var errUnsupportedType = api.Err(415, "ATTACHMENT_UNSUPPORTED", "Unsupported file type")

// fitsUpload refuses a descriptor no upload makes: one of a file uploads do
// not take, an image without its bytes, or holding more than an upload of
// its file's group. A turn names an image its chat holds by id alone.
func (d *Sent) fitsUpload() error {
	group := attachmentGroup(d.FileName)
	// A file of no type uploads take was read as text.
	if group == nil && d.Kind == TextUploads.kind && d.Base64 == "" && len(d.Pages) == 0 {
		group = &TextUploads
	}
	if group == nil || group.kind != d.Kind {
		return errUnsupportedType
	}
	if group == &ImageUploads && d.Base64 == "" {
		return api.Err(400, "BAD_REQUEST", "An image attachment requires its bytes")
	}
	content := len(d.TextContent) + base64.StdEncoding.DecodedLen(len(d.Base64))
	for _, page := range d.Pages {
		content += len(page.Text) + len(page.Image)
	}
	if content > group.maxContent() {
		return api.Err(413, "ATTACHMENT_TOO_LARGE", "The attachment exceeds the size limit for its type")
	}
	return nil
}

// stored is the form of an attachment a turn sends whole that its chat
// stores: an upload's descriptor, and an image's bytes no larger than the
// copy an upload makes for models.
func (d *Sent) stored() (domain.Attachment, error) {
	a := domain.Attachment{ID: d.ID, Kind: d.Kind, FileName: d.FileName, MimeType: d.MimeType, Size: d.Size, Thumbnail: domain.ValueOr(d.Thumbnail, ""), TextContent: d.TextContent, Description: d.Description, Pages: d.Pages, Base64: d.Base64}
	if !a.IsImage() {
		return a, nil
	}
	data, err := base64.StdEncoding.DecodeString(a.Base64)
	if err != nil {
		return domain.Attachment{}, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
	}
	defer clear(data)
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return domain.Attachment{}, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
	}
	// An image within the bounds, as the copy an upload makes is, is kept.
	if max(cfg.Width, cfg.Height) <= modelImageSide {
		return a, nil
	}
	resized, thumb, err := ResizeImage(data)
	if err != nil {
		return domain.Attachment{}, err
	}
	a.Base64, a.Thumbnail = base64.StdEncoding.EncodeToString(resized), base64.StdEncoding.EncodeToString(thumb)
	return a, nil
}

// ResizePixels is how many pixels storing the attachment decodes to resize
// it: an image's, when it is larger than the copy models read.
func (d *Sent) ResizePixels() int64 {
	if d.Kind != "image" || d.Base64 == "" {
		return 0
	}
	cfg, _, err := image.DecodeConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(d.Base64)))
	if err != nil || max(cfg.Width, cfg.Height) <= modelImageSide {
		return 0
	}
	return int64(cfg.Width) * int64(cfg.Height)
}

func (at *Service) Preprocess(ctx context.Context, filename string, data []byte) (domain.Attachment, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	group := attachmentGroup(filename)
	if group == nil && sniffsAsText(data) {
		group = &TextUploads
	}
	if group == nil {
		return domain.Attachment{}, errUnsupportedType
	}
	descriptor := domain.Attachment{Kind: group.kind, FileName: filename, MimeType: http.DetectContentType(data), Size: new(int64(len(data)))}
	if len(data) > group.MaxBytes {
		return domain.Attachment{}, api.Err(413, "ATTACHMENT_TOO_LARGE", "The file exceeds the size limit for its type")
	}
	switch group {
	case &TextUploads:
		if bytes.ContainsRune(data, 0) {
			return domain.Attachment{}, api.Err(415, "ATTACHMENT_UNSUPPORTED", "Text uploads must contain UTF-8 text")
		}
		// As the web client read text, what is not UTF-8 reads as U+FFFD,
		// which can make the text longer than the file.
		descriptor.TextContent = strings.ToValidUTF8(string(data), string(utf8.RuneError))
		if len(descriptor.TextContent) > group.maxContent() {
			return domain.Attachment{}, api.Err(413, "ATTACHMENT_TOO_LARGE", "The file exceeds the size limit for its type")
		}
		if m := mime.TypeByExtension(ext); m != "" {
			descriptor.MimeType = m
		}
	case &ImageUploads:
		resized, thumb, err := ResizeImage(data)
		if err != nil {
			return domain.Attachment{}, err
		}
		descriptor.Base64, descriptor.Thumbnail = base64.StdEncoding.EncodeToString(resized), base64.StdEncoding.EncodeToString(thumb)
	case &documentUploads:
		if ext == ".pdf" && !bytes.HasPrefix(data, []byte("%PDF-")) || ext != ".pdf" && !bytes.HasPrefix(data, []byte("PK\x03\x04")) {
			return domain.Attachment{}, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The file contents do not match the document type")
		}
		result, err := at.Convert(ctx, filename, data)
		if err != nil {
			return domain.Attachment{}, err
		}
		// The converter answers with the document, or with it nested.
		type conversion struct {
			Markdown string                `json:"md_content"`
			Pages    []domain.DocumentPage `json:"pages"`
		}
		var converted struct {
			conversion
			Document *conversion `json:"document"`
		}
		var mistyped *json.UnmarshalTypeError
		if err := json.Unmarshal(result, &converted); err != nil && !errors.As(err, &mistyped) {
			return domain.Attachment{}, api.Err(502, "UPSTREAM_REFUSED", "Invalid document response")
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
		text, err := at.transcribe(ctx, filename, data)
		if err != nil {
			return domain.Attachment{}, err
		}
		descriptor.TextContent = text
	}
	return descriptor, nil
}
func ResizeImage(data []byte) ([]byte, []byte, error) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width)*int64(cfg.Height) > maxImagePixels {
		return nil, nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The image cannot be decoded within the pixel limit")
	}
	im, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
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
var ConversionDeadline = 10 * time.Minute

func (at *Service) Convert(ctx context.Context, filename string, data []byte) ([]byte, error) {
	router := at.Models.Services["router"]
	if router == nil {
		return nil, api.Err(503, "TOOL_UNAVAILABLE", "Document conversion is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, ConversionDeadline)
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
	var e *api.Error
	if !errors.As(err, &e) || e.Code == "QUOTA_EXHAUSTED" || e.Status != http.StatusTooManyRequests && e.Status != http.StatusServiceUnavailable {
		return 0
	}
	return time.Duration(e.RetryAfter) * time.Second
}

func multipartCall(ctx context.Context, service *inference.Service, path, fileField, name string, data []byte, fields map[string]string) ([]byte, error) {
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
	if key := auth.CallerKey(ctx); key != "" {
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
		return nil, api.Err(413, "ATTACHMENT_TOO_LARGE", "The converted document exceeds the text limit")
	}
	if resp.StatusCode/100 != 2 {
		return nil, api.WithRetryAfter(api.DownstreamError(resp.StatusCode, b), resp.Header)
	}
	return b, nil
}
func (at *Service) transcribe(ctx context.Context, filename string, data []byte) (string, error) {
	if len(data) > audioUploads.MaxBytes {
		return "", api.Err(413, "ATTACHMENT_TOO_LARGE", "The recording exceeds the size limit")
	}
	if !audioUploads.accepts(filename) {
		return "", api.Err(415, "ATTACHMENT_UNSUPPORTED", "Unsupported recording type")
	}
	// The router reads the model from the form, as the web client's SDK sends it.
	router := at.Models.Services["router"]
	if router == nil {
		return "", api.Err(503, "TOOL_UNAVAILABLE", "Transcription is unavailable")
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
func (at *Service) attachment(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, threadID string) (*domain.Attachment, []byte, error) {
	if p.Anonymous {
		return nil, nil, api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	if key == (auth.ContentKey{}) {
		return nil, nil, api.Err(401, "KEY_REQUIRED", "A content key is required")
	}
	var descriptor *domain.Attachment
	if threadID != "" {
		row, err := at.Store.LoadThread(ctx, p, key, threadID)
		if err != nil {
			return nil, nil, err
		}
		descriptor = row.FindAttachment(id)
	}
	if descriptor == nil {
		return nil, nil, api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	b, err := at.Bytes(ctx, p, descriptor)
	return descriptor, b, err
}

// Bytes is an attachment's content: the copy it carries, or its blob.
func (at *Service) Bytes(ctx context.Context, p *auth.Principal, a *domain.Attachment) ([]byte, error) {
	if a.Base64 != "" {
		return base64.StdEncoding.DecodeString(a.Base64)
	}
	return at.Store.GetAttachment(ctx, p, a.ID, a.StorageKey())
}

// OpaqueContentType is the type every downloaded attachment is sent as.
const OpaqueContentType = "application/octet-stream"

func (at *Service) HandleAttachmentGet(w http.ResponseWriter, r *http.Request) {
	p, err := at.Auth.Caller(r, true)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	body, err := api.ReadBody(w, r, api.MaxRequestBody)
	defer clear(body)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	public := r.URL.Path == "/v1/attachments/get-public"
	var in attachmentRequest
	defer in.ClearKey()
	if err := api.DecodeBody(body, &in); err != nil {
		api.Respond(w, nil, err)
		return
	}
	if err := in.Validate(); err != nil {
		api.Respond(w, nil, err)
		return
	}
	var descriptor *domain.Attachment
	var data []byte
	if public {
		encodedKey := []byte(in.AttKey)
		key, keyErr := auth.DecodeBase64Key(encodedKey)
		clear(encodedKey)
		clear(key[:])
		if keyErr != nil {
			api.Respond(w, nil, keyErr)
			return
		}
		// A share's client names the attachment and the key its link carries.
		descriptor = &domain.Attachment{ID: in.ID, EncryptionKey: in.AttKey, Kind: cmp.Or(in.Kind, in.Type), FileName: in.FileName, MimeType: in.MimeType, TextContent: in.TextContent, Pages: in.Pages}
		data, err = at.Bytes(r.Context(), &auth.Principal{Anonymous: true}, descriptor)
	} else {
		key, keyErr := auth.RequestKey(in.Key)
		in.ClearKey()
		if keyErr != nil {
			api.Respond(w, nil, keyErr)
			return
		}
		defer clear(key[:])
		descriptor, data, err = at.attachment(r.Context(), p, key, in.ID, in.ThreadID)
		clear(key[:])
	}
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer clear(data)
	if documentRequest(descriptor, data) {
		// A stored document keeps its extracted content, not the uploaded file.
		content := Content(descriptor)
		if content.empty() {
			if content, err = DecodeDocument(data); err != nil {
				api.Respond(w, nil, err)
				return
			}
		}
		clear(data)
		data = []byte(DocumentText(content))
	}
	// EHBP seals the body but not the headers, so the response names neither
	// the file nor its type; the client reads both from the chat it holds.
	w.Header().Set("Content-Type", OpaqueContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Write(data)
}

// A shared chat's client may not say what an attachment is. A document blob is
// JSON, and no supported image format starts with a brace.
func documentRequest(a *domain.Attachment, data []byte) bool {
	switch a.Kind {
	case "image":
		return false
	case "":
		trimmed := bytes.TrimSpace(data)
		return len(trimmed) > 0 && trimmed[0] == '{'
	}
	return true
}

// OfTurn resolves the attachments a turn names: an upload sent
// whole, or by id one the thread already holds, as an edit sends it.
func OfTurn(attachments []Sent, t *domain.Thread) ([]domain.Attachment, error) {
	result := []domain.Attachment{}
	for i := range attachments {
		sent := &attachments[i]
		if sent.whole() {
			stored, err := sent.stored()
			if err != nil {
				return nil, err
			}
			result = append(result, stored)
			continue
		}
		held := t.FindAttachment(sent.ID)
		if held == nil {
			return nil, api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")
		}
		result = append(result, *held)
	}
	return result, nil
}

// A local transcript may omit older images' bytes to stay small. The model
// then reads such an image through its description, or not at all.
func (at *Service) HydrateContext(ctx context.Context, p *auth.Principal, t *domain.Thread, keyless bool) error {
	vision := at.Models.Catalog.Get().Vision(t.Model)
	return t.EachAttachment(func(a *domain.Attachment) error {
		if !a.IsImage() || !vision && a.Description != "" {
			return nil
		}
		// Without its bytes, only its thumbnail was kept.
		if a.Base64 == "" && (keyless || a.StorageKey() == "") {
			return nil
		}
		data, err := at.Bytes(ctx, p, a)
		if Unreachable(err) {
			return nil
		}
		if err != nil {
			return err
		}
		resized, _, err := ResizeImage(data)
		clear(data)
		if err != nil {
			return err
		}
		encoded := base64.StdEncoding.EncodeToString(resized)
		clear(resized)
		if vision {
			a.Base64, a.InferenceMimeType = encoded, "image/jpeg"
			return nil
		}
		response, err := at.describeImage(ctx, encoded)
		if err != nil {
			return err
		}
		a.Description = response
		return nil
	})
}

const imageDescriptionTokens = 2048

func (at *Service) describeImage(ctx context.Context, encoded string) (string, error) {
	for _, m := range at.Models.Offer.Get() {
		if m.Vision {
			return at.Models.Completion(ctx, m, api.Object{"model": m.Name, "messages": []api.Object{{"role": "user", "content": []api.Object{{"type": "text", "text": "Describe this image faithfully for a text-only assistant. Include visible text and details relevant to understanding it."}, {"type": "image_url", "image_url": api.Object{"url": "data:image/jpeg;base64," + encoded}}}}}, "max_tokens": imageDescriptionTokens})
		}
	}
	return "", api.Err(404, "MODEL_UNAVAILABLE", "No vision model can describe the image")
}
