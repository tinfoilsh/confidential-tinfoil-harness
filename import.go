package main

import (
	"archive/zip"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/importer"
)

// An import reads an uploaded archive in the harness: a native backup of
// any version, or a ChatGPT, Claude or Tinfoil export. What goes to the
// cloud is written as a native cloud import, which sync's import job
// stores. Chats for the device are streamed back as NDJSON, one thread per
// line, and nothing of them is kept.

const (
	// maxImportBody bounds an upload: the archive and its options.
	maxImportBody = 512<<20 + 1<<20
	// maxCloudImport is the largest archive sync's import job takes.
	maxCloudImport = 512 << 20
	// importChunk is the size sync's import job takes an archive in.
	importChunk = 8 << 20
	// importSpaceFactor is the space an import expects to need: its upload
	// and the cloud import written from it, which is no larger than the
	// upload it comes from.
	importSpaceFactor = 2
	// Sync's import job refuses an archive past these.
	maxImportChats       = 100_000
	maxImportMessages    = 2_000_000
	maxImportAttachments = 100_000
	cloudImportFormat    = "tinfoil-native-cloud-import"
	cloudImportSource    = "tinfoil_backup"
)

// importNamespace names the ids an import derives, so importing the same
// conversation again gives it the same ids.
var importNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://chat.tinfoil.sh/import"))

// sniffedImages are the image types an import stores, as sync accepts them.
var sniffedImages = []string{"image/png", "image/jpeg", "image/gif", "image/webp", "image/bmp"}

type importOptions struct {
	api.KeyRequest
	Target string `json:"target"`
}

func (h *harness) handleImport(w http.ResponseWriter, r *http.Request) {
	// An archive calls no model, so any signed-in caller may make one,
	// with or without chat access.
	p, err := h.auth.authenticate(r, false)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	size, err := requestSize(r, maxImportBody)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	lease, err := h.scratch.lease(r.Context(), importSpaceFactor*size)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer lease.release()
	lift := h.scratch.idleBody(w, r)
	r.Body = http.MaxBytesReader(w, r.Body, maxImportBody)
	upload, options, err := readImportForm(r, lease, func(options importOptions) error {
		if options.Target != "cloud" {
			return nil
		}
		if len(options.Key) == 0 {
			return api.Err(401, "KEY_REQUIRED", "A content key is required")
		}
		return h.authorizeArchive(r.Context(), p, options.Key, "import")
	})
	defer options.ClearKey()
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	lift()
	run := &importRun{h: h, p: p, lease: lease, out: &importStream{w: w}, seen: map[string]bool{}}
	switch options.Target {
	case "device":
		options.ClearKey()
	case "cloud":
		key, err := decodeContentKey(options.Key)
		options.ClearKey()
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		defer clear(key[:])
		run.key = &key
	default:
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "target must be cloud or device"))
		return
	}
	err = run.run(r.Context(), upload)
	run.out.finish(run.jobID, run.warnings.list(), err)
}

// requestSize is the space a request body may take: its declared length,
// or the most it may hold.
func requestSize(r *http.Request, limit int64) (int64, error) {
	switch {
	case r.ContentLength > limit:
		return 0, api.Err(413, "PAYLOAD_TOO_LARGE", "The request exceeds the size limit")
	case r.ContentLength < 0:
		return limit, nil
	}
	return r.ContentLength, nil
}

func readImportForm(r *http.Request, lease *scratchLease, authorize func(importOptions) error) (*scratchFile, importOptions, error) {
	var options importOptions
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, options, api.Err(400, "BAD_REQUEST", "Expected multipart form data")
	}
	var upload *scratchFile
	hasOptions := false
	for count := 0; ; count++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= maxFormParts {
			return nil, options, formError(err)
		}
		switch part.FormName() {
		case "file":
			if !hasOptions {
				return nil, options, api.Err(400, "BAD_REQUEST", "options must precede the archive")
			}
			if upload != nil {
				return nil, options, api.Err(400, "BAD_REQUEST", "Expected one archive")
			}
			if upload, err = lease.create(); err != nil {
				return nil, options, err
			}
			if _, err := io.Copy(upload, part); err != nil {
				return nil, options, formError(err)
			}
		case "options":
			if hasOptions {
				return nil, options, api.Err(400, "BAD_REQUEST", "Duplicate multipart field")
			}
			hasOptions = true
			if err := decodeFormField(part, &options); err != nil {
				return nil, options, err
			}
			if err := authorize(options); err != nil {
				return nil, options, err
			}
		default:
			return nil, options, api.Err(400, "BAD_REQUEST", "Unknown multipart field: "+part.FormName())
		}
		part.Close()
	}
	if upload == nil || upload.Size() == 0 || !hasOptions {
		return nil, options, api.Err(400, "BAD_REQUEST", "An archive and options are required")
	}
	return upload, options, nil
}

// formError is a multipart body's failure: the harness's own refusal, a
// body over its limit, or a malformed form.
func formError(err error) error {
	var refused *api.Error
	if errors.As(err, &refused) {
		return refused
	}
	if err == nil {
		return api.Err(400, "BAD_REQUEST", "The form has too many parts")
	}
	return api.BodyError(err, "Invalid multipart form")
}

func decodeFormField(part *multipart.Part, v any) error {
	b, err := api.ReadWipeable(io.LimitReader(part, maxFormField+1))
	defer clear(b)
	if err != nil || len(b) > maxFormField {
		return api.Err(400, "BAD_REQUEST", "Multipart field is too large")
	}
	if !api.IsObject(b) {
		return api.Err(400, "BAD_REQUEST", part.FormName()+" must be a JSON object")
	}
	return api.DecodeRequest(b, v)
}

// importRun is one import: where its chats go and what it left out.
type importRun struct {
	h     *harness
	p     *principal
	key   *contentKey // set when the import goes to the cloud
	lease *scratchLease
	out   *importStream
	cloud *cloudArchive
	jobID *string
	// source names what the chats came from, which their derived ids keep.
	source string
	// seen holds the conversations already read, which a split export repeats.
	seen                         map[string]bool
	chats, messages, attachments int
	recognized                   bool
	warnings                     importWarnings
}

func (j *importRun) run(ctx context.Context, upload *scratchFile) error {
	zr, err := zip.NewReader(upload, upload.Size())
	if err != nil {
		head := make([]byte, 4)
		upload.ReadAt(head, 0)
		if bytes.Equal(head, []byte("PK\x03\x04")) {
			return api.Err(400, "IMPORT_INVALID", "The archive is damaged")
		}
		if err := importer.Records(upload.section(0, upload.Size()), func(record json.RawMessage) error { return j.record(record, nil, nil) }); err != nil {
			return importError(err)
		}
		return j.complete(ctx)
	}
	if len(zr.File) > maxArchiveEntries {
		return api.Err(413, "IMPORT_TOO_LARGE", "The archive holds too many files")
	}
	backup, err := openNativeBackup(zr)
	if err != nil {
		return err
	}
	if backup != nil {
		return j.native(ctx, backup)
	}
	return j.export(ctx, zr)
}

// importError is an error reading an export, as the client is told it.
func importError(err error) error {
	if errors.Is(err, importer.ErrInvalid) {
		return api.Err(400, "IMPORT_INVALID", "The export cannot be read: "+strings.TrimPrefix(err.Error(), importer.ErrInvalid.Error()+": "))
	}
	return err
}

// complete stores what goes to the cloud once every record is read.
func (j *importRun) complete(ctx context.Context) error {
	if !j.recognized {
		return api.Err(400, "IMPORT_INVALID", "This file is not a Tinfoil backup or a ChatGPT, Claude or Tinfoil export")
	}
	if j.key == nil && !j.out.started && j.warnings.projectsSkipped > 0 {
		return api.Err(400, "PROJECTS_REQUIRE_CLOUD", "Projects are kept in the cloud; turn on cloud sync to import them")
	}
	if j.cloud == nil {
		return nil
	}
	if err := j.cloud.finish(); err != nil {
		return err
	}
	job, err := j.h.stageImport(ctx, j.p, *j.key, j.cloud.file)
	if err != nil {
		return err
	}
	j.jobID = &job
	return nil
}

func (j *importRun) record(record json.RawMessage, index *importer.Index, read func(string) ([]byte, error)) error {
	format := importer.Classify(record)
	j.recognized = j.recognized || format != ""
	if j.source == "" {
		j.source = string(format)
	}
	switch format {
	case importer.ChatGPT:
		c, err := importer.ReadChatGPT(record, index)
		if err != nil {
			return err
		}
		return j.conversation(c, read)
	case importer.Claude:
		c, err := importer.ReadClaude(record, index)
		if err != nil {
			return err
		}
		return j.conversation(c, read)
	case importer.ClaudeProjects:
		project, err := importer.ReadClaudeProject(record)
		if err != nil || project == nil {
			return err
		}
		return j.project(project)
	}
	return api.Err(400, "IMPORT_INVALID", "This file is not a Tinfoil backup or a ChatGPT, Claude or Tinfoil export")
}

// export reads a ZIP export of ChatGPT, Claude or Tinfoil.
func (j *importRun) export(ctx context.Context, zr *zip.Reader) error {
	files := map[string]*zip.File{}
	var names []string
	expanded := uint64(0)
	for _, f := range zr.File {
		if !safeEntryName(f) || files[f.Name] != nil {
			continue
		}
		if expanded += f.UncompressedSize64; expanded > maxArchiveExpansion {
			return api.Err(413, "IMPORT_TOO_LARGE", "The archive expands past the size limit")
		}
		files[f.Name] = f
		names = append(names, f.Name)
	}
	index := importer.NewIndex(names)
	read := func(entry string) ([]byte, error) {
		f := files[entry]
		if f == nil {
			return nil, errAttachmentMissing
		}
		return readEntry(f, maxArchiveImage)
	}
	records := append(conversationFiles(names), projectFiles(names)...)
	if len(records) == 0 {
		return api.Err(400, "IMPORT_INVALID", "The archive holds no conversations or projects")
	}
	for _, name := range records {
		r, err := files[name].Open()
		if err != nil {
			return api.Err(400, "IMPORT_INVALID", "An archive entry cannot be read")
		}
		err = importer.Records(io.LimitReader(r, maxArchiveJSONEntry), func(record json.RawMessage) error { return j.record(record, index, read) })
		r.Close()
		if err != nil {
			return importError(err)
		}
	}
	return j.complete(ctx)
}

var splitConversations = regexp.MustCompile(`^conversations-([0-9]+)\.json$`)

// conversationFiles are the conversations of an export: conversations.json
// and the parts ChatGPT splits a large history into, in the shallowest
// folder that has them.
func conversationFiles(names []string) []string {
	var found []string
	for _, name := range names {
		if base := path.Base(name); base == "conversations.json" || splitConversations.MatchString(base) {
			found = append(found, name)
		}
	}
	found = shallowest(found)
	order := func(name string) int {
		m := splitConversations.FindStringSubmatch(path.Base(name))
		if m == nil {
			return -1
		}
		n, _ := strconv.Atoi(m[1])
		return n
	}
	sort.SliceStable(found, func(a, b int) bool { return order(found[a]) < order(found[b]) })
	return found
}

// projectFiles are Claude's projects: one projects.json, or one file per
// project in a projects folder at the top of the archive.
func projectFiles(names []string) []string {
	var found, split []string
	for _, name := range names {
		switch {
		case path.Base(name) == "projects.json":
			found = append(found, name)
		case path.Dir(name) == "projects" && path.Ext(name) == ".json":
			split = append(split, name)
		}
	}
	return append(shallowest(found), split...)
}

func shallowest(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	depth := func(name string) int { return strings.Count(name, "/") }
	least := depth(names[0])
	for _, name := range names {
		least = min(least, depth(name))
	}
	var kept []string
	for _, name := range names {
		if depth(name) == least {
			kept = append(kept, name)
		}
	}
	return kept
}

// conversation imports one chat of an export.
func (j *importRun) conversation(c *importer.Conversation, read func(string) ([]byte, error)) error {
	if c == nil || j.seen[c.Key] {
		return nil
	}
	j.seen[c.Key] = true
	t := j.conversationThread(c, read)
	if j.key != nil {
		return j.toCloud(c.Key, "", &t)
	}
	return j.toDevice(c.Key, &t)
}

// conversationThread is an export's conversation in the thread format.
// Its message ids derive from the conversation, so the cloud and the
// device get the same thread.
func (j *importRun) conversationThread(c *importer.Conversation, read func(string) ([]byte, error)) Thread {
	var first time.Time
	if len(c.Messages) > 0 {
		first = c.Messages[0].CreatedAt
	}
	created := firstTime(c.CreatedAt, c.UpdatedAt, first, api.Now())
	t := Thread{Title: cmp.Or(strings.TrimSpace(c.Title), "Imported Chat"), TitleState: "generated", CreatedAt: isoString(created), UpdatedAt: isoString(firstTime(c.UpdatedAt, created)),
		Model: autoModelName, WebSearchEnabled: true, Messages: []Message{}}
	for i, m := range c.Messages {
		id := uuid.NewSHA1(importNamespace, []byte(c.Key+"\x00"+strconv.Itoa(i))).String()
		message := Message{ID: id, Role: m.Role, CreatedAt: isoString(firstTime(m.CreatedAt, created)), Content: m.Content}
		if m.Role == "assistant" {
			if m.Thoughts != "" {
				message.Timeline = append(message.Timeline, Block{ID: id + "-thinking", Type: "thinking", Content: m.Thoughts})
			}
			if m.Content != "" {
				message.Timeline = append(message.Timeline, Block{ID: id + "-content", Type: "content", Content: m.Content})
			}
		}
		for k, a := range m.Attachments {
			attachment := Attachment{ID: cmp.Or(a.ID, id+"-attachment-"+strconv.Itoa(k)), Kind: "document", FileName: a.FileName, MimeType: a.MimeType, TextContent: a.TextContent}
			if a.Size > 0 {
				attachment.Size = new(a.Size)
			}
			if a.Image {
				attachment.Kind = "image"
				if a.Entry != "" {
					if data, err := read(a.Entry); err == nil && slices.Contains(sniffedImages, http.DetectContentType(data)) {
						attachment.Base64, attachment.MimeType, attachment.Size = base64.StdEncoding.EncodeToString(data), http.DetectContentType(data), new(int64(len(data)))
					}
				}
			}
			message.Attachments = append(message.Attachments, attachment)
		}
		t.Messages = append(t.Messages, message)
	}
	return t
}

func firstTime(times ...time.Time) time.Time {
	for _, t := range times {
		if !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func isoString(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// project imports a Claude project with its documents. Projects are kept
// in the cloud only.
func (j *importRun) project(project *importer.Project) error {
	if j.key == nil {
		j.warnings.projectsSkipped++
		return nil
	}
	cloud, err := j.cloudArchive()
	if err != nil {
		return err
	}
	if err := cloud.entity("project", project.Key, "", cloudProject{Name: cmp.Or(project.Name, "Imported Project"), Description: project.Description, SystemInstructions: project.Instructions, Memory: []json.RawMessage{}}); err != nil {
		return err
	}
	for i, doc := range project.Documents {
		size := int64(len(doc.Content))
		source := project.Key + "/" + cmp.Or(doc.Key, strconv.Itoa(i))
		if err := cloud.entity("document", source, project.Key, cloudDocument{Filename: doc.FileName, ContentType: "text/markdown", SourceSizeBytes: size, SizeBytes: size, Content: doc.Content}); err != nil {
			return err
		}
	}
	return nil
}

// prepare leaves out of an imported chat what no client may keep: run
// state, server keys, messages of other roles, and attachments without
// their content. It counts what it leaves out.
func (j *importRun) prepare(t *Thread) error {
	t.ActiveRun, t.ContextUsage, t.HiddenCount, t.Flagged = nil, nil, 0, false
	t.LastRequestID, t.LastRequestKind = "", ""
	if _, err := time.Parse(time.RFC3339Nano, t.CreatedAt); err != nil {
		t.CreatedAt = cmp.Or(isoTime(t.UpdatedAt), api.Timestamp())
	}
	if _, err := time.Parse(time.RFC3339Nano, t.UpdatedAt); err != nil {
		t.UpdatedAt = t.CreatedAt
	}
	messages := t.Messages[:0]
	for _, m := range t.Messages {
		if !slices.Contains(messageRoles, m.Role) {
			continue
		}
		if _, err := time.Parse(time.RFC3339Nano, m.CreatedAt); err != nil {
			m.CreatedAt = t.CreatedAt
		}
		var attachments []Attachment
		for _, a := range m.Attachments {
			a.EncryptionKey, a.Key, a.StorageThreadID = "", "", ""
			a.FileName = cmp.Or(a.FileName, "attachment")
			switch {
			case a.isImage() && a.Base64 == "":
				j.warnings.imagesLeftOut++
			case !a.isImage() && !a.hasContent():
				j.warnings.documentsLeftOut++
			default:
				attachments = append(attachments, a)
			}
		}
		m.Attachments = attachments
		messages = append(messages, m)
		j.attachments += len(attachments)
	}
	t.Messages = messages
	j.chats++
	j.messages += len(messages)
	if j.chats > maxImportChats || j.messages > maxImportMessages || j.attachments > maxImportAttachments {
		return api.Err(413, "IMPORT_TOO_LARGE", "The archive holds more chats, messages or attachments than an import takes")
	}
	return nil
}

// toDevice streams a chat back to its client under an id derived from the
// account, the source and the chat, so importing it again names it the same.
func (j *importRun) toDevice(sourceID string, t *Thread) error {
	if err := j.prepare(t); err != nil {
		return err
	}
	if t.ProjectID != nil {
		j.warnings.projectLinksDropped++
	}
	created, _ := time.Parse(time.RFC3339Nano, t.CreatedAt)
	t.ID = fmt.Sprintf("%013d_%s", max(0, maxReverseMillis-created.UnixMilli()), uuid.NewSHA1(importNamespace, []byte(j.p.scope()+"\x00"+j.source+"\x00"+sourceID)))
	t.ProjectID, t.Pinned = nil, false
	return j.out.thread(withOriginals(t))
}

// importThread is a device chat as its line carries it: each image as the
// copy and thumbnail an upload makes, with the archive's original beside it
// for its client to keep apart from the thread.
type importThread struct {
	Thread
	Messages []importMessage `json:"messages"`
}

type importMessage struct {
	Message
	Attachments []importAttachment `json:"attachments,omitzero"`
}

type importAttachment struct {
	Attachment
	OriginalBase64 string `json:"originalBase64,omitempty"`
}

func withOriginals(t *Thread) *importThread {
	line := &importThread{Thread: *t, Messages: make([]importMessage, len(t.Messages))}
	line.RecordVersion = recordVersion
	for i, m := range t.Messages {
		message := importMessage{Message: m}
		for _, a := range m.Attachments {
			attachment := importAttachment{Attachment: a}
			if a.isImage() {
				attachment.OriginalBase64 = a.Base64
				modelImageCopy(&attachment.Attachment)
			}
			message.Attachments = append(message.Attachments, attachment)
		}
		line.Messages[i] = message
	}
	return line
}

func (j *importRun) toCloud(sourceID, projectSourceID string, t *Thread) error {
	if err := j.prepare(t); err != nil {
		return err
	}
	cloud, err := j.cloudArchive()
	if err != nil {
		return err
	}
	return cloud.chat(sourceID, projectSourceID, t)
}

func (j *importRun) cloudArchive() (*cloudArchive, error) {
	if j.cloud == nil {
		file, err := j.lease.create()
		if err != nil {
			return nil, err
		}
		j.cloud = newCloudArchive(file, j.source)
	}
	return j.cloud, nil
}

// importWarnings count what an import left out.
type importWarnings struct {
	imagesLeftOut, documentsLeftOut, projectsSkipped, projectLinksDropped, backupOmissions int
}

func (w importWarnings) list() []string {
	out := []string{}
	for _, warning := range []struct {
		count int
		text  string
	}{
		{w.imagesLeftOut, "%d images were left out: the archive does not hold them, or they are not supported images."},
		{w.documentsLeftOut, "%d documents were left out: the archive holds no text for them."},
		{w.projectsSkipped, "%d projects were not imported: projects are kept in the cloud."},
		{w.projectLinksDropped, "%d chats were imported without their project."},
		{w.backupOmissions, "The backup left out %d items when it was made."},
	} {
		if warning.count > 0 {
			out = append(out, fmt.Sprintf(warning.text, warning.count))
		}
	}
	return out
}

// importStream answers an import: a line per device chat, then one that
// says it is done. An import that fails before its first line answers with
// an error alone; one that fails after ends with an error line.
type importStream struct {
	w       http.ResponseWriter
	enc     *json.Encoder
	started bool
}

type importLine struct {
	Thread *importThread `json:"thread,omitempty"`
	Done   *importDone   `json:"done,omitempty"`
	Error  *api.Error    `json:"error,omitempty"`
}

type importDone struct {
	JobID    *string  `json:"jobId"`
	Warnings []string `json:"warnings"`
}

func (s *importStream) start() {
	if s.started {
		return
	}
	s.started = true
	s.w.Header().Set("Content-Type", "application/x-ndjson")
	s.w.Header().Set("Cache-Control", "no-store")
	s.w.WriteHeader(http.StatusOK)
	s.enc = json.NewEncoder(s.w)
}

func (s *importStream) thread(t *importThread) error {
	s.start()
	if err := s.enc.Encode(importLine{Thread: t}); err != nil {
		return err
	}
	http.NewResponseController(s.w).Flush()
	return nil
}

func (s *importStream) finish(jobID *string, warnings []string, err error) {
	if err != nil && !s.started {
		api.Respond(s.w, nil, err)
		return
	}
	s.start()
	if err != nil {
		s.enc.Encode(importLine{Error: api.AsError(err)})
		return
	}
	s.enc.Encode(importLine{Done: &importDone{JobID: jobID, Warnings: warnings}})
}

// errAttachmentMissing is an attachment whose bytes its source does not hold.
var errAttachmentMissing = api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")

// A native cloud import is the archive sync's import job stores: one entry
// per project, document and chat, each chat image's bytes as a blob, and a
// manifest listing them. Sync maps every source id to an id of its own and
// skips an entity it stored from the same source before.
type cloudArchive struct {
	file     *scratchFile
	zip      *zip.Writer
	manifest cloudManifest
}

type cloudManifest struct {
	Format         string        `json:"format"`
	Version        int           `json:"version"`
	SourceBackupID string        `json:"source_backup_id"`
	Counts         cloudCounts   `json:"counts"`
	Entities       []cloudEntity `json:"entities"`
	Blobs          []cloudBlob   `json:"blobs"`
}

type cloudCounts struct {
	Projects  int `json:"projects"`
	Documents int `json:"documents"`
	Chats     int `json:"chats"`
	Blobs     int `json:"blobs"`
}

type cloudEntity struct {
	Kind            string `json:"kind"`
	SourceID        string `json:"source_id"`
	ProjectSourceID string `json:"project_source_id,omitempty"`
	Path            string `json:"path"`
	SHA256          string `json:"sha256"`
	SizeBytes       int    `json:"size_bytes"`
}

type cloudBlob struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	SizeBytes int    `json:"size_bytes"`
}

// cloudProject and cloudDocument hold exactly the fields sync's import
// takes for them.
type cloudProject struct {
	Name               string            `json:"name"`
	Description        string            `json:"description,omitempty"`
	SystemInstructions string            `json:"systemInstructions,omitempty"`
	Color              string            `json:"color,omitempty"`
	Memory             []json.RawMessage `json:"memory"`
}

type cloudDocument struct {
	Filename        string `json:"filename"`
	ContentType     string `json:"contentType"`
	SourceSizeBytes int64  `json:"sourceSizeBytes"`
	SizeBytes       int64  `json:"sizeBytes"`
	Content         string `json:"content"`
}

// cloudChat is a thread with the fields sync's import reads: each message's
// time, each attachment's type, and the blob that holds an image.
type cloudChat struct {
	Thread
	Messages    []cloudMessage `json:"messages"`
	IsLocalOnly bool           `json:"isLocalOnly"`
}

type cloudMessage struct {
	Message
	Timestamp   string            `json:"timestamp"`
	Attachments []cloudAttachment `json:"attachments,omitzero"`
}

type cloudAttachment struct {
	Attachment
	Type        string `json:"type"`
	ArchivePath string `json:"archivePath,omitempty"`
}

func newCloudArchive(file *scratchFile, source string) *cloudArchive {
	return &cloudArchive{file: file, zip: zip.NewWriter(file), manifest: cloudManifest{Format: cloudImportFormat, Version: 1, SourceBackupID: source, Entities: []cloudEntity{}, Blobs: []cloudBlob{}}}
}

func (c *cloudArchive) entity(kind, sourceID, projectSourceID string, payload any) error {
	data := api.Raw(payload)
	name := "entities/" + kind + "/" + strconv.Itoa(len(c.manifest.Entities)) + ".json"
	if err := writeEntry(c.zip, name, data, true); err != nil {
		return err
	}
	c.manifest.Entities = append(c.manifest.Entities, cloudEntity{Kind: kind, SourceID: sourceID, ProjectSourceID: projectSourceID, Path: name, SHA256: hashHex(data), SizeBytes: len(data)})
	switch kind {
	case "project":
		c.manifest.Counts.Projects++
	case "document":
		c.manifest.Counts.Documents++
	case "chat":
		c.manifest.Counts.Chats++
	}
	return nil
}

// chat writes a prepared thread, its images as blobs.
func (c *cloudArchive) chat(sourceID, projectSourceID string, t *Thread) error {
	payload := cloudChat{Thread: *t, Messages: make([]cloudMessage, 0, len(t.Messages))}
	payload.ProjectID = nil
	for _, m := range t.Messages {
		message := cloudMessage{Message: m, Timestamp: m.CreatedAt}
		for _, a := range m.Attachments {
			attachment := cloudAttachment{Attachment: a, Type: "document"}
			if a.isImage() {
				data, err := base64.StdEncoding.DecodeString(a.Base64)
				if err != nil {
					return api.Err(400, "IMPORT_INVALID", "An image is not valid base64")
				}
				name := "blobs/" + strconv.Itoa(len(c.manifest.Blobs)) + ".bin"
				if err := writeEntry(c.zip, name, data, false); err != nil {
					return err
				}
				c.manifest.Blobs = append(c.manifest.Blobs, cloudBlob{Path: name, SHA256: hashHex(data), SizeBytes: len(data)})
				c.manifest.Counts.Blobs++
				attachment.Type, attachment.ArchivePath, attachment.Base64 = "image", name, ""
			}
			message.Attachments = append(message.Attachments, attachment)
		}
		payload.Messages = append(payload.Messages, message)
	}
	return c.entity("chat", sourceID, projectSourceID, payload)
}

func (c *cloudArchive) finish() error {
	if err := writeEntry(c.zip, manifestPath, api.Raw(c.manifest), true); err != nil {
		return err
	}
	return c.zip.Close()
}

// Sync takes an archive in chunks, then starts the job that stores it.

type importCreate struct {
	Source        string `json:"source"`
	TotalBytes    int64  `json:"total_bytes"`
	TotalChunks   int64  `json:"total_chunks"`
	ArchiveSHA256 string `json:"archive_sha256"`
}

type importChunkUpload struct {
	UploadID    string `json:"upload_id"`
	ChunkIndex  int64  `json:"chunk_index"`
	ChunkSHA256 string `json:"chunk_sha256"`
	Data        string `json:"data"`
}

func (h *harness) stageImport(ctx context.Context, p *principal, key contentKey, archive *scratchFile) (string, error) {
	defer clear(key[:])
	size := archive.Size()
	if size > maxCloudImport {
		return "", api.Err(413, "IMPORT_TOO_LARGE", "The chats for the cloud exceed what one import takes")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, archive.section(0, size)); err != nil {
		return "", err
	}
	chunks := (size + importChunk - 1) / importChunk
	var created struct {
		JobID    string `json:"job_id"`
		UploadID string `json:"upload_id"`
	}
	if err := h.syncImport(ctx, p, "create", importCreate{Source: cloudImportSource, TotalBytes: size, TotalChunks: chunks, ArchiveSHA256: hex.EncodeToString(hash.Sum(nil))}, &created); err != nil {
		return "", err
	}
	for i := int64(0); i < chunks; i++ {
		data := make([]byte, min(importChunk, size-i*importChunk))
		if _, err := archive.ReadAt(data, i*importChunk); err != nil && err != io.EOF {
			return "", err
		}
		err := h.syncImport(ctx, p, "upload", importChunkUpload{UploadID: created.UploadID, ChunkIndex: i, ChunkSHA256: hashHex(data), Data: base64.StdEncoding.EncodeToString(data)}, nil)
		clear(data)
		if err != nil {
			return "", err
		}
	}
	return created.JobID, h.syncAPI.callKey(ctx, p, "/v1/import/start", key, api.Object{"job_id": created.JobID}, nil)
}

// importKindCounts is what sync's job did with one kind of item.
type importKindCounts struct {
	Imported int `json:"imported"`
	Skipped  int `json:"skipped"`
	Failed   int `json:"failed"`
	Blocked  int `json:"blocked"`
}

// importStatusView is a job's status as sync reports it, in the client's
// names.
type importStatusView struct {
	JobID         string                      `json:"jobId"`
	Status        string                      `json:"status"`
	Phase         string                      `json:"phase,omitempty"`
	Imported      int                         `json:"imported"`
	Failed        int                         `json:"failed"`
	Total         int                         `json:"total"`
	Counts        map[string]importKindCounts `json:"counts,omitempty"`
	Warnings      []string                    `json:"warnings,omitempty"`
	Errors        []string                    `json:"errors,omitempty"`
	FailureReason string                      `json:"failureReason,omitempty"`
	FailureDetail string                      `json:"failureDetail,omitempty"`
}

func (h *harness) importStatus(ctx context.Context, p *principal, _ contentKey, in *importStatusRequest) (any, error) {
	var status struct {
		importStatusView
		FailureReason string `json:"failure_reason"`
		FailureDetail string `json:"failure_detail"`
	}
	if err := h.syncImport(ctx, p, "status", struct {
		JobID string `json:"job_id"`
	}{in.JobID}, &status); err != nil {
		return nil, err
	}
	view := status.importStatusView
	view.JobID = in.JobID
	view.FailureReason, view.FailureDetail = status.FailureReason, status.FailureDetail
	return view, nil
}
