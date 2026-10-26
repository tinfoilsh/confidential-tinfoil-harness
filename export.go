package main

import (
	"archive/zip"
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func (h *harness) shareThread(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *shareRequest) (any, error) {
	defer in.ClearKey()
	id := in.ThreadID
	var t domain.Thread
	if in.Local {
		var err error
		if t, err = domain.ReadDeviceThread(id, in.Thread); err != nil {
			return nil, err
		}
	} else {
		key, err := auth.DecodeContentKey(in.Key)
		in.ClearKey()
		if err != nil {
			return nil, err
		}
		defer clear(key[:])
		row, err := h.loadThread(ctx, p, key, id)
		clear(key[:])
		if err != nil {
			return nil, err
		}
		t = row.Thread
	}
	payload := domain.SharePayload{V: 1, Title: t.Title, Messages: []domain.ShareMessage{}, CreatedAt: domain.Millis(t.CreatedAt)}
	for _, m := range t.Messages {
		shared := domain.ShareMessage{Role: m.Role, Content: m.Content, Timestamp: domain.Millis(m.CreatedAt), Timeline: domain.CheckedWidgets(m.Timeline), ModelDisplayName: m.ModelDisplayName, IsError: m.IsError, Quote: m.Quote, Annotations: m.Annotations}
		for _, a := range m.Attachments {
			view := domain.ViewAttachment(&a)
			view.EncryptionKey = a.EncryptionKey
			shared.Attachments = append(shared.Attachments, view)
		}
		payload.Messages = append(payload.Messages, shared)
	}
	ciphertext, shareKey, err := h.sealShare(ctx, p, api.Raw(payload))
	if err != nil {
		return nil, err
	}
	// A share is keyed by its chat, like the web client's: sync keeps a shared
	// chat's blobs, and the owner can revoke the link by chat.
	if _, err := h.shareRequest(ctx, p, http.MethodPut, id, ciphertext); err != nil {
		return nil, err
	}
	return struct {
		URL string `json:"url"`
	}{domain.ChatURL + "/share/" + url.PathEscape(id) + "#v2:" + shareKey}, nil
}

// The share endpoints authenticate the owner's Clerk session.
func (h *harness) shareRequest(ctx context.Context, p *auth.Principal, method, id string, body []byte) ([]byte, error) {
	ctx = auth.WithCallerKey(ctx, string(p.JWT))
	path := h.controlplane + "/api/shares/" + url.PathEscape(id)
	if method == http.MethodGet {
		path += "/status"
	}
	req, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Format-Version", "1")
	}
	resp, err := h.cpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode/100 != 2 {
		return nil, api.DownstreamError(resp.StatusCode, b)
	}
	return b, nil
}

func (h *harness) revokeShare(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *threadRequest) (any, error) {
	if _, err := h.shareRequest(ctx, p, http.MethodDelete, in.ThreadID, nil); err != nil && api.AsError(err).HTTP != 404 {
		return nil, err
	}
	return struct{}{}, nil
}

func (h *harness) shareStatus(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *threadRequest) (any, error) {
	b, err := h.shareRequest(ctx, p, http.MethodGet, in.ThreadID, nil)
	if err != nil {
		return nil, err
	}
	var status struct {
		Shared *bool `json:"shared"`
	}
	if json.Unmarshal(b, &status) != nil || status.Shared == nil {
		return nil, api.Err(502, "UPSTREAM_REFUSED", "Invalid share status")
	}
	return struct {
		Shared bool `json:"shared"`
	}{*status.Shared}, nil
}

// sharedThreadView is an opened share's payload, read like any old chat.
func sharedThreadView(id string, payload []byte) (domain.SharedThread, error) {
	t, err := domain.ReadThread(id, payload)
	if err != nil {
		return domain.SharedThread{}, err
	}
	shared := domain.SharedThread{Title: t.Title, CreatedAt: domain.Present(t.CreatedAt), Messages: []domain.SharedMessage{}}
	for i := range t.Messages {
		m := &t.Messages[i]
		view := domain.SharedMessage{MessageView: domain.ViewMessage(m)}
		for j, a := range view.MessageView.Attachments {
			view.Attachments = append(view.Attachments, domain.SharedAttachment{Attachment: a, AttKey: domain.Present(m.Attachments[j].EncryptionKey)})
		}
		shared.Messages = append(shared.Messages, view)
	}
	return shared, nil
}

func (h *harness) openShare(ctx context.Context, _ *auth.Principal, _ auth.ContentKey, in *openShareRequest) (any, error) {
	id := in.ShareID
	fragment, ok := strings.CutPrefix(in.Fragment, "v2:")
	if !ok {
		return nil, api.Err(400, "BAD_REQUEST", "A v2 share fragment is required")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", h.controlplane+"/api/shares/"+url.PathEscape(id), nil)
	resp, err := h.cpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	ciphertext, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, api.DownstreamError(resp.StatusCode, ciphertext)
	}
	plain, err := h.openSealedShare(ctx, fragment, ciphertext)
	if err != nil {
		return nil, err
	}
	defer clear(plain)
	var version struct {
		V float64 `json:"v"`
	}
	if trimmed := bytes.TrimSpace(plain); len(trimmed) == 0 || trimmed[0] != '{' || json.Unmarshal(trimmed, &version) != nil || int(version.V) != 1 {
		return nil, api.Err(400, "BAD_REQUEST", "Invalid shared thread")
	}
	return sharedThreadView(id, plain)
}

// An export is one streamed ZIP archive of the chats a client keeps on its
// device, which it sends, and, with its content key, everything it stores
// in the cloud. Native backups hold every chat whole; a conversations
// export holds them in the Claude-compatible format the web app wrote.

const (
	// maxExportBody bounds an export request: its device chats and the
	// images they show.
	maxExportBody = 512 << 20
	// maxExportParts bounds the chats and images an export request sends.
	maxExportParts = maxArchiveEntries
	// maxProjectsExport bounds the Claude-compatible projects file.
	maxProjectsExport = 64 << 20
)

type exportOptions struct {
	api.KeyRequest
	Format string `json:"format"`
}

// exportForm is an export request: its options, and its device chats and
// the originals of their images, spooled to scratch space.
type exportForm struct {
	options   exportOptions
	spool     *scratchFile
	threads   []spooledPart
	originals map[string]spooledPart
}

type spooledPart struct {
	offset, size int64
}

func (h *harness) handleExport(w http.ResponseWriter, r *http.Request) {
	// An archive calls no model, so any signed-in caller may make one,
	// with or without chat access.
	p, err := h.auth.Authenticate(r, false)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	size, err := requestSize(r, maxExportBody)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	lease, err := h.scratch.lease(r.Context(), size)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer lease.release()
	lift := h.scratch.idleBody(w, r)
	r.Body = http.MaxBytesReader(w, r.Body, maxExportBody)
	form, err := readExportForm(r, lease, func(options exportOptions) error { return h.authorizeArchive(r.Context(), p, options.Key, "export") })
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer form.options.ClearKey()
	lift()
	var key *auth.ContentKey
	if len(form.options.Key) != 0 {
		decoded, err := auth.DecodeContentKey(form.options.Key)
		form.options.ClearKey()
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		defer clear(decoded[:])
		key = &decoded
	}
	ctx := r.Context()
	switch form.options.Format {
	case "native":
		streamArchive(w, "tinfoil-backup.zip", func(out io.Writer) error { return h.writeBackup(ctx, p, key, form, out) })
	case "conversations":
		streamArchive(w, "tinfoil-chats.zip", func(out io.Writer) error { return h.writeConversations(ctx, p, key, form, lease, out) })
	case "claude-projects":
		if key == nil {
			api.Respond(w, nil, api.Err(401, "KEY_REQUIRED", "A content key is required"))
			return
		}
		projects, err := h.claudeProjects(ctx, p, *key)
		if err == nil {
			w.Header().Set("Content-Disposition", `attachment; filename="projects.json"`)
		}
		api.Respond(w, projects, err)
	default:
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "format must be native, conversations or claude-projects"))
	}
}

func readExportForm(r *http.Request, lease *scratchLease, authorize func(exportOptions) error) (_ *exportForm, err error) {
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, api.Err(400, "BAD_REQUEST", "Expected multipart form data")
	}
	spool, err := lease.create()
	if err != nil {
		return nil, err
	}
	form := &exportForm{spool: spool, originals: map[string]spooledPart{}}
	defer func() {
		if err != nil {
			form.options.ClearKey()
		}
	}()
	hasOptions, ids := false, map[string]bool{}
	for count := 0; ; count++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= maxExportParts {
			return nil, formError(err)
		}
		switch part.FormName() {
		case "options":
			if hasOptions {
				return nil, api.Err(400, "BAD_REQUEST", "Duplicate multipart field")
			}
			hasOptions = true
			if err := decodeFormField(part, &form.options); err != nil {
				return nil, err
			}
			if err := authorize(form.options); err != nil {
				return nil, err
			}
		case "thread":
			if !hasOptions {
				return nil, api.Err(400, "BAD_REQUEST", "options must precede the chats")
			}
			data, err := io.ReadAll(io.LimitReader(part, domain.MaxTurnBody+1))
			if err != nil {
				return nil, formError(err)
			}
			if len(data) > domain.MaxTurnBody {
				return nil, api.Err(413, "PAYLOAD_TOO_LARGE", "A chat exceeds the size limit")
			}
			if !api.IsObject(data) {
				return nil, api.Err(400, "BAD_REQUEST", "Each thread must be an object")
			}
			var named idRequest
			json.Unmarshal(data, &named)
			if err := named.validate(); err != nil {
				return nil, err
			}
			if err := validateLocalThread(data); err != nil {
				return nil, err
			}
			if ids[named.ID] {
				return nil, api.Err(400, "BAD_REQUEST", "Each thread is sent once")
			}
			ids[named.ID] = true
			spooled, err := form.spoolPart(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			form.threads = append(form.threads, spooled)
		case "file":
			if !hasOptions {
				return nil, api.Err(400, "BAD_REQUEST", "options must precede the files")
			}
			id := part.FileName()
			if err := api.RequireID("file name", id); err != nil {
				return nil, err
			}
			if _, ok := form.originals[id]; ok {
				return nil, api.Err(400, "BAD_REQUEST", "Each file is sent once")
			}
			spooled, err := form.spoolPart(io.LimitReader(part, int64(imageUploads.MaxBytes)+1))
			if err != nil {
				return nil, err
			}
			if spooled.size > int64(imageUploads.MaxBytes) {
				return nil, api.Err(413, "ATTACHMENT_TOO_LARGE", "A file exceeds the size limit")
			}
			form.originals[id] = spooled
		default:
			return nil, api.Err(400, "BAD_REQUEST", "Unknown multipart field: "+part.FormName())
		}
		part.Close()
	}
	if !hasOptions {
		return nil, api.Err(400, "BAD_REQUEST", "options are required")
	}
	return form, nil
}

func (f *exportForm) spoolPart(r io.Reader) (spooledPart, error) {
	offset := f.spool.Size()
	n, err := io.Copy(f.spool, r)
	if err != nil {
		return spooledPart{}, formError(err)
	}
	return spooledPart{offset: offset, size: n}, nil
}

func (f *exportForm) read(part spooledPart) ([]byte, error) {
	return io.ReadAll(f.spool.section(part.offset, part.size))
}

// thread reads one of the request's device chats.
func (f *exportForm) thread(i int) (domain.Thread, error) {
	data, err := f.read(f.threads[i])
	if err != nil {
		return domain.Thread{}, err
	}
	var named idRequest
	json.Unmarshal(data, &named)
	return domain.ReadDeviceThread(named.ID, data)
}

// deviceImage is a device chat image's bytes: the original its client
// sent, or the copy the chat holds.
func (f *exportForm) deviceImage(a *domain.Attachment) ([]byte, error) {
	if part, ok := f.originals[a.ID]; ok {
		return f.read(part)
	}
	if a.Base64 == "" {
		return nil, errAttachmentMissing
	}
	data, err := base64.StdEncoding.DecodeString(a.Base64)
	if err != nil {
		return nil, errAttachmentMissing
	}
	return data, nil
}

// noDocument is a device chat's document, which holds whatever content it has.
func noDocument(*domain.Attachment) error { return errAttachmentMissing }

// cloudImage is a stored image's bytes; one sync no longer holds is missing.
func (h *harness) cloudImage(ctx context.Context, p *auth.Principal) func(*domain.Attachment) ([]byte, error) {
	return func(a *domain.Attachment) ([]byte, error) {
		if a.Base64 == "" && a.StorageKey() == "" {
			return nil, errAttachmentMissing
		}
		data, err := h.attachmentBytes(ctx, p, a, false)
		if unreachable(err) {
			return nil, errAttachmentMissing
		}
		return data, err
	}
}

func (h *harness) cloudDocument(ctx context.Context, p *auth.Principal) func(*domain.Attachment) error {
	return func(a *domain.Attachment) error {
		err := h.inflateDocument(ctx, p, a)
		if unreachable(err) {
			return errAttachmentMissing
		}
		return err
	}
}

func missing(err error) bool { return err == errAttachmentMissing }

// streamArchive answers with an archive as it is written. A failure before
// its first bytes is answered as an error; one after cuts the response,
// so the client never keeps a partial archive.
func streamArchive(w http.ResponseWriter, name string, write func(io.Writer) error) {
	out := &archiveResponse{w: w, name: name}
	if err := write(out); err != nil {
		if !out.started {
			api.Respond(w, nil, err)
			return
		}
		panic(http.ErrAbortHandler)
	}
}

type archiveResponse struct {
	w       http.ResponseWriter
	name    string
	started bool
}

func (a *archiveResponse) Write(p []byte) (int, error) {
	if !a.started {
		a.started = true
		a.w.Header().Set("Content-Type", "application/zip")
		a.w.Header().Set("Cache-Control", "no-store")
		a.w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": a.name}))
		a.w.WriteHeader(http.StatusOK)
	}
	return a.w.Write(p)
}

// writeBackup writes a native backup: the cloud's projects, documents and
// chats when the key is given, then the device's chats.
func (h *harness) writeBackup(ctx context.Context, p *auth.Principal, key *auth.ContentKey, form *exportForm, out io.Writer) error {
	b := newBackupWriter(out)
	rel := backupRelationships{ProjectChats: []projectChatLink{}, ProjectDocuments: []projectDocumentLink{}, ChatImages: []chatImageLink{}}
	cloudChats := map[string]bool{}
	if key != nil {
		projects := map[string]bool{}
		err := h.eachRowOrUnreadable(ctx, p, *key, "project", "", func(row *domain.StoredRow) error {
			project, err := domain.ReadProject(row)
			if err != nil {
				b.omit(backupOmission{Kind: "project", SourceID: row.ID, Category: "invalid", Reason: "row_unreadable"})
				return nil
			}
			if err := b.addJSON("projects", backupEntityPath("projects", project.ID), project); err != nil {
				return err
			}
			b.manifest.Counts.Projects++
			projects[project.ID] = true
			docs, err := h.projectDocuments(ctx, p, *key, project.ID, func(row string, _ error) error {
				b.omit(backupOmission{Kind: "project_document", SourceID: strings.TrimPrefix(row, project.ID+"/"), ParentSourceID: project.ID, Category: "invalid", Reason: "row_unreadable"})
				return nil
			})
			if err != nil {
				return err
			}
			for _, doc := range docs {
				entry := backupDocument{ID: doc.ID, ProjectID: project.ID, Filename: doc.Filename, ContentType: doc.ContentType, SizeBytes: doc.SizeBytes, ExtractedText: doc.Content, CreatedAt: domain.LooseTime(doc.CreatedAt), UpdatedAt: domain.LooseTime(doc.UpdatedAt)}
				if err := b.addJSON("project_documents", backupDocumentPath(project.ID, doc.ID), entry); err != nil {
					return err
				}
				b.manifest.Counts.ProjectDocuments++
				rel.ProjectDocuments = append(rel.ProjectDocuments, projectDocumentLink{project.ID, doc.ID})
			}
			return nil
		}, func(id string) {
			b.omit(backupOmission{Kind: "project", SourceID: id, Category: "invalid", Reason: "row_unreadable"})
		})
		if err != nil {
			return err
		}
		err = h.eachRowOrUnreadable(ctx, p, *key, "chat", "", func(row *domain.StoredRow) error {
			read, err := domain.ReadThreadRow(row)
			if err != nil {
				b.omit(backupOmission{Kind: "cloud_chat", SourceID: row.ID, Category: "invalid", Reason: "row_unreadable"})
				return nil
			}
			t := read.Thread
			cloudChats[t.ID] = true
			if project := domain.ValueOr(t.ProjectID, ""); project != "" {
				if projects[project] {
					rel.ProjectChats = append(rel.ProjectChats, projectChatLink{project, t.ID})
				} else {
					t.ProjectID = nil
					b.omit(backupOmission{Kind: "relationship", SourceID: t.ID, ParentSourceID: project, Category: "unavailable", Reason: "project_reference_unavailable"})
				}
			}
			return backupChat(b, &rel, "cloud_chats", &t, h.cloudImage(ctx, p), h.cloudDocument(ctx, p))
		}, func(id string) {
			b.omit(backupOmission{Kind: "cloud_chat", SourceID: id, Category: "invalid", Reason: "row_unreadable"})
		})
		if err != nil {
			return err
		}
	}
	for i := range form.threads {
		t, err := form.thread(i)
		if err != nil {
			return err
		}
		// A chat moved to the cloud whose device copy remains is kept once.
		if cloudChats[t.ID] {
			b.omit(backupOmission{Kind: "local_chat", SourceID: t.ID, Category: "invalid", Reason: "duplicate_chat_id"})
			continue
		}
		if err := backupChat(b, &rel, "local_chats", &t, form.deviceImage, noDocument); err != nil {
			return err
		}
	}
	if err := b.addJSON("relationships", relationshipsPath, rel); err != nil {
		return err
	}
	b.manifest.Counts.Relationships = rel.count()
	return b.close(uuid.NewString(), api.Now())
}

// backupChat writes a chat with each of its images beside it. An image or
// document whose content cannot be read is left out and listed as omitted.
func backupChat(b *backupWriter, rel *backupRelationships, kind string, t *domain.Thread, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error) error {
	entry := backupThread{Thread: *t, Messages: make([]backupMessage, 0, len(t.Messages))}
	entry.ActiveRun, entry.LastRequestID, entry.LastRequestKind, entry.Flagged, entry.HiddenCount, entry.RecordVersion = nil, "", "", false, 0, 0
	omit := func(id string) {
		b.omit(backupOmission{Kind: "attachment", SourceID: id, ParentSourceID: t.ID, Category: "unavailable", Reason: "attachment_not_found"})
	}
	addImage := func(meta backupImage, data []byte) error {
		meta.MimeType, meta.SizeBytes = http.DetectContentType(data), new(int64(len(data)))
		if err := b.addJSON("images", backupImagePath(meta.ID, ".json"), meta); err != nil {
			return err
		}
		if err := b.add("images", backupImagePath(meta.ID, ".bin"), data); err != nil {
			return err
		}
		b.manifest.Counts.Images++
		rel.ChatImages = append(rel.ChatImages, chatImageLink{t.ID, meta.ID})
		return nil
	}
	for i, m := range t.Messages {
		message := backupMessage{Message: m}
		for j, a := range m.Attachments {
			id := fmt.Sprintf("%s/%d/%d", t.ID, i, j)
			if !a.IsImage() && !a.HasContent() && a.StorageKey() != "" {
				if err := document(&a); missing(err) {
					omit(id)
					continue
				} else if err != nil {
					return err
				}
			}
			attachment := backupAttachment{Attachment: a}
			attachment.Base64, attachment.EncryptionKey, attachment.Key, attachment.StorageThreadID, attachment.Attachment.Pages = "", "", "", "", nil
			if a.IsImage() {
				data, err := image(&a)
				if missing(err) {
					omit(id)
					continue
				}
				if err != nil {
					return err
				}
				if err := addImage(backupImage{ID: id, ChatID: t.ID, MessageIndex: i, AttachmentID: a.ID, FileName: cmp.Or(a.FileName, "image"), Description: a.Description}, data); err != nil {
					return err
				}
				attachment.ImageID = id
			}
			for k, page := range a.Pages {
				shown := backupPage{Page: page.Page, Text: page.Text, IsScanned: page.IsScanned}
				if page.Image != "" {
					pageID := fmt.Sprintf("%s/page/%d", id, k)
					_, encoded, found := strings.Cut(page.Image, ",")
					if !found || !strings.HasPrefix(page.Image, "data:") {
						encoded = page.Image
					}
					data, err := base64.StdEncoding.DecodeString(encoded)
					if err != nil {
						omit(pageID)
					} else if err := addImage(backupImage{ID: pageID, ChatID: t.ID, MessageIndex: i, AttachmentID: a.ID, Page: new(page.Page), FileName: fmt.Sprintf("%s-page-%d.jpg", a.ID, page.Page)}, data); err != nil {
						return err
					} else {
						shown.ImageID = pageID
					}
				}
				attachment.Pages = append(attachment.Pages, shown)
			}
			message.Attachments = append(message.Attachments, attachment)
		}
		entry.Messages = append(entry.Messages, message)
	}
	if err := b.addJSON(kind, backupEntityPath(kind, t.ID), entry); err != nil {
		return err
	}
	if kind == "cloud_chats" {
		b.manifest.Counts.CloudChats++
	} else {
		b.manifest.Counts.LocalChats++
	}
	return nil
}

// The conversations export is Claude's conversation format with Tinfoil's
// attachments, as the web app wrote it: conversations.json, each image's
// bytes under attachments/, and a manifest of them.

type exportConversation struct {
	UUID         string          `json:"uuid"`
	Name         string          `json:"name"`
	CreatedAt    string          `json:"created_at"`
	UpdatedAt    string          `json:"updated_at"`
	ProjectID    string          `json:"projectId,omitempty"`
	ChatMessages []exportMessage `json:"chat_messages"`
}

type exportMessage struct {
	UUID        string             `json:"uuid"`
	Text        string             `json:"text"`
	Sender      string             `json:"sender"`
	CreatedAt   string             `json:"created_at"`
	Content     []exportThinking   `json:"content,omitempty"`
	Attachments []exportAttachment `json:"attachments,omitempty"`
}

type exportThinking struct {
	Type     string `json:"type"`
	Thinking string `json:"thinking"`
}

type exportAttachment struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	FileName    string `json:"fileName"`
	MimeType    string `json:"mimeType,omitempty"`
	FileSize    *int64 `json:"fileSize,omitempty"`
	ExportPath  string `json:"exportPath,omitempty"`
	TextContent string `json:"textContent,omitempty"`
}

type conversationsManifest struct {
	Version         int            `json:"version"`
	CreatedAt       string         `json:"created_at"`
	ChatCount       int            `json:"chat_count"`
	AttachmentCount int            `json:"attachment_count"`
	Attachments     []exportedFile `json:"attachments"`
	Warnings        []string       `json:"warnings"`
}

type exportedFile struct {
	ExportPath string `json:"exportPath"`
	SHA256     string `json:"sha256"`
	FileSize   int    `json:"fileSize"`
}

// conversationsExport writes images as it meets them and collects the
// conversations in scratch space, to write them once all are read.
type conversationsExport struct {
	zip           *zip.Writer
	conversations *scratchFile
	used          map[string]bool
	manifest      conversationsManifest
}

func (h *harness) writeConversations(ctx context.Context, p *auth.Principal, key *auth.ContentKey, form *exportForm, lease *scratchLease, out io.Writer) error {
	text, err := lease.create()
	if err != nil {
		return err
	}
	e := &conversationsExport{zip: zip.NewWriter(out), conversations: text, used: map[string]bool{}, manifest: conversationsManifest{Version: 1, Attachments: []exportedFile{}, Warnings: []string{}}}
	device := map[string]bool{}
	for i := range form.threads {
		t, err := form.thread(i)
		if err != nil {
			return err
		}
		device[t.ID] = true
		if err := e.chat(&t, "", form.deviceImage, noDocument); err != nil {
			return err
		}
	}
	if key != nil {
		err := h.eachRow(ctx, p, *key, "chat", "", func(row *domain.StoredRow) error {
			// A chat this harness cannot read is left out, as one sync cannot
			// open is.
			read, err := domain.ReadThreadRow(row)
			if err != nil {
				return nil
			}
			t := read.Thread
			if device[t.ID] {
				return nil
			}
			return e.chat(&t, domain.ValueOr(t.ProjectID, ""), h.cloudImage(ctx, p), h.cloudDocument(ctx, p))
		})
		if err != nil {
			return err
		}
	}
	end := "\n]"
	if e.manifest.ChatCount == 0 {
		end = "[]"
	}
	if _, err := io.WriteString(text, end); err != nil {
		return err
	}
	w, err := e.zip.CreateHeader(&zip.FileHeader{Name: "conversations.json", Method: zip.Deflate, Modified: api.Now()})
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, text.section(0, text.Size())); err != nil {
		return err
	}
	e.manifest.CreatedAt = archiveTime(api.Now())
	e.manifest.AttachmentCount = len(e.manifest.Attachments)
	manifest, err := indentedJSON(e.manifest, "")
	if err != nil {
		return err
	}
	if err := writeEntry(e.zip, manifestPath, manifest, true); err != nil {
		return err
	}
	return e.zip.Close()
}

// indentedJSON is a value as JSON.stringify(value, null, 2) writes it,
// each line after the first prefixed.
func indentedJSON(v any, prefix string) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent(prefix, "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

func (e *conversationsExport) chat(t *domain.Thread, projectID string, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error) error {
	c := exportConversation{UUID: t.ID, Name: t.Title, CreatedAt: exportTime(t.CreatedAt, t.UpdatedAt), UpdatedAt: exportTime(t.UpdatedAt, t.CreatedAt), ProjectID: projectID, ChatMessages: []exportMessage{}}
	for index, m := range t.Messages {
		sender := "assistant"
		if m.Role == "user" {
			sender = "human"
		}
		message := exportMessage{UUID: fmt.Sprintf("%s_msg_%d", t.ID, index), Text: m.Content, Sender: sender, CreatedAt: exportTime(m.CreatedAt, t.CreatedAt)}
		if m.Role == "assistant" {
			message.Text = cmp.Or(m.Content, timelineText(m.Timeline, "content"))
			if thoughts := timelineText(m.Timeline, "thinking"); thoughts != "" {
				message.Content = []exportThinking{{Type: "thinking", Thinking: thoughts}}
			}
		}
		for _, a := range m.Attachments {
			exported, err := e.attachment(a, image, document)
			if err != nil {
				return err
			}
			message.Attachments = append(message.Attachments, exported)
		}
		c.ChatMessages = append(c.ChatMessages, message)
	}
	encoded, err := indentedJSON(c, "  ")
	if err != nil {
		return err
	}
	separator := ",\n  "
	if e.manifest.ChatCount == 0 {
		separator = "[\n  "
	}
	e.manifest.ChatCount++
	if _, err := io.WriteString(e.conversations, separator); err != nil {
		return err
	}
	_, err = e.conversations.Write(encoded)
	return err
}

func (e *conversationsExport) attachment(a domain.Attachment, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error) (exportAttachment, error) {
	if !a.IsImage() {
		if !a.HasContent() {
			if err := document(&a); missing(err) {
				e.manifest.Warnings = append(e.manifest.Warnings, "Could not fetch document "+a.ID)
			} else if err != nil {
				return exportAttachment{}, err
			}
		}
		return exportAttachment{ID: a.ID, Type: "document", FileName: a.FileName, MimeType: a.MimeType, FileSize: a.Size, TextContent: exportDocumentText(a.TextContent, a.Pages)}, nil
	}
	safeID := sanitizeFilename(a.ID, "attachment")
	safeName := sanitizeFilename(a.FileName, safeID+".bin")
	exportPath := e.uniquePath(safeID, safeName)
	exported := exportAttachment{ID: a.ID, Type: "image", FileName: safeName, MimeType: a.MimeType, FileSize: a.Size}
	data, err := image(&a)
	if missing(err) {
		e.manifest.Warnings = append(e.manifest.Warnings, "Could not fetch attachment "+a.ID)
		return exported, nil
	}
	if err != nil {
		return exportAttachment{}, err
	}
	if err := writeEntry(e.zip, exportPath, data, false); err != nil {
		return exportAttachment{}, err
	}
	exported.ExportPath, exported.FileSize = exportPath, new(int64(len(data)))
	e.manifest.Attachments = append(e.manifest.Attachments, exportedFile{ExportPath: exportPath, SHA256: api.HashHex(data), FileSize: len(data)})
	return exported, nil
}

// uniquePath is where an image is written, numbered past any image of the
// same id and name before it.
func (e *conversationsExport) uniquePath(safeID, safeName string) string {
	base := "attachments/" + safeID + "/" + safeName
	if !e.used[base] {
		e.used[base] = true
		return base
	}
	root, extension := safeName, ""
	if dot := strings.LastIndex(safeName, "."); dot > 0 {
		root, extension = safeName[:dot], safeName[dot:]
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("attachments/%s/%s-%d%s", safeID, root, suffix, extension)
		if !e.used[candidate] {
			e.used[candidate] = true
			return candidate
		}
	}
}

// sanitizeFilename is a name made safe as one archive path segment, as the
// web app made it: its last segment, with every character outside a small
// set, counted in UTF-16 units, replaced.
func sanitizeFilename(name, fallback string) string {
	return cmp.Or(cleanFilename(name), cleanFilename(fallback), "file")
}

func cleanFilename(name string) string {
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	var b strings.Builder
	for _, r := range name {
		switch {
		case r < 0x20 || r == 0x7f:
		case r < 0x80 && (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._ ()+-", r)):
			b.WriteRune(r)
		case r > 0xffff:
			b.WriteString("__")
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(strings.TrimLeft(b.String(), "."), " ")
}

// exportDocumentText is a document's text, or its pages' when it has none.
func exportDocumentText(text string, pages []domain.DocumentPage) string {
	if strings.TrimSpace(text) != "" {
		return text
	}
	var parts []string
	for _, page := range pages {
		if strings.TrimSpace(page.Text) != "" {
			parts = append(parts, page.Text)
		}
	}
	joined := strings.Join(parts, "\n\n---\n\n")
	if strings.TrimSpace(joined) == "" {
		return ""
	}
	return joined
}

// timelineText joins the text of a reply's blocks of one type.
func timelineText(timeline []domain.Block, kind string) string {
	var b strings.Builder
	for _, block := range timeline {
		if block.Type == kind {
			b.WriteString(block.Content)
		}
	}
	return b.String()
}

// exportTime is the first time given that parses, written as JavaScript
// writes a date, or the epoch when none does.
func exportTime(values ...string) string {
	for _, value := range values {
		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return archiveTime(t)
		}
	}
	return archiveTime(time.UnixMilli(0))
}

// claudeProject is a project in Claude's export format, with its
// documents' text.
type claudeProject struct {
	UUID           string           `json:"uuid"`
	Name           string           `json:"name"`
	Description    string           `json:"description"`
	PromptTemplate string           `json:"prompt_template"`
	CreatedAt      string           `json:"created_at"`
	UpdatedAt      string           `json:"updated_at"`
	Docs           []claudeDocument `json:"docs"`
}

type claudeDocument struct {
	UUID      string `json:"uuid"`
	Filename  string `json:"filename"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

func (h *harness) claudeProjects(ctx context.Context, p *auth.Principal, key auth.ContentKey) ([]claudeProject, error) {
	projects := []claudeProject{}
	total := 0
	err := h.eachRow(ctx, p, key, "project", "", func(row *domain.StoredRow) error {
		// A document this harness cannot read is left out, as a chat is.
		docs, err := h.projectDocuments(ctx, p, key, row.ID, func(string, error) error { return nil })
		if err != nil {
			return err
		}
		project, err := domain.ReadProject(row)
		if err != nil {
			return nil
		}
		exported := claudeProject{UUID: project.ID, Name: project.Name, Description: project.Description, PromptTemplate: project.SystemInstructions, CreatedAt: project.CreatedAt, UpdatedAt: project.UpdatedAt, Docs: []claudeDocument{}}
		for _, doc := range docs {
			exported.Docs = append(exported.Docs, claudeDocument{UUID: doc.ID, Filename: doc.Filename, Content: doc.Content, CreatedAt: doc.CreatedAt})
		}
		if total += len(api.Raw(exported)); total > maxProjectsExport {
			return api.Err(413, "ATTACHMENT_TOO_LARGE", "The project export exceeds 64 MiB")
		}
		projects = append(projects, exported)
		return nil
	})
	return projects, err
}
