package main

import (
	"archive/zip"
	"encoding/hex"
	"encoding/json"
	"io"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A native backup is a ZIP archive: one JSON entry per project, project
// document and chat, each image's bytes beside its description, the
// relationships between them, and a manifest listing every entry with its
// hash. Versions 1 and 2 hold chats in the format the web app kept them in;
// version 3 holds them in the thread format. The legacy reader reads both.

const (
	nativeBackupFormat  = "tinfoil-native-backup"
	nativeBackupVersion = 3
	manifestPath        = "manifest.json"
	relationshipsPath   = "relationships.json"
)

// Limits a backup is read and written within. A backup holds any image
// uploads take.
const (
	maxArchiveEntries      = 50_000
	maxArchiveExpansion    = 2 << 30
	maxArchiveJSONEntry    = 256 << 20
	maxArchiveImage        = maxUploadBytes
	maxBackupManifestBytes = 32 << 20
)

// backupEntryLimit is the most an entry of a backup may hold.
func backupEntryLimit(name string) uint64 {
	if path.Ext(name) == ".bin" {
		return maxArchiveImage
	}
	return maxArchiveJSONEntry
}

type backupManifest struct {
	Format    string           `json:"format"`
	Version   int              `json:"version"`
	BackupID  string           `json:"backup_id"`
	CreatedAt string           `json:"created_at"`
	Complete  bool             `json:"complete"`
	Counts    backupCounts     `json:"counts"`
	Notices   backupNotices    `json:"notices"`
	Files     []backupFile     `json:"files"`
	Omissions []backupOmission `json:"omissions"`
	Warnings  []backupWarning  `json:"warnings"`
}

type backupCounts struct {
	Projects         int `json:"projects"`
	ProjectDocuments int `json:"project_documents"`
	CloudChats       int `json:"cloud_chats"`
	LocalChats       int `json:"local_chats"`
	Relationships    int `json:"relationships"`
	Images           int `json:"images"`
	Files            int `json:"files"`
}

type backupNotices struct {
	ContainsPlaintext             bool `json:"contains_plaintext"`
	DocumentsAreExtractedTextOnly bool `json:"documents_are_extracted_text_only"`
}

type backupFile struct {
	Path      string `json:"path"`
	Kind      string `json:"kind"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

// backupOmission names an item a backup could not include.
type backupOmission struct {
	Kind           string `json:"kind"`
	SourceID       string `json:"source_id"`
	ParentSourceID string `json:"parent_source_id,omitempty"`
	Category       string `json:"category"`
	Reason         string `json:"reason"`
}

type backupWarning struct {
	Code     string `json:"code"`
	Category string `json:"category"`
	Count    int    `json:"count"`
}

type backupRelationships struct {
	ProjectChats     []projectChatLink     `json:"projectChats"`
	ProjectDocuments []projectDocumentLink `json:"projectDocuments"`
	ChatImages       []chatImageLink       `json:"chatImages"`
}

type projectChatLink struct {
	ProjectID string `json:"projectId"`
	ChatID    string `json:"chatId"`
}

type projectDocumentLink struct {
	ProjectID  string `json:"projectId"`
	DocumentID string `json:"documentId"`
}

type chatImageLink struct {
	ChatID  string `json:"chatId"`
	ImageID string `json:"imageId"`
}

func (r *backupRelationships) count() int {
	return len(r.ProjectChats) + len(r.ProjectDocuments) + len(r.ChatImages)
}

// backupImage describes an image a backup holds and where its chat names it.
type backupImage struct {
	ID           string `json:"id"`
	ChatID       string `json:"chatId"`
	MessageIndex int    `json:"messageIndex"`
	AttachmentID string `json:"attachmentId,omitempty"`
	Page         *int   `json:"page,omitempty"`
	LegacyIndex  *int   `json:"legacyIndex,omitempty"`
	FileName     string `json:"fileName"`
	MimeType     string `json:"mimeType"`
	SizeBytes    *int64 `json:"sizeBytes,omitempty"`
	Description  string `json:"description,omitempty"`
}

// backupProject is a project as every version holds it.
type backupProject struct {
	domain.Project
	CreatedAt domain.LooseTime `json:"createdAt"`
	UpdatedAt domain.LooseTime `json:"updatedAt"`
}

// backupDocument is a project document; its text is extractedText.
type backupDocument struct {
	ID            string           `json:"id"`
	ProjectID     string           `json:"projectId"`
	Filename      string           `json:"filename"`
	ContentType   string           `json:"contentType"`
	SizeBytes     int              `json:"sizeBytes"`
	ExtractedText string           `json:"extractedText"`
	CreatedAt     domain.LooseTime `json:"createdAt"`
	UpdatedAt     domain.LooseTime `json:"updatedAt"`
}

// backupThread is a chat as version 3 holds it: the thread, with each image
// named by the image the backup holds instead of its bytes.
type backupThread struct {
	domain.Thread
	Messages []backupMessage `json:"messages"`
}

type backupMessage struct {
	domain.Message
	Attachments []backupAttachment `json:"attachments,omitzero"`
}

type backupAttachment struct {
	domain.Attachment
	ImageID string       `json:"imageId,omitempty"`
	Pages   []backupPage `json:"pages,omitempty"`
}

type backupPage struct {
	Page      int    `json:"page"`
	Text      string `json:"text"`
	IsScanned bool   `json:"is_scanned"`
	ImageID   string `json:"imageId,omitempty"`
}

func idComponent(id string) string { return "id-" + hex.EncodeToString([]byte(id)) }

func backupEntityPath(kind, id string) string { return kind + "/" + idComponent(id) + ".json" }

func backupDocumentPath(projectID, id string) string {
	return "project_documents/" + idComponent(projectID) + "/" + idComponent(id) + ".json"
}

func backupImagePath(id, extension string) string { return "images/" + idComponent(id) + extension }

// backupPaths are the entries a native backup may hold.
var backupPaths = regexp.MustCompile(`^(manifest\.json|relationships\.json|(projects|cloud_chats|local_chats)/id-[0-9a-f]+\.json|project_documents/id-[0-9a-f]+/id-[0-9a-f]+\.json|images/id-[0-9a-f]+\.(json|bin))$`)

// backupKind is the kind the manifest must list an entry under.
func backupKind(name string) string {
	if name == relationshipsPath {
		return "relationships"
	}
	kind, _, _ := strings.Cut(name, "/")
	return kind
}

// safeEntryName reports whether a ZIP entry names a file inside the archive.
func safeEntryName(f *zip.File) bool {
	name := f.Name
	return name != "" && name == path.Clean(name) && !strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "../") && name != ".." &&
		!strings.ContainsAny(name, "\\\x00") && f.Mode().IsRegular()
}

// readEntry reads a whole entry, refusing one larger than limit or than its
// header says.
func readEntry(f *zip.File, limit int64) ([]byte, error) {
	if f.UncompressedSize64 > uint64(limit) {
		return nil, api.Err(413, "IMPORT_TOO_LARGE", "An archive entry exceeds the size limit")
	}
	r, err := f.Open()
	if err != nil {
		return nil, api.Err(400, "IMPORT_INVALID", "An archive entry cannot be read")
	}
	defer r.Close()
	b, err := io.ReadAll(io.LimitReader(r, int64(f.UncompressedSize64)+1))
	if err != nil || uint64(len(b)) != f.UncompressedSize64 {
		return nil, api.Err(400, "IMPORT_INVALID", "An archive entry does not match its size")
	}
	return b, nil
}

// archiveTime is a time as the web app wrote it in archives.
func archiveTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// backupWriter writes a backup's entries and lists them in its manifest.
type backupWriter struct {
	zip      *zip.Writer
	manifest backupManifest
	expanded uint64
}

func newBackupWriter(w io.Writer) *backupWriter {
	return &backupWriter{zip: zip.NewWriter(w), manifest: backupManifest{Format: nativeBackupFormat, Version: nativeBackupVersion, Notices: backupNotices{true, true}, Files: []backupFile{}, Omissions: []backupOmission{}}}
}

func (b *backupWriter) add(kind, name string, data []byte) error {
	if len(b.manifest.Files)+1 >= maxArchiveEntries {
		return api.Err(413, "ARCHIVE_TOO_LARGE", "The backup holds too many items")
	}
	if err := b.write(name, data, backupEntryLimit(name), kind != "images" || path.Ext(name) == ".json"); err != nil {
		return err
	}
	b.manifest.Files = append(b.manifest.Files, backupFile{Path: name, Kind: kind, SHA256: api.HashHex(data), SizeBytes: int64(len(data))})
	return nil
}

// write adds an entry, refusing one a restore would refuse: one past its
// limit, or one that expands the backup past the limit.
func (b *backupWriter) write(name string, data []byte, limit uint64, compress bool) error {
	b.expanded += uint64(len(data))
	if uint64(len(data)) > limit || b.expanded > maxArchiveExpansion {
		return api.Err(413, "ARCHIVE_TOO_LARGE", "The backup exceeds the size a restore takes")
	}
	return writeEntry(b.zip, name, data, compress)
}

func (b *backupWriter) addJSON(kind, name string, value any) error {
	return b.add(kind, name, api.Raw(value))
}

func (b *backupWriter) omit(omission backupOmission) {
	b.manifest.Omissions = append(b.manifest.Omissions, omission)
}

// close writes the manifest last, once every entry is listed.
func (b *backupWriter) close(id string, created time.Time) error {
	m := &b.manifest
	m.BackupID, m.CreatedAt, m.Complete, m.Counts.Files = id, archiveTime(created), len(m.Omissions) == 0, len(m.Files)
	m.Warnings = []backupWarning{}
	omitted, detached := 0, 0
	for _, o := range m.Omissions {
		if o.Kind == "relationship" {
			detached++
		} else {
			omitted++
		}
	}
	if omitted > 0 {
		m.Warnings = append(m.Warnings, backupWarning{Code: "source_items_omitted", Category: "source_coverage", Count: omitted})
	}
	if detached > 0 {
		m.Warnings = append(m.Warnings, backupWarning{Code: "chats_detached_from_omitted_projects", Category: "relationship_adjustment", Count: detached})
	}
	if err := b.write(manifestPath, api.Raw(m), maxBackupManifestBytes, true); err != nil {
		return err
	}
	return b.zip.Close()
}

// writeEntry adds one file to an archive, compressed unless it already is.
func writeEntry(zw *zip.Writer, name string, data []byte, compress bool) error {
	header := &zip.FileHeader{Name: name, Method: zip.Store}
	if compress {
		header.Method = zip.Deflate
	}
	header.Modified = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	w, err := zw.CreateHeader(header)
	if err == nil {
		_, err = w.Write(data)
	}
	return err
}

// decodeEntry reads a JSON entry of an archive into v.
func decodeEntry(f *zip.File, v any) error {
	b, err := readEntry(f, maxArchiveJSONEntry)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return api.Err(400, "IMPORT_INVALID", "An archive entry is not valid JSON: "+f.Name)
	}
	return nil
}
