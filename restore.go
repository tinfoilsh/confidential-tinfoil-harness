package main

import (
	"archive/zip"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"
)

// nativeBackup is a native backup whose every entry was found listed in
// its manifest and matching its hash.
type nativeBackup struct {
	manifest backupManifest
	entries  map[string]*zip.File
	// images are the backup's images by id, and their ids by chat.
	images     map[string]backupImage
	chatImages map[string][]string
}

var errInvalidBackup = func(why string) error { return apiErr(400, "IMPORT_INVALID", "The backup is invalid: "+why) }

// openNativeBackup checks an archive that names itself a native backup,
// or returns nil for any other archive.
func openNativeBackup(zr *zip.Reader) (*nativeBackup, error) {
	var manifestFile *zip.File
	for _, f := range zr.File {
		if f.Name == manifestPath {
			manifestFile = f
		}
	}
	if manifestFile == nil {
		return nil, nil
	}
	var head struct {
		Format string `json:"format"`
	}
	if b, err := readEntry(manifestFile, maxBackupManifestBytes); err != nil || json.Unmarshal(b, &head) != nil || head.Format != nativeBackupFormat {
		return nil, nil
	}
	b := &nativeBackup{entries: map[string]*zip.File{}, images: map[string]backupImage{}, chatImages: map[string][]string{}}
	expanded := uint64(0)
	for _, f := range zr.File {
		if !safeEntryName(f) || f.Flags&0x1 != 0 || !backupPaths.MatchString(f.Name) || b.entries[f.Name] != nil {
			return nil, errInvalidBackup("it holds an unknown, unsafe or repeated entry")
		}
		limit := uint64(maxArchiveJSONEntry)
		if path.Ext(f.Name) == ".bin" {
			limit = maxArchiveImage
		}
		if expanded += f.UncompressedSize64; f.UncompressedSize64 > limit || expanded > maxArchiveExpansion {
			return nil, apiErr(413, "IMPORT_TOO_LARGE", "The backup exceeds the size limits")
		}
		b.entries[f.Name] = f
	}
	if err := decodeEntry(manifestFile, &b.manifest); err != nil {
		return nil, err
	}
	m := &b.manifest
	if m.Version < 1 || m.Version > nativeBackupVersion {
		return nil, errInvalidBackup("its version is not supported")
	}
	counts := backupCounts{Files: len(m.Files)}
	listed := map[string]bool{}
	for _, file := range m.Files {
		f := b.entries[file.Path]
		if f == nil || listed[file.Path] || file.Path == manifestPath || backupKind(file.Path) != file.Kind || int64(f.UncompressedSize64) != file.SizeBytes {
			return nil, errInvalidBackup("its manifest does not match its entries")
		}
		listed[file.Path] = true
		if err := checkEntryHash(f, file.SHA256); err != nil {
			return nil, err
		}
		switch file.Kind {
		case "projects":
			counts.Projects++
		case "project_documents":
			counts.ProjectDocuments++
		case "cloud_chats":
			counts.CloudChats++
		case "local_chats":
			counts.LocalChats++
		case "images":
			if path.Ext(file.Path) == ".json" {
				counts.Images++
				if err := b.readImage(f); err != nil {
					return nil, err
				}
			}
		case "relationships":
			var relationships backupRelationships
			if err := decodeEntry(f, &relationships); err != nil {
				return nil, err
			}
			counts.Relationships = relationships.count()
		}
	}
	if len(listed)+1 != len(b.entries) {
		return nil, errInvalidBackup("it holds entries its manifest does not list")
	}
	if counts != m.Counts {
		return nil, errInvalidBackup("its counts do not match its entries")
	}
	return b, nil
}

func checkEntryHash(f *zip.File, want string) error {
	r, err := f.Open()
	if err != nil {
		return errInvalidBackup("an entry cannot be read")
	}
	defer r.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, r); err != nil || hex.EncodeToString(hash.Sum(nil)) != want {
		return errInvalidBackup("an entry does not match its hash")
	}
	return nil
}

// readImage reads an image's description, which must sit beside its bytes.
func (b *nativeBackup) readImage(f *zip.File) error {
	var image backupImage
	if err := decodeEntry(f, &image); err != nil {
		return err
	}
	if image.ID == "" || backupImagePath(image.ID, ".json") != f.Name || b.entries[backupImagePath(image.ID, ".bin")] == nil {
		return errInvalidBackup("an image is described apart from its bytes")
	}
	b.images[image.ID] = image
	b.chatImages[image.ChatID] = append(b.chatImages[image.ChatID], image.ID)
	return nil
}

// listed are the backup's entries of one kind, in manifest order.
func (b *nativeBackup) listed(kind string) []*zip.File {
	var files []*zip.File
	for _, file := range b.manifest.Files {
		if file.Kind == kind {
			files = append(files, b.entries[file.Path])
		}
	}
	return files
}

// chat reads one of the backup's chats with its images.
func (b *nativeBackup) chat(f *zip.File) (string, Thread, error) {
	data, err := readEntry(f, maxArchiveJSONEntry)
	if err != nil {
		return "", Thread{}, err
	}
	var head struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(data, &head) != nil || head.ID == "" || backupEntityPath(backupKind(f.Name), head.ID) != f.Name {
		return "", Thread{}, errInvalidBackup("a chat is filed under another id")
	}
	images := backupImages{}
	for _, id := range b.chatImages[head.ID] {
		bytes, err := readEntry(b.entries[backupImagePath(id, ".bin")], maxArchiveImage)
		if err != nil {
			return "", Thread{}, err
		}
		meta := b.images[id]
		if kind := http.DetectContentType(bytes); slices.Contains(sniffedImages, kind) {
			images[id] = heldImage{FileName: meta.FileName, MimeType: kind, Description: meta.Description, Size: int64(len(bytes)), Base64: base64.StdEncoding.EncodeToString(bytes)}
		}
	}
	return head.ID, readBackupThread(head.ID, data, images), nil
}

// native restores a backup. With cloud storage its projects, documents and
// cloud chats go to the cloud, and its local chats to the device; on the
// device alone every chat goes to the device and projects are left out.
func (j *importRun) native(ctx context.Context, b *nativeBackup) error {
	j.recognized, j.source = true, b.manifest.BackupID
	for _, o := range b.manifest.Omissions {
		if o.Kind != "relationship" && o.Kind != "local_inventory" {
			j.warnings.backupOmissions++
		}
	}
	device := append(b.listed("cloud_chats"), b.listed("local_chats")...)
	if j.key != nil {
		if err := j.nativeCloud(b); err != nil {
			return err
		}
		if err := j.complete(ctx); err != nil {
			return err
		}
		device = b.listed("local_chats")
	} else {
		j.warnings.projectsSkipped += b.manifest.Counts.Projects
	}
	for _, f := range device {
		id, t, err := b.chat(f)
		if err != nil {
			return err
		}
		if err := j.toDevice(id, &t); err != nil {
			return err
		}
	}
	if j.key == nil {
		// A backup holding only projects has nothing for the device.
		return j.complete(ctx)
	}
	return nil
}

func (j *importRun) nativeCloud(b *nativeBackup) error {
	projects := map[string]bool{}
	for _, f := range b.listed("projects") {
		var project backupProject
		if err := decodeEntry(f, &project); err != nil {
			return err
		}
		if project.ID == "" || backupEntityPath("projects", project.ID) != f.Name {
			return errInvalidBackup("a project is filed under another id")
		}
		cloud, err := j.cloudArchive()
		if err != nil {
			return err
		}
		projects[project.ID] = true
		if err := cloud.entity("project", project.ID, "", cloudProject{Name: cmp.Or(strings.TrimSpace(project.Name), "Imported Project"), Description: project.Description, SystemInstructions: project.SystemInstructions, Color: valueOr(project.Color, ""), Memory: []json.RawMessage{}}); err != nil {
			return err
		}
	}
	for _, f := range b.listed("project_documents") {
		var doc backupDocument
		if err := decodeEntry(f, &doc); err != nil {
			return err
		}
		if !projects[doc.ProjectID] || backupDocumentPath(doc.ProjectID, doc.ID) != f.Name {
			return errInvalidBackup("a document is filed apart from its project")
		}
		size := int64(len(doc.ExtractedText))
		if err := j.cloud.entity("document", doc.ProjectID+"/"+doc.ID, doc.ProjectID, cloudDocument{Filename: cmp.Or(doc.Filename, "document"), ContentType: cmp.Or(doc.ContentType, "text/plain"), SourceSizeBytes: int64(max(doc.SizeBytes, 0)), SizeBytes: size, Content: doc.ExtractedText}); err != nil {
			return err
		}
	}
	for _, f := range b.listed("cloud_chats") {
		id, t, err := b.chat(f)
		if err != nil {
			return err
		}
		project := valueOr(t.ProjectID, "")
		if project != "" && !projects[project] {
			j.warnings.projectLinksDropped++
			project = ""
		}
		if err := j.toCloud(id, project, &t); err != nil {
			return err
		}
	}
	return nil
}
