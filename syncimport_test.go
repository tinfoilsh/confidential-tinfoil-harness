package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// fakeImport is an import job of the fake sync enclave. Started, it stores
// a native cloud import the way sync's job does, after checking it the way
// sync's job checks it, so an archive sync would refuse fails here too.
type fakeImport struct {
	total  int64
	chunks map[int][]byte
	sha    string
	status api.Object
}

func (f *fakeSync) importStep(t *testing.T, h *harness, r *http.Request, in api.Object) (int, api.Object) {
	t.Helper()
	switch r.URL.Path {
	case "/v1/import/create":
		if in["source"] != "tinfoil_backup" || api.Int(in["total_bytes"]) <= 0 || api.Int(in["total_bytes"]) > 512<<20 {
			return 400, api.Object{"code": "BAD_REQUEST"}
		}
		id := strconv.Itoa(len(f.imports) + 1)
		f.imports[id] = &fakeImport{total: int64(api.Int(in["total_bytes"])), chunks: map[int][]byte{}, sha: api.Str(in["archive_sha256"])}
		return 200, api.Object{"job_id": id, "upload_id": id}
	case "/v1/import/upload":
		job := f.imports[api.Str(in["upload_id"])]
		data, err := base64.StdEncoding.DecodeString(api.Str(in["data"]))
		if job == nil || err != nil || api.HashHex(data) != api.Str(in["chunk_sha256"]) || len(data) > importChunk {
			return 400, api.Object{"code": "BAD_REQUEST"}
		}
		job.chunks[api.Int(in["chunk_index"])] = data
		return 200, api.Object{"ok": true}
	case "/v1/import/start":
		job := f.imports[api.Str(in["job_id"])]
		key, err := auth.DecodeBase64Key([]byte(api.Str(in["key"])))
		if job == nil || err != nil {
			return 400, api.Object{"code": "BAD_REQUEST"}
		}
		var archive []byte
		for i := 0; i < len(job.chunks); i++ {
			archive = append(archive, job.chunks[i]...)
		}
		if int64(len(archive)) != job.total || api.HashHex(archive) != job.sha {
			return 400, api.Object{"code": "BAD_REQUEST"}
		}
		user := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		user = strings.TrimPrefix(strings.TrimPrefix(user, "clerk-"), "sync-archive:")
		p := &auth.Principal{ID: user}
		counts, err := f.storeImport(h.storeRows.(*memoryRows), p, key, archive)
		if err != nil {
			t.Errorf("sync would refuse the import: %v", err)
			job.status = api.Object{"status": "failed", "failure_reason": "invalid_archive"}
		} else {
			job.status = api.Object{"status": "completed", "imported": counts, "failed": 0, "total": counts}
		}
		return 200, api.Object{"status": "running"}
	default:
		job := f.imports[api.Str(in["job_id"])]
		if job == nil || job.status == nil {
			return 404, api.Object{"code": "NOT_FOUND"}
		}
		return 200, job.status
	}
}

// strictJSON decodes data into v, refusing fields v does not name.
func strictJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func hasKeys(data []byte, keys ...string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return false
	}
	for _, key := range keys {
		if _, ok := fields[key]; !ok {
			return false
		}
	}
	return true
}

func validTime(raw json.RawMessage) bool {
	var value string
	if string(raw) == "null" {
		return true
	}
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
}

// mappedID is the id sync stores an imported entity under.
func mappedID(p *auth.Principal, backup, kind, source string) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("tinfoil-restore-id-v1\x00%s\x00%s\x00%s\x00%s\x000", p.ID, backup, kind, source)))
	s := hex.EncodeToString(h[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}

// storeImport checks and stores a native cloud import as sync's job does.
func (f *fakeSync) storeImport(rows *memoryRows, p *auth.Principal, key auth.ContentKey, archive []byte) (int, error) {
	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return 0, err
	}
	files := map[string]*zip.File{}
	for _, file := range zr.File {
		files[file.Name] = file
	}
	read := func(name string) ([]byte, error) {
		file := files[name]
		if file == nil {
			return nil, errors.New("missing entry " + name)
		}
		r, err := file.Open()
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	}
	manifestBytes, err := read("manifest.json")
	if err != nil {
		return 0, err
	}
	var manifest struct {
		Format         string `json:"format"`
		Version        int    `json:"version"`
		SourceBackupID string `json:"source_backup_id"`
		Counts         *struct {
			Projects  int `json:"projects"`
			Documents int `json:"documents"`
			Chats     int `json:"chats"`
			Blobs     int `json:"blobs"`
		} `json:"counts"`
		Entities []struct {
			Kind            string `json:"kind"`
			SourceID        string `json:"source_id"`
			ProjectSourceID string `json:"project_source_id"`
			Path            string `json:"path"`
			SHA256          string `json:"sha256"`
			SizeBytes       int    `json:"size_bytes"`
		} `json:"entities"`
		Blobs []struct {
			Path      string `json:"path"`
			SHA256    string `json:"sha256"`
			SizeBytes int    `json:"size_bytes"`
		} `json:"blobs"`
	}
	if err := strictJSON(manifestBytes, &manifest); err != nil || !hasKeys(manifestBytes, "format", "version", "source_backup_id", "counts", "entities", "blobs") || manifest.Counts == nil || manifest.Entities == nil || manifest.Blobs == nil {
		return 0, fmt.Errorf("manifest: %v", err)
	}
	if manifest.Format != "tinfoil-native-cloud-import" || manifest.Version != 1 || manifest.SourceBackupID == "" {
		return 0, errors.New("manifest format")
	}
	listed := map[string]bool{"manifest.json": true}
	blobs := map[string]bool{}
	for _, blob := range manifest.Blobs {
		data, err := read(blob.Path)
		if err != nil || listed[blob.Path] || len(data) != blob.SizeBytes || api.HashHex(data) != blob.SHA256 {
			return 0, errors.New("blob " + blob.Path)
		}
		listed[blob.Path], blobs[blob.Path] = true, true
	}
	projects, sources := map[string]string{}, map[string]bool{}
	counted := map[string]int{}
	stored := 0
	for _, entity := range manifest.Entities {
		data, err := read(entity.Path)
		if err != nil || listed[entity.Path] || len(data) != entity.SizeBytes || api.HashHex(data) != entity.SHA256 || entity.SourceID == "" || sources[entity.Kind+"\x00"+entity.SourceID] {
			return 0, errors.New("entity " + entity.Path)
		}
		listed[entity.Path], sources[entity.Kind+"\x00"+entity.SourceID] = true, true
		counted[entity.Kind]++
		id := mappedID(p, manifest.SourceBackupID, entity.Kind, entity.SourceID)
		marker := api.Object{"format": "tinfoil-native-cloud-import", "sourceBackupId": manifest.SourceBackupID, "kind": entity.Kind, "sourceId": entity.SourceID, "generation": 0}
		switch entity.Kind {
		case "project":
			var project struct {
				Name               string            `json:"name"`
				Description        string            `json:"description"`
				SystemInstructions string            `json:"systemInstructions"`
				Color              string            `json:"color"`
				Memory             []json.RawMessage `json:"memory"`
			}
			if err := strictJSON(data, &project); err != nil || project.Name == "" || project.Memory == nil {
				return 0, fmt.Errorf("project %s: %v", entity.SourceID, err)
			}
			projects[entity.SourceID] = id
			row := &storedRow{ID: id, Data: api.Raw(api.Object{"id": id, "name": project.Name, "description": project.Description, "systemInstructions": project.SystemInstructions, "color": project.Color, "memory": project.Memory, "_restore": marker})}
			if _, err := rows.push(context.Background(), p, key, "project", row, ""); err != nil {
				return 0, err
			}
		case "document":
			var doc struct {
				Filename        string `json:"filename"`
				ContentType     string `json:"contentType"`
				SourceSizeBytes int64  `json:"sourceSizeBytes"`
				SizeBytes       int64  `json:"sizeBytes"`
				Content         string `json:"content"`
			}
			project, ok := projects[entity.ProjectSourceID]
			if err := strictJSON(data, &doc); err != nil || !ok || !hasKeys(data, "filename", "contentType", "sourceSizeBytes", "sizeBytes", "content") || doc.Filename == "" || doc.ContentType == "" {
				return 0, fmt.Errorf("document %s: %v", entity.SourceID, err)
			}
			row := &storedRow{ID: project + "/" + id, Data: api.Raw(api.Object{"id": id, "projectId": project, "filename": doc.Filename, "contentType": doc.ContentType, "sourceSizeBytes": doc.SourceSizeBytes, "sizeBytes": doc.SizeBytes, "content": doc.Content, "_restore": marker}), project: &project, projectSet: true}
			if _, err := rows.push(context.Background(), p, key, "project_document", row, ""); err != nil {
				return 0, err
			}
		case "chat":
			chat, err := f.importedChat(data, entity.ProjectSourceID, id, blobs, read)
			if err != nil {
				return 0, fmt.Errorf("chat %s: %v", entity.SourceID, err)
			}
			chat["_restore"] = api.Raw(marker)
			row := &storedRow{ID: id, projectSet: true}
			if project := entity.ProjectSourceID; project != "" {
				mapped, ok := projects[project]
				if !ok {
					return 0, errors.New("chat references an unknown project")
				}
				row.project, chat["projectId"] = &mapped, api.Raw(mapped)
			} else {
				delete(chat, "projectId")
			}
			row.Data = api.Raw(chat)
			if _, err := rows.push(context.Background(), p, key, "chat", row, ""); err != nil {
				return 0, err
			}
		default:
			return 0, errors.New("entity kind " + entity.Kind)
		}
		stored++
	}
	if counted["project"] != manifest.Counts.Projects || counted["document"] != manifest.Counts.Documents || counted["chat"] != manifest.Counts.Chats || len(blobs) != manifest.Counts.Blobs {
		return 0, errors.New("manifest counts")
	}
	for name := range files {
		if !listed[name] {
			return 0, errors.New("unlisted entry " + name)
		}
	}
	return stored, nil
}

// importedChat checks a chat payload and stores its images, as sync does.
func (f *fakeSync) importedChat(data []byte, projectSource, id string, blobs map[string]bool, read func(string) ([]byte, error)) (map[string]json.RawMessage, error) {
	var chat map[string]json.RawMessage
	var title string
	var isLocalOnly *bool
	var projectID string
	if json.Unmarshal(data, &chat) != nil || !hasKeys(data, "title", "messages", "createdAt", "isLocalOnly") || json.Unmarshal(chat["title"], &title) != nil ||
		json.Unmarshal(chat["isLocalOnly"], &isLocalOnly) != nil || isLocalOnly == nil || *isLocalOnly || !validTime(chat["createdAt"]) {
		return nil, errors.New("invalid chat payload")
	}
	json.Unmarshal(chat["projectId"], &projectID)
	if projectID != "" && projectID != projectSource {
		return nil, errors.New("conflicting project reference")
	}
	var messages []map[string]json.RawMessage
	if json.Unmarshal(chat["messages"], &messages) != nil || messages == nil {
		return nil, errors.New("invalid messages")
	}
	for i, message := range messages {
		var role string
		var attachments []map[string]json.RawMessage
		if json.Unmarshal(message["role"], &role) != nil || role == "" || !validTime(message["timestamp"]) {
			return nil, errors.New("invalid message")
		}
		if listed, ok := message["attachments"]; ok && json.Unmarshal(listed, &attachments) != nil {
			return nil, errors.New("invalid attachments")
		}
		kept := []map[string]json.RawMessage{}
		for j, attachment := range attachments {
			var described struct {
				Type        string `json:"type"`
				FileName    string `json:"fileName"`
				FileSize    int64  `json:"fileSize"`
				ArchivePath string `json:"archivePath"`
				TextContent string `json:"textContent"`
				Pages       []struct {
					Text  *string `json:"text"`
					Image *string `json:"image"`
				} `json:"pages"`
			}
			if err := json.Unmarshal(api.Raw(attachment), &described); err != nil || described.Type != "image" && described.Type != "document" || described.FileName == "" || described.FileSize < 0 {
				return nil, errors.New("invalid attachment")
			}
			for _, field := range []string{"archivePath", "backup_path", "base64", "thumbnailBase64", "encryptionKey", "key"} {
				delete(attachment, field)
			}
			if described.Type == "document" {
				if described.ArchivePath != "" {
					return nil, errors.New("binary document")
				}
				content := strings.TrimSpace(described.TextContent) != ""
				for _, page := range described.Pages {
					content = content || page.Text != nil && strings.TrimSpace(*page.Text) != "" || page.Image != nil && *page.Image != ""
				}
				if content {
					kept = append(kept, attachment)
				}
				continue
			}
			if !blobs[described.ArchivePath] {
				return nil, errors.New("image without its blob")
			}
			bytes, _ := read(described.ArchivePath)
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%d", id, described.ArchivePath, i, j)))
			blobID, blobKey := hex.EncodeToString(sum[:18]), base64.StdEncoding.EncodeToString(sum[:])
			f.blobs[blobID] = fakeBlob{chat: id, key: blobKey, data: bytes}
			attachment["id"], attachment["encryptionKey"], attachment["mimeType"] = api.Raw(blobID), api.Raw(blobKey), api.Raw(http.DetectContentType(bytes))
			kept = append(kept, attachment)
		}
		if len(kept) > 0 {
			message["attachments"] = api.Raw(kept)
		} else {
			delete(message, "attachments")
		}
	}
	chat["messages"], chat["id"], chat["isLocalOnly"] = api.Raw(messages), api.Raw(id), api.Raw(false)
	return chat, nil
}
