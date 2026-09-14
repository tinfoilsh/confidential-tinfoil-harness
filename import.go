package main

import (
	"archive/zip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
)

const spoolChunk = 1 << 20
const importLimit = 512 << 20
const importChunk = 8 << 20

// Temporary archives are encrypted before every filesystem write. The key is
// random, exists only in this process, and is discarded when the request ends.
type sealedSpool struct {
	file     *os.File
	aead     cipher.AEAD
	size     int64
	pending  []byte
	finished bool
	chunks   uint64
}

func newSealedSpool() (*sealedSpool, error) {
	key := randomBytes(32)
	block, err := aes.NewCipher(key)
	clear(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return nil, err
	}
	file, err := os.CreateTemp("", "harness-sealed-*")
	if err != nil {
		return nil, err
	}
	return &sealedSpool{file: file, aead: aead}, nil
}
func (s *sealedSpool) Write(p []byte) (int, error) {
	if s.finished || s.size+int64(len(p)) > importLimit {
		return 0, apiErr(413, "ATTACHMENT_TOO_LARGE", "The archive exceeds 512 MiB")
	}
	n := len(p)
	s.size += int64(n)
	for len(p) > 0 {
		size := min(spoolChunk-len(s.pending), len(p))
		s.pending = append(s.pending, p[:size]...)
		p = p[size:]
		if len(s.pending) == spoolChunk {
			if err := s.flush(); err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}
func (s *sealedSpool) flush() error {
	if len(s.pending) == 0 {
		return nil
	}
	sealed := s.aead.Seal(nil, nil, s.pending, binary.BigEndian.AppendUint64(nil, s.chunks))
	s.chunks++
	_, err := s.file.Write(sealed)
	clear(s.pending)
	s.pending = s.pending[:0]
	return err
}
func (s *sealedSpool) Finish() error {
	if s.finished {
		return nil
	}
	s.finished = true
	return s.flush()
}
func (s *sealedSpool) Size() int64 { return s.size }
func (s *sealedSpool) ReadAt(p []byte, offset int64) (int, error) {
	if !s.finished || offset < 0 {
		return 0, errors.New("invalid archive read")
	}
	if offset >= s.size {
		return 0, io.EOF
	}
	written := 0
	for len(p) > 0 && offset < s.size {
		index, within := offset/spoolChunk, offset%spoolChunk
		plainSize := min(int64(spoolChunk), s.size-index*spoolChunk)
		sealed := make([]byte, plainSize+int64(s.aead.Overhead()))
		if _, err := s.file.ReadAt(sealed, index*int64(spoolChunk+s.aead.Overhead())); err != nil {
			return written, err
		}
		plain, err := s.aead.Open(nil, nil, sealed, binary.BigEndian.AppendUint64(nil, uint64(index)))
		if err != nil {
			return written, err
		}
		n := copy(p, plain[within:])
		clear(plain)
		p = p[n:]
		offset += int64(n)
		written += n
	}
	if len(p) > 0 {
		return written, io.EOF
	}
	return written, nil
}
func (s *sealedSpool) Close() error {
	clear(s.pending)
	s.aead = nil
	name := s.file.Name()
	err := s.file.Close()
	os.Remove(name)
	return err
}

func (h *harness) handleImport(w http.ResponseWriter, r *http.Request, p *principal) {
	r.Body = http.MaxBytesReader(w, r.Body, importLimit+(1<<20))
	reader, err := r.MultipartReader()
	if err != nil {
		respond(w, nil, apiErr(400, "IMPORT_INVALID", "Expected multipart form data"))
		return
	}
	fields := object{}
	var spool *sealedSpool
	defer func() {
		if spool != nil {
			spool.Close()
		}
	}()
	for count := 0; ; count++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= 8 {
			respond(w, nil, apiErr(400, "IMPORT_INVALID", "Invalid multipart archive"))
			return
		}
		if part.FormName() == "file" {
			if spool != nil {
				respond(w, nil, apiErr(400, "IMPORT_INVALID", "Expected one archive"))
				return
			}
			spool, err = newSealedSpool()
			if err == nil {
				_, err = io.Copy(spool, part)
			}
			if err == nil {
				err = spool.Finish()
			}
			if err != nil {
				respond(w, nil, err)
				return
			}
		} else {
			b, err := io.ReadAll(io.LimitReader(part, 8193))
			if err != nil || len(b) > 8192 {
				respond(w, nil, apiErr(400, "IMPORT_INVALID", "Invalid multipart field"))
				return
			}
			fields[part.FormName()] = string(b)
		}
		part.Close()
	}
	key, err := decodeContentKey(str(fields["key"]))
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	source := str(fields["source"])
	if spool == nil || !slices.Contains([]string{"chatgpt", "claude", "tinfoil"}, source) {
		respond(w, nil, apiErr(400, "IMPORT_INVALID", "An archive and supported source are required"))
		return
	}
	if source == "tinfoil" {
		converted, native, err := convertNativeArchive(spool)
		if err != nil {
			respond(w, nil, err)
			return
		}
		if native {
			defer converted.Close()
			spool.Close()
			spool = converted
			source = "tinfoil_backup"
		}
	}
	job, err := h.stageImport(r.Context(), p, key, source, spool)
	respond(w, job, err)
}
func (h *harness) stageImport(ctx context.Context, p *principal, key contentKey, source string, archive *sealedSpool) (any, error) {
	hash := sha256.New()
	if _, err := io.Copy(hash, io.NewSectionReader(archive, 0, archive.Size())); err != nil {
		return nil, err
	}
	var created object
	chunks := (archive.Size() + importChunk - 1) / importChunk
	err := h.syncAPI.call(ctx, p, "/v1/import/create", object{"source": source, "total_bytes": archive.Size(), "total_chunks": chunks, "archive_sha256": hex.EncodeToString(hash.Sum(nil))}, &created)
	if err != nil {
		return nil, err
	}
	for i := int64(0); i < chunks; i++ {
		data := make([]byte, min(int64(importChunk), archive.Size()-i*importChunk))
		if _, err := archive.ReadAt(data, i*importChunk); err != nil {
			return nil, err
		}
		err := h.syncAPI.call(ctx, p, "/v1/import/upload", object{"upload_id": created["upload_id"], "chunk_index": i, "chunk_sha256": hashHex(data), "data": base64.StdEncoding.EncodeToString(data)}, nil)
		clear(data)
		if err != nil {
			return nil, err
		}
	}
	var result object
	err = h.syncAPI.call(ctx, p, "/v1/import/start", object{"job_id": created["job_id"], "key": key.base64()}, &result)
	return object{"jobId": created["job_id"]}, err
}
func (h *harness) importStatus(ctx context.Context, p *principal, _ contentKey, in object) (any, error) {
	id, err := requiredID(in, "jobId")
	if err != nil {
		return nil, err
	}
	var result object
	err = h.syncAPI.call(ctx, p, "/v1/import/status", object{"job_id": id}, &result)
	if err != nil {
		return nil, err
	}
	if value, ok := result["failure_reason"]; ok {
		result["failureReason"] = value
	}
	result["jobId"] = result["job_id"]
	return result, nil
}

// The sync enclave accepts the cloud-import archive. Native backups use a
// different manifest and must be validated and repackaged, including local chats.
func convertNativeArchive(input *sealedSpool) (*sealedSpool, bool, error) {
	zr, err := zip.NewReader(input, input.Size())
	if err != nil {
		return nil, false, nil
	}
	files := map[string]*zip.File{}
	total := uint64(0)
	for _, f := range zr.File {
		if len(files) >= 50000 || f.Name != path.Clean(f.Name) || strings.HasPrefix(f.Name, "/") || strings.HasPrefix(f.Name, "../") || strings.Contains(f.Name, "\\") || files[f.Name] != nil {
			return nil, false, apiErr(400, "IMPORT_INVALID", "Unsafe or duplicate archive path")
		}
		total += f.UncompressedSize64
		if total > 2<<30 {
			return nil, false, apiErr(400, "IMPORT_INVALID", "Archive expansion limit exceeded")
		}
		files[f.Name] = f
	}
	manifestFile := files["manifest.json"]
	if manifestFile == nil {
		return nil, false, nil
	}
	data, err := zipBytes(manifestFile)
	if err != nil {
		return nil, false, err
	}
	var manifest object
	if json.Unmarshal(data, &manifest) != nil || manifest["format"] != "tinfoil-native-backup" {
		return nil, false, nil
	}
	if version := integer(manifest["version"]); version != 1 && version != 2 {
		return nil, true, apiErr(400, "IMPORT_INVALID", "Unsupported native backup version")
	}
	listed := map[string]object{}
	entities := []object{}
	imageMeta := map[string]object{}
	for _, item := range arr(manifest["files"]) {
		meta := obj(item)
		name := str(meta["path"])
		file := files[name]
		if file == nil || listed[name] != nil {
			return nil, true, apiErr(400, "IMPORT_INVALID", "The manifest does not match the archive")
		}
		b, err := zipBytes(file)
		if err != nil {
			return nil, true, err
		}
		if integer(meta["size_bytes"]) != len(b) || str(meta["sha256"]) != hashHex(b) {
			return nil, true, apiErr(400, "IMPORT_INVALID", "Archive integrity check failed")
		}
		listed[name] = meta
		if strings.HasPrefix(name, "images/") && strings.HasSuffix(name, ".json") {
			var image object
			if json.Unmarshal(b, &image) != nil {
				return nil, true, apiErr(400, "IMPORT_INVALID", "Invalid image metadata")
			}
			image["archivePath"] = strings.TrimSuffix(name, ".json") + ".bin"
			imageMeta[str(image["id"])] = image
		}
		clear(b)
	}
	if len(listed)+1 != len(files) {
		return nil, true, apiErr(400, "IMPORT_INVALID", "The archive contains unlisted entries")
	}
	output, err := newSealedSpool()
	if err != nil {
		return nil, true, err
	}
	success := false
	defer func() {
		if !success {
			output.Close()
		}
	}()
	zw := zip.NewWriter(output)
	blobs := []object{}
	counts := object{"projects": 0, "documents": 0, "chats": 0, "blobs": 0}
	inlineImage := func(id string) (string, error) {
		meta := imageMeta[id]
		file := files[str(meta["archivePath"])]
		if meta == nil || file == nil {
			return "", apiErr(400, "IMPORT_INVALID", "A document page image is missing")
		}
		data, err := zipBytes(file)
		if err != nil {
			return "", err
		}
		defer clear(data)
		return base64.StdEncoding.EncodeToString(data), nil
	}
	write := func(name string, b []byte) error {
		w, err := zw.Create(name)
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		return err
	}
	for _, item := range arr(manifest["files"]) {
		meta := obj(item)
		name, kind := str(meta["path"]), str(meta["kind"])
		if kind == "relationships" || kind == "images" {
			continue
		}
		b, err := zipBytes(files[name])
		if err != nil {
			return nil, true, err
		}
		var payload object
		if json.Unmarshal(b, &payload) != nil {
			return nil, true, apiErr(400, "IMPORT_INVALID", "Invalid backup entity")
		}
		entityKind, sourceID, parentID := kind, str(payload["id"]), str(payload["projectId"])
		switch kind {
		case "projects":
			entityKind = "project"
			counts["projects"] = integer(counts["projects"]) + 1
			payload = selectFields(payload, "name description systemInstructions color memory")
		case "project_documents":
			entityKind = "document"
			counts["documents"] = integer(counts["documents"]) + 1
			payload["content"] = str(payload["extractedText"])
			delete(payload, "extractedText")
			delete(payload, "id")
			delete(payload, "projectId")
		case "cloud_chats", "local_chats":
			entityKind = "chat"
			counts["chats"] = integer(counts["chats"]) + 1
			delete(payload, "id")
			delete(payload, "projectId")
			payload["isLocalOnly"] = false
			for _, m := range arr(payload["messages"]) {
				message := obj(m)
				for _, value := range arr(message["imageData"]) {
					image := obj(value)
					encoded, err := inlineImage(str(image["imageId"]))
					if err != nil {
						return nil, true, err
					}
					image["base64"] = encoded
					delete(image, "imageId")
				}
				for i, a := range arr(message["attachments"]) {
					attachment := obj(a)
					if attachment["type"] != "image" {
						for _, value := range arr(attachment["pages"]) {
							page := obj(value)
							if id := str(page["imageId"]); id != "" {
								encoded, err := inlineImage(id)
								if err != nil {
									return nil, true, err
								}
								page["image"] = encoded
								delete(page, "imageId")
							}
						}
						continue
					}
					image := imageMeta[str(attachment["imageId"])]
					if image == nil {
						return nil, true, apiErr(400, "IMPORT_INVALID", "An attachment image is missing")
					}
					imagePath := str(image["archivePath"])
					attachment = object{"type": "image", "fileName": image["fileName"], "mimeType": image["mimeType"], "fileSize": image["sizeBytes"], "archivePath": imagePath}
					arr(message["attachments"])[i] = attachment
				}
			}
		default:
			return nil, true, apiErr(400, "IMPORT_INVALID", "Unknown backup entity kind")
		}
		encoded := raw(payload)
		outputPath := "entities/" + entityKind + "/" + jsonNumber(len(entities)) + ".json"
		if err := write(outputPath, encoded); err != nil {
			return nil, true, err
		}
		entry := object{"kind": entityKind, "source_id": sourceID, "path": outputPath, "sha256": hashHex(encoded), "size_bytes": len(encoded)}
		if parentID != "" {
			entry["project_source_id"] = parentID
		}
		entities = append(entities, entry)
	}
	for name, meta := range listed {
		if str(meta["kind"]) != "images" || !strings.HasSuffix(name, ".bin") {
			continue
		}
		b, err := zipBytes(files[name])
		if err != nil {
			return nil, true, err
		}
		if err := write(name, b); err != nil {
			return nil, true, err
		}
		blobs = append(blobs, object{"path": name, "sha256": hashHex(b), "size_bytes": len(b)})
		clear(b)
	}
	counts["blobs"] = len(blobs)
	cloud := object{"format": "tinfoil-native-cloud-import", "version": 1, "source_backup_id": manifest["backup_id"], "counts": counts, "entities": entities, "blobs": blobs}
	if err := write("manifest.json", raw(cloud)); err != nil {
		return nil, true, err
	}
	if err := zw.Close(); err != nil {
		return nil, true, err
	}
	if err := output.Finish(); err != nil {
		return nil, true, err
	}
	success = true
	return output, true, nil
}
func zipBytes(file *zip.File) ([]byte, error) {
	if file.UncompressedSize64 > 256<<20 {
		return nil, apiErr(400, "IMPORT_INVALID", "Archive entry is too large")
	}
	r, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	b, err := io.ReadAll(io.LimitReader(r, int64(file.UncompressedSize64)+1))
	if err != nil || uint64(len(b)) != file.UncompressedSize64 {
		return nil, apiErr(400, "IMPORT_INVALID", "Archive entry size mismatch")
	}
	return b, nil
}
