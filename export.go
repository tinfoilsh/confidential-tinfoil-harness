package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

func millis(value any) int64 {
	if t, err := time.Parse(time.RFC3339Nano, str(value)); err == nil {
		return t.UnixMilli()
	}
	return int64(integer(value))
}
func (h *harness) shareThread(ctx context.Context, p *principal, key contentKey, in object) (any, error) {
	id, err := requiredID(in, "threadId")
	if err != nil {
		return nil, err
	}
	row, err := h.storeRows.load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	t := normalizeThread(row)
	messages := []any{}
	for _, item := range arr(t["messages"]) {
		m := obj(item)
		out := object{"role": m["role"], "content": str(m["content"]), "timestamp": millis(firstString(m["createdAt"], m["timestamp"])), "timeline": m["timeline"]}
		for _, f := range []string{"thoughts", "thinkingDuration", "modelDisplayName", "isError", "quote"} {
			if v, ok := m[f]; ok {
				out[f] = v
			}
		}
		attachments := []any{}
		for _, item := range arr(m["attachments"]) {
			a := obj(item)
			d := object{}
			for _, f := range []string{"id", "type", "fileName", "mimeType", "thumbnailBase64", "encryptionKey", "textContent", "description"} {
				if v, ok := a[f]; ok {
					d[f] = v
				}
			}
			attachments = append(attachments, d)
		}
		if len(attachments) > 0 {
			out["attachments"] = attachments
		}
		messages = append(messages, out)
	}
	payload := object{"v": 1, "title": t["title"], "messages": messages, "createdAt": millis(t["createdAt"])}
	var sealed object
	if err := h.syncAPI.call(ctx, p, "/v1/share/seal", object{"plaintext": base64.StdEncoding.EncodeToString(raw(payload))}, &sealed); err != nil {
		return nil, err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(str(sealed["ciphertext"]))
	if err != nil {
		return nil, err
	}
	cred, err := h.auth.inference(ctx, p)
	if err != nil {
		return nil, err
	}
	shareID := token()
	ctx = context.WithValue(ctx, apiKeyKey{}, string(cred.Key))
	req, _ := http.NewRequestWithContext(ctx, "PUT", h.controlplane+"/api/shares/"+shareID, bytes.NewReader(ciphertext))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Format-Version", "2")
	resp, err := h.cpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, downstreamError(resp.StatusCode, b)
	}
	return object{"url": strings.TrimRight(env("TINFOIL_CHAT_URL", "https://chat.tinfoil.sh"), "/") + "/share/" + shareID + "#v2:" + str(sealed["share_key"])}, nil
}
func sharedThreadView(id string, payload object) object {
	thread := normalizeThread(&storedRow{ID: id, Data: payload})
	messages := []any{}
	for _, value := range arr(thread["messages"]) {
		message := obj(value)
		out := publicMessage(message)
		for i, value := range arr(out["attachments"]) {
			obj(value)["attKey"] = obj(arr(message["attachments"])[i])["encryptionKey"]
		}
		messages = append(messages, out)
	}
	return object{"title": thread["title"], "createdAt": thread["createdAt"], "messages": messages}
}

func (h *harness) openShare(ctx context.Context, _ *principal, _ contentKey, in object) (any, error) {
	id, err := requiredID(in, "shareId")
	if err != nil {
		return nil, err
	}
	fragment, ok := strings.CutPrefix(str(in["fragment"]), "v2:")
	key, decodeErr := hex.DecodeString(fragment)
	if !ok || decodeErr != nil || len(key) != 32 {
		return nil, apiErr(400, "BAD_REQUEST", "A v2 share fragment is required")
	}
	clear(key)
	req, _ := http.NewRequestWithContext(ctx, "GET", h.controlplane+"/api/shares/"+id, nil)
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
		return nil, downstreamError(resp.StatusCode, ciphertext)
	}
	var opened object
	err = h.syncAPI.call(ctx, &principal{Anonymous: true}, "/v1/share/open", object{"share_key": fragment, "ciphertext": base64.StdEncoding.EncodeToString(ciphertext)}, &opened)
	if err != nil {
		return nil, err
	}
	plain, err := base64.StdEncoding.DecodeString(str(opened["plaintext"]))
	if err != nil {
		return nil, err
	}
	defer clear(plain)
	var payload object
	if json.Unmarshal(plain, &payload) != nil || integer(payload["v"]) != 1 {
		return nil, apiErr(400, "BAD_REQUEST", "Invalid shared thread")
	}
	return sharedThreadView(id, payload), nil
}

func (h *harness) handleExport(w http.ResponseWriter, r *http.Request) {
	in, err := decodeJSON(w, r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	p, err := h.auth.authenticate(r, false)
	if err != nil {
		respond(w, nil, err)
		return
	}
	key, err := decodeContentKey(str(in["key"]))
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer clear(key[:])
	format := str(in["format"])
	if format == "claude-projects" {
		result, err := h.claudeProjects(r.Context(), p, key, str(in["projectId"]))
		if err != nil {
			respond(w, nil, err)
			return
		}
		w.Header().Set("Content-Disposition", `attachment; filename="projects.json"`)
		respond(w, result, nil)
		return
	}
	if format != "tinfoil-backup" {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "Unknown export format"))
		return
	}
	// Build into an encrypted spool so an upstream failure is still a JSON error,
	// and plaintext ZIP bytes are only emitted after the archive is complete.
	spool, err := newSealedSpool()
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer spool.Close()
	err = h.writeBackup(r.Context(), p, key, in, spool)
	if err == nil {
		err = spool.Finish()
	}
	if err != nil {
		respond(w, nil, err)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Disposition", `attachment; filename="tinfoil-backup.zip"`)
	io.Copy(w, io.NewSectionReader(spool, 0, spool.Size()))
}
func (h *harness) claudeProjects(ctx context.Context, p *principal, key contentKey, projectID string) ([]object, error) {
	projects := []object{}
	total := 0
	err := h.eachRow(ctx, p, key, "project", "", func(row *storedRow) error {
		if projectID != "" && row.ID != projectID {
			return nil
		}
		docs, err := h.projectDocuments(ctx, p, key, row.ID)
		if err != nil {
			return err
		}
		documents := []object{}
		for _, d := range docs {
			documents = append(documents, object{"uuid": d["id"], "filename": d["filename"], "content": d["content"], "created_at": d["createdAt"]})
		}
		project := object{"uuid": row.ID, "name": row.Data["name"], "description": row.Data["description"], "prompt_template": row.Data["systemInstructions"], "created_at": row.Data["createdAt"], "updated_at": row.Data["updatedAt"], "docs": documents}
		total += len(raw(project))
		if total > 64<<20 {
			return apiErr(413, "ATTACHMENT_TOO_LARGE", "The project export exceeds 64 MiB")
		}
		projects = append(projects, project)
		return nil
	})
	return projects, err
}
func backupPath(kind, id string) string {
	return kind + "/id-" + hex.EncodeToString([]byte(id)) + ".json"
}
func hashHex(data []byte) string { d := sha256.Sum256(data); return hex.EncodeToString(d[:]) }

type backupWriter struct {
	zip    *zip.Writer
	files  []object
	counts object
	total  int64
}

func (b *backupWriter) add(kind, path string, value any) error {
	return b.bytes(kind, path, raw(value))
}
func (b *backupWriter) bytes(kind, path string, data []byte) error {
	b.total += int64(len(data))
	if b.total > 1<<30 || len(b.files) >= 50000 {
		return apiErr(413, "ATTACHMENT_TOO_LARGE", "The backup exceeds the archive limits")
	}
	header := &zip.FileHeader{Name: path, Method: zip.Deflate}
	header.SetModTime(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC))
	writer, err := b.zip.CreateHeader(header)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	b.files = append(b.files, object{"path": path, "kind": kind, "sha256": hashHex(data), "size_bytes": len(data)})
	return nil
}
func selectFields(data object, fields string) object {
	out := object{}
	for _, field := range strings.Fields(fields) {
		if v, ok := data[field]; ok && v != nil {
			out[field] = v
		}
	}
	return out
}
func (h *harness) writeBackup(ctx context.Context, p *principal, key contentKey, in object, output io.Writer) error {
	b := &backupWriter{zip: zip.NewWriter(output), counts: object{"projects": 0, "project_documents": 0, "cloud_chats": 0, "local_chats": 0, "relationships": 0, "images": 0, "files": 0}}
	relationships := object{"projectChats": []any{}, "projectDocuments": []any{}, "chatImages": []any{}}
	selectedProjects := map[string]bool{}
	projectID := str(in["projectId"])
	err := h.eachRow(ctx, p, key, "project", "", func(row *storedRow) error {
		if projectID != "" && row.ID != projectID {
			return nil
		}
		project := selectFields(row.Data, "name description systemInstructions color memory createdAt updatedAt")
		project["id"] = row.ID
		if project["memory"] == nil {
			project["memory"] = []any{}
		}
		selectedProjects[row.ID] = true
		if err := b.add("projects", backupPath("projects", row.ID), project); err != nil {
			return err
		}
		b.counts["projects"] = integer(b.counts["projects"]) + 1
		docs, err := h.projectDocuments(ctx, p, key, row.ID)
		if err != nil {
			return err
		}
		for _, d := range docs {
			doc := selectFields(d, "id projectId filename contentType sizeBytes createdAt updatedAt")
			doc["extractedText"] = str(d["content"])
			path := "project_documents/id-" + hex.EncodeToString([]byte(row.ID)) + "/id-" + hex.EncodeToString([]byte(str(d["id"]))) + ".json"
			if err := b.add("project_documents", path, doc); err != nil {
				return err
			}
			b.counts["project_documents"] = integer(b.counts["project_documents"]) + 1
			relationships["projectDocuments"] = append(arr(relationships["projectDocuments"]), object{"projectId": row.ID, "documentId": d["id"]})
		}
		return nil
	})
	if err != nil {
		return err
	}
	ids := map[string]bool{}
	for _, id := range arr(in["threadIds"]) {
		ids[str(id)] = true
	}
	err = h.eachRow(ctx, p, key, "chat", projectID, func(row *storedRow) error {
		if len(ids) > 0 && !ids[row.ID] {
			return nil
		}
		t := normalizeThread(row)
		chat := selectFields(t, "id title titleState createdAt updatedAt projectId presetId model webSearchEnabled")
		if project := str(chat["projectId"]); project != "" {
			if selectedProjects[project] {
				relationships["projectChats"] = append(arr(relationships["projectChats"]), object{"projectId": project, "chatId": row.ID})
			} else {
				chat["projectId"] = nil
			}
		}
		messages := []any{}
		for index, item := range arr(t["messages"]) {
			m := obj(item)
			message := selectFields(m, "role content turnId modelDisplayName thoughts thinkingDuration isError quote webSearch urlFetches codeExecCalls annotations searchReasoning webSearchBeforeThinking")
			message["timestamp"] = firstString(m["createdAt"], m["timestamp"], t["createdAt"])
			message["content"] = str(m["content"])
			blocks := []any{}
			for _, item := range arr(m["timeline"]) {
				block := obj(item)
				switch block["type"] {
				case "content":
					blocks = append(blocks, selectFields(block, "type id content"))
				case "thinking":
					copy := selectFields(block, "type id content duration")
					copy["isThinking"] = false
					blocks = append(blocks, copy)
				case "tool_call":
					if result, exists := block["result"]; exists && (!strings.HasPrefix(str(block["name"]), "render_") || block["name"] == "render_link_preview") {
						// Native v2 has no result field on tool_call. Its existing
						// code_exec output string carries the typed result losslessly.
						arguments := obj(decodeValue([]byte(str(block["arguments"]))))
						call := object{"id": firstString(block["toolCallId"], block["id"]), "toolName": block["name"], "arguments": arguments, "status": "completed", "output": string(raw(result))}
						blocks = append(blocks, object{"type": "code_exec", "id": block["id"], "calls": []any{call}})
						continue
					}
					copy := selectFields(block, "type id toolCallId name arguments resolution")
					if block["resolvedAt"] != nil {
						copy["resolvedAt"] = millis(block["resolvedAt"])
					}
					blocks = append(blocks, copy)
				}
			}
			if len(blocks) > 0 {
				message["timeline"] = blocks
			}
			attachments := []any{}
			for _, item := range arr(m["attachments"]) {
				a := obj(item)
				if a["type"] != "image" {
					document := selectFields(a, "id fileName mimeType textContent description fileSize")
					document["type"] = "document"
					pages := []any{}
					for _, value := range arr(a["pages"]) {
						page := obj(value)
						portable := object{"page": integer(page["page"]), "text": str(page["text"]), "is_scanned": boolean(page["is_scanned"], false)}
						imageRef := obj(page["imageAttachment"])
						if str(imageRef["id"]) != "" || str(page["image"]) != "" {
							if str(page["image"]) != "" {
								imageRef = object{"base64": page["image"], "mimeType": "image/png"}
							}
							imageRef = clone(imageRef)
							imageRef["fileName"] = firstString(a["fileName"], "page") + ".png"
							imageID, err := h.backupImage(ctx, p, b, relationships, row.ID, index, a, imageRef, page["page"])
							if err != nil {
								return err
							}
							portable["imageId"] = imageID
						}
						pages = append(pages, portable)
					}
					if len(pages) > 0 {
						document["pages"] = pages
					}
					attachments = append(attachments, document)
					continue
				}
				imageID, err := h.backupImage(ctx, p, b, relationships, row.ID, index, a, a, nil)
				if err != nil {
					return err
				}
				attachments = append(attachments, object{"type": "image", "id": a["id"], "imageId": imageID})
			}
			if len(attachments) > 0 {
				message["attachments"] = attachments
			}
			messages = append(messages, message)
		}
		chat["messages"] = messages
		if err := b.add("cloud_chats", backupPath("cloud_chats", row.ID), chat); err != nil {
			return err
		}
		b.counts["cloud_chats"] = integer(b.counts["cloud_chats"]) + 1
		return nil
	})
	if err != nil {
		return err
	}
	if err := b.add("relationships", "relationships.json", relationships); err != nil {
		return err
	}
	b.counts["relationships"] = len(arr(relationships["projectChats"])) + len(arr(relationships["projectDocuments"])) + len(arr(relationships["chatImages"]))
	b.counts["files"] = len(b.files)
	manifest := object{"format": "tinfoil-native-backup", "version": 2, "backup_id": uuid.NewString(), "created_at": timestamp(), "complete": true, "counts": b.counts, "notices": object{"contains_plaintext": true, "documents_are_extracted_text_only": true}, "files": b.files, "omissions": []any{}, "warnings": []any{}}
	writer, err := b.zip.Create("manifest.json")
	if err != nil {
		return err
	}
	if _, err := writer.Write(raw(manifest)); err != nil {
		return err
	}
	return b.zip.Close()
}

func (h *harness) backupImage(ctx context.Context, p *principal, b *backupWriter, relationships object, chatID string, index int, attachment, descriptor object, page any) (string, error) {
	data, err := h.attachmentBytes(ctx, p, descriptor, false)
	if err != nil {
		return "", err
	}
	defer clear(data)
	contentType := firstString(descriptor["mimeType"], http.DetectContentType(data))
	switch contentType {
	case "image/png", "image/jpeg", "image/gif", "image/webp", "image/bmp":
	default:
		im, _, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			return "", err
		}
		var converted bytes.Buffer
		if err := png.Encode(&converted, im); err != nil {
			return "", err
		}
		data = converted.Bytes()
		defer clear(data)
		contentType = "image/png"
	}
	imageID := chatID + "/" + jsonNumber(index) + "/" + jsonNumber(integer(b.counts["images"]))
	meta := object{"id": imageID, "chatId": chatID, "messageIndex": index, "attachmentId": attachment["id"], "fileName": firstString(descriptor["fileName"], "image"), "mimeType": contentType, "sizeBytes": len(data)}
	if page != nil {
		meta["page"] = integer(page)
	}
	if text := str(descriptor["description"]); text != "" {
		meta["description"] = text
	}
	name := strings.TrimSuffix(backupPath("images", imageID), ".json")
	if err := b.add("images", name+".json", meta); err != nil {
		return "", err
	}
	if err := b.bytes("images", name+".bin", data); err != nil {
		return "", err
	}
	b.counts["images"] = integer(b.counts["images"]) + 1
	relationships["chatImages"] = append(arr(relationships["chatImages"]), object{"chatId": chatID, "imageId": imageID})
	return imageID, nil
}
