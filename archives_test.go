package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"mime/multipart"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type formPart struct {
	name, filename string
	data           []byte
}

func optionsPart(options object) formPart { return formPart{name: "options", data: raw(options)} }
func threadPart(thread any) formPart      { return formPart{name: "thread", data: raw(thread)} }

func archiveRoute(h *harness, path, user string, parts ...formPart) *httptest.ResponseRecorder {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for _, part := range parts {
		var w io.Writer
		if part.filename != "" {
			w, _ = form.CreateFormFile(part.name, part.filename)
		} else {
			w, _ = form.CreateFormField(part.name)
		}
		w.Write(part.data)
	}
	form.Close()
	r := httptest.NewRequest("POST", path, &body)
	r.Header.Set("Content-Type", form.FormDataContentType())
	if user != "" {
		r.Header.Set("Authorization", "Bearer clerk-"+user)
	} else {
		r.Header.Set("Authorization", "Bearer free_anonymous-key")
	}
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	return w
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// unzipped is an archive's entries by name, in the order it holds them.
func unzipped(t *testing.T, data []byte) (map[string][]byte, []string) {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("not a ZIP archive: %v: %.200s", err, data)
	}
	files, names := map[string][]byte{}, []string{}
	for _, f := range zr.File {
		r, _ := f.Open()
		b, _ := io.ReadAll(r)
		r.Close()
		files[f.Name] = b
		names = append(names, f.Name)
	}
	return files, names
}

type imported struct {
	threads []Thread
	done    *importDone
}

// importOf posts an archive to /v1/import and reads its lines.
func importOf(t *testing.T, h *harness, user string, archive []byte, name string, options object) imported {
	t.Helper()
	w := archiveRoute(h, "/v1/import", user, optionsPart(options), formPart{name: "file", filename: name, data: archive})
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("import: HTTP %d: %s", w.Code, w.Body.String())
	}
	var out imported
	scanner := bufio.NewScanner(w.Body)
	scanner.Buffer(nil, 64<<20)
	for scanner.Scan() {
		var line struct {
			Thread *Thread     `json:"thread"`
			Done   *importDone `json:"done"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil || out.done != nil {
			t.Fatalf("a line after the last, or not JSON: %s", scanner.Bytes())
		}
		switch {
		case line.Thread != nil:
			out.threads = append(out.threads, *line.Thread)
		case line.Done != nil:
			out.done = line.Done
		default:
			t.Fatalf("import failed: %s", scanner.Bytes())
		}
	}
	if out.done == nil {
		t.Fatalf("the import never said it was done: %s", w.Body.String())
	}
	return out
}

// storedChats are a user's stored chats read as the harness reads them.
func storedChats(rows *memoryRows, user string) map[string]Thread {
	chats := map[string]Thread{}
	page, _ := rows.list(context.Background(), &principal{ID: user}, rows.key, "chat", "", "", 0)
	for _, row := range page.Rows {
		t := readThreadRow(&row).Thread
		chats[t.Title] = t
	}
	return chats
}

func blobOf(t *testing.T, fake *fakeSync, a Attachment) []byte {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	blob, ok := fake.blobs[a.ID]
	if !ok || blob.key != a.EncryptionKey {
		t.Fatalf("no blob under %+v", a)
	}
	return blob.data
}

func base64Bytes(t *testing.T, encoded string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func pngFixture(t *testing.T, w, h int, shade uint8) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for i := range img.Pix {
		img.Pix[i] = shade
	}
	var b bytes.Buffer
	if err := png.Encode(&b, img); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// A backup holds the cloud and the device: export it, import it elsewhere,
// and the cloud holds the same projects and chats while the device gets its
// chats back with the originals of their images.
func TestNativeBackupRoundTrips(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	alice := &principal{ID: "alice"}
	project := "project-1"
	rows.rows[rowMapKey(alice, "project", project)] = storedRow{ID: project, ETag: "1", Data: raw(object{"name": "Research", "description": "Notes", "systemInstructions": "Cite sources", "color": "teal", "createdAt": "2026-01-01T00:00:00Z"})}
	rows.rows[rowMapKey(alice, "project_document", project+"/doc-1")] = storedRow{ID: project + "/doc-1", ETag: "1", Data: raw(object{"filename": "paper.md", "contentType": "text/markdown", "sizeBytes": 12, "content": "Paper text"}), project: &project, projectSet: true}
	stored := pngFixture(t, 8, 6, 40)
	imageID, imageKey := fake.seed("chat-1", stored)
	docID, docKey := fake.seed("chat-1", []byte(`{"textContent":"Stored document"}`))
	timeline := []Block{{ID: "think", Type: "thinking", Content: "Check sources"}, {ID: "call", Type: "tool_call", ToolCallID: "call", Name: "web_search", Arguments: `{"query":"growth"}`, Complete: true, Result: raw(object{"query": "growth", "results": []object{{"url": "https://example.com", "title": "Example"}}})}, {ID: "text", Type: "content", Content: "It grew."}}
	cloudChat := object{"title": "Evidence", "titleState": "manual", "createdAt": "2026-01-02T00:00:00Z", "updatedAt": "2026-01-02T01:00:00Z", "model": "kimi-k3", "pinned": true, "messages": []any{
		object{"id": "m1", "role": "user", "createdAt": "2026-01-02T00:00:00Z", "content": "Explain", "attachments": []any{
			object{"id": imageID, "kind": "image", "fileName": "chart.png", "mimeType": "image/png", "encryptionKey": imageKey, "thumbnail": "dGh1bWI=", "description": "A chart"},
			object{"id": docID, "kind": "document", "fileName": "report.txt", "mimeType": "text/plain", "encryptionKey": docKey},
			object{"id": strings.Repeat("e", 36), "kind": "image", "fileName": "gone.png", "encryptionKey": "gone"},
		}},
		object{"id": "m2", "role": "assistant", "createdAt": "2026-01-02T00:01:00Z", "content": "It grew.", "timeline": timeline, "annotations": []any{object{"type": "url_citation", "url_citation": object{"url": "https://example.com", "title": "Example", "start_index": 0, "end_index": 2}}}},
	}}
	rows.rows[rowMapKey(alice, "chat", "chat-1")] = storedRow{ID: "chat-1", ETag: "1", Data: raw(cloudChat), project: &project, projectSet: true}

	modelCopy, original, page := pngFixture(t, 4, 4, 90), pngFixture(t, 40, 30, 90), pngFixture(t, 3, 9, 200)
	device := object{"id": "9999990000000_00000000-0000-4000-8000-00000000000d", "title": "On my device", "titleState": "generated", "createdAt": "2026-02-01T00:00:00Z", "updatedAt": "2026-02-01T00:00:00Z", "model": "auto", "webSearchEnabled": false, "messages": []any{
		object{"id": "d1", "role": "user", "createdAt": "2026-02-01T00:00:00Z", "content": "Look", "attachments": []any{
			object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/jpeg", "size": 900, "thumbnail": "dGh1bWI=", "base64": base64.StdEncoding.EncodeToString(modelCopy)},
			object{"id": "att_scan", "kind": "document", "fileName": "scan.pdf", "mimeType": "application/pdf", "textContent": "Scanned", "pages": []any{object{"page": 1, "text": "One", "is_scanned": true, "image": base64.StdEncoding.EncodeToString(page)}}},
		}},
		object{"id": "d2", "role": "assistant", "createdAt": "2026-02-01T00:00:01Z", "content": "Seen", "timeline": []any{object{"id": "d2-content", "type": "content", "content": "Seen"}}},
	}}
	w := archiveRoute(h, "/v1/export", "alice", optionsPart(object{"format": "native", "key": key.base64()}), threadPart(device), formPart{name: "file", filename: "att_photo", data: original})
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/zip" {
		t.Fatalf("export: HTTP %d: %s", w.Code, w.Body.String())
	}
	files, _ := unzipped(t, w.Body.Bytes())
	var manifest backupManifest
	json.Unmarshal(files[manifestPath], &manifest)
	for _, file := range manifest.Files {
		if data, ok := files[file.Path]; !ok || hashHex(data) != file.SHA256 || int64(len(data)) != file.SizeBytes || backupKind(file.Path) != file.Kind {
			t.Fatalf("the manifest does not describe %s", file.Path)
		}
	}
	want := backupCounts{Projects: 1, ProjectDocuments: 1, CloudChats: 1, LocalChats: 1, Relationships: 5, Images: 3, Files: len(files) - 1}
	if manifest.Format != nativeBackupFormat || manifest.Version != 3 || manifest.Counts != want || len(manifest.Files) != len(files)-1 || manifest.Complete ||
		len(manifest.Omissions) != 1 || manifest.Omissions[0].ParentSourceID != "chat-1" || len(manifest.Warnings) != 1 || manifest.Warnings[0].Count != 1 {
		t.Fatalf("manifest: %+v", manifest)
	}

	restored, restoredRows, _ := testChatHarness(t, nil)
	restoredSync := newFakeSync(t, restored)
	result := importOf(t, restored, "alice", w.Body.Bytes(), "tinfoil-backup.zip", object{"target": "cloud", "key": key.base64()})
	if result.done.JobID == nil || len(result.threads) != 1 || !reflect.DeepEqual(result.done.Warnings, []string{"The backup left out 1 items when it was made."}) {
		t.Fatalf("restore: %+v %v", result.done, result.threads)
	}
	if status := postRoute(restored, "/v1/import/status", object{"jobId": *result.done.JobID}, "alice"); !strings.Contains(status.Body.String(), `"status":"completed"`) {
		t.Fatalf("job status: %s", status.Body.String())
	}
	thread := result.threads[0]
	photo, scan := thread.Messages[0].Attachments[0], thread.Messages[0].Attachments[1]
	if !rowIDPattern.MatchString(thread.ID) || thread.ID == device["id"] || thread.Title != "On my device" || thread.WebSearchEnabled || len(thread.Messages) != 2 || thread.Messages[1].Content != "Seen" {
		t.Fatalf("device chat: %+v", thread)
	}
	if !bytes.Equal(base64Bytes(t, photo.Base64), original) || photo.MimeType != "image/png" || photo.Thumbnail != "dGh1bWI=" || !bytes.Equal(base64Bytes(t, scan.Pages[0].Image), page) || scan.TextContent != "Scanned" {
		t.Fatalf("device chat lost its images: %+v %+v", photo, scan)
	}
	again := importOf(t, restored, "alice", w.Body.Bytes(), "tinfoil-backup.zip", object{"target": "device"})
	if len(again.threads) != 2 || again.threads[1].ID != thread.ID || again.done.JobID != nil {
		t.Fatalf("a second restore named the device chat anew, or kept cloud chats off the device: %+v", again)
	}

	chats := storedChats(restoredRows, "alice")
	chat, ok := chats["Evidence"]
	if !ok || len(chat.Messages) != 2 || !chat.Pinned || chat.Model != "kimi-k3" || chat.TitleState != "manual" {
		t.Fatalf("cloud chat: %+v", chats)
	}
	if !reflect.DeepEqual(decodeValue(raw(chat.Messages[1].Timeline)), decodeValue(raw(readThread("chat-1", raw(cloudChat)).Messages[1].Timeline))) || len(chat.Messages[1].Annotations) != 1 {
		t.Fatalf("the reply changed: %s", raw(chat.Messages[1]))
	}
	attachments := chat.Messages[0].Attachments
	if len(attachments) != 2 || attachments[0].Description != "A chart" || attachments[0].Thumbnail != "dGh1bWI=" || !bytes.Equal(blobOf(t, restoredSync, attachments[0]), stored) || attachments[1].TextContent != "Stored document" {
		t.Fatalf("cloud attachments: %+v", attachments)
	}
	projects, _ := restoredRows.list(context.Background(), alice, key, "project", "", "", 0)
	if len(projects.Rows) != 1 || valueOr(chat.ProjectID, "") != projects.Rows[0].ID {
		t.Fatalf("the chat lost its project: %v", chat.ProjectID)
	}
	restoredProject := readProject(&projects.Rows[0])
	docs, _ := restored.projectDocuments(context.Background(), alice, key, restoredProject.ID)
	if restoredProject.Name != "Research" || restoredProject.SystemInstructions != "Cite sources" || valueOr(restoredProject.Color, "") != "teal" || len(docs) != 1 || docs[0].Content != "Paper text" || docs[0].Filename != "paper.md" {
		t.Fatalf("project: %+v %+v", restoredProject, docs)
	}
}

// Backups the web app made before the harness wrote them hold chats in its
// old format, including chats it kept on the device.
func TestWebAppBackupsRestore(t *testing.T) {
	green, blue := "web-backup green", "web-backup blue"
	for _, tc := range []struct {
		fixture  string
		warnings []string
	}{
		{"web-backup-v1.zip", []string{}},
		{"web-backup-v2.zip", []string{"The backup left out 1 items when it was made."}},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			h, rows, key := testChatHarness(t, nil)
			fake := newFakeSync(t, h)
			result := importOf(t, h, "alice", fixture(t, tc.fixture), tc.fixture, object{"target": "cloud", "key": key.base64()})
			if result.done.JobID == nil || !reflect.DeepEqual(result.done.Warnings, tc.warnings) || len(result.threads) != 2 {
				t.Fatalf("restore: %+v, %d device chats", result.done, len(result.threads))
			}
			legacy, scanned := result.threads[0], result.threads[1]
			if legacy.Title != "Old device chat" || scanned.Title != "Scanned notes" {
				t.Fatalf("device chats: %s, %s", legacy.Title, scanned.Title)
			}
			pictures := legacy.Messages[0].Attachments
			if len(pictures) != 2 || pictures[0].TextContent != "Legacy document text" || pictures[1].Kind != "image" || !strings.HasPrefix(string(base64Bytes(t, pictures[1].Base64)), "\x89PNG") {
				t.Fatalf("%s: the legacy picture was lost: %+v", green, pictures)
			}
			reply := legacy.Messages[1].Timeline
			if len(reply) != 3 || reply[0].Content != "It is green" || reply[1].Name != "web_search" || reply[1].Display == nil || reply[1].Display.Query != "green squares" || reply[2].Content != "A green square." {
				t.Fatalf("the old reply was not read: %s", raw(reply))
			}
			scan := scanned.Messages[0].Attachments[0]
			if scan.TextContent != "Scanned text" || len(scan.Pages) != 1 || !strings.HasPrefix(string(base64Bytes(t, scan.Pages[0].Image)), "\x89PNG") {
				t.Fatalf("%s: the scanned page was lost: %+v", blue, scanned.Messages)
			}

			chat := storedChats(rows, "alice")["Evidence"]
			if len(chat.Messages) != 2 || chat.ProjectID == nil || chat.Model != "gpt-oss-120b" {
				t.Fatalf("cloud chat: %+v", chat)
			}
			image := chat.Messages[0].Attachments[0]
			if image.Description != "A red chart" || image.FileName != "chart.png" || !strings.HasPrefix(string(blobOf(t, fake, image)), "\x89PNG") {
				t.Fatalf("cloud image: %+v", image)
			}
			names := []string{}
			for _, block := range chat.Messages[1].Timeline {
				names = append(names, block.Type+":"+block.Name)
			}
			if strings.Join(names, " ") != "thinking: tool_call:web_search tool_call:web_fetch tool_call:python content:" || len(chat.Messages[1].Annotations) != 1 {
				t.Fatalf("the old timeline was not read: %v", names)
			}
			projects, _ := rows.list(context.Background(), &principal{ID: "alice"}, key, "project", "", "", 0)
			docs, _ := h.projectDocuments(context.Background(), &principal{ID: "alice"}, key, *chat.ProjectID)
			if len(projects.Rows) != 1 || readProject(&projects.Rows[0]).Name != "Research" || len(docs) != 1 || docs[0].Content != "Extracted paper text" {
				t.Fatalf("project: %v %v", projects.Rows, docs)
			}
		})
	}
	h, _, _ := testChatHarness(t, nil)
	result := importOf(t, h, "", fixture(t, "web-backup-v1.zip"), "backup.zip", object{"target": "device"})
	if len(result.threads) != 3 || result.threads[0].Title != "Evidence" || result.threads[0].ProjectID != nil || result.done.JobID != nil ||
		!reflect.DeepEqual(result.done.Warnings, []string{"1 projects were not imported: projects are kept in the cloud.", "1 chats were imported without their project."}) {
		t.Fatalf("a restore to the device: %d chats, %+v", len(result.threads), result.done)
	}
}

// sameForm is a thread as both destinations keep it: the cloud stores an
// image under a blob and names it anew, a device keeps its bytes.
func sameForm(t *testing.T, fake *fakeSync, thread Thread) string {
	t.Helper()
	thread = thread.clone()
	for i := range thread.Messages {
		for j := range thread.Messages[i].Attachments {
			a := &thread.Messages[i].Attachments[j]
			if a.EncryptionKey != "" {
				a.Base64 = base64.StdEncoding.EncodeToString(blobOf(t, fake, *a))
			}
			if a.isImage() {
				a.ID = ""
			}
			a.EncryptionKey = ""
		}
	}
	thread.ID, thread.ProjectID, thread.UpdatedAt = "", nil, ""
	return string(raw(thread))
}

// Exports of other apps import into the same threads in the cloud and on
// the device.
func TestExportsImportAlikeToCloudAndDevice(t *testing.T) {
	documentLeftOut := []string{"1 documents were left out: the archive holds no text for them."}
	for _, tc := range []struct {
		fixture           string
		chats             int
		onDevice, inCloud []string
		check             func(t *testing.T, threads map[string]Thread)
	}{
		{"chatgpt-export.zip", 2, documentLeftOut, documentLeftOut, func(t *testing.T, threads map[string]Thread) {
			branch := threads["Branching"]
			if len(branch.Messages) != 2 || branch.Messages[1].Content != "The second one." || branch.Messages[1].Timeline[0].Content != "Compare the two sizes" {
				t.Fatalf("not the current branch: %s", raw(branch.Messages))
			}
			photo := threads["Photo question"].Messages[0]
			if photo.Content != "What is in this picture?" || len(photo.Attachments) != 1 || photo.Attachments[0].FileName != "cat.png" || !strings.HasPrefix(string(base64Bytes(t, photo.Attachments[0].Base64)), "\x89PNG") {
				t.Fatalf("the photo was lost: %s", raw(photo))
			}
		}},
		{"claude-export.zip", 1, []string{"1 projects were not imported: projects are kept in the cloud."}, []string{}, func(t *testing.T, threads map[string]Thread) {
			messages := threads["Reading a spec"].Messages
			if len(messages) != 2 || messages[0].Attachments[0].TextContent != "The contents" || messages[0].Attachments[1].FileName != "diagram.png" || messages[1].Timeline[0].Content != "Reading carefully" {
				t.Fatalf("claude chat: %s", raw(messages))
			}
		}},
		{"tinfoil-export.zip", 2, []string{}, []string{}, func(t *testing.T, threads map[string]Thread) {
			trip := threads["Trip plans"].Messages
			if len(trip) != 2 || trip[0].Attachments[0].ID != "tf-photo" || trip[0].Attachments[1].TextContent != "# Packing list" || trip[1].Timeline[0].Content != "It is sunny" || trip[0].CreatedAt != "2026-04-01T08:00:00Z" {
				t.Fatalf("tinfoil chat: %s", raw(trip))
			}
			if len(threads["Sketches"].Messages[0].Attachments) != 1 {
				t.Fatal("the sketch was lost")
			}
		}},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			h, rows, key := testChatHarness(t, nil)
			fake := newFakeSync(t, h)
			archive := fixture(t, tc.fixture)
			onDevice := importOf(t, h, "alice", archive, tc.fixture, object{"target": "device"})
			inCloud := importOf(t, h, "alice", archive, tc.fixture, object{"target": "cloud", "key": key.base64()})
			if len(onDevice.threads) != tc.chats || inCloud.threads != nil || onDevice.done.JobID != nil || inCloud.done.JobID == nil || !reflect.DeepEqual(onDevice.done.Warnings, tc.onDevice) || !reflect.DeepEqual(inCloud.done.Warnings, tc.inCloud) {
				t.Fatalf("device %d chats %+v, cloud %+v", len(onDevice.threads), onDevice.done, inCloud.done)
			}
			cloud := storedChats(rows, "alice")
			byTitle := map[string]Thread{}
			for _, thread := range onDevice.threads {
				byTitle[thread.Title] = thread
				stored, ok := cloud[thread.Title]
				if !ok || !rowIDPattern.MatchString(thread.ID) || sameForm(t, fake, stored) != sameForm(t, fake, thread) {
					t.Fatalf("the cloud and the device differ:\n%s\n%s", sameForm(t, fake, stored), sameForm(t, fake, thread))
				}
			}
			if len(cloud) != tc.chats {
				t.Fatalf("cloud chats: %d", len(cloud))
			}
			tc.check(t, byTitle)
		})
	}
}

// Claude's projects go to the cloud with their documents' text; on the
// device alone they cannot be imported.
func TestClaudeProjectsRequireTheCloud(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	newFakeSync(t, h)
	projects := fixture(t, "claude-projects.json")
	w := archiveRoute(h, "/v1/import", "alice", optionsPart(object{"target": "device"}), formPart{name: "file", filename: "projects.json", data: projects})
	if w.Code != 400 || !strings.Contains(w.Body.String(), "PROJECTS_REQUIRE_CLOUD") {
		t.Fatalf("projects imported to the device: HTTP %d %s", w.Code, w.Body.String())
	}
	result := importOf(t, h, "alice", projects, "projects.json", object{"target": "cloud", "key": key.base64()})
	stored, _ := rows.list(context.Background(), &principal{ID: "alice"}, key, "project", "", "", 0)
	if result.done.JobID == nil || len(stored.Rows) != 1 {
		t.Fatalf("projects: %d stored, done %+v", len(stored.Rows), result.done)
	}
	project := readProject(&stored.Rows[0])
	docs, _ := h.projectDocuments(context.Background(), &principal{ID: "alice"}, key, project.ID)
	if project.Name != "Release Notes" || project.SystemInstructions != "Write concise release notes." || len(docs) != 1 || docs[0].Filename != "style-guide.md" || docs[0].Content != "# Style guide" {
		t.Fatalf("project: %+v %+v", project, docs)
	}
	// Claude's newer exports hold each project in a file of its own.
	var split []json.RawMessage
	json.Unmarshal(projects, &split)
	one := importOf(t, h, "bob", split[0], "release-notes.json", object{"target": "cloud", "key": key.base64()})
	if stored, _ := rows.list(context.Background(), &principal{ID: "bob"}, key, "project", "", "", 0); one.done.JobID == nil || len(stored.Rows) != 1 {
		t.Fatalf("a project file of its own was not imported: %+v", one.done)
	}
	full := importOf(t, h, "alice", fixture(t, "claude-export.zip"), "claude.zip", object{"target": "device"})
	if len(full.threads) != 1 || !reflect.DeepEqual(full.done.Warnings, []string{"1 projects were not imported: projects are kept in the cloud."}) {
		t.Fatalf("a full export to the device: %+v", full.done)
	}
}

// The conversations export is the one the web app wrote for the same chats.
func TestConversationsExportMatchesTheWebApp(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	fake := newFakeSync(t, h)
	var threads struct {
		Device []json.RawMessage `json:"device"`
		Stored Thread            `json:"stored"`
	}
	if err := json.Unmarshal(fixture(t, "conversations-threads.json"), &threads); err != nil {
		t.Fatal(err)
	}
	// The stored chat keeps its image and document in blobs.
	stored := threads.Stored
	for i := range stored.Messages {
		for j := range stored.Messages[i].Attachments {
			a := &stored.Messages[i].Attachments[j]
			a.EncryptionKey = "key-" + a.ID
			fake.blobs[a.ID] = fakeBlob{chat: stored.ID, key: a.EncryptionKey, data: decodedOr(a.Base64, raw(documentBlob{TextContent: a.TextContent}))}
			a.Base64, a.TextContent = "", ""
		}
	}
	project := valueOr(stored.ProjectID, "")
	rows.rows[rowMapKey(&principal{ID: "alice"}, "chat", stored.ID)] = storedRow{ID: stored.ID, ETag: "1", Data: raw(stored), project: &project, projectSet: true}
	parts := []formPart{optionsPart(object{"format": "conversations", "key": key.base64()})}
	for _, thread := range threads.Device {
		parts = append(parts, formPart{name: "thread", data: thread})
	}
	w := archiveRoute(h, "/v1/export", "alice", parts...)
	if w.Code != 200 {
		t.Fatalf("export: HTTP %d: %s", w.Code, w.Body.String())
	}
	got, gotNames := unzipped(t, w.Body.Bytes())
	want, wantNames := unzipped(t, fixture(t, "web-conversations-export.zip"))
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("entries %v, want %v", gotNames, wantNames)
	}
	for _, name := range wantNames {
		gotEntry, wantEntry := got[name], want[name]
		if name == manifestPath {
			var gotManifest, wantManifest conversationsManifest
			json.Unmarshal(gotEntry, &gotManifest)
			json.Unmarshal(wantEntry, &wantManifest)
			if _, err := time.Parse(time.RFC3339, gotManifest.CreatedAt); err != nil {
				t.Fatalf("manifest time: %q", gotManifest.CreatedAt)
			}
			gotManifest.CreatedAt, wantManifest.CreatedAt = "", ""
			gotEntry, wantEntry = raw(gotManifest), raw(wantManifest)
		}
		if !bytes.Equal(gotEntry, wantEntry) {
			t.Fatalf("%s differs from the web app's:\n%s\nwant\n%s", name, gotEntry, wantEntry)
		}
	}
}

// decodedOr is base64 text decoded, or fallback when there is none.
func decodedOr(encoded string, fallback []byte) []byte {
	if encoded == "" {
		return fallback
	}
	b, _ := base64.StdEncoding.DecodeString(encoded)
	return b
}

// A signed-out visitor exports the chats it sends, and nothing is read
// from sync for it.
func TestSignedOutExportHoldsOnlyItsChats(t *testing.T) {
	h, rows, key := testChatHarness(t, nil)
	thread := object{"id": "local-chat", "title": "Tab chat", "messages": []any{object{"id": "m", "role": "user", "content": "hi"}}}
	if w := archiveRoute(h, "/v1/export", "", optionsPart(object{"format": "native", "key": key.base64()}), threadPart(thread)); w.Code != 401 {
		t.Fatalf("a signed-out export read the cloud: HTTP %d", w.Code)
	}
	w := archiveRoute(h, "/v1/export", "", optionsPart(object{"format": "native"}), threadPart(thread))
	if w.Code != 200 {
		t.Fatalf("export: HTTP %d %s", w.Code, w.Body.String())
	}
	files, _ := unzipped(t, w.Body.Bytes())
	var manifest backupManifest
	json.Unmarshal(files[manifestPath], &manifest)
	if manifest.Counts != (backupCounts{LocalChats: 1, Files: 2}) || files[backupEntityPath("local_chats", "local-chat")] == nil || rows.calls != 0 {
		t.Fatalf("manifest %+v after %d store calls", manifest.Counts, rows.calls)
	}
}

// Archives share the scratch space: a request that could never fit is
// refused, one that fits waits for space, and one that waits too long is
// told to retry.
func TestArchivesWaitForScratchSpace(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	h.scratch = newScratchSpace(t.TempDir(), 64<<10)
	thread := threadPart(object{"id": "local-chat", "title": "Tab chat", "messages": []any{object{"id": "m", "role": "user", "content": strings.Repeat("x", 8<<10)}}})
	if w := archiveRoute(h, "/v1/import", "alice", optionsPart(object{"target": "device"}), formPart{name: "file", filename: "big.json", data: bytes.Repeat([]byte(" "), 40<<10)}); w.Code != 413 || !strings.Contains(w.Body.String(), "ARCHIVE_TOO_LARGE") {
		t.Fatalf("an import that cannot fit: HTTP %d %s", w.Code, w.Body.String())
	}
	held, err := h.scratch.lease(context.Background(), 60<<10)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder)
	go func() {
		done <- archiveRoute(h, "/v1/export", "", optionsPart(object{"format": "conversations"}), thread)
	}()
	for h.scratch.waiting.Load() == 0 {
		select {
		case w := <-done:
			t.Fatalf("an export ran without space: HTTP %d", w.Code)
		case <-time.After(time.Millisecond):
		}
	}
	held.release()
	if w := <-done; w.Code != 200 {
		t.Fatalf("the waiting export: HTTP %d %s", w.Code, w.Body.String())
	}
	held, _ = h.scratch.lease(context.Background(), 60<<10)
	h.scratch.wait = 10 * time.Millisecond
	if w := archiveRoute(h, "/v1/export", "", optionsPart(object{"format": "conversations"}), thread); w.Code != 503 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), "TEMP_SPACE_BUSY") {
		t.Fatalf("an export that waited too long: HTTP %d %s", w.Code, w.Body.String())
	}
	held.release()
	// The export's conversations outgrow the request it leased space for.
	held, _ = h.scratch.lease(context.Background(), 52<<10)
	defer held.release()
	if w := archiveRoute(h, "/v1/export", "", optionsPart(object{"format": "conversations"}), thread); w.Code != 503 || !strings.Contains(w.Body.String(), "TEMP_SPACE_BUSY") {
		t.Fatalf("an export wrote past the space it holds: HTTP %d %s", w.Code, w.Body.String())
	}
}
