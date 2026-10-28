package archives

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"image"
	"image/png"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets/bucketstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane/cpstub"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

const (
	testBucket = "test-attachments-bucket"
	testSecret = "test-enclave-secret"
	testSource = "backup-1"
)

// importHarness is an archives service whose store seals rows at a stub
// of the control plane and holds attachments in a stub of buckets, for
// one caller whose content key is the registered one.
type importHarness struct {
	cp           *cpstub.Store
	bk           *bucketstub.Store
	cpURL, bkURL string
	ar           *Service
	p            *auth.Principal
	key          auth.ContentKey
	kid          string
}

func newImportHarness(t *testing.T) *importHarness {
	t.Helper()
	cp := cpstub.New()
	cp.Secret = testSecret
	cpServer := httptest.NewServer(cp)
	t.Cleanup(cpServer.Close)
	bk := bucketstub.NewStore()
	mux := http.NewServeMux()
	mux.HandleFunc("/"+testBucket+"/{key}", bk.Handle)
	bkServer := httptest.NewServer(mux)
	t.Cleanup(bkServer.Close)
	var key auth.ContentKey
	rand.Read(key[:])
	kid := keyIDOf(t, key)
	cp.CurrentKeyID = kid
	h := &importHarness{cp: cp, bk: bk, cpURL: cpServer.URL, bkURL: bkServer.URL, p: &auth.Principal{ID: "user_abc", JWT: "clerk-abc"}, key: key, kid: kid}
	h.ar = &Service{Scratch: NewScratch(t.TempDir(), 1<<30), Imports: jobs.New[ImportReport](ImportBudget, ImportRetention)}
	h.useControlplane(cpServer.URL)
	return h
}

// keyIDOf is the id a content key is registered under.
func keyIDOf(t *testing.T, key auth.ContentKey) string {
	t.Helper()
	id, err := seal.DeriveKeyID(key[:])
	if err != nil {
		t.Fatal(err)
	}
	return seal.KeyIDHex(id)
}

// useControlplane points the store at a control plane, which a test may
// put a proxy in front of.
func (h *importHarness) useControlplane(cpURL string) {
	cpClient := controlplane.NewClient(cpURL, nil, controlplane.WithServiceSecret(testSecret))
	rows := &store.Rows{Controlplane: cpClient, Buckets: buckets.NewClient(h.bkURL, testBucket, nil)}
	h.ar.Store = &store.Store{Rows: rows, Controlplane: cpClient, Buckets: rows.Buckets}
}

// archive writes a cloud archive through the writer an import uses, in
// the harness's scratch space, and is the file that holds it.
func (h *importHarness) archive(t *testing.T, write func(c *cloudArchive) error) *scratchFile {
	t.Helper()
	lease, err := h.ar.Scratch.Lease(context.Background(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	file, err := lease.create()
	if err != nil {
		t.Fatal(err)
	}
	c := newCloudArchive(file, testSource)
	if err := write(c); err != nil {
		t.Fatal(err)
	}
	if err := c.finish(); err != nil {
		t.Fatal(err)
	}
	return file
}

// research is a cloud archive of a project with a document, a chat in the
// project with an image and two documents, one without any content, and a
// chat outside any project.
func research(t *testing.T) func(c *cloudArchive) error {
	t.Helper()
	return func(c *cloudArchive) error {
		if err := c.entity("project", "p1", "", cloudProject{Name: "Research", Description: "Notes", SystemInstructions: "Cite sources", Color: "teal"}); err != nil {
			return err
		}
		if err := c.entity("document", "p1/d1", "p1", cloudDocument{Filename: "paper.md", ContentType: "text/markdown", SizeBytes: 4096, Content: "Paper text"}); err != nil {
			return err
		}
		plan := chatOf("Plan", "look at this", domain.Attachment{ID: "img", Kind: "image", FileName: "chart.png", MimeType: "image/png", Description: "A chart", Base64: base64.StdEncoding.EncodeToString(pngBytes(t))},
			domain.Attachment{ID: "doc", Kind: "document", FileName: "notes.txt", TextContent: "The notes"},
			domain.Attachment{ID: "blank", Kind: "document", FileName: "blank.pdf", Pages: []domain.DocumentPage{{Page: 1, Text: " "}}})
		if err := c.chat("c1", "p1", &plan); err != nil {
			return err
		}
		aside := chatOf("Aside", "hello")
		return c.chat("c2", "", &aside)
	}
}

func chatOf(title, text string, attachments ...domain.Attachment) domain.Thread {
	return domain.Thread{Title: title, TitleState: "generated", CreatedAt: "2026-01-02T00:00:00Z", UpdatedAt: "2026-01-02T01:00:00Z", Model: "auto", WebSearchEnabled: true,
		Messages: []domain.Message{{ID: "m1", Role: "user", CreatedAt: "2026-01-02T00:00:00Z", Content: text, Attachments: attachments}}}
}

func pngBytes(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewGray(image.Rect(0, 0, 3, 3))); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// imported runs an import of the archive to its end, and is its status
// and the job's id.
func (h *importHarness) imported(t *testing.T, file *scratchFile) (jobs.Status[ImportReport], string) {
	t.Helper()
	id, err := h.ar.startImport(context.Background(), h.p, h.key, file, testSource)
	if err != nil {
		t.Fatalf("start the import: %v", err)
	}
	job := h.ar.Imports.Get(h.p.ID)
	if job == nil || job.ID() != id {
		t.Fatalf("the started job %s is not the caller's", id)
	}
	<-job.Done()
	return job.Status(), id
}

// opened is the control plane's copy of a row, opened under the key, or
// nil without one.
func (h *importHarness) opened(t *testing.T, scope, id string) *domain.StoredRow {
	t.Helper()
	h.cp.Mu.Lock()
	blob := h.cp.Blobs[cpstub.Key(scope, id)]
	h.cp.Mu.Unlock()
	if blob == nil {
		return nil
	}
	dec, err := seal.DecryptV2(blob.Body, []seal.Key{{Bytes: h.key[:], KeyIDHex: h.kid}}, seal.AAD{Scope: seal.Scope(scope), ID: id, ClerkUserID: h.p.ID})
	if err != nil {
		t.Fatalf("open %s/%s: %v", scope, id, err)
	}
	return &domain.StoredRow{ID: id, ETag: cpstub.FormatETag(blob.ETag), Data: dec.Plaintext, Project: blob.ProjectID, ProjectSet: blob.ProjectIDSet}
}

// rowIDs are the ids of the rows the control plane holds, by scope.
func (h *importHarness) rowIDs(scope string) []string {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	var ids []string
	for key := range h.cp.Blobs {
		if s, id, _ := strings.Cut(key, "/"); s == scope {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func (h *importHarness) indexed() map[string]string {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	indexed := map[string]string{}
	for id, chat := range h.cp.AttachmentIndex {
		indexed[id] = chat
	}
	return indexed
}

func (h *importHarness) outcomes() []controlplane.ImportOutcome {
	h.cp.Mu.Lock()
	defer h.cp.Mu.Unlock()
	return slices.Clone(h.cp.ImportOutcomes)
}

// sealed seeds a row at the control plane under key, as another writer
// stored it.
func (h *importHarness) sealed(t *testing.T, key auth.ContentKey, scope, id, plaintext string) {
	t.Helper()
	kid := keyIDOf(t, key)
	envelope, err := seal.Encrypt(key[:], []byte(plaintext), seal.AAD{KeyIDHex: kid, Scope: seal.Scope(scope), ID: id, ClerkUserID: h.p.ID})
	if err != nil {
		t.Fatal(err)
	}
	h.cp.SetBlob(scope, id, cpstub.Blob{ETag: 1, KeyID: kid, Body: envelope})
}

func TestACloudImportStoresProjectsDocumentsAndChatsWithTheirImages(t *testing.T) {
	h := newImportHarness(t)
	// The first write waits until the job has been seen running.
	gate, seen := make(chan struct{}), make(chan struct{})
	h.cp.BeforePutBlob = func(string, string) {
		h.cp.BeforePutBlob = nil
		close(seen)
		<-gate
	}
	id, err := h.ar.startImport(context.Background(), h.p, h.key, h.archive(t, research(t)), testSource)
	if err != nil {
		t.Fatalf("start the import: %v", err)
	}
	<-seen
	job := h.ar.Imports.Get(h.p.ID)
	if status := job.Status(); job.ID() != id || status.State != jobs.Running || status.Report.Phase != "projects" || status.Report.Total != 4 {
		t.Fatalf("the status while running = %+v", status)
	}
	close(gate)
	<-job.Done()

	status := job.Status()
	want := ImportReport{Phase: "complete", Imported: 4, Total: 4, Counts: map[string]ImportKindCounts{"project": {Imported: 1}, "document": {Imported: 1}, "chat": {Imported: 2}}, Warnings: []string{"document attachment without content skipped"}}
	if status.State != jobs.Complete || !reflect.DeepEqual(status.Report, want) {
		t.Fatalf("the import ended %s with %+v, want %+v", status.State, status.Report, want)
	}
	projectID := importedID(h.p.ID, testSource, "project", "p1", 0)
	documentID := projectID + "/" + importedID(h.p.ID, testSource, "document", "p1/d1", 0)
	planID, asideID := importedID(h.p.ID, testSource, "chat", "c1", 0), importedID(h.p.ID, testSource, "chat", "c2", 0)
	project, err := domain.ReadProject(h.opened(t, "project", projectID))
	if err != nil || project.Name != "Research" || project.SystemInstructions != "Cite sources" || domain.ValueOr(project.Color, "") != "teal" || project.CreatedAt == "" {
		t.Fatalf("the project = %+v, %v", project, err)
	}
	if origin := domain.ReadImportOrigin(h.opened(t, "project", projectID)); origin == nil || *origin != (domain.ImportOrigin{Format: cloudImportFormat, Source: testSource, Kind: "project", SourceID: "p1"}) {
		t.Fatalf("the project's origin = %+v", origin)
	}
	doc, err := domain.ReadProjectDocument(h.opened(t, "project_document", documentID), projectID)
	if err != nil || doc.Filename != "paper.md" || doc.Content != "Paper text" || doc.SizeBytes != 4096 || doc.ProjectID != projectID {
		t.Fatalf("the document = %+v, %v", doc, err)
	}
	planRow := h.opened(t, "chat", planID)
	plan, err := domain.ReadThreadRow(planRow)
	if err != nil || plan.Title != "Plan" || domain.ValueOr(plan.ProjectID, "") != projectID || plan.ImportedFrom == nil || plan.ImportedFrom.SourceID != "c1" || plan.RecordVersion != domain.RecordVersion {
		t.Fatalf("the chat in the project = %+v, %v", plan, err)
	}
	attachments := plan.Messages[0].Attachments
	if len(attachments) != 2 || !domain.EnclaveAttachmentID.MatchString(attachments[0].ID) || attachments[0].EncryptionKey == "" || attachments[0].MimeType != "image/png" || attachments[0].Base64 != "" || attachments[0].Description != "A chart" ||
		attachments[1].ID != "doc" || attachments[1].TextContent != "The notes" {
		t.Fatalf("the chat's attachments = %+v, want the image stored and named anew, the document inline, and the blank one left out", attachments)
	}
	item, ok := h.bk.Item(attachments[0].ID)
	attKey, _ := base64.StdEncoding.DecodeString(attachments[0].EncryptionKey)
	if !ok || !bytes.Equal(item.Value, pngBytes(t)) || len(item.EncryptionKeys) != 1 || !bytes.Equal(item.EncryptionKeys[0], attKey) || !reflect.DeepEqual(h.indexed(), map[string]string{attachments[0].ID: planID}) {
		t.Fatalf("the image is held as %+v (present=%v), indexed as %v", item, ok, h.indexed())
	}
	aside, err := domain.ReadThreadRow(h.opened(t, "chat", asideID))
	if err != nil || aside.ProjectID != nil || aside.Messages[0].Content != "hello" {
		t.Fatalf("the chat outside a project = %+v, %v", aside, err)
	}
	if told := h.outcomes(); len(told) != 1 || told[0] != (controlplane.ImportOutcome{ClerkUserID: h.p.ID, JobID: id, Source: cloudImportSource, Status: controlplane.ImportOutcomeCompleted, Imported: 4}) {
		t.Fatalf("the control plane was told %+v", told)
	}
	if !h.ar.Scratch.Empty() {
		t.Fatal("the ended job holds scratch space")
	}
}

func TestACloudImportRunAgainSkipsWhatItStoredAndPassesOverRowsInTheWay(t *testing.T) {
	h := newImportHarness(t)
	// Another key's row holds the project's first id, and a chat of the
	// caller's own holds the chat's.
	h.sealed(t, randomKey(), "project", importedID(h.p.ID, testSource, "project", "p1", 0), `{"name":"Theirs"}`)
	h.sealed(t, h.key, "chat", importedID(h.p.ID, testSource, "chat", "c1", 0), `{"title":"Mine","messages":[]}`)
	status, _ := h.imported(t, h.archive(t, research(t)))
	if status.State != jobs.Complete || status.Report.Imported != 4 || status.Report.Failed != 0 {
		t.Fatalf("the first run ended %s with %+v", status.State, status.Report)
	}
	projectID := importedID(h.p.ID, testSource, "project", "p1", 1)
	if project, err := domain.ReadProject(h.opened(t, "project", projectID)); err != nil || project.Name != "Research" {
		t.Fatalf("the project at its second id = %+v, %v", project, err)
	}
	if origin := domain.ReadImportOrigin(h.opened(t, "project", projectID)); origin == nil || origin.Generation != 1 {
		t.Fatalf("the project's origin = %+v, want the second generation", origin)
	}
	if doc, err := domain.ReadProjectDocument(h.opened(t, "project_document", projectID+"/"+importedID(h.p.ID, testSource, "document", "p1/d1", 0)), projectID); err != nil || doc.ProjectID != projectID {
		t.Fatalf("the document is not filed under the project's second id: %+v, %v", doc, err)
	}
	plan, err := domain.ReadThreadRow(h.opened(t, "chat", importedID(h.p.ID, testSource, "chat", "c1", 1)))
	if err != nil || plan.Title != "Plan" || domain.ValueOr(plan.ProjectID, "") != projectID || plan.ImportedFrom.Generation != 1 {
		t.Fatalf("the chat at its second id = %+v, %v", plan, err)
	}
	rows, chats, attachments := h.rowIDs("project"), h.rowIDs("chat"), h.bk.Tokens()
	if len(rows) != 2 || len(chats) != 3 || len(attachments) != 1 {
		t.Fatalf("after the first run: %d projects, %d chats, %d attachments", len(rows), len(chats), len(attachments))
	}
	// The rows are edited in between, as a user does; where they came
	// from survives the harness's own rewrites.
	if _, err := h.ar.Store.MutateProject(context.Background(), h.p, h.key, projectID, false, func(project *domain.Project) { project.Name = "Renamed" }); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ar.Store.MutateThread(context.Background(), h.p, h.key, importedID(h.p.ID, testSource, "chat", "c1", 1), func(thread *domain.Thread) error {
		thread.Title = "Renamed"
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	again, _ := h.imported(t, h.archive(t, research(t)))
	want := ImportReport{Phase: "complete", Total: 4, Counts: map[string]ImportKindCounts{"project": {Skipped: 1}, "document": {Skipped: 1}, "chat": {Skipped: 2}}}
	if again.State != jobs.Complete || !reflect.DeepEqual(again.Report, want) {
		t.Fatalf("the second run ended %s with %+v, want %+v", again.State, again.Report, want)
	}
	if !reflect.DeepEqual(h.rowIDs("project"), rows) || !reflect.DeepEqual(h.rowIDs("chat"), chats) || !reflect.DeepEqual(h.bk.Tokens(), attachments) {
		t.Fatal("the second run stored rows or attachments")
	}
}

func TestACloudImportBlocksWhatDependsOnAProjectItCouldNotStore(t *testing.T) {
	h := newImportHarness(t)
	h.cp.Mu.Lock()
	h.cp.PutBlobFailures["project/"+importedID(h.p.ID, testSource, "project", "p1", 0)] = controlplane.PutBlobMaxAttempts
	h.cp.Mu.Unlock()
	status, id := h.imported(t, h.archive(t, research(t)))
	want := ImportReport{Phase: "complete", Imported: 1, Failed: 1, Total: 4, Counts: map[string]ImportKindCounts{"project": {Failed: 1}, "document": {Blocked: 1}, "chat": {Imported: 1, Blocked: 1}}, Errors: []string{"project failed"}}
	if status.State != jobs.Complete || !reflect.DeepEqual(status.Report, want) {
		t.Fatalf("the import ended %s with %+v, want %+v", status.State, status.Report, want)
	}
	if projects, documents, chats := h.rowIDs("project"), h.rowIDs("project_document"), h.rowIDs("chat"); len(projects) != 0 || len(documents) != 0 || !reflect.DeepEqual(chats, []string{importedID(h.p.ID, testSource, "chat", "c2", 0)}) {
		t.Fatalf("stored %v, %v and %v, want only the chat outside the project", projects, documents, chats)
	}
	if told := h.outcomes(); len(told) != 1 || told[0] != (controlplane.ImportOutcome{ClerkUserID: h.p.ID, JobID: id, Source: cloudImportSource, Status: controlplane.ImportOutcomeCompleted, Imported: 1, Failed: 1}) {
		t.Fatalf("the control plane was told %+v", told)
	}
}

func TestACloudImportRetiresTheImagesOfAChatItCouldNotStore(t *testing.T) {
	chatID := func(h *importHarness) string { return importedID(h.p.ID, testSource, "chat", "c1", 0) }
	h := newImportHarness(t)
	h.cp.Mu.Lock()
	h.cp.PutBlobFailures["chat/"+chatID(h)] = controlplane.PutBlobMaxAttempts
	h.cp.Mu.Unlock()
	status, _ := h.imported(t, h.archive(t, research(t)))
	if status.Report.Counts["chat"] != (ImportKindCounts{Imported: 1, Failed: 1}) {
		t.Fatalf("the chats = %+v, want one failed", status.Report.Counts["chat"])
	}
	if indexed, held := h.indexed(), h.bk.Tokens(); len(indexed) != 0 || len(held) != 0 {
		t.Fatalf("the failed chat left %v indexed and %v in buckets", indexed, held)
	}

	// An image whose index entry the control plane could not drop keeps
	// its bytes, since a row may still name it.
	kept := newImportHarness(t)
	kept.cp.Mu.Lock()
	kept.cp.PutBlobFailures["chat/"+chatID(kept)] = controlplane.PutBlobMaxAttempts
	kept.cp.Mu.Unlock()
	kept.cp.CaptureHeaders = func(r *http.Request) {
		if aid, ok := strings.CutPrefix(r.URL.Path, "/api/sync/attachment-index/"); ok && r.Method == http.MethodPost {
			kept.cp.Mu.Lock()
			kept.cp.DeleteAttachmentIndexFailures[aid] = 1
			kept.cp.Mu.Unlock()
		}
	}
	kept.imported(t, kept.archive(t, research(t)))
	if indexed, held := kept.indexed(), kept.bk.Tokens(); len(indexed) != 1 || len(held) != 1 {
		t.Fatalf("after the index refused to drop the image: %v indexed and %v in buckets, want both kept", indexed, held)
	}
}

func TestACloudImportSettlesAWriteWhoseAnswerWasLost(t *testing.T) {
	h := newImportHarness(t)
	chatID := importedID(h.p.ID, testSource, "chat", "c1", 0)
	// The chat's write reaches the control plane; its first answer does not
	// come back.
	target, err := url.Parse(h.cpURL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorLog = log.New(io.Discard, "", 0)
	var lost atomic.Bool
	proxy.ModifyResponse = func(resp *http.Response) error {
		if resp.Request.Method == http.MethodPut && strings.HasSuffix(resp.Request.URL.Path, "/chat/"+chatID) && lost.CompareAndSwap(false, true) {
			return errors.New("the answer was lost")
		}
		return nil
	}
	proxyServer := httptest.NewServer(proxy)
	t.Cleanup(proxyServer.Close)
	h.useControlplane(proxyServer.URL)
	status, _ := h.imported(t, h.archive(t, research(t)))
	chats := status.Report.Counts["chat"]
	if !lost.Load() || status.State != jobs.Complete || chats.Imported+chats.Skipped != 2 || chats.Failed != 0 {
		t.Fatalf("the import ended %s with %+v (answer lost=%v), want the chat counted as stored", status.State, status.Report, lost.Load())
	}
	plan, err := domain.ReadThreadRow(h.opened(t, "chat", chatID))
	if err != nil || len(plan.Messages[0].Attachments) != 2 {
		t.Fatalf("the chat = %+v, %v", plan, err)
	}
	image := plan.Messages[0].Attachments[0]
	if !h.bk.Has(image.ID) || h.indexed()[image.ID] != chatID {
		t.Fatal("the stored chat's image was retired")
	}
}

func TestACloudImportOutOfTimeFailsAsTimedOut(t *testing.T) {
	h := newImportHarness(t)
	const budget = 20 * time.Millisecond
	h.ar.Imports = jobs.New[ImportReport](budget, ImportRetention)
	h.cp.BeforePutBlob = func(string, string) { time.Sleep(2 * budget) }
	status, id := h.imported(t, h.archive(t, research(t)))
	if status.State != jobs.Failed || status.Report.FailureReason != importFailureTimeout || status.Report.Phase != "projects" || !slices.Contains(status.Report.Errors, "import timed out") {
		t.Fatalf("the import ended %s with %+v, want failed as timed out", status.State, status.Report)
	}
	if told := h.outcomes(); len(told) != 1 || told[0].JobID != id || told[0].Status != controlplane.ImportOutcomeFailed || told[0].FailureReason != importFailureTimeout {
		t.Fatalf("the control plane was told %+v", told)
	}
	if !h.ar.Scratch.Empty() {
		t.Fatal("the ended job holds scratch space")
	}
}

func TestACallerHasOneCloudImportAtATime(t *testing.T) {
	h := newImportHarness(t)
	gate := make(chan struct{})
	h.cp.BeforePutBlob = func(string, string) { <-gate }
	first, err := h.ar.startImport(context.Background(), h.p, h.key, h.archive(t, research(t)), testSource)
	if err != nil {
		t.Fatal(err)
	}
	second := h.archive(t, research(t))
	if _, err := h.ar.startImport(context.Background(), h.p, h.key, second, testSource); api.AsError(err).Code != "RUN_IN_FLIGHT" {
		t.Fatalf("a second import while one runs: %v", err)
	}
	close(gate)
	<-h.ar.Imports.Get(h.p.ID).Done()
	if !h.ar.Scratch.Empty() {
		t.Fatal("the refused import's archive holds scratch space")
	}
	// Once the first has ended, the next replaces it.
	next, err := h.ar.startImport(context.Background(), h.p, h.key, h.archive(t, research(t)), testSource)
	if err != nil || next == first {
		t.Fatalf("an import after the last ended: %s, %v", next, err)
	}
	<-h.ar.Imports.Get(h.p.ID).Done()
	if job := h.ar.Imports.Get(h.p.ID); job.ID() != next || job.Status().Report.Counts["chat"].Skipped != 2 {
		t.Fatalf("the caller's job is %s with %+v, want the next import, which found the first's rows", job.ID(), job.Status().Report)
	}
}

func randomKey() auth.ContentKey {
	var key auth.ContentKey
	rand.Read(key[:])
	return key
}
