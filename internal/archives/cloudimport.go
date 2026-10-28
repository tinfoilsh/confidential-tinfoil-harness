package archives

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
)

// A cloud import stores what an archive put in the cloud: its projects,
// then their documents, then its chats with their images, in the
// background while the client polls. Every row it stores records where it
// came from, under an id derived from the source, so the same import run
// again skips what it stored and takes the next id for an entity when
// another row holds the one it maps to.

const (
	// ImportBudget bounds one import. Each entity costs a few round trips
	// to the control plane, so an archive at the import's limits takes
	// well over an hour; one that takes longer fails as timed out and keeps
	// what it stored, which the import run again skips.
	ImportBudget = 2 * time.Hour
	// ImportRetention is how long an ended import still answers for the
	// caller, so a client polling for its end sees it.
	ImportRetention = 5 * time.Minute
	// maxImportGenerations bounds the ids tried for one entity whose
	// mapped rows other rows hold.
	maxImportGenerations = 100
	// maxImportNotes bounds the warnings, and the errors, a report keeps.
	maxImportNotes = 100
	// importNotifyTimeout bounds telling the control plane how an import
	// ended, which outlives the job.
	importNotifyTimeout = 30 * time.Second
	// The derivations of an imported row's id, of its write's nonce and of
	// its images' identities are a stored format: the rows an earlier
	// import stored, and the attachments under them, are named by them.
	importIDInfo    = "tinfoil-restore-id-v1"
	importWriteInfo = "tinfoil-restore-write-v1"
	importImageInfo = "import-att"
	// An imported row's id is a UUID-shaped slice of a digest.
	importIDHexLen = 32
	// importNonceBytes is the length of a write's nonce before hex.
	importNonceBytes = 16
	// Why an import ended before it was done, as the control plane's
	// email and the client name it.
	importFailureTimeout       = "timeout"
	importFailureAuthorization = "authorization_failed"
	importFailureWorker        = "worker_failed"
	importFailureInternal      = "internal"
)

var (
	errImportNotFound = api.Err(404, "THREAD_NOT_FOUND", "The import job was not found")
	errImportRunning  = api.Err(409, "RUN_IN_FLIGHT", "Another import is still running for this account")
)

// importStates are a job's states as a client reads them.
var importStates = map[jobs.State]string{jobs.Running: "running", jobs.Complete: "completed", jobs.Failed: "failed"}

// importFailureMessages say in the report why an import ended early; the
// control plane's email carries the longer explanation.
var importFailureMessages = map[string]string{
	importFailureTimeout:       "import timed out",
	importFailureAuthorization: "Tinfoil could not authorize access to cloud storage for the import; please retry from a signed-in device",
	importFailureWorker:        "the import worker stopped unexpectedly; please retry the import",
	importFailureInternal:      "import failed",
}

// ImportKindCounts is what an import did with one kind of entity: stored,
// found stored by an earlier run, failed, or blocked by its project's
// failure.
type ImportKindCounts struct {
	Imported int `json:"imported"`
	Skipped  int `json:"skipped"`
	Failed   int `json:"failed"`
	Blocked  int `json:"blocked"`
}

// ImportReport is where a cloud import stands, as the client polls it.
type ImportReport struct {
	Phase         string                      `json:"phase,omitempty"`
	Imported      int                         `json:"imported"`
	Failed        int                         `json:"failed"`
	Total         int                         `json:"total"`
	Counts        map[string]ImportKindCounts `json:"counts,omitempty"`
	Warnings      []string                    `json:"warnings,omitempty"`
	Errors        []string                    `json:"errors,omitempty"`
	FailureReason string                      `json:"failureReason,omitempty"`
}

// importStatusView is an import job as its client polls it.
type importStatusView struct {
	JobID  string `json:"jobId"`
	Status string `json:"status"`
	ImportReport
}

func (ar *Service) ImportStatus(_ context.Context, p *auth.Principal, _ auth.ContentKey, in *importStatusRequest) (any, error) {
	job := ar.Imports.Get(p.ID)
	if job == nil || job.ID() != in.JobID {
		return nil, errImportNotFound
	}
	status := job.Status()
	return importStatusView{JobID: job.ID(), Status: importStates[status.State], ImportReport: status.Report}, nil
}

// startImport runs the job that stores a finished cloud archive, which the
// job owns from here, and is the job's id. A caller has one import at a
// time; one that ended gives way to the next.
func (ar *Service) startImport(ctx context.Context, p *auth.Principal, key auth.ContentKey, archive *scratchFile, source string) (string, error) {
	defer clear(key[:])
	// The job outlives the request, so it takes copies of the key and of
	// who the caller is, and the archive leaves the request's lease.
	imp := &cloudImport{ar: ar, p: &auth.Principal{ID: p.ID, JWT: p.JWT}, key: key, file: archive, source: source, named: make(chan string, 1)}
	archive.lease.detach(archive)
	job, started := ar.Imports.Restart(ctx, p.ID, imp.run)
	if !started {
		archive.release()
		clear(imp.key[:])
		return "", errImportRunning
	}
	imp.named <- job.ID()
	return job.ID(), nil
}

// cloudImport is one import's job: who it is for, under which key, the
// archive it reads, and the report it keeps up to date.
type cloudImport struct {
	ar     *Service
	p      *auth.Principal
	key    auth.ContentKey
	file   *scratchFile
	source string
	// named carries the job's id, known once it has started, which the
	// control plane is told at the end.
	named chan string
	id    string

	entries map[string]*zip.File
	report  ImportReport
	publish func(ImportReport)
}

func (j *cloudImport) run(ctx context.Context, report func(ImportReport)) (out ImportReport, err error) {
	defer j.file.release()
	defer clear(j.key[:])
	j.id, j.publish = <-j.named, report
	defer func() {
		if recovered := recover(); recovered != nil {
			err = api.Rescued("a cloud import", recovered)
			j.report.FailureReason = importFailureWorker
		}
		if err != nil {
			j.note(&j.report.Errors, importFailureMessages[j.report.FailureReason])
		}
		out = j.snapshot()
		j.tell(ctx, err)
	}()
	zr, err := zip.NewReader(j.file, j.file.Size())
	if err != nil {
		return out, j.fail(importFailureInternal, err)
	}
	j.entries = make(map[string]*zip.File, len(zr.File))
	for _, f := range zr.File {
		j.entries[f.Name] = f
	}
	var manifest cloudManifest
	if err := j.entity(ManifestPath, &manifest); err != nil {
		return out, j.fail(importFailureInternal, err)
	}
	return out, j.store(ctx, manifest.Entities)
}

// store stores every entity, projects first so that documents and chats
// can name theirs. An entity's failure counts against it alone, except
// one that every later entity would meet too: the job's time running
// out, or the caller's credential refused.
func (j *cloudImport) store(ctx context.Context, entities []cloudEntity) error {
	j.report.Total = len(entities)
	j.report.Counts = map[string]ImportKindCounts{"project": {}, "document": {}, "chat": {}}
	projects, failedProjects := map[string]string{}, map[string]bool{}
	j.phase("projects")
	for _, entity := range entities {
		if entity.Kind != "project" {
			continue
		}
		id, outcome, err := j.restore(ctx, entity, "", func(id string, origin *domain.ImportOrigin) (*domain.StoredRow, []string, error) {
			var project cloudProject
			if err := j.entity(entity.Path, &project); err != nil {
				return nil, nil, err
			}
			now := api.Timestamp()
			stored := domain.StoredProject{Project: domain.Project{ID: id, Name: project.Name, Description: project.Description, SystemInstructions: project.SystemInstructions, Color: domain.Present(project.Color), CreatedAt: now, UpdatedAt: now},
				RecordVersion: domain.RecordVersion, ImportedFrom: origin}
			return &domain.StoredRow{Data: api.Raw(stored)}, nil, nil
		})
		if err != nil {
			failedProjects[entity.SourceID] = true
		} else {
			projects[entity.SourceID] = id
		}
		if stop := j.count(ctx, entity.Kind, outcome, err); stop != nil {
			return stop
		}
	}
	j.phase("documents")
	for _, entity := range entities {
		if entity.Kind != "document" {
			continue
		}
		projectID, ok := projects[entity.ProjectSourceID]
		if !ok || failedProjects[entity.ProjectSourceID] {
			j.block(entity.Kind)
			continue
		}
		_, outcome, err := j.restore(ctx, entity, projectID, func(id string, origin *domain.ImportOrigin) (*domain.StoredRow, []string, error) {
			var doc cloudDocument
			if err := j.entity(entity.Path, &doc); err != nil {
				return nil, nil, err
			}
			now := api.Timestamp()
			stored := domain.StoredDocument{ProjectDocument: domain.ProjectDocument{ID: id, ProjectID: projectID, Filename: doc.Filename, ContentType: doc.ContentType, SizeBytes: doc.SizeBytes, Content: doc.Content, CreatedAt: now, UpdatedAt: now},
				RecordVersion: domain.RecordVersion, ImportedFrom: origin}
			return &domain.StoredRow{Data: api.Raw(stored), Project: &projectID, ProjectSet: true}, nil, nil
		})
		if stop := j.count(ctx, entity.Kind, outcome, err); stop != nil {
			return stop
		}
	}
	j.phase("chats")
	for _, entity := range entities {
		if entity.Kind != "chat" {
			continue
		}
		projectID := ""
		if entity.ProjectSourceID != "" {
			var ok bool
			if projectID, ok = projects[entity.ProjectSourceID]; !ok || failedProjects[entity.ProjectSourceID] {
				j.block(entity.Kind)
				continue
			}
		}
		_, outcome, err := j.restore(ctx, entity, "", func(id string, origin *domain.ImportOrigin) (*domain.StoredRow, []string, error) {
			var chat cloudChat
			if err := j.entity(entity.Path, &chat); err != nil {
				return nil, nil, err
			}
			thread, images := j.chat(ctx, id, projectID, origin, &chat)
			row := &domain.StoredRow{Data: api.Raw(thread), ProjectSet: true}
			if projectID != "" {
				row.Project = &projectID
			}
			return row, images, nil
		})
		if stop := j.count(ctx, entity.Kind, outcome, err); stop != nil {
			return stop
		}
	}
	j.phase("complete")
	return nil
}

// restored is what became of one entity: stored now, or found stored by
// an earlier run of the same import.
type restored int

const (
	storedNow restored = iota
	storedBefore
)

// occupant is what holds the row an entity maps to: nothing, the entity
// itself as an earlier run of the import stored it, or another row.
type occupant int

const (
	vacant occupant = iota
	sameImport
	anotherRow
)

// restore stores one entity under the first of its ids whose row is
// vacant, and is that id without the project a document's row is filed
// under. A row the same import stored before is found rather than stored
// again; one another row holds is passed over. build makes the row for an
// id, and names the attachments it stored for it, which are retired when
// the row is not stored after all. A write whose answer was lost is
// settled by reading the row back.
func (j *cloudImport) restore(ctx context.Context, entity cloudEntity, projectID string, build func(id string, origin *domain.ImportOrigin) (*domain.StoredRow, []string, error)) (string, restored, error) {
	scope := entity.Kind
	if entity.Kind == "document" {
		scope = "project_document"
	}
	for generation := 0; generation < maxImportGenerations; generation++ {
		id := importedID(j.p.ID, j.source, entity.Kind, entity.SourceID, generation)
		rowID := id
		if entity.Kind == "document" {
			rowID = projectID + "/" + id
		}
		origin := &domain.ImportOrigin{Format: cloudImportFormat, Source: j.source, Kind: entity.Kind, SourceID: entity.SourceID, Generation: generation}
		switch holder, err := j.inspect(ctx, scope, rowID, origin); {
		case err != nil:
			return "", 0, err
		case holder == sameImport:
			return id, storedBefore, nil
		case holder == anotherRow:
			continue
		}
		row, attachments, err := build(id, origin)
		if err != nil {
			return "", 0, err
		}
		row.ID = rowID
		_, err = j.ar.Store.Rows.Push(ctx, j.p, j.key, scope, row, importedNonce(j.p.ID, j.source, entity.Kind, entity.SourceID, generation))
		if err == nil {
			return id, storedNow, nil
		}
		holder, inspectErr := j.inspect(ctx, scope, rowID, origin)
		if inspectErr != nil {
			return "", 0, err
		}
		if holder == sameImport {
			if conflicted(err) {
				return id, storedBefore, nil
			}
			return id, storedNow, nil
		}
		j.ar.Store.RetireAttachments(ctx, j.p, attachments)
		if conflicted(err) {
			continue
		}
		return "", 0, err
	}
	return "", 0, errors.New("every id for the entity is taken")
}

// inspect is what holds a row: a row sealed under another key is another
// row, and one that cannot be read in time is not known either way.
func (j *cloudImport) inspect(ctx context.Context, scope, id string, origin *domain.ImportOrigin) (occupant, error) {
	row, err := j.ar.Store.Rows.Load(ctx, j.p, j.key, scope, id)
	if err != nil {
		switch e := api.AsError(err); {
		case e.HTTP == http.StatusNotFound:
			return vacant, nil
		case e.Code == "KEY_MISMATCH":
			return anotherRow, nil
		}
		return 0, err
	}
	if found := domain.ReadImportOrigin(row); found != nil && *found == *origin {
		return sameImport, nil
	}
	return anotherRow, nil
}

// conflicted is whether a create met a row already at its id.
func conflicted(err error) bool { return api.AsError(err).Code == "REVISION_CONFLICT" }

// importedID is the id an entity is stored under: a UUID-shaped digest of
// who imports it, the import's source, the entity and which copy this is.
func importedID(user, source, kind, sourceID string, generation int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s\x00%s\x00%s\x00%d", importIDInfo, user, source, kind, sourceID, generation))
	h := hex.EncodeToString(sum[:])[:importIDHexLen]
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:])
}

// importedNonce is the nonce the entity's write carries, the same each
// time the import runs, so a write whose answer was lost is matched.
func importedNonce(user, source, kind, sourceID string, generation int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s\x00%s\x00%s\x00%d", importWriteInfo, user, source, kind, sourceID, generation))
	return hex.EncodeToString(sum[:importNonceBytes])
}

// importedImage names an image upload for the attachment store, which
// derives the attachment's id and key from it: the chat, the blob and
// which upload of the chat's this is.
func importedImage(chatID, blobPath string, index int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s:%s:%s:%d", importImageInfo, chatID, blobPath, index))
	return hex.EncodeToString(sum[:importNonceBytes])
}

// chat is a prepared thread as the import stores it: its images stored
// as attachments of the chat, under keys the chat embeds, and documents
// without any content left out. It names the attachments stored.
func (j *cloudImport) chat(ctx context.Context, id, projectID string, origin *domain.ImportOrigin, chat *cloudChat) (*domain.Thread, []string) {
	t := chat.Thread
	t.ID, t.ImportedFrom, t.RecordVersion, t.ProjectID = id, origin, domain.RecordVersion, nil
	if projectID != "" {
		t.ProjectID = &projectID
	}
	t.Messages = make([]domain.Message, 0, len(chat.Messages))
	var stored []string
	uploads := 0
	for _, message := range chat.Messages {
		m := message.Message
		m.Attachments = nil
		for _, attachment := range message.Attachments {
			a := attachment.Attachment
			if a.IsImage() {
				data, err := j.blob(attachment.ArchivePath)
				if err != nil {
					j.warn("image attachment blob missing")
					continue
				}
				kind := http.DetectContentType(data)
				if !slices.Contains(sniffedImages, kind) {
					j.warn("image attachment type rejected")
					continue
				}
				attID, attKey, err := j.ar.Store.PutAttachment(ctx, j.p, id, importedImage(id, attachment.ArchivePath, uploads), data)
				uploads++
				if err != nil {
					j.warn("image attachment upload failed")
					continue
				}
				stored = append(stored, attID)
				a.ID, a.EncryptionKey, a.MimeType, a.Base64 = attID, attKey, kind, ""
			} else if !documentHasContent(&a) {
				j.warn("document attachment without content skipped")
				continue
			}
			m.Attachments = append(m.Attachments, a)
		}
		t.Messages = append(t.Messages, m)
	}
	return &t, stored
}

// documentHasContent is whether a document attachment holds any text, or
// a page with text or an image.
func documentHasContent(a *domain.Attachment) bool {
	if strings.TrimSpace(a.TextContent) != "" {
		return true
	}
	for _, page := range a.Pages {
		if strings.TrimSpace(page.Text) != "" || strings.TrimSpace(page.Image) != "" {
			return true
		}
	}
	return false
}

// entity reads one of the archive's JSON entries.
func (j *cloudImport) entity(name string, v any) error {
	f := j.entries[name]
	if f == nil {
		return fmt.Errorf("the archive holds no %s", name)
	}
	data, err := readEntry(f, maxArchiveJSONEntry)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// blob reads one of the archive's image entries.
func (j *cloudImport) blob(name string) ([]byte, error) {
	f := j.entries[name]
	if f == nil {
		return nil, fmt.Errorf("the archive holds no %s", name)
	}
	return readEntry(f, maxArchiveImage)
}

// count records what became of an entity and publishes the report. It is
// the failure that ends the import when the entity's is one every later
// entity would meet too.
func (j *cloudImport) count(ctx context.Context, kind string, outcome restored, err error) error {
	counts := j.report.Counts[kind]
	switch {
	case err != nil:
		counts.Failed++
		j.note(&j.report.Errors, kind+" failed")
	case outcome == storedBefore:
		counts.Skipped++
	default:
		counts.Imported++
	}
	j.report.Counts[kind] = counts
	j.progress()
	if err == nil {
		return nil
	}
	switch {
	case ctx.Err() != nil:
		return j.fail(importFailureTimeout, ctx.Err())
	case api.AsError(err).HTTP == http.StatusUnauthorized:
		return j.fail(importFailureAuthorization, err)
	}
	return nil
}

// block records an entity whose project was not stored.
func (j *cloudImport) block(kind string) {
	counts := j.report.Counts[kind]
	counts.Blocked++
	j.report.Counts[kind] = counts
	j.progress()
}

func (j *cloudImport) phase(phase string) {
	j.report.Phase = phase
	j.progress()
}

func (j *cloudImport) warn(text string) {
	j.note(&j.report.Warnings, text)
	j.progress()
}

// note keeps text in a report's list, up to the list's bound.
func (j *cloudImport) note(list *[]string, text string) {
	if len(*list) < maxImportNotes {
		*list = append(*list, text)
	}
}

// fail records why the import ends before it is done, and is the error
// that ends it.
func (j *cloudImport) fail(reason string, err error) error {
	j.report.FailureReason = reason
	return err
}

// progress publishes the report as it stands, with the totals brought up
// to date.
func (j *cloudImport) progress() {
	j.report.Imported, j.report.Failed = 0, 0
	for _, counts := range j.report.Counts {
		j.report.Imported += counts.Imported
		j.report.Failed += counts.Failed
	}
	j.publish(j.snapshot())
}

// snapshot is a copy of the report that later changes leave as it is.
func (j *cloudImport) snapshot() ImportReport {
	out := j.report
	out.Counts, out.Warnings, out.Errors = maps.Clone(j.report.Counts), slices.Clone(j.report.Warnings), slices.Clone(j.report.Errors)
	return out
}

// tell has the control plane tell the user how the import ended. The
// report already says, so a notification that fails is let go.
func (j *cloudImport) tell(ctx context.Context, err error) {
	notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), importNotifyTimeout)
	defer cancel()
	outcome := controlplane.ImportOutcome{ClerkUserID: j.p.ID, JobID: j.id, Source: cloudImportSource, Status: controlplane.ImportOutcomeCompleted, Imported: j.report.Imported, Failed: j.report.Failed}
	if err != nil {
		outcome.Status, outcome.FailureReason = controlplane.ImportOutcomeFailed, j.report.FailureReason
	}
	_ = j.ar.Store.Controlplane.NotifyImportOutcome(notifyCtx, outcome)
}
