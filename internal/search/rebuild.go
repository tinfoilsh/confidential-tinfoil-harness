package search

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

// A rebuild reads every stored chat of a caller, a page at a time, and
// makes the index over again, in the background: an account may hold
// thousands of chats, each to be embedded. It keeps the caller's keys for
// as long as it runs, as the reseal does, since it must open chats and
// seal the index on their behalf; they live only in this process and
// only for the budget. A clean page checkpoints its cursor inside the
// sealed index, so a later rebuild picks up where this one stopped.

const (
	// RebuildBudget bounds one rebuild, and so how long it keeps the
	// caller's keys; a larger corpus resumes from the checkpoint.
	RebuildBudget = 20 * time.Minute
	// RebuildRetention is how long an ended rebuild still answers for the
	// caller, so a client polling for its end sees it rather than
	// starting another.
	RebuildRetention = 5 * time.Minute
	// rebuildPage is how many chats one page reads and embeds, within
	// the embedder's batch.
	rebuildPage = 16
)

var (
	// errRebuildIncomplete ends a rebuild that drained the chats but left
	// some out, so the caller asks again.
	errRebuildIncomplete = errors.New("search: the rebuild left chats out")
	errRebuildFailed     = api.Err(502, "SEARCH_INDEX_FAILED", "The search index could not be rebuilt")
)

// RebuildReport is where a rebuild stands: the chats indexed and the
// chats left out so far, how many the index holds, and whether it
// stopped short of the whole corpus.
type RebuildReport struct {
	Indexed      int
	Failed       int
	TotalIndexed int
	Partial      bool
}

// rebuildFingerprint is what a rebuild is for: the key it reads and
// seals under, and whether it only repairs embeddings. A request for
// another cannot join a job whose index it could not read.
type rebuildFingerprint struct {
	kid           string
	embeddingOnly bool
}

// startedRebuild is a caller's last rebuild and what it was for.
type startedRebuild struct {
	job         *jobs.Job[RebuildReport]
	fingerprint rebuildFingerprint
}

// Rebuild rebuilds the caller's index from their stored chats in the
// background, resuming a checkpoint when one exists, and reports whether
// indexing is still under way when the call returns. A rebuild that
// ended partial leaves indexing under way, since the next one resumes
// it; one that failed is reported once, and the next call starts over.
func (s *Service) Rebuild(ctx context.Context, p *auth.Principal, key auth.ContentKey) (indexing bool, err error) {
	defer clear(key[:])
	if !s.configured() {
		return false, errUnavailable
	}
	c, err := s.callerOf(p, key)
	if err != nil {
		return false, err
	}
	defer c.zero()
	job, err := s.startRebuild(ctx, c, key, false)
	if err != nil {
		return false, err
	}
	switch status := job.Status(); status.State {
	case jobs.Running:
		return true, nil
	case jobs.Complete:
		return status.Report.Partial, nil
	}
	return false, errRebuildFailed
}

// startEmbeddingRepair embeds, in the background, the entries indexed by
// keyword alone while the embedding service was down.
func (s *Service) startEmbeddingRepair(ctx context.Context, c caller, key auth.ContentKey) {
	_, _ = s.startRebuild(ctx, c, key, true)
}

// startRebuild is the caller's rebuild: the one running when it is for
// the same key, or any when only embeddings are to be repaired, which
// joins rather than interrupts broader work; the one ended cleanly when
// the index needs no repair; otherwise a new one, after the running one
// is stopped and has wound down, so two never write the same index.
func (s *Service) startRebuild(ctx context.Context, c caller, key auth.ContentKey, embeddingOnly bool) (*jobs.Job[RebuildReport], error) {
	owner := c.owner()
	fingerprint := rebuildFingerprint{kid: c.kid, embeddingOnly: embeddingOnly}
	for {
		s.mu.Lock()
		prev, ok := s.lastRebuild(owner)
		if ok {
			status := prev.job.Status()
			switch {
			case status.State == jobs.Running && (embeddingOnly || prev.fingerprint == fingerprint):
				s.mu.Unlock()
				return prev.job, nil
			case status.State == jobs.Failed && !embeddingOnly:
				// Reported to this caller; the next request starts over.
				s.forgetLocked(owner)
				s.mu.Unlock()
				return prev.job, nil
			case status.State == jobs.Complete && !status.Report.Partial && prev.fingerprint == fingerprint:
				s.mu.Unlock()
				needs, err := s.needsRepair(ctx, c, embeddingOnly)
				if err != nil {
					return nil, err
				}
				s.mu.Lock()
				if current, ok := s.started[owner]; !ok || current.job != prev.job {
					s.mu.Unlock()
					continue
				}
				if !needs {
					s.mu.Unlock()
					return prev.job, nil
				}
			}
		}
		predecessor := s.Rebuilds.Cancel(owner)
		work := c.detached(key)
		job, _ := s.Rebuilds.Start(ctx, owner, func(ctx context.Context, report func(RebuildReport)) (RebuildReport, error) {
			defer work.zero()
			if predecessor != nil {
				select {
				case <-predecessor.Done():
				case <-ctx.Done():
				}
			}
			if embeddingOnly {
				return s.repairEmbeddings(ctx, work, report)
			}
			return s.rebuild(ctx, work, report)
		})
		s.started[owner] = startedRebuild{job: job, fingerprint: fingerprint}
		s.mu.Unlock()
		go s.forgetEnded(owner, job)
		return job, nil
	}
}

// forgetEnded drops the record of a rebuild once it has ended and its
// retention has passed, so the records held are those of recent rebuilds.
func (s *Service) forgetEnded(owner string, job *jobs.Job[RebuildReport]) {
	<-job.Done()
	time.AfterFunc(RebuildRetention, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if current, ok := s.started[owner]; ok && current.job == job {
			delete(s.started, owner)
		}
	})
}

// lastRebuild is the caller's last rebuild, running or ended within the
// retention. Callers hold s.mu.
func (s *Service) lastRebuild(owner string) (startedRebuild, bool) {
	prev, ok := s.started[owner]
	if !ok {
		return startedRebuild{}, false
	}
	if status := prev.job.Status(); status.State != jobs.Running && time.Since(status.UpdatedAt) > RebuildRetention {
		delete(s.started, owner)
		return startedRebuild{}, false
	}
	return prev, true
}

// forgetRebuild ends and forgets the caller's rebuild, running or ended.
func (s *Service) forgetRebuild(owner string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetLocked(owner)
}

func (s *Service) forgetLocked(owner string) {
	delete(s.started, owner)
	s.Rebuilds.Cancel(owner)
}

// rebuild drains the caller's chats page by page, resuming a clean
// checkpoint when one exists, and reports after each page. The budget or
// a cancellation mid-page leaves that page to the next rebuild; the pages
// done survive.
func (s *Service) rebuild(ctx context.Context, c caller, report func(RebuildReport)) (RebuildReport, error) {
	var out RebuildReport
	publication, _, err := s.publication(ctx, c)
	if err != nil {
		if ctx.Err() != nil {
			out.Partial = true
			return out, nil
		}
		return out, err
	}
	cursor, startedAt, target, embed := "", time.Now().UTC(), publication.SourceRevision, true
	checkpoint, resume, err := s.loadCheckpoint(ctx, c, publication)
	if err != nil {
		if ctx.Err() != nil {
			out.Partial = true
			return out, nil
		}
		return out, err
	}
	if resume {
		cursor, startedAt, target = checkpoint.NextCursor, entryTime(checkpoint.StartedAt), checkpoint.TargetSourceRevision
	}
	coverageFailure := false
	stopped := func() (RebuildReport, error) {
		out.Partial = true
		if coverageFailure {
			return out, errRebuildIncomplete
		}
		return out, nil
	}
	for {
		if ctx.Err() != nil {
			return stopped()
		}
		page, err := s.rebuildPage(ctx, c, cursor, startedAt, target, coverageFailure, embed)
		if err != nil {
			// A budget that ran out mid-page surfaces as a context error
			// wrapped in the failure of some request: that is partial,
			// not failed, so the client asks again rather than alarm.
			if ctx.Err() != nil {
				return stopped()
			}
			return out, err
		}
		out.Indexed += page.indexed
		out.Failed += page.failed
		out.TotalIndexed = page.total
		if page.embeddingFailed {
			// One outage detected, the rest of the corpus is indexed by
			// keyword alone, for a later query to repair.
			embed = false
		}
		if page.failed > 0 {
			out.Partial = true
			coverageFailure = true
		}
		if page.done && page.partial {
			out.Partial = true
		}
		report(out)
		if page.done {
			if coverageFailure || page.partial {
				return out, errRebuildIncomplete
			}
			return out, nil
		}
		cursor, startedAt, target = page.next, page.resumeStartedAt, page.resumeTarget
	}
}

// loadCheckpoint is the checkpoint a rebuild resumes: the one the stored
// index holds, when the index is this caller's, incomplete, and
// published at the checkpoint's own target.
func (s *Service) loadCheckpoint(ctx context.Context, c caller, publication *controlplane.SearchIndexState) (*searchindex.ReindexProgress, bool, error) {
	if publication.ObjectKey == "" || !publication.PublicationIncomplete || s.readableObject(publication, c) == "" {
		return nil, false, nil
	}
	owner := c.owner()
	runlock := s.rlock(owner)
	defer runlock()
	ix, state, err := s.load(ctx, owner, publication.ObjectKey, c.indexKey)
	if err != nil {
		return nil, false, err
	}
	if state != loadOK || ix.Reindex == nil || !ix.Incomplete || publication.PublishedSourceRevision != ix.Reindex.TargetSourceRevision {
		return nil, false, nil
	}
	return ix.Reindex, true, nil
}

// pageResult is one page of a rebuild: what it indexed and left out,
// the cursor the next page reads from, and the checkpoint it resumes
// when the stored index holds another rebuild's.
type pageResult struct {
	indexed, failed int
	embeddingFailed bool
	next            string
	done, partial   bool
	total           int
	resumeStartedAt time.Time
	resumeTarget    int64
}

// pendingChat is a chat read for a page, with its text in chunks.
type pendingChat struct {
	id, etag, kid string
	chunks        []string
}

// rebuildPage rebuilds one page of the caller's index from their stored
// chats. An empty cursor starts over: entries from before startedAt go,
// which drops chats deleted since, while entries written since the
// rebuild began stay. Every clean page but the last checkpoints its
// cursor in the index, so a later rebuild resumes it.
func (s *Service) rebuildPage(ctx context.Context, c caller, cursor string, startedAt time.Time, target int64, priorFailure, embed bool) (*pageResult, error) {
	owner := c.owner()
	listed, err := s.Rows.List(ctx, c.p, c.key, "chat", cursor, "", rebuildPage)
	if err != nil {
		return nil, err
	}
	failed := len(listed.Unreadable)
	page := make([]pendingChat, 0, len(listed.Rows))
	var texts []string
	for i := range listed.Rows {
		row := &listed.Rows[i]
		chunks := chatChunks(row.Data)
		seal.Zero(row.Data)
		page = append(page, pendingChat{id: row.ID, etag: row.ETag, kid: c.kid, chunks: chunks})
		texts = append(texts, chunks...)
	}

	var vectors [][]float32
	embeddingFailed := len(texts) > 0 && !embed
	if len(texts) > 0 && embed {
		vectors, err = s.embedChunks(ctx, owner, docPrefix, texts, true)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			embeddingFailed, vectors = true, nil
		}
	}

	unlock := s.lock(owner)
	defer unlock()
	for attempt := 0; attempt < publishAttempts; attempt++ {
		publication, objectKey, err := s.publication(ctx, c)
		if err != nil {
			return nil, err
		}
		compatible := publication.ObjectKey == "" || objectKey != ""
		ix, state, err := s.load(ctx, owner, objectKey, c.indexKey)
		if err != nil {
			return nil, err
		}
		if ix.Reindex != nil {
			checkpointStartedAt := entryTime(ix.Reindex.StartedAt)
			if cursor != ix.Reindex.NextCursor || target != ix.Reindex.TargetSourceRevision || !startedAt.Equal(checkpointStartedAt) {
				return &pageResult{next: ix.Reindex.NextCursor, total: len(ix.Chats), resumeStartedAt: checkpointStartedAt, resumeTarget: ix.Reindex.TargetSourceRevision}, nil
			}
		} else if cursor != "" {
			return &pageResult{done: true, partial: ix.Incomplete || publication.PublicationIncomplete, total: len(ix.Chats), resumeStartedAt: startedAt, resumeTarget: target}, nil
		}
		if cursor == "" {
			var stale []string
			for id, entry := range ix.Chats {
				if entry.SourceRevision > 0 {
					if entry.SourceRevision <= target {
						stale = append(stale, id)
					}
				} else if entryTime(entry.UpdatedAt).Before(startedAt) {
					stale = append(stale, id)
				}
			}
			for _, id := range stale {
				ix.Remove(id)
			}
		}
		indexed, attemptFailed, vectorAt := 0, failed, 0
		for _, chat := range page {
			var chatVectors [][]float32
			if vectors != nil {
				chatVectors = vectors[vectorAt : vectorAt+len(chat.chunks)]
				vectorAt += len(chat.chunks)
			}
			snapshot, err := s.snapshotState(ctx, c, chat.id, chat.etag)
			if err != nil {
				return nil, err
			}
			if snapshot == snapshotDeleted {
				ix.Remove(chat.id)
				continue
			}
			if existing, ok := ix.Chats[chat.id]; ok && entrySupersedes(existing, chat.etag, target, startedAt) {
				indexed++
				continue
			}
			if snapshot == snapshotStale {
				continue
			}
			entry := searchindex.Entry{
				ETag:             chat.etag,
				SourceRevision:   target,
				UpdatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
				Vectors:          quantizeAll(chatVectors),
				EmbeddingPending: len(chat.chunks) > 0 && vectors == nil,
			}
			if entry.EmbeddingPending {
				entry.EmbeddingKeyID = chat.kid
			}
			if err := ix.Upsert(chat.id, entry, searchindex.Tokenize(strings.Join(chat.chunks, "\n"))); err != nil {
				attemptFailed++
				continue
			}
			indexed++
		}
		incomplete := listed.Next != "" || priorFailure || attemptFailed > 0
		if (!compatible || state != loadOK) && cursor != "" {
			incomplete = true
		}
		if publication.PublishedSourceRevision > target && publication.PublicationIncomplete {
			incomplete = true
		}
		covered := max(target, publication.PublishedSourceRevision)
		switch {
		case !incomplete:
			ix.Reindex = nil
		case !priorFailure && attemptFailed == 0 && listed.Next != "":
			ix.Reindex = &searchindex.ReindexProgress{NextCursor: listed.Next, TargetSourceRevision: target, StartedAt: startedAt.UTC().Format(time.RFC3339Nano)}
		default:
			ix.Reindex = nil
		}
		ix.Incomplete = incomplete
		if _, err := s.publish(ctx, c, publication, covered, incomplete, ix); err != nil {
			if isConflict(err) {
				continue
			}
			return nil, err
		}
		return &pageResult{
			indexed:         indexed,
			failed:          attemptFailed,
			embeddingFailed: embeddingFailed,
			next:            listed.Next,
			done:            listed.Next == "",
			partial:         incomplete,
			total:           len(ix.Chats),
			resumeStartedAt: startedAt,
			resumeTarget:    target,
		}, nil
	}
	return nil, errIndexContended
}

// A pending entry is repaired with vectors for the chat at the revision
// the entry was built from; one whose chat another key seals is marked
// with that key, so only a query under it tries again.
type embeddingRepair struct {
	id, etag string
	vectors  []searchindex.Vector
}

type embeddingBlocked struct {
	id, etag, kid string
}

// repairEmbeddings embeds the entries this key can repair, a page at a
// time, and reports after each. The index must be this caller's and
// readable; anything less is left to a rebuild.
func (s *Service) repairEmbeddings(ctx context.Context, c caller, report func(RebuildReport)) (RebuildReport, error) {
	var out RebuildReport
	owner := c.owner()
	_, objectKey, err := s.publication(ctx, c)
	if err != nil {
		return out, err
	}
	if objectKey == "" {
		out.Partial = true
		return out, nil
	}
	runlock := s.rlock(owner)
	ix, state, err := s.load(ctx, owner, objectKey, c.indexKey)
	if err != nil {
		runlock()
		return out, err
	}
	if state != loadOK {
		runlock()
		out.Partial = true
		return out, nil
	}
	ids := make([]string, 0)
	for id, entry := range ix.Chats {
		if entry.EmbeddingPending && (entry.EmbeddingKeyID == "" || entry.EmbeddingKeyID == c.kid) {
			ids = append(ids, id)
		}
	}
	runlock()
	if len(ids) == 0 {
		return out, nil
	}
	sort.Strings(ids)

	for start := 0; start < len(ids); start += rebuildPage {
		if ctx.Err() != nil {
			out.Partial = true
			return out, nil
		}
		end := min(start+rebuildPage, len(ids))
		rows, unreadable, err := s.Rows.Pull(ctx, c.p, c.key, "chat", ids[start:end])
		if err != nil {
			if ctx.Err() != nil {
				out.Partial = true
				return out, nil
			}
			return out, err
		}
		pulled := map[string]*domain.StoredRow{}
		for i := range rows {
			pulled[rows[i].ID] = &rows[i]
		}
		pending := make([]pendingChat, 0, len(rows))
		blocked := make([]embeddingBlocked, 0)
		var gone []string
		var texts []string
		failed := 0
		for _, id := range ids[start:end] {
			row, ok := pulled[id]
			if !ok {
				if unreadable[id] == nil {
					// The chat was deleted since it was indexed.
					gone = append(gone, id)
					continue
				}
				if e := api.AsError(unreadable[id]); e.Code == "KEY_MISMATCH" && e.KeyID != "" {
					if etag, exists, err := s.currentETag(ctx, c, id); err == nil && exists {
						blocked = append(blocked, embeddingBlocked{id: id, etag: etag, kid: e.KeyID})
						continue
					}
				}
				failed++
				continue
			}
			chunks := chatChunks(row.Data)
			seal.Zero(row.Data)
			pending = append(pending, pendingChat{id: id, etag: row.ETag, chunks: chunks})
			texts = append(texts, chunks...)
		}

		var vectors [][]float32
		if len(texts) > 0 {
			vectors, err = s.embedChunks(ctx, owner, docPrefix, texts, true)
			if err != nil {
				if ctx.Err() != nil {
					out.Partial = true
					return out, nil
				}
				_, total, applyErr := s.applyRepairs(ctx, c, nil, blocked, gone)
				if applyErr != nil {
					return out, applyErr
				}
				out.Failed += len(pending) + failed
				out.TotalIndexed = total
				out.Partial = true
				report(out)
				return out, nil
			}
		}
		repairs := make([]embeddingRepair, 0, len(pending))
		vectorAt := 0
		for _, chat := range pending {
			chatVectors := vectors[vectorAt : vectorAt+len(chat.chunks)]
			vectorAt += len(chat.chunks)
			repairs = append(repairs, embeddingRepair{id: chat.id, etag: chat.etag, vectors: quantizeAll(chatVectors)})
		}
		repaired, total, err := s.applyRepairs(ctx, c, repairs, blocked, gone)
		if err != nil {
			return out, err
		}
		out.Indexed += repaired
		out.Failed += failed
		out.TotalIndexed = total
		if failed > 0 {
			out.Partial = true
		}
		report(out)
	}
	return out, nil
}

// applyRepairs writes the repairs and the marks into the index, as long
// as it is still this caller's and each entry is still the one that was
// read, drops the entries of chats that are gone, and is how many entries
// were repaired and how many the index holds.
func (s *Service) applyRepairs(ctx context.Context, c caller, repairs []embeddingRepair, blocked []embeddingBlocked, gone []string) (repaired, total int, err error) {
	owner := c.owner()
	unlock := s.lock(owner)
	defer unlock()
	for attempt := 0; attempt < publishAttempts; attempt++ {
		publication, objectKey, err := s.publication(ctx, c)
		if err != nil {
			return 0, 0, err
		}
		if objectKey == "" {
			return 0, 0, nil
		}
		ix, state, err := s.load(ctx, owner, objectKey, c.indexKey)
		if err != nil {
			return 0, 0, err
		}
		if state != loadOK {
			return 0, 0, nil
		}
		mutated, repaired := false, 0
		for _, id := range gone {
			if entry, ok := ix.Chats[id]; ok && entry.EmbeddingPending {
				ix.Remove(id)
				mutated = true
			}
		}
		for _, item := range blocked {
			entry, ok := ix.Chats[item.id]
			if !ok || !entry.EmbeddingPending || entry.ETag != item.etag {
				continue
			}
			if ix.MarkEmbeddingKey(item.id, item.kid) {
				mutated = true
			}
		}
		for _, repair := range repairs {
			entry, ok := ix.Chats[repair.id]
			if !ok || !entry.EmbeddingPending || entry.ETag != repair.etag {
				continue
			}
			if err := ix.RepairEmbedding(repair.id, repair.vectors); err != nil {
				return 0, 0, err
			}
			mutated = true
			repaired++
		}
		if !mutated {
			return 0, len(ix.Chats), nil
		}
		incomplete := publication.PublicationIncomplete || ix.Incomplete
		if _, err := s.publish(ctx, c, publication, publication.PublishedSourceRevision, incomplete, ix); err != nil {
			if isConflict(err) {
				continue
			}
			return 0, 0, err
		}
		return repaired, len(ix.Chats), nil
	}
	return 0, 0, errIndexContended
}
