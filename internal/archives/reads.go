package archives

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// An export reads each attachment a stored chat keeps from the storage
// services. A read they fail to answer is tried again a few times; one that
// still fails leaves the attachment out, as one they no longer hold is, so a
// passing fault costs an export an attachment rather than the whole export.
// Most chats keep one or two attachments, so an export reads those of a page
// of chats a few at a time, ahead of the chat it writes, and writes the chats
// in the order the page lists them.

const (
	// attachmentReadAttempts bounds the reads of a stored attachment the
	// storage services fail to answer; each wait is a step longer than the
	// last.
	attachmentReadAttempts = 3
	attachmentRetryStep    = 500 * time.Millisecond
	// readsAhead bounds the stored attachments an export reads at once, and
	// so the ones it holds before writing them.
	readsAhead = 4
)

// errAttachmentUnavailable is a stored attachment the storage services did
// not serve, however often they were asked.
var errAttachmentUnavailable = api.Err(502, "UPSTREAM_REFUSED", "The attachment could not be read")

// passing is whether a storage service's failure may pass: a refusal for
// load, or a failure or timeout of the service.
func passing(err error) bool {
	var e *api.Error
	return errors.As(err, &e) && (e.HTTP >= http.StatusInternalServerError || e.HTTP == http.StatusTooManyRequests)
}

// readStored reads a stored attachment, again after a failure that may pass.
// One the storage services no longer hold, or that cannot be read as its
// kind, is missing; one they keep failing to serve is unavailable.
func readStored[T any](ctx context.Context, read func() (T, error)) (T, error) {
	for attempt := 1; ; attempt++ {
		v, err := read()
		switch {
		case err == nil:
			return v, nil
		case attach.Unreachable(err):
			return v, errAttachmentMissing
		case ctx.Err() != nil || !passing(err):
			return v, err
		case attempt == attachmentReadAttempts:
			return v, errAttachmentUnavailable
		}
		wait := time.NewTimer(time.Duration(attempt) * attachmentRetryStep)
		select {
		case <-wait.C:
		case <-ctx.Done():
			wait.Stop()
			return v, ctx.Err()
		}
	}
}

// pageReads reads the stored attachments of a page of chats ahead of the
// writer, which takes them chat by chat in the order the page lists them.
type pageReads struct {
	cancel   context.CancelFunc
	image    func(*domain.Attachment) ([]byte, error)
	document func(*domain.Attachment) error
	wg       sync.WaitGroup
	// reads are in the order the page lists them; one the writer took is
	// nil, so the page no longer holds its bytes.
	reads []*storedRead
	// at finds a read by its chat's place in the page and its attachment.
	at      map[readKey]int
	started int
}

type readKey struct {
	chat int
	id   string
}

// storedRead is one attachment's read: an image's bytes, or a document's
// content set on its copy.
type storedRead struct {
	attachment domain.Attachment
	data       []byte
	err        error
	done       chan struct{}
}

// readAhead starts reading the attachments a page of stored chats keeps in
// the cloud: each image, and each document whose content only its blob
// holds.
func (ar *Service) readAhead(ctx context.Context, p *auth.Principal, chats []*domain.Thread) *pageReads {
	ctx, cancel := context.WithCancel(ctx)
	r := &pageReads{cancel: cancel, image: ar.cloudImage(ctx, p), document: ar.cloudDocument(ctx, p), at: map[readKey]int{}}
	for k, t := range chats {
		for _, m := range t.Messages {
			for _, a := range m.Attachments {
				key := readKey{k, a.ID}
				if _, ok := r.at[key]; ok || !a.IsImage() && (a.HasContent() || a.StorageKey() == "") {
					continue
				}
				r.at[key] = len(r.reads)
				r.reads = append(r.reads, &storedRead{attachment: a, done: make(chan struct{})})
			}
		}
	}
	r.startBefore(readsAhead)
	return r
}

// startBefore starts every read before the nth.
func (r *pageReads) startBefore(n int) {
	for ; r.started < min(n, len(r.reads)); r.started++ {
		read := r.reads[r.started]
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer close(read.done)
			if read.attachment.IsImage() {
				read.data, read.err = r.image(&read.attachment)
			} else {
				read.err = r.document(&read.attachment)
			}
		}()
	}
}

// chat is how the writer reads the attachments of the page's kth chat: from
// the reads ahead of it, or directly for one they do not hold.
func (r *pageReads) chat(k int) (func(*domain.Attachment) ([]byte, error), func(*domain.Attachment) error) {
	image := func(a *domain.Attachment) ([]byte, error) {
		read := r.take(k, a)
		if read == nil {
			return r.image(a)
		}
		return read.data, read.err
	}
	document := func(a *domain.Attachment) error {
		read := r.take(k, a)
		if read == nil {
			return r.document(a)
		}
		a.TextContent, a.Pages = read.attachment.TextContent, read.attachment.Pages
		return read.err
	}
	return image, document
}

// take hands over an attachment's read once it is done, after starting the
// reads that follow it.
func (r *pageReads) take(k int, a *domain.Attachment) *storedRead {
	i, ok := r.at[readKey{k, a.ID}]
	if !ok || r.reads[i] == nil {
		return nil
	}
	r.startBefore(i + readsAhead)
	read := r.reads[i]
	<-read.done
	r.reads[i] = nil
	return read
}

// close ends the reads the writer did not take.
func (r *pageReads) close() {
	r.cancel()
	r.wg.Wait()
}
