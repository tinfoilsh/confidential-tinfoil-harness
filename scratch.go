package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/semaphore"
)

// Archives are spooled to the temporary directory: a tmpfs whose size the
// enclave's configuration sets, held in the confidential VM's encrypted
// memory, which has no swap device. A request takes space as the bytes it
// spools arrive. Until it holds some, it waits while other requests hold
// it; once it does, it never waits for more, since two requests could then
// each wait for the space the other holds. One that cannot grow is told to
// retry, freeing what it holds. A request that could never fit is refused
// at once, and one whose body stops arriving is ended, freeing what it
// holds.

const (
	// scratchWait bounds how long a request waits for space before it is
	// told to retry.
	scratchWait = 2 * time.Minute
	// scratchIdle bounds how long a request's body may send nothing.
	scratchIdle = 30 * time.Second
)

var (
	errArchiveTooLarge = apiErr(413, "ARCHIVE_TOO_LARGE", "The archive needs more temporary space than this service has")
	errScratchBusy     = &apiError{HTTP: 503, Code: "TEMP_SPACE_BUSY", Message: "Other archives are using this service's temporary space; retry shortly", Action: actionRetry, RetryAfter: 30}
)

type scratchSpace struct {
	dir        string
	size       int64
	sem        *semaphore.Weighted
	wait, idle time.Duration
	// waiting counts requests waiting for space.
	waiting atomic.Int32
}

func newScratchSpace(dir string, size int64) *scratchSpace {
	return &scratchSpace{dir: dir, size: size, sem: semaphore.NewWeighted(size), wait: scratchWait, idle: scratchIdle}
}

// measuredScratch budgets the temporary directory as large as it is.
func measuredScratch() (*scratchSpace, error) {
	dir := os.TempDir()
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return nil, err
	}
	return newScratchSpace(dir, int64(stat.Blocks)*int64(stat.Bsize)), nil
}

// scratchLease is the space one request holds: what its files take.
type scratchLease struct {
	ctx   context.Context
	space *scratchSpace
	held  int64
	files []*os.File
}

// lease starts a request's lease, which holds nothing until its files are
// written. A request expected to need more than the whole space is refused.
func (s *scratchSpace) lease(ctx context.Context, expected int64) (*scratchLease, error) {
	if expected > s.size {
		return nil, errArchiveTooLarge
	}
	return &scratchLease{ctx: ctx, space: s}, nil
}

func (l *scratchLease) take(n int64) error {
	if l.held+n > l.space.size {
		return errArchiveTooLarge
	}
	if l.space.sem.TryAcquire(n) {
		l.held += n
		return nil
	}
	if l.held > 0 {
		return errScratchBusy
	}
	waitCtx, cancel := context.WithTimeout(l.ctx, l.space.wait)
	defer cancel()
	l.space.waiting.Add(1)
	err := l.space.sem.Acquire(waitCtx, n)
	l.space.waiting.Add(-1)
	if err != nil {
		if l.ctx.Err() != nil {
			return l.ctx.Err()
		}
		return errScratchBusy
	}
	l.held += n
	return nil
}

// idleBody ends a request whose body sends nothing for the space's idle
// time. The returned func lifts the limit once the body is read, so it
// cannot end the request after; left in place, it also bounds the server
// reading what a refused request still sends.
func (s *scratchSpace) idleBody(w http.ResponseWriter, r *http.Request) func() {
	rc := http.NewResponseController(w)
	r.Body = &idleReader{ReadCloser: r.Body, rc: rc, idle: s.idle}
	return func() { rc.SetReadDeadline(time.Time{}) }
}

// idleReader gives each read of a body until the idle time to return.
type idleReader struct {
	io.ReadCloser
	rc   *http.ResponseController
	idle time.Duration
}

func (r *idleReader) Read(p []byte) (int, error) {
	r.rc.SetReadDeadline(time.Now().Add(r.idle))
	return r.ReadCloser.Read(p)
}

// release deletes the lease's files and frees its space.
func (l *scratchLease) release() {
	for _, f := range l.files {
		f.Close()
		os.Remove(f.Name())
	}
	l.files = nil
	l.space.sem.Release(l.held)
	l.held = 0
}

// scratchFile is a temporary file written through its lease. It is read
// back at offsets.
type scratchFile struct {
	file  *os.File
	lease *scratchLease
	size  int64
}

func (l *scratchLease) create() (*scratchFile, error) {
	f, err := os.CreateTemp(l.space.dir, "harness-archive-*")
	if err != nil {
		return nil, err
	}
	l.files = append(l.files, f)
	return &scratchFile{file: f, lease: l}, nil
}

func (f *scratchFile) Write(p []byte) (int, error) {
	if err := f.lease.take(int64(len(p))); err != nil {
		return 0, err
	}
	n, err := f.file.Write(p)
	f.size += int64(n)
	return n, err
}

func (f *scratchFile) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= f.size {
		return 0, io.EOF
	}
	if limit := f.size - offset; int64(len(p)) > limit {
		n, err := f.file.ReadAt(p[:limit], offset)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return f.file.ReadAt(p, offset)
}

func (f *scratchFile) Size() int64 { return f.size }

// section reads length bytes of the file from offset.
func (f *scratchFile) section(offset, length int64) *io.SectionReader {
	return io.NewSectionReader(f, offset, length)
}
