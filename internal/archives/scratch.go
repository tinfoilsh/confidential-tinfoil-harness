package archives

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
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
	errArchiveTooLarge = api.Err(413, "ARCHIVE_TOO_LARGE", "The archive needs more temporary space than this service has")
	errScratchBusy     = &api.Error{HTTP: 503, Code: "TEMP_SPACE_BUSY", Message: "Other archives are using this service's temporary space; retry shortly", Action: api.ActionRetry, RetryAfter: 30}
)

type Scratch struct {
	Dir        string
	size       int64
	sem        *semaphore.Weighted
	Wait, Idle time.Duration
	// waiters counts requests waiting for space.
	waiters atomic.Int32
}

// Waiting is how many requests are waiting for space.
func (s *Scratch) Waiting() int32 { return s.waiters.Load() }

// Empty reports whether no request holds any of the space.
func (s *Scratch) Empty() bool {
	if !s.sem.TryAcquire(s.size) {
		return false
	}
	s.sem.Release(s.size)
	return true
}

func NewScratch(dir string, size int64) *Scratch {
	return &Scratch{Dir: dir, size: size, sem: semaphore.NewWeighted(size), Wait: scratchWait, Idle: scratchIdle}
}

// sweepScratch removes the spool files a process that ended before releasing
// them left in dir.
func sweepScratch(dir string) error {
	left, err := filepath.Glob(filepath.Join(dir, ScratchPattern))
	if err != nil {
		return err
	}
	for _, name := range left {
		if err := os.Remove(name); err != nil {
			return err
		}
	}
	return nil
}

// ScratchPattern names the spool files, so a later process can tell what
// its predecessor left from anything else in the directory.
const ScratchPattern = "harness-archive-*"

// MeasuredScratch budgets the temporary directory as large as it is, once
// the spool files a previous process left in it are removed.
func MeasuredScratch() (*Scratch, error) {
	dir := os.TempDir()
	if err := sweepScratch(dir); err != nil {
		return nil, err
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return nil, err
	}
	return NewScratch(dir, int64(stat.Blocks)*int64(stat.Bsize)), nil
}

// Lease is the space one request holds: what its files take.
type Lease struct {
	ctx   context.Context
	space *Scratch
	held  int64
	files []*os.File
}

// Lease starts a request's lease, which holds nothing until its files are
// written. A request expected to need more than the whole space is refused.
func (s *Scratch) Lease(ctx context.Context, expected int64) (*Lease, error) {
	if expected > s.size {
		return nil, errArchiveTooLarge
	}
	return &Lease{ctx: ctx, space: s}, nil
}

func (l *Lease) Take(n int64) error {
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
	waitCtx, cancel := context.WithTimeout(l.ctx, l.space.Wait)
	defer cancel()
	l.space.waiters.Add(1)
	err := l.space.sem.Acquire(waitCtx, n)
	l.space.waiters.Add(-1)
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
func (s *Scratch) idleBody(w http.ResponseWriter, r *http.Request) func() {
	rc := http.NewResponseController(w)
	r.Body = &idleReader{ReadCloser: r.Body, rc: rc, idle: s.Idle}
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

// Release deletes the lease's files and frees its space.
func (l *Lease) Release() {
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
	lease *Lease
	size  int64
}

func (l *Lease) create() (*scratchFile, error) {
	f, err := os.CreateTemp(l.space.Dir, ScratchPattern)
	if err != nil {
		return nil, err
	}
	l.files = append(l.files, f)
	return &scratchFile{file: f, lease: l}, nil
}

func (f *scratchFile) Write(p []byte) (int, error) {
	if err := f.lease.Take(int64(len(p))); err != nil {
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
