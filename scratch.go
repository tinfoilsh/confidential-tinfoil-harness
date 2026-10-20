package main

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/semaphore"
)

// Archives are spooled to the temporary directory: a tmpfs whose size the
// enclave's configuration sets, held in the confidential VM's encrypted
// memory, which has no swap device. Every request that spools leases the
// space it expects to use first, and waits while other requests hold it.
// A request that could never fit is refused at once.

// scratchWait bounds how long a request waits for space before it is
// told to retry.
const scratchWait = 2 * time.Minute

var (
	errArchiveTooLarge = apiErr(413, "ARCHIVE_TOO_LARGE", "The archive needs more temporary space than this service has")
	errScratchBusy     = &apiError{HTTP: 503, Code: "TEMP_SPACE_BUSY", Message: "Other archives are using this service's temporary space; retry shortly", RetryAfter: 30}
)

type scratchSpace struct {
	dir  string
	size int64
	sem  *semaphore.Weighted
	wait time.Duration
	// waiting counts requests waiting for space.
	waiting atomic.Int32
}

func newScratchSpace(dir string, size int64) *scratchSpace {
	return &scratchSpace{dir: dir, size: size, sem: semaphore.NewWeighted(size), wait: scratchWait}
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

// scratchLease is the space one request holds. Its files count against it,
// and it grows past what was expected only while space is free.
type scratchLease struct {
	space      *scratchSpace
	held, used int64
	files      []*os.File
}

func (s *scratchSpace) lease(ctx context.Context, expected int64) (*scratchLease, error) {
	if expected > s.size {
		return nil, errArchiveTooLarge
	}
	waitCtx, cancel := context.WithTimeout(ctx, s.wait)
	defer cancel()
	s.waiting.Add(1)
	err := s.sem.Acquire(waitCtx, expected)
	s.waiting.Add(-1)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errScratchBusy
	}
	return &scratchLease{space: s, held: expected}, nil
}

func (l *scratchLease) take(n int64) error {
	if l.used+n > l.space.size {
		return errArchiveTooLarge
	}
	if over := l.used + n - l.held; over > 0 {
		if !l.space.sem.TryAcquire(over) {
			return errScratchBusy
		}
		l.held += over
	}
	l.used += n
	return nil
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
