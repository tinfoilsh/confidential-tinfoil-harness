package main

import (
	"bytes"
	"context"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sync"
	"time"
)

const (
	// A run holds its whole log in memory, so this is its share of the enclave.
	maxRunLog     = 8 << 20
	runTimeout    = 30 * time.Minute
	spillBatch    = 128 << 10
	spillInterval = 2 * time.Second
	coldIdle      = 30 * time.Second
)

var (
	errDone      = errors.New("run finished")
	errAbandoned = errors.New("run did not survive the harness that started it")
	errTooLong   = errors.New("run outgrew the log this harness can hold for it")
)

// rescued logs a panic and turns it into an error worth surfacing. Without one
// of these on every goroutine the harness spawns, a panic in any single run
// takes the process, and with it every other run this enclave holds in memory.
// The panic value may contain plaintext, so only the stack is logged.
//
// It takes the recovered value rather than recovering itself, because recover
// works only in the function a defer names, which is one frame above this.
func rescued(what string, p any) error {
	if p == nil {
		return nil
	}
	slog.Error("recovered a panic", "in", what, "stack", string(debug.Stack()))
	return fmt.Errorf("%s failed", what)
}

type runlog struct {
	mu     sync.Mutex
	frames [][]byte
	bytes  int
	closed error
	wake   chan struct{}
}

func newRunlog() *runlog { return &runlog{wake: make(chan struct{})} }

// grow appends one sealed frame and reports the error the log has closed with.
func (l *runlog) grow(seal func(id int) []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed == nil {
		frame := seal(len(l.frames))
		if l.bytes+len(frame) > maxRunLog {
			l.closed = errTooLong
		} else {
			l.frames, l.bytes = append(l.frames, frame), l.bytes+len(frame)
		}
		l.signal()
	}
	return l.closed
}

func (l *runlog) signal() {
	close(l.wake)
	l.wake = make(chan struct{})
}

func (l *runlog) close(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed != nil {
		return
	}
	l.closed = err
	l.signal()
}

func (l *runlog) read(from int) ([][]byte, <-chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if from < len(l.frames) {
		return l.frames[from:], l.wake, l.closed
	}
	return nil, l.wake, l.closed
}

func (l *runlog) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.frames)
}

type run struct {
	id   string
	aead cipher.AEAD
	log  *runlog

	stop    func() // cancels the run itself
	abandon func() // the caller deleted the log: stop writing to the store
}

func (r *run) seal(id int, plain []byte) []byte {
	return r.aead.Seal(nil, nil, plain, r.aad(id))
}

func (r *run) open(id int, sealed []byte) ([]byte, error) {
	return r.aead.Open(nil, nil, sealed, r.aad(id))
}

func (r *run) aad(id int) []byte {
	return binary.BigEndian.AppendUint64([]byte(r.id), uint64(id))
}

func (h *harness) spill(ctx context.Context, rn *run) {
	if !h.push(ctx, rn.id, nil) {
		return
	}
	tick := time.NewTicker(spillInterval)
	defer tick.Stop()
	from, batch := 0, []byte(nil)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		if !h.push(ctx, rn.id+"/chunks", batch) {
			return false
		}
		batch = batch[:0]
		return true
	}
	for {
		frames, wake, closed := rn.log.read(from)
		for _, frame := range frames {
			batch = binary.BigEndian.AppendUint32(batch, uint32(len(frame)))
			batch = append(batch, frame...)
		}
		from += len(frames)
		if (len(batch) >= spillBatch || closed != nil) && !flush() {
			return
		}
		if closed != nil {
			h.push(ctx, rn.id+"/complete", nil)
			return
		}
		select {
		case <-wake:
		case <-tick.C:
			if !flush() {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (h *harness) store(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	ctx = context.WithValue(ctx, usageContextKey{}, false)
	req, err := http.NewRequestWithContext(ctx, method, h.controlplane+"/recovery/"+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return h.cpClient.Do(req)
}

func (h *harness) push(ctx context.Context, path string, body []byte) bool {
	resp, err := h.store(ctx, http.MethodPost, path, body)
	if err == nil {
		defer resp.Body.Close()
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<12))
		if resp.StatusCode/100 == 2 {
			return true
		}
		err = errors.New(resp.Status)
	}
	slog.Warn("run log", "storage", path, "error", err)
	return false
}

func (h *harness) dispose(ctx context.Context, id string) error {
	resp, err := h.store(ctx, http.MethodDelete, id, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<12))
	// A log that was never written is a log the caller asked to be gone.
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return errors.New(resp.Status)
	}
	return nil
}

func (h *harness) fetch(ctx context.Context, id string) []byte {
	ctx, cancel := context.WithTimeout(ctx, coldIdle)
	defer cancel()
	resp, err := h.store(ctx, http.MethodGet, id, nil)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxRunLog+4*(maxRunLog/32)+1))
	return body
}

func (r *run) rehydrate(raw []byte) error {
	if len(raw) > maxRunLog+4*(maxRunLog/32) {
		return errTooLong
	}
	r.log.frames = nil
	r.log.bytes = 0
	for len(raw) >= 4 {
		size := int(binary.BigEndian.Uint32(raw[:4]))
		if size == 0 || size+4 > len(raw) {
			break
		}
		r.log.frames, raw = append(r.log.frames, raw[4:4+size]), raw[4+size:]
		r.log.bytes += size
		if r.log.bytes > maxRunLog {
			return errTooLong
		}
	}
	if len(r.log.frames) == 0 {
		return errors.New("nothing stored")
	}
	for index, frame := range r.log.frames {
		if _, err := r.open(index, frame); err != nil {
			return err
		}
	}
	last := len(r.log.frames) - 1
	r.log.closed = errAbandoned
	if plain, err := r.open(last, r.log.frames[last]); err == nil && terminal(plain) {
		r.log.closed = errDone
	}
	return nil
}
