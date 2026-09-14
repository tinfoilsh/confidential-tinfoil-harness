package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	usagereporting "github.com/tinfoilsh/usage-reporting-go"
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
	errTaken     = errors.New("another run is already using this storage id")
	errNotYours  = errors.New("not a recoverable run")
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

	begin   func() // the caller let go: detach and spill, or cancel a run nobody can return to
	stop    func() // cancels the run itself
	abandon func() // the caller deleted the log: stop writing to the store
}

// The caller's API key salts the derivation, so a session id and secret that
// belong to another key open nothing here, warm or cold. Opening the log is
// still the whole of the authorization; the key just scopes whose log it is.
func newRun(ctx context.Context, id string, key secret) (*run, error) {
	material, err := hkdf.Key(sha256.New, []byte(key),
		[]byte(usagereporting.HashAPIKey(callerKey(ctx))),
		"confidential-tinfoil-harness run log v1", 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(material)
	if err != nil {
		return nil, err
	}
	// Random nonces, not the frame index: a caller reusing a secret would repeat one.
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return nil, err
	}
	idle := func() {}
	return &run{id: id, aead: aead, log: newRunlog(), begin: idle, stop: idle, abandon: idle}, nil
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

// Opening frame 0 is the whole authorization, and a run is enlisted only once
// its RUN_STARTED is framed, so every registered run has one to open.
func (h *harness) lookup(ctx context.Context, id string, key secret) (*run, error) {
	h.mu.Lock()
	rn := h.runs[id]
	h.mu.Unlock()
	if rn == nil {
		return nil, nil
	}
	probe, err := newRun(ctx, id, key)
	if err != nil {
		return nil, errNotYours
	}
	frames, _, _ := rn.log.read(0)
	if len(frames) == 0 {
		return nil, errNotYours
	}
	if _, err := probe.open(0, frames[0]); err != nil {
		return nil, errNotYours
	}
	return rn, nil
}

func (h *harness) stored(ctx context.Context, id string, key secret) (*run, error) {
	rn, err := newRun(ctx, id, key)
	if err != nil {
		return nil, err
	}
	return rn, rn.rehydrate(h.fetch(ctx, id))
}

func (h *harness) start(ctx context.Context, req *request, m *model) (*run, error) {
	rn, err := newRun(ctx, req.storageID, req.secret)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runTimeout)
	rn.stop, rn.begin = cancel, cancel
	if req.storageID != "" {
		// The spill outlives the run: a run that ends is still owed its last frames.
		spillCtx, endSpill := context.WithTimeout(context.WithoutCancel(runCtx), runTimeout)
		rn.abandon = endSpill
		rn.begin = sync.OnceFunc(func() {
			go func() {
				defer endSpill()
				defer func() { rescued("run log spill", recover()) }()
				h.spill(spillCtx, rn)
			}()
		})
	}
	// Framed before the run is published: the caller sees RUN_STARTED without
	// waiting on the gateway, and a reattach never lands on an unopenable log.
	out := newStream(rn, req.threadID, req.runID)
	// Published only once begin is final, or a racing reattach cancels the run.
	if err := h.enlist(req.storageID, rn); err != nil {
		rn.abandon()
		cancel()
		return nil, err
	}
	go func() {
		defer cancel()
		defer h.retire(req.storageID, rn)
		defer out.done()
		// Last deferred, so it unwinds first and the panic becomes this run's
		// RUN_ERROR: out.done below then finds the log already closed.
		defer func() {
			if err := rescued("run", recover()); err != nil {
				out.fail(err)
			}
		}()
		began := time.Now()
		if err := h.loop(runCtx, out, req, m); err != nil {
			slog.Error("run", "model", m.name, "error", err)
			out.fail(err)
			return
		}
		slog.Info("served", "model", m.name, "elapsed", time.Since(began).Round(time.Millisecond))
	}()
	return rn, nil
}

// enlist makes a storage id the property of exactly one run.
func (h *harness) enlist(id string, rn *run) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if id != "" {
		if _, taken := h.runs[id]; taken {
			return errTaken
		}
		h.runs[id] = rn
	}
	return nil
}

// retire unregisters this run only; an id it no longer holds belongs to another.
func (h *harness) retire(id string, rn *run) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.runs[id] == rn {
		delete(h.runs, id)
	}
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
