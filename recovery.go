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
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const (
	maxRunLog     = 4 << 20
	maxLiveRuns   = 128
	runTimeout    = 30 * time.Minute
	spillBatch    = 128 << 10
	spillInterval = 2 * time.Second
	coldIdle      = 30 * time.Second
)

var (
	errDone      = errors.New("run finished")
	errAbandoned = errors.New("run did not survive the harness that started it")
	errTooLong   = errors.New("run outgrew the log this harness can hold for it")
	errBusy      = errors.New("too many runs in flight")
	errTaken     = errors.New("another run is already using this storage id")
	errPending   = errors.New("run has not framed anything yet")
	errNotYours  = errors.New("not a recoverable run")
)

type runlog struct {
	mu     sync.Mutex
	frames [][]byte
	bytes  int
	closed error
	wake   chan struct{}
}

func newRunlog() *runlog { return &runlog{wake: make(chan struct{})} }

// grow appends one sealed frame and reports the error the log has closed with,
// which is how a run that outgrew its log learns to stop producing.
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

func newRun(id string, key secret) (*run, error) {
	material, err := hkdf.Key(sha256.New, []byte(key), nil, "confidential-tinfoil-harness run log v1", 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(material)
	if err != nil {
		return nil, err
	}
	// Random nonces rather than the frame index. The key is the caller's secret
	// and nothing else -- it has to be, since deriving it is how a returning
	// caller is authorized -- so a secret used twice would otherwise seal frame
	// N under the same key and nonce twice and hand the store both halves.
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

// lookup finds a live run and authorizes the secret against it. Opening frame 0
// is the whole of the authorization, so a run that has not framed anything yet
// cannot be authorized at all -- which is not the same as being refused.
func (h *harness) lookup(id string, key secret) (*run, error) {
	h.mu.Lock()
	rn := h.runs[id]
	h.mu.Unlock()
	if rn == nil {
		return nil, nil
	}
	probe, err := newRun(id, key)
	if err != nil {
		return nil, errNotYours
	}
	frames, _, _ := rn.log.read(0)
	if len(frames) == 0 {
		return nil, errPending
	}
	if _, err := probe.open(0, frames[0]); err != nil {
		return nil, errNotYours
	}
	return rn, nil
}

// stored reads a detached run back out of the store, authorized the same way.
func (h *harness) stored(ctx context.Context, id string, key secret) (*run, error) {
	rn, err := newRun(id, key)
	if err != nil {
		return nil, err
	}
	return rn, rn.rehydrate(h.fetch(ctx, id))
}

func (h *harness) start(ctx context.Context, req *request, m *model) (*run, error) {
	rn, err := newRun(req.storageID, req.secret)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runTimeout)
	rn.stop, rn.begin = cancel, cancel
	if req.storageID != "" {
		// The spill outlives the run's own context, since a run that ends is
		// still owed its last frames; only a delete under it stops the writing.
		spillCtx, endSpill := context.WithTimeout(context.WithoutCancel(runCtx), runTimeout)
		rn.abandon = endSpill
		rn.begin = sync.OnceFunc(func() {
			go func() {
				defer endSpill()
				h.spill(spillCtx, rn)
			}()
		})
	}
	// Published only once begin is its final self: a reattach racing this would
	// otherwise read the begin that cancels the run it meant to detach.
	if err := h.enlist(req.storageID, rn); err != nil {
		rn.abandon()
		cancel()
		return nil, err
	}
	go func() {
		defer cancel()
		defer h.retire(req.storageID, rn)
		out := newStream(rn, req.threadID, req.runID)
		defer out.done()
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

// enlist admits a run against the log every live run holds in memory, and makes
// a storage id the property of exactly one of them.
func (h *harness) enlist(id string, rn *run) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.live >= maxLiveRuns {
		return errBusy
	}
	if id != "" {
		if _, taken := h.runs[id]; taken {
			return errTaken
		}
		h.runs[id] = rn
	}
	h.live++
	return nil
}

// retire unregisters this run, and only this one: an id it no longer holds
// belongs to whoever took it after.
func (h *harness) retire(id string, rn *run) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.live--
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

// dispose deletes a stored log and says whether it is really gone: a caller
// told its log was dropped has no other way to find out that it was not.
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
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxRunLog))
	return body
}

func (r *run) rehydrate(raw []byte) error {
	for len(raw) >= 4 {
		size := int(binary.BigEndian.Uint32(raw[:4]))
		if size == 0 || size+4 > len(raw) {
			break
		}
		r.log.frames, raw = append(r.log.frames, raw[4:4+size]), raw[4+size:]
	}
	if len(r.log.frames) == 0 {
		return errors.New("nothing stored")
	}
	if _, err := r.open(0, r.log.frames[0]); err != nil {
		return err
	}
	last := len(r.log.frames) - 1
	r.log.closed = errAbandoned
	if plain, err := r.open(last, r.log.frames[last]); err == nil && terminal(plain) {
		r.log.closed = errDone
	}
	return nil
}
