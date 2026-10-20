package main

import (
	"bytes"
	"context"
	"io"
	"mime"
	"net/http"
	"sync"
	"time"
	"unicode/utf8"
)

// Read-aloud audio is mono 16-bit 24 kHz PCM. Each piece of a message is
// synthesized apart, at most speechParallelism at once, and the pieces
// stream back in order as one response.
const (
	speechModel        = "qwen3-tts"
	speechVoice        = "aiden"
	speechInstructions = "Read in a calm, natural conversational voice with a steady pace, consistent pitch range, and restrained expression. Use natural sentence pauses and maintain an even narration style."
	speechMediaType    = "audio/pcm"
	maxSpeechBytes     = 120 * 24000 * 2 // two minutes of audio, more than any piece takes
	speechTimeout      = 2 * time.Minute // for each piece
	speechParallelism  = 2
	speechReadSize     = 32 << 10
)

func (h *harness) handleSpeech(w http.ResponseWriter, r *http.Request) {
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	body, err := readBody(w, r, maxTurnBody)
	if err != nil {
		respond(w, nil, err)
		return
	}
	in := &speechRequest{}
	if err := decodeRequest(body, in); err != nil {
		respond(w, nil, err)
		return
	}
	if err := in.validate(); err != nil {
		respond(w, nil, err)
		return
	}
	text, err := h.speechText(r.Context(), p, in)
	if err != nil {
		respond(w, nil, err)
		return
	}
	pieces := splitSpeech(text)
	if len(pieces) == 0 {
		respond(w, nil, apiErr(400, "BAD_REQUEST", "There is nothing to read aloud"))
		return
	}
	router := h.services["router"]
	if router == nil {
		respond(w, nil, apiErr(503, "TOOL_UNAVAILABLE", "Speech is unavailable"))
		return
	}
	credential, err := h.auth.inference(r.Context(), p)
	if err != nil {
		respond(w, nil, h.withQuota(p, credential.Limit, err))
		return
	}
	ctx := context.WithValue(r.Context(), apiKeyKey{}, string(credential.Key))
	if err := streamSpeech(ctx, w, router, pieces); err != nil {
		respond(w, nil, h.withQuota(p, credential.Limit, err))
	}
}

// speechText is what a request reads aloud: a message's markdown, or text
// a client selected, as it is spoken.
func (h *harness) speechText(ctx context.Context, p *principal, in *speechRequest) (string, error) {
	source := valueOr(in.Text, "")
	if in.MessageID != "" {
		var thread Thread
		switch {
		case in.Local:
			thread = readDeviceThread(in.ThreadID, in.Thread)
		case p.Anonymous:
			return "", errAnonymousCloud
		default:
			key, err := decodeContentKey(in.Key)
			if err != nil {
				return "", err
			}
			defer clear(key[:])
			row, err := h.loadThread(ctx, p, key, in.ThreadID)
			if err != nil {
				return "", err
			}
			thread = row.Thread
		}
		index := thread.messageIndex(in.MessageID)
		if index < 0 {
			return "", apiErr(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		source = thread.Messages[index].Content
	}
	if utf8.RuneCountInString(source) > maxSpeechText {
		return "", apiErr(413, "PAYLOAD_TOO_LARGE", "The text is too long to read aloud")
	}
	return speakableText(source), nil
}

// speechAudio is one piece's audio as it arrives, held until it is written,
// so a piece synthesized ahead never waits on the one being written.
type speechAudio struct {
	mu   sync.Mutex
	data []byte
	done bool
	err  error
	wake chan struct{} // closed and replaced whenever the audio changes
}

func newSpeechAudio() *speechAudio { return &speechAudio{wake: make(chan struct{})} }

func (a *speechAudio) add(audio []byte, done bool, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.data, a.done, a.err = append(a.data, audio...), done, err
	close(a.wake)
	a.wake = make(chan struct{})
}

// from is the audio past offset, whether the piece ended, and how.
func (a *speechAudio) from(offset int) ([]byte, bool, error, <-chan struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.data[offset:], a.done, a.err, a.wake
}

// streamSpeech writes the audio of each piece in order. A piece is
// synthesized once a slot frees up, and its slot frees up once its audio
// is written. A failure before any audio is returned; one after aborts the
// response, so its client reads the audio as incomplete.
func streamSpeech(ctx context.Context, w http.ResponseWriter, router *downstreamService, pieces []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	audio := make([]*speechAudio, len(pieces))
	for i := range audio {
		audio[i] = newSpeechAudio()
	}
	slots := make(chan struct{}, speechParallelism)
	go func() {
		for i, piece := range pieces {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go synthesize(ctx, router, piece, audio[i])
		}
	}()
	started := false
	flush := http.NewResponseController(w)
	for i := range pieces {
		for written := 0; ; {
			data, done, err, wake := audio[i].from(written)
			if len(data) > 0 {
				if !started {
					w.Header().Set("Content-Type", speechMediaType)
					w.Header().Set("Cache-Control", "no-store")
					w.Header().Set("X-Content-Type-Options", "nosniff")
					started = true
				}
				if _, err := w.Write(data); err != nil || flush.Flush() != nil {
					return nil
				}
				written += len(data)
				continue
			}
			if err != nil && !started {
				return err
			}
			if err != nil {
				panic(http.ErrAbortHandler)
			}
			if done {
				break
			}
			select {
			case <-wake:
			case <-ctx.Done():
				return nil
			}
		}
		audio[i] = nil
		<-slots
	}
	if !started {
		return apiErr(502, "UPSTREAM_REFUSED", "The speech service returned no audio")
	}
	return nil
}

// synthesize reads the audio the router speaks one piece of text with.
func synthesize(ctx context.Context, router *downstreamService, text string, out *speechAudio) {
	var err error
	defer func() {
		if failure := rescued("speech synthesis", recover()); failure != nil {
			err = failure
		}
		out.add(nil, true, err)
	}()
	ctx, cancel := context.WithTimeout(ctx, speechTimeout)
	defer cancel()
	speech := raw(object{"model": speechModel, "voice": speechVoice, "instructions": speechInstructions, "input": text, "response_format": "pcm", "stream_format": "audio"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, router.URL+"/v1/audio/speech", bytes.NewReader(speech))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := router.Client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		err = withRetryAfter(downstreamError(resp.StatusCode, b), resp.Header)
		return
	}
	if media, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); media != speechMediaType {
		err = apiErr(502, "UPSTREAM_REFUSED", "The speech service returned unsupported audio")
		return
	}
	body, chunk := io.LimitReader(resp.Body, maxSpeechBytes), make([]byte, speechReadSize)
	for {
		n, readErr := body.Read(chunk)
		if n > 0 {
			out.add(chunk[:n], false, nil)
		}
		if readErr == io.EOF {
			return
		}
		if readErr != nil {
			err = readErr
			return
		}
	}
}
