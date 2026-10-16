package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

// Read-aloud as the web client requests it: mono 16-bit 24 kHz PCM for text
// split into chunks of at most maxSpeechCharacters.
const (
	speechModel         = "qwen3-tts"
	speechVoice         = "aiden"
	speechInstructions  = "Read in a calm, natural conversational voice with a steady pace, consistent pitch range, and restrained expression. Use natural sentence pauses and maintain an even narration style."
	maxSpeechCharacters = 600
	maxSpeechBytes      = 120 * 24000 * 2 // the two minutes of audio the client accepts per chunk
	speechTimeout       = 2 * time.Minute
)

func (h *harness) handleSpeech(w http.ResponseWriter, r *http.Request) {
	in, err := decodeJSON(w, r)
	if err != nil {
		respond(w, nil, err)
		return
	}
	p, err := h.auth.authenticate(r, true)
	if err != nil {
		respond(w, nil, err)
		return
	}
	if err := validateFields(in, "text voice instructions", "", ""); err != nil {
		respond(w, nil, err)
		return
	}
	text := strings.TrimSpace(str(in["text"]))
	voice, instructions := firstString(in["voice"], speechVoice), firstString(in["instructions"], speechInstructions)
	if text == "" || utf8.RuneCountInString(text) > maxSpeechCharacters || len(voice) > 64 || utf8.RuneCountInString(instructions) > 1000 {
		respond(w, nil, apiErr(400, "BAD_REQUEST", fmt.Sprintf("text must have 1 to %d characters, voice at most 64 bytes, and instructions at most 1000 characters", maxSpeechCharacters)))
		return
	}
	router := h.services["router"]
	if router == nil {
		respond(w, nil, apiErr(503, "TOOL_UNAVAILABLE", "Speech is unavailable"))
		return
	}
	credential, err := h.auth.inference(r.Context(), p)
	if err != nil {
		respond(w, nil, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), apiKeyKey{}, string(credential.Key)), speechTimeout)
	defer cancel()
	body := raw(object{"model": speechModel, "voice": voice, "instructions": instructions, "input": text, "response_format": "pcm", "stream_format": "audio"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, router.URL+"/v1/audio/speech", bytes.NewReader(body))
	if err != nil {
		respond(w, nil, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := router.Client.Do(req)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		respond(w, nil, withRetryAfter(downstreamError(resp.StatusCode, b), resp.Header))
		return
	}
	if media, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); media != "audio/pcm" {
		respond(w, nil, apiErr(502, "UPSTREAM_REFUSED", "The speech service returned unsupported audio"))
		return
	}
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	flush := http.NewResponseController(w)
	audio, chunk := io.LimitReader(resp.Body, maxSpeechBytes), make([]byte, 32<<10)
	for {
		n, err := audio.Read(chunk)
		if n > 0 {
			if _, err := w.Write(chunk[:n]); err != nil || flush.Flush() != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
