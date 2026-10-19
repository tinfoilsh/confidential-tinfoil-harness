package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSpeechStreamsTheRoutersAudio(t *testing.T) {
	pcm := []byte{0x01, 0x00, 0xff, 0x7f, 0x00, 0x80}
	var got atomic.Value
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		got.Store(object{"path": r.URL.Path, "auth": r.Header.Get("Authorization"), "body": body})
		w.Header().Set("Content-Type", "audio/pcm")
		w.Write(pcm[:2])
		w.(http.Flusher).Flush()
		w.Write(pcm[2:])
	})
	for _, tc := range []struct {
		user, auth string
		in         object
	}{
		{"", "Bearer free_anonymous-key", object{"text": "  Hello there.  "}},
		{"alice", "Bearer inference-alice", object{"text": "Hello there."}},
	} {
		response := postRoute(h, "/v1/speech", tc.in, tc.user)
		if response.Code != 200 || response.Header().Get("Content-Type") != "audio/pcm" || response.Header().Get("Cache-Control") != "no-store" || !bytes.Equal(response.Body.Bytes(), pcm) {
			t.Fatalf("HTTP %d %q: %x", response.Code, response.Header().Get("Content-Type"), response.Body.Bytes())
		}
		want := object{"path": "/v1/audio/speech", "auth": tc.auth, "body": map[string]any{"model": "qwen3-tts", "voice": speechVoice, "instructions": speechInstructions, "input": "Hello there.", "response_format": "pcm", "stream_format": "audio"}}
		if !reflect.DeepEqual(got.Load(), want) {
			t.Fatalf("router got %v, want %v", got.Load(), want)
		}
	}
	var session struct {
		Features map[string]bool `json:"features"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", object{}, "").Body.Bytes(), &session)
	if !session.Features["speech"] {
		t.Fatal("the session does not advertise speech")
	}
}

func TestSpeechRefusesWhatTheClientWouldNotSend(t *testing.T) {
	var calls atomic.Int32
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "audio/pcm")
		w.Write([]byte{0, 0})
	})
	for _, tc := range []struct {
		in   object
		want int
	}{
		{object{"text": strings.Repeat("é", maxSpeechCharacters)}, 200},
		{object{"text": strings.Repeat("é", maxSpeechCharacters+1)}, 400},
		{object{"text": " \n "}, 400},
		{object{}, 400},
		{object{"text": 42}, 400},
	} {
		if response := postRoute(h, "/v1/speech", tc.in, ""); response.Code != tc.want {
			t.Errorf("%v: HTTP %d, want %d", tc.in, response.Code, tc.want)
		}
	}
	r := httptest.NewRequest("POST", "/v1/speech", bytes.NewReader(raw(object{"text": "Hello"})))
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	if w.Code != 401 || calls.Load() != 1 {
		t.Fatalf("speech without a credential: HTTP %d, router calls %d", w.Code, calls.Load())
	}
}

func TestSpeechPassesOnRefusalsWithTheirDelay(t *testing.T) {
	for _, tc := range []struct {
		name, contentType, retryAfter, body string
		status, want                        int
		code                                string
	}{
		{"rate limited", "application/json", "4", `{"error":{"message":"Rate limit reached for requests. Retry after 4 seconds.","type":"rate_limit_error","code":"rate_limit_exceeded"}}`, 429, 429, "RATE_LIMITED"},
		{"exhausted quota", "application/json", "", `{"error":{"message":"Daily request limit reached.","type":"insufficient_quota","code":"insufficient_quota"}}`, 429, 429, "QUOTA_EXHAUSTED"},
		{"not audio", "audio/wav", "", "RIFF", 200, 502, "UPSTREAM_REFUSED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			response := postRoute(h, "/v1/speech", object{"text": "Hello"}, "")
			var body struct {
				Error apiError `json:"error"`
			}
			json.Unmarshal(response.Body.Bytes(), &body)
			if response.Code != tc.want || body.Error.Code != tc.code || response.Header().Get("Retry-After") != tc.retryAfter {
				t.Fatalf("HTTP %d Retry-After %q: %s", response.Code, response.Header().Get("Retry-After"), response.Body.String())
			}
		})
	}
}
