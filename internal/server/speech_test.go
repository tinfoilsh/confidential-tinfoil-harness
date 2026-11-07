package server

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func TestSpeechStreamsTheRoutersAudio(t *testing.T) {
	pcm := []byte{0x01, 0x00, 0xff, 0x7f, 0x00, 0x80}
	var got atomic.Value
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		got.Store(api.Object{"path": r.URL.Path, "auth": r.Header.Get("Authorization"), "body": body})
		w.Header().Set("Content-Type", "audio/pcm")
		w.Write(pcm[:2])
		w.(http.Flusher).Flush()
		w.Write(pcm[2:])
	})
	for _, tc := range []struct {
		user, auth string
		in         api.Object
	}{
		{"", "Bearer free_anonymous-key", api.Object{"text": "  Hello there.  "}},
		{"alice", "Bearer " + chatTokenOf("alice"), api.Object{"text": "Hello there."}},
	} {
		response := postRoute(h, "/v1/speech", tc.in, tc.user)
		if response.Code != 200 || response.Header().Get("Content-Type") != "audio/pcm" || response.Header().Get("Cache-Control") != "no-store" || !bytes.Equal(response.Body.Bytes(), pcm) {
			t.Fatalf("HTTP %d %q: %x", response.Code, response.Header().Get("Content-Type"), response.Body.Bytes())
		}
		want := api.Object{"path": "/v1/audio/speech", "auth": tc.auth, "body": map[string]any{"model": "qwen3-tts", "voice": attach.SpeechVoice, "instructions": attach.SpeechInstructions, "input": "Hello there.", "response_format": "pcm", "stream_format": "audio"}}
		if !reflect.DeepEqual(got.Load(), want) {
			t.Fatalf("router got %v, want %v", got.Load(), want)
		}
	}
	var session struct {
		Features map[string]bool `json:"features"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", api.Object{}, "").Body.Bytes(), &session)
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
		in   api.Object
		want int
		code string
	}{
		{api.Object{"text": strings.Repeat("a", attach.MaxSpeechText+1)}, 413, "PAYLOAD_TOO_LARGE"},
		{api.Object{"text": " \n "}, 400, "BAD_REQUEST"},
		{api.Object{}, 400, "BAD_REQUEST"},
		{api.Object{"text": 42}, 400, "BAD_REQUEST"},
		{api.Object{"messageId": "m1"}, 400, "BAD_REQUEST"},
		{api.Object{"threadId": "chat-1", "messageId": "m1", "key": keyBase64(randomKey())}, 401, "UNAUTHENTICATED"},
		{api.Object{"threadId": "chat-1", "messageId": "m1", "local": true}, 400, "BAD_REQUEST"},
		{api.Object{"threadId": "chat-1", "messageId": "m9", "local": true, "thread": priorThread()}, 404, "THREAD_NOT_FOUND"},
		// A reply of only code has nothing to read, which its client tells apart from a request it got wrong.
		{api.Object{"threadId": "chat-1", "messageId": "m2", "local": true, "thread": api.Object{"messages": []any{api.Object{"id": "m2", "role": "assistant", "content": "```\ncode\n```"}}}}, 400, "NOTHING_TO_READ"},
	} {
		response := postRoute(h, "/v1/speech", tc.in, "")
		var body struct {
			Error api.Error `json:"error"`
		}
		json.Unmarshal(response.Body.Bytes(), &body)
		if response.Code != tc.want || body.Error.Code != tc.code {
			t.Errorf("%v: HTTP %d %s, want %d %s", tc.in, response.Code, body.Error.Code, tc.want, tc.code)
		}
	}
	r := httptest.NewRequest("POST", "/v1/speech", bytes.NewReader(api.Raw(api.Object{"text": "Hello"})))
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	if w.Code != 401 || calls.Load() != 0 {
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
			response := postRoute(h, "/v1/speech", api.Object{"text": "Hello"}, "")
			var body struct {
				Error api.Error `json:"error"`
			}
			json.Unmarshal(response.Body.Bytes(), &body)
			if response.Code != tc.want || body.Error.Code != tc.code || response.Header().Get("Retry-After") != tc.retryAfter {
				t.Fatalf("HTTP %d Retry-After %q: %s", response.Code, response.Header().Get("Retry-After"), response.Body.String())
			}
		})
	}
}

// A message is read aloud from its markdown as it is spoken: its pieces are
// synthesized two at a time and stream back in order as one response, and
// its code never reaches the speech model.
func TestAMessageIsReadAloudInOrderWithoutItsCode(t *testing.T) {
	var mu sync.Mutex
	var asked []string
	second := make(chan struct{})
	another := sync.OnceFunc(func() { close(second) })
	var overlapped atomic.Bool
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input string `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		asked = append(asked, body.Input)
		mu.Unlock()
		if strings.HasPrefix(body.Input, "Sentence 1 ") {
			// The first piece answers only once another is asked for.
			select {
			case <-second:
				overlapped.Store(true)
			case <-time.After(5 * time.Second):
			}
		} else if strings.HasPrefix(body.Input, "Sentence ") {
			another()
		}
		w.Header().Set("Content-Type", "audio/pcm")
		io.WriteString(w, "<"+body.Input+">")
	})
	rows := h.store.Rows.(*memoryRows)
	key := rows.key
	reply := "# Trip plan\n\nPack light, see [the guide](https://guide.test) and [1](https://cite.test). More at https://bare.test.\n\n" +
		"```python\nprint(\"never read\")\n```\n\n![a map](https://img.test/map.png)\n\nUse `rsync` and compute $x^2$ steps.[^1]\n\n[^1]: A footnote nobody hears.\n"
	var long strings.Builder
	for i := 1; i <= 12; i++ {
		fmt.Fprintf(&long, "Sentence %d walks through the plan in enough detail to fill a piece. ", i)
	}
	thread := api.Object{"title": "Trip", "messages": []any{api.Object{"id": "q", "role": "user", "content": "Plan it"}, api.Object{"id": "short", "role": "assistant", "content": reply}, api.Object{"id": "long", "role": "assistant", "content": long.String()}}}
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")] = domain.StoredRow{ID: "chat-1", ETag: "1", Data: api.Raw(thread)}
	read := func(in api.Object, user string) (string, []string) {
		t.Helper()
		mu.Lock()
		asked = nil
		mu.Unlock()
		w := postRoute(h, "/v1/speech", in, user)
		if w.Code != 200 || w.Header().Get("Content-Type") != "audio/pcm" {
			t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
		}
		mu.Lock()
		defer mu.Unlock()
		return w.Body.String(), asked
	}
	// Text a client selected reads by the same rules, without an account.
	for _, tc := range []struct {
		in   api.Object
		user string
	}{{api.Object{"key": keyBase64(key), "threadId": "chat-1", "messageId": "short"}, "alice"}, {api.Object{"text": reply}, ""}} {
		audio, pieces := read(tc.in, tc.user)
		if want := "Trip plan\nPack light, see the guide and . More at . Use rsync and compute x^2 steps."; len(pieces) != 1 || pieces[0] != want || audio != "<"+want+">" {
			t.Fatalf("%v was read as %q", tc.in, pieces)
		}
	}
	for _, in := range []api.Object{{"key": keyBase64(key), "threadId": "chat-1", "messageId": "long"}, {"text": long.String()}} {
		audio, pieces := read(in, "alice")
		slices.SortFunc(pieces, func(a, b string) int { return cmp.Compare(leadingNumber(a), leadingNumber(b)) })
		inOrder := ""
		for _, piece := range pieces {
			inOrder += "<" + piece + ">"
		}
		if len(pieces) < 2 || strings.Join(pieces, " ") != strings.TrimSpace(long.String()) || audio != inOrder || !overlapped.Load() {
			t.Fatalf("%v was asked as %q (overlapping %v) and streamed as %q", in["messageId"], pieces, overlapped.Load(), audio)
		}
	}
	// A device chat's message reads the same, without an account, and
	// without its id, as released clients send it.
	for _, in := range []api.Object{{"local": true, "threadId": "chat-1", "thread": thread, "messageId": "short"}, {"local": true, "thread": thread, "messageId": "short"}} {
		if device, _ := read(in, ""); !strings.HasPrefix(device, "<Trip plan\n") {
			t.Fatalf("the device chat's reply was read as %q", device)
		}
	}
}

// A piece the speech service fails before any of its audio arrives is asked
// for again, so a passing failure does not end the reading.
func TestSpeechAsksAgainForAPieceThatFailedBeforeItsAudio(t *testing.T) {
	var calls atomic.Int32
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "audio/pcm")
		io.WriteString(w, "<audio>")
	})
	if w := postRoute(h, "/v1/speech", api.Object{"text": "Hello there."}, ""); w.Code != 200 || w.Body.String() != "<audio>" || calls.Load() != 2 {
		t.Fatalf("HTTP %d %q after %d calls", w.Code, w.Body.String(), calls.Load())
	}
}

// A test tone: toneSeconds of toneHertz, as the speech service streams it,
// and how far a test lets its length and pitch drift.
const (
	speechSampleRate = 24000
	toneHertz        = 220
	toneSeconds      = 2
	toneTolerance    = 0.05
)

// tone is the test tone as mono 16-bit PCM.
func tone() []byte {
	pcm := []byte{}
	for i := range toneSeconds * speechSampleRate {
		pcm = binary.LittleEndian.AppendUint16(pcm, uint16(int16(8000*math.Sin(2*math.Pi*toneHertz*float64(i)/speechSampleRate))))
	}
	return pcm
}

// pitchOf is the frequency of a tone in mono 16-bit PCM, from how often it
// crosses zero going up.
func pitchOf(pcm []byte) float64 {
	rises := 0
	for i := 2; i+1 < len(pcm); i += 2 {
		if int16(binary.LittleEndian.Uint16(pcm[i-2:])) < 0 && int16(binary.LittleEndian.Uint16(pcm[i:])) >= 0 {
			rises++
		}
	}
	return float64(rises) / (float64(len(pcm)/2) / speechSampleRate)
}

// Audio read twice as fast takes half as long and keeps the voice's pitch,
// even when its samples arrive split across reads.
func TestSpeechReadsFasterWithoutRaisingThePitch(t *testing.T) {
	audio := tone()
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/pcm")
		w.Write(audio[:len(audio)/3|1])
		w.(http.Flusher).Flush()
		w.Write(audio[len(audio)/3|1:])
	})
	w := postRoute(h, "/v1/speech", api.Object{"text": "Hello there.", "speed": 2}, "")
	seconds := float64(w.Body.Len()/2) / speechSampleRate
	if w.Code != 200 || math.Abs(seconds-toneSeconds/2.0) > toneSeconds/2.0*toneTolerance || math.Abs(pitchOf(w.Body.Bytes())-toneHertz) > toneHertz*toneTolerance {
		t.Fatalf("HTTP %d: %.2f s at %.0f Hz", w.Code, seconds, pitchOf(w.Body.Bytes()))
	}
	if w := postRoute(h, "/v1/speech", api.Object{"text": "Hello there.", "speed": 4}, ""); w.Code != 400 {
		t.Fatalf("an unknown speed: HTTP %d", w.Code)
	}
}

// loopingAudioBytes is about a minute of audio, far more than a short
// sentence takes, as a speech model that runs on in a loop streams.
const loopingAudioBytes = 60 * 24000 * 2

// A voice that runs on past what its text takes is cut off soon after.
func TestSpeechCutsOffAVoiceThatRunsOn(t *testing.T) {
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/pcm")
		w.Write(make([]byte, loopingAudioBytes))
	})
	if w := postRoute(h, "/v1/speech", api.Object{"text": "Hello there."}, ""); w.Code != 200 || w.Body.Len() == 0 || w.Body.Len() >= loopingAudioBytes/4 {
		t.Fatalf("HTTP %d streamed %d bytes", w.Code, w.Body.Len())
	}
}

// widePieceCharacters is the most Chinese characters a piece holds: each
// takes about as long to read as two English ones, so a piece holds half as
// many, and never so many that its audio runs past what a piece may take.
const widePieceCharacters = 300

// Chinese text, whose sentences no space follows, is read in pieces of whole
// sentences no longer to read than an English piece.
func TestWideTextIsReadAloudInWholeSentences(t *testing.T) {
	var mu sync.Mutex
	var asked []string
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input string `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		asked = append(asked, body.Input)
		mu.Unlock()
		w.Header().Set("Content-Type", "audio/pcm")
		io.WriteString(w, "a")
	})
	text := strings.Repeat("今天我们讨论旅行计划的每一个细节，包括交通、住宿和预算。", 20)
	if w := postRoute(h, "/v1/speech", api.Object{"text": text}, ""); w.Code != 200 {
		t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	read := ""
	for _, piece := range asked {
		if !strings.HasSuffix(piece, "。") || utf8.RuneCountInString(piece) > widePieceCharacters {
			t.Fatalf("the text was read in pieces %q", asked)
		}
		read += strings.ReplaceAll(piece, " ", "")
	}
	if len(asked) < 2 || utf8.RuneCountInString(read) != utf8.RuneCountInString(text) {
		t.Fatalf("the text was read in pieces %q", asked)
	}
}

// A piece that fails once audio has streamed cuts the response short, so
// its client does not take the audio for the whole message.
func TestSpeechThatFailsPartwayEndsIncomplete(t *testing.T) {
	h := routerHarness(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input string `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if !strings.HasPrefix(body.Input, "Sentence 1 ") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "audio/pcm")
		io.WriteString(w, "<first>")
	})
	server := httptest.NewServer(h.routes())
	t.Cleanup(server.Close)
	var long strings.Builder
	for i := 1; i <= 12; i++ {
		fmt.Fprintf(&long, "Sentence %d walks through the plan in enough detail to fill a piece. ", i)
	}
	req, _ := http.NewRequest("POST", server.URL+"/v1/speech", bytes.NewReader(api.Raw(api.Object{"text": long.String()})))
	signIn(req, "alice")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	audio, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(audio) != "<first>" || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("HTTP %d streamed %q and ended with %v", resp.StatusCode, audio, err)
	}
}

// leadingNumber is the number a test sentence starts with.
func leadingNumber(piece string) int {
	n, _ := strconv.Atoi(strings.Fields(piece)[1])
	return n
}
