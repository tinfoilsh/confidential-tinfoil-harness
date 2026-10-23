package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestAnonymousInferenceValidatesTheClientKey(t *testing.T) {
	var calls atomic.Int32
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body object
		if r.Method != http.MethodPost || r.URL.Path != "/api/shim/validate-key" || json.NewDecoder(r.Body).Decode(&body) != nil || str(body["api_key"]) != "free_client-key" || body["include_rate_limit"] != true {
			t.Error("did not validate the supplied key using the existing endpoint")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, header := range []string{"Authorization", "X-Forwarded-For", "X-Tinfoil-Client-IP", "X-Tinfoil-Client-IP-Signature"} {
			if r.Header.Get(header) != "" {
				t.Errorf("forwarded an unexpected header: %s", header)
			}
		}
		io.WriteString(w, "OK")
	}))
	defer cp.Close()
	a := &authenticator{client: cp.Client(), controlplane: cp.URL, verify: func(context.Context, string) (string, error) {
		return "", errors.New("invalid Clerk session")
	}}
	r := httptest.NewRequest(http.MethodPost, "/v1/session", nil)
	r.Header.Set("Authorization", "Bearer free_client-key")
	p, err := a.authenticate(r, true)
	if err != nil || !p.Anonymous || p.JWT != "" {
		t.Fatalf("client key was not accepted as an anonymous credential: %v", err)
	}
	for range 2 {
		credential, err := a.inference(context.Background(), p)
		if err != nil || credential.Key != "free_client-key" || credential.Limit.Kind != nil || !credential.Expires.IsZero() {
			t.Fatalf("incorrect credential or invented quota/expiry metadata: err=%v", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("a free key's quota was not read for each request")
	}
	if _, err := a.authenticate(r, false); err == nil {
		t.Fatal("free key authorized a signed-in route")
	}
	r.Header.Set("Authorization", "Bearer invalid-clerk-token")
	if _, err := a.authenticate(r, true); err == nil {
		t.Fatal("invalid Clerk session downgraded to anonymous")
	}
	if _, err := a.inference(context.Background(), &principal{Anonymous: true}); asAPIError(err).HTTP != 401 || calls.Load() != 2 {
		t.Fatal("missing client key reached controlplane")
	}
}

func TestSessionReadsFreeQuotaOncePerRequest(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	var calls atomic.Int32
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(object{"ok": true, "rate_limit": object{"max_requests": 10, "remaining": 10 - calls.Add(1)}})
	}))
	defer cp.Close()
	h.auth.client, h.auth.controlplane = cp.Client(), cp.URL
	for _, remaining := range []int{9, 8} {
		w := postRoute(h, "/v1/session", object{}, "")
		var session struct {
			RateLimit rateLimit `json:"rateLimit"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &session); err != nil || w.Code != 200 || session.RateLimit.Remaining == nil || *session.RateLimit.Remaining != remaining {
			t.Fatalf("session quota: HTTP %d %s", w.Code, w.Body.String())
		}
	}
}

type watchedBody struct{ read atomic.Bool }

func (b *watchedBody) Read([]byte) (int, error) { b.read.Store(true); return 0, io.EOF }

// A turn or an archive may be tens of mebibytes; only a caller who signed in,
// or holds a free key the control plane accepts, gets one read, and only a
// caller who signed in gets an archive read.
func TestLargeBodiesAreReadOnlyAfterAuthentication(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	for _, tc := range []struct {
		path, session, credential string
		code                      int
	}{
		{"/v1/threads/turn", "forged-session", "", 401},
		{"/v1/threads/turn", "free_refused-key", "", 401},
		{"/v1/threads/import", "forged-session", "", 401},
		// Upgrading old device chats requires a signed-in caller.
		{"/v1/threads/upgrade", "free_anonymous-key", "", 401},
		{"/v1/threads/update", "", "", 401},
		{"/v1/threads/update", "free_refused-key", "", 401},
		{"/v1/threads/get", "free_refused-key", "", 401},
		{"/v1/threads/follow", "free_refused-key", "", 401},
		{"/v1/attachments/get-public", "free_refused-key", "", 401},
		{"/v1/speech", "free_refused-key", "", 401},
		{"/v1/import", "free_anonymous-key", "", 401},
		{"/v1/export", "free_anonymous-key", "", 401},
	} {
		body := &watchedBody{}
		r := httptest.NewRequest(http.MethodPost, tc.path, body)
		if tc.session != "" {
			r.Header.Set("Authorization", "Bearer "+tc.session)
		}
		if tc.credential != "" {
			r.Header.Set(inferenceKeyHeader, tc.credential)
		}
		w := httptest.NewRecorder()
		h.routes().ServeHTTP(w, r)
		if w.Code != tc.code || body.read.Load() {
			t.Fatalf("%s as %q: HTTP %d, body read: %v", tc.path, tc.session, w.Code, body.read.Load())
		}
	}
}

func TestAnonymousKeyValidationRefusesWorkBeforeAllocatingThreads(t *testing.T) {
	for _, tc := range []struct {
		name, body, retryAfter string
		status, want           int
		code                   string
	}{
		{"invalid or expired", `{"error":{"code":"invalid_api_key"}}`, "", 401, 401, "UNAUTHENTICATED"},
		{"disabled", `{"error":{"code":"insufficient_quota"}}`, "", 402, 403, "UPSTREAM_REFUSED"},
		{"exhausted", `{"error":{"code":"insufficient_quota"}}`, "", 429, 429, "QUOTA_EXHAUSTED"},
		{"unregistered host", `{"error":"Client unauthorized"}`, "", 401, 503, "UPSTREAM_REFUSED"},
		{"validation unavailable", `{"error":{"message":"Key validation is temporarily unavailable.","type":"server_error"}}`, "2", 503, 503, "UPSTREAM_REFUSED"},
		{"unexpected success body", `{}`, "", 200, 502, "UPSTREAM_REFUSED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var modelCalls atomic.Int32
			h, rows, _ := testChatHarness(t, func(http.ResponseWriter, *http.Request) { modelCalls.Add(1) })
			cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer cp.Close()
			h.auth.client, h.auth.controlplane = cp.Client(), cp.URL
			response := postRoute(h, "/v1/threads/turn", localInput("", object{}), "")
			var refused struct {
				Error apiError `json:"error"`
			}
			json.Unmarshal(response.Body.Bytes(), &refused)
			threads, runs, requests := liveCount(h)
			if response.Code != tc.want || refused.Error.Code != tc.code || response.Header().Get("Retry-After") != tc.retryAfter || modelCalls.Load() != 0 || rows.calls != 0 || threads+runs+requests != 0 {
				t.Fatalf("validation did not refuse work: HTTP %d %s, models=%d, rows=%d, live=%d/%d/%d", response.Code, response.Body.String(), modelCalls.Load(), rows.calls, threads, runs, requests)
			}
			if tc.status == 429 {
				session := postRoute(h, "/v1/session", object{}, "")
				var body object
				json.Unmarshal(session.Body.Bytes(), &body)
				if session.Code != 200 || str(obj(body["rateLimit"])["kind"]) != "free_daily" || integer(obj(body["rateLimit"])["remaining"]) != 0 {
					t.Fatal("exhausted session cannot display its quota")
				}
			}
		})
	}
}

func TestAnonymousReconnectUsesKeyPossessionInsteadOfIP(t *testing.T) {
	model, release := heldModel()
	h, _, _ := testChatHarness(t, model)
	t.Cleanup(release)
	run, err := h.acceptTurn(context.Background(), &principal{Anonymous: true, AnonymousID: hashHex([]byte("free_anonymous-key")), InferenceKey: "free_anonymous-key"}, inferenceCredential{Key: "free_anonymous-key"}, contentKey{}, requestOf[turnRequest](localInput("", object{})))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.log, "TEXT_MESSAGE_CHUNK")
	if err := h.auth.recognize(context.Background(), run.principal); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		key, ip string
		want    int
	}{
		{"free_anonymous-key", "198.51.100.9:1234", 200},
		{"free_other-client", "192.0.2.1:1234", 401},
		{"", "192.0.2.1:1234", 401},
	} {
		body := raw(object{"threadId": run.thread.id, "runId": run.log.id})
		req := httptest.NewRequest(http.MethodPost, "/v1/threads/follow", bytes.NewReader(body))
		req.RemoteAddr = tc.ip
		if tc.key != "" {
			req.Header.Set("Authorization", "Bearer "+tc.key)
		}
		// A refused caller is answered at once; a follower stops listening
		// once it has what the run framed.
		response := httptest.NewRecorder()
		if tc.want == 200 {
			response = replayOf(h, req)
		} else {
			h.routes().ServeHTTP(response, req)
		}
		if response.Code != tc.want {
			t.Fatalf("incorrect reconnect authorization: want %d, got %d", tc.want, response.Code)
		}
		if strings.Contains(response.Body.String(), "free_anonymous-key") {
			t.Fatal("credential leaked into run events")
		}
	}
}

func TestAnonymousRunThatExhaustsItsQuotaReportsNoneLeft(t *testing.T) {
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"message":"Daily request limit reached.","type":"insufficient_quota","code":"insufficient_quota"}}`)
	})
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("", object{}), ""))
	var limit json.RawMessage
	for _, e := range events {
		if e.Type == "STATE_SNAPSHOT" {
			limit = raw(obj(e.Snapshot)["rateLimit"])
		}
	}
	if last := events[len(events)-1]; last.Code != "QUOTA_EXHAUSTED" || string(limit) != `{"kind":"free_daily","maxRequests":0,"remaining":0}` {
		t.Fatalf("run ended %s with rateLimit %s", last.Code, limit)
	}
}

// endlessJSON is a JSON object whose one string never ends.
type endlessJSON struct{ opened bool }

func (b *endlessJSON) Read(p []byte) (int, error) {
	n := 0
	if !b.opened {
		b.opened, n = true, copy(p, `{"pad":"`)
	}
	for i := n; i < len(p); i++ {
		p[i] = 'a'
	}
	return len(p), nil
}

// A body over the size limit is refused as too large, not as malformed, so
// clients need not measure what they send.
func TestOversizedBodiesAreRefusedAsTooLarge(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	r := httptest.NewRequest(http.MethodPost, "/v1/threads/get", &endlessJSON{})
	signIn(r, "alice")
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	var refused struct {
		Error apiError `json:"error"`
	}
	json.Unmarshal(w.Body.Bytes(), &refused)
	if w.Code != http.StatusRequestEntityTooLarge || refused.Error.Code != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("an oversized body: HTTP %d %s", w.Code, w.Body.String())
	}
}

// A chat is screened when its credential is the Tinfoil Chat token the
// control plane mints for subscribers, never a free key.
func TestOnlyChatTokenChatsAreScreened(t *testing.T) {
	for _, subscribed := range []bool{true, false} {
		t.Run(strconv.FormatBool(subscribed), func(t *testing.T) {
			h, _, key := testChatHarness(t, nil)
			drained := screenedBy(t, h)
			r := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(raw(sendInput(key))))
			signIn(r, "alice")
			if !subscribed {
				r.Header.Set(inferenceKeyHeader, "free_anonymous-key")
			}
			w := httptest.NewRecorder()
			h.routes().ServeHTTP(w, r)
			if events := eventsIn(t, w); events[len(events)-1].Type != "RUN_FINISHED" {
				t.Fatalf("the run ended with %+v", events[len(events)-1])
			}
			got := drained()
			if subscribed && (len(got) != 1 || got[0].Credential != chatTokenOf("alice")) || !subscribed && len(got) != 0 {
				t.Fatalf("subscribed %v: the sidecar received %+v", subscribed, got)
			}
		})
	}
}

// A signed-in client mints its chat token, and only one the control plane
// signed for the Tinfoil Chat client and for that caller reaches a model.
func TestChatTokensAreVerified(t *testing.T) {
	var modelCalls atomic.Int32
	h, _, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		modelCalls.Add(1)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
	})
	claim := func(name string, value any) func(map[string]any, jwt.MapClaims) {
		return func(_ map[string]any, claims jwt.MapClaims) { claims[name] = value }
	}
	_, forger, _ := ed25519.GenerateKey(rand.Reader)
	forged, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"iss": testChatIssuer, "sub": "alice", "aud": chatTokenAudience, "client_id": chatTokenClient, "scope": chatTokenScope,
		"product": chatTokenProduct, "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(), "jti": "forged"}).SignedString(forger)
	turn := func(credential string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(raw(sendInput(key))))
		r.Header.Set("Authorization", "Bearer clerk-alice")
		r.Header.Set(inferenceKeyHeader, credential)
		w := httptest.NewRecorder()
		h.routes().ServeHTTP(w, r)
		return w
	}
	for name, credential := range map[string]string{
		"none":                        "",
		"another signer":              forged,
		"another caller's":            mintChatToken("bob", nil),
		"another client's":            mintChatToken("alice", claim("client_id", "third-party-app")),
		"another product's":           mintChatToken("alice", claim("product", "api")),
		"another audience's":          mintChatToken("alice", claim("aud", "https://example.com")),
		"another issuer's":            mintChatToken("alice", claim("iss", "https://evil.example")),
		"expired":                     mintChatToken("alice", claim("exp", time.Now().Add(-time.Hour).Unix())),
		"without the inference scope": mintChatToken("alice", claim("scope", "openid")),
		"not an access token":         mintChatToken("alice", func(header map[string]any, _ jwt.MapClaims) { header["typ"] = "JWT" }),
		"a free key nobody issued":    "free_refused-key",
	} {
		if w := turn(credential); w.Code != 401 || modelCalls.Load() != 0 {
			t.Fatalf("%s: HTTP %d after %d model calls", name, w.Code, modelCalls.Load())
		}
	}
	for _, credential := range []string{chatTokenOf("alice"), "free_anonymous-key"} {
		if events := eventsIn(t, turn(credential)); events[len(events)-1].Type != "RUN_FINISHED" {
			t.Fatalf("a valid credential was refused: %+v", events[len(events)-1])
		}
	}
}

// A signed-in client opens the app before or without chat access, as at its
// hourly cap, which the control plane told it of when it asked for access.
func TestASignedInSessionOpensWithoutChatAccess(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	r := httptest.NewRequest(http.MethodPost, "/v1/session", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer clerk-alice")
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	var session struct {
		User      sessionUser `json:"user"`
		RateLimit object      `json:"rateLimit"`
	}
	json.Unmarshal(w.Body.Bytes(), &session)
	if w.Code != 200 || session.User.ID != "alice" || session.RateLimit["kind"] != nil {
		t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
	}
}

// A free key's validation reports the key's quota, which the session shows
// and each run's spend comes off, as a subscriber's does. A control plane
// that reports none still validates the key.
func TestASignedOutCallersQuotaComesFromItsKey(t *testing.T) {
	var refuse atomic.Bool
	quotaRefusal := `{"error":{"message":"Daily request limit reached.","type":"insufficient_quota","code":"insufficient_quota"}}`
	refusing := func(w http.ResponseWriter) bool {
		if refuse.Load() {
			w.WriteHeader(http.StatusPaymentRequired)
			io.WriteString(w, quotaRefusal)
		}
		return refuse.Load()
	}
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if !refusing(w) {
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n")
		}
	})
	router := routerHarness(t, func(w http.ResponseWriter, r *http.Request) { refusing(w) })
	resets := nowUTC().Add(5 * time.Hour).Format(time.RFC3339)
	cp := httptest.NewServer(unflagged(func(w http.ResponseWriter, r *http.Request) {
		var in object
		json.NewDecoder(r.Body).Decode(&in)
		if in["include_rate_limit"] != true {
			io.WriteString(w, "OK")
			return
		}
		json.NewEncoder(w).Encode(object{"ok": true, "rate_limit": object{"max_requests": 10, "remaining": 7, "max_input_tokens": 1000, "input_tokens_used": 100, "input_tokens_remaining": 900,
			"max_output_tokens": 500, "output_tokens_used": 0, "output_tokens_remaining": 500, "resets_at": resets}})
	}))
	defer cp.Close()
	for _, harness := range []*harness{h, router} {
		harness.auth.client, harness.auth.controlplane = cp.Client(), cp.URL
	}
	var session struct {
		RateLimit json.RawMessage `json:"rateLimit"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", object{}, "").Body.Bytes(), &session)
	if want := `{"kind":"free_daily","maxRequests":10,"remaining":7,"inputTokens":{"max":1000,"used":100,"remaining":900},"outputTokens":{"max":500,"used":0,"remaining":500},"resetsAt":"` + resets + `"}`; string(session.RateLimit) != want {
		t.Fatalf("the session reports %s, want %s", session.RateLimit, want)
	}
	reported := func(e event) rateLimit {
		var metadata runMetadata
		json.Unmarshal(e.Metadata, &metadata)
		return valueOr(metadata.RateLimit, rateLimit{})
	}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("", object{}), ""))
	want := `{"kind":"free_daily","maxRequests":10,"remaining":6,"inputTokens":{"max":1000,"used":112,"remaining":888},"outputTokens":{"max":500,"used":3,"remaining":497},"resetsAt":"` + resets + `"}`
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" || string(raw(reported(last))) != want {
		t.Fatalf("the run ended %s reporting %s, want %s", last.Type, raw(reported(last)), want)
	}
	refuse.Store(true)
	events = eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("", object{}), ""))
	spent := `{"kind":"free_daily","maxRequests":10,"remaining":0,"inputTokens":{"max":1000,"used":100,"remaining":900},"outputTokens":{"max":500,"used":0,"remaining":500},"resetsAt":"` + resets + `"}`
	if last := events[len(events)-1]; last.Code != "QUOTA_EXHAUSTED" || string(raw(reported(last))) != spent {
		t.Fatalf("the refused run ended %s reporting %s", last.Code, raw(reported(last)))
	}
	// A refusal before a run starts, as when a model is asked to describe an
	// image, or from another service reports the same quota.
	var photo bytes.Buffer
	png.Encode(&photo, image.NewRGBA(image.Rect(0, 0, 4, 4)))
	h.offer.models = append(h.offer.models, &model{name: "gemma4-31b", vision: true, context: 8192, client: h.offer.models[0].client})
	described := localInput("", object{})
	described["attachments"] = []any{object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "base64": base64.StdEncoding.EncodeToString(photo.Bytes())}}
	for _, w := range []*httptest.ResponseRecorder{postRoute(h, "/v1/threads/turn", described, ""), uploadRoute(router, "/v1/transcriptions", "memo.mp3", []byte("ID3 audio"), nil, "")} {
		var refused struct {
			Error apiError `json:"error"`
		}
		json.Unmarshal(w.Body.Bytes(), &refused)
		if w.Code != 429 || string(raw(refused.Error.RateLimit)) != spent {
			t.Fatalf("a refusal: HTTP %d %s", w.Code, w.Body.String())
		}
	}
}
