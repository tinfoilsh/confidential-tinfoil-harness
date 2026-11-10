package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
)

// A Clerk session token names its caller for ten minutes after it expires,
// so one sent with a chat that takes minutes to arrive is still accepted,
// and is refused after that.
func TestASessionTokenIsAcceptedForAWhileAfterItExpires(t *testing.T) {
	const issuer = "https://clerk.example"
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := auth.Configured(auth.Config{Verify: auth.SessionVerifier(issuer, func(context.Context) jwt.Keyfunc {
		return func(*jwt.Token) (any, error) { return &key.PublicKey, nil }
	})})
	expired := func(ago time.Duration) *http.Request {
		claims := jwt.RegisteredClaims{Issuer: issuer, Subject: "alice", ExpiresAt: jwt.NewNumericDate(time.Now().Add(-ago))}
		token, err := jwt.NewWithClaims(jwt.SigningMethodES256, claims).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, "/v1/session", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		return r
	}
	if p, err := a.Authenticate(expired(9*time.Minute), false); err != nil || p.ID != "alice" {
		t.Fatalf("a session token that expired nine minutes ago was refused: %v", err)
	}
	if _, err := a.Authenticate(expired(11*time.Minute), false); api.AsError(err).HTTP != http.StatusUnauthorized || api.AsError(err).Code != "UNAUTHENTICATED" {
		t.Fatalf("a session token that expired eleven minutes ago was answered %v", err)
	}
}

func TestAnonymousInferenceValidatesTheClientKey(t *testing.T) {
	var calls atomic.Int32
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body api.Object
		if r.Method != http.MethodPost || r.URL.Path != "/api/shim/validate-key" || json.NewDecoder(r.Body).Decode(&body) != nil || api.Str(body["api_key"]) != "free_client-key" || body["include_rate_limit"] != true {
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
	a := auth.Configured(auth.Config{Client: cp.Client(), Controlplane: cp.URL, Verify: func(context.Context, string) (string, error) {
		return "", errors.New("invalid Clerk session")
	}})
	r := httptest.NewRequest(http.MethodPost, "/v1/session", nil)
	r.Header.Set("Authorization", "Bearer free_client-key")
	p, err := a.Authenticate(r, true)
	if err != nil || !p.Anonymous || p.JWT != "" {
		t.Fatalf("client key was not accepted as an anonymous credential: %v", err)
	}
	for range 2 {
		credential, err := a.Inference(context.Background(), p)
		if err != nil || credential.Key != "free_client-key" || credential.Limit.Kind != nil || !credential.Expires.IsZero() || credential.APIKey {
			t.Fatalf("incorrect credential or invented quota/expiry metadata: err=%v", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("a free key's quota was not read for each request")
	}
	if _, err := a.Authenticate(r, false); err == nil {
		t.Fatal("free key authorized a signed-in route")
	}
	r.Header.Set("Authorization", "Bearer invalid-clerk-token")
	if _, err := a.Authenticate(r, true); err == nil {
		t.Fatal("invalid Clerk session downgraded to anonymous")
	}
	if _, err := a.Inference(context.Background(), &auth.Principal{Anonymous: true}); api.AsError(err).HTTP != 401 || calls.Load() != 2 {
		t.Fatal("missing client key reached controlplane")
	}
}

// An API key signs in as a signed-out caller does with a free key, checked by
// the control plane, and is not a signed-in client's chat access. One the
// control plane refuses for its spent budget is refused, not shown a free
// key's daily quota.
func TestAnAPIKeySignsInAsASignedOutCaller(t *testing.T) {
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body api.Object
		json.NewDecoder(r.Body).Decode(&body)
		if api.Str(body["api_key"]) == "tk_spent-key" {
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":{"message":"API key output token limit reached","code":"insufficient_quota"}}`)
			return
		}
		io.WriteString(w, `{"ok":true}`)
	}))
	defer cp.Close()
	a := auth.Configured(auth.Config{Client: cp.Client(), Controlplane: cp.URL, Verify: func(_ context.Context, token string) (string, error) {
		if token != "clerk-alice" {
			return "", errors.New("invalid Clerk session")
		}
		return "alice", nil
	}})
	signedIn := func(key string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/v1/session", nil)
		r.Header.Set("Authorization", "Bearer "+key)
		return r
	}
	p, err := a.Caller(signedIn("tk_api-key"), true)
	if err != nil || !p.Anonymous || p.InferenceKey != "tk_api-key" {
		t.Fatalf("an API key did not sign in as a signed-out caller: %+v %v", p, err)
	}
	if credential, err := a.Inference(context.Background(), p); err != nil || credential.Key != "tk_api-key" || !credential.APIKey || credential.Limit.Kind != nil {
		t.Fatalf("an API key's credential was %+v, %v", credential, err)
	}
	if _, err := a.Authenticate(signedIn("tk_api-key"), false); err == nil {
		t.Fatal("an API key reached a signed-in route")
	}
	if _, err := a.Caller(signedIn("admin_key"), true); err == nil {
		t.Fatal("an admin key signed in")
	}
	if _, err := a.Caller(signedIn("tk_spent-key"), true); api.AsError(err).HTTP != http.StatusTooManyRequests {
		t.Fatalf("a spent API key was answered %v", err)
	}
	minted := signedIn("clerk-alice")
	minted.Header.Set(auth.InferenceKeyHeader, "tk_api-key")
	client, err := a.Caller(minted, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Inference(context.Background(), client); api.AsError(err).HTTP != http.StatusUnauthorized {
		t.Fatalf("a signed-in client used an API key as its chat access: %v", err)
	}
}

func TestSessionReadsFreeQuotaOncePerRequest(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	var calls atomic.Int32
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(api.Object{"ok": true, "rate_limit": api.Object{"max_requests": 10, "remaining": 10 - calls.Add(1)}})
	}))
	defer cp.Close()
	h.useAuth(testAuth(cp))
	for _, remaining := range []int{9, 8} {
		w := postRoute(h, "/v1/session", api.Object{}, "")
		var session struct {
			RateLimit api.RateLimit `json:"rateLimit"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &session); err != nil || w.Code != 200 || session.RateLimit.Remaining == nil || *session.RateLimit.Remaining != remaining {
			t.Fatalf("session quota: HTTP %d %s", w.Code, w.Body.String())
		}
	}
}

type watchedBody struct{ read atomic.Bool }

func (b *watchedBody) Read([]byte) (int, error) { b.read.Store(true); return 0, io.EOF }

// A turn or an archive may be tens of mebibytes; only a caller who signed in,
// or holds a free key the control plane accepts, gets one read.
func TestLargeBodiesAreReadOnlyAfterAuthentication(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	for _, tc := range []struct {
		path, session, credential string
		code                      int
	}{
		{"/v1/threads/turn", "forged-session", "", 401},
		{"/v1/threads/turn", "free_refused-key", "", 401},
		{"/v1/threads/import", "forged-session", "", 401},
		{"/v1/threads/upgrade", "free_refused-key", "", 401},
		{"/v1/threads/update", "", "", 401},
		{"/v1/threads/update", "free_refused-key", "", 401},
		{"/v1/threads/get", "free_refused-key", "", 401},
		{"/v1/threads/follow", "forged-session", "", 401},
		{"/v1/attachments/get-public", "free_refused-key", "", 401},
		{"/v1/speech", "free_refused-key", "", 401},
		{"/v1/import", "forged-session", "", 401},
		{"/v1/import", "free_refused-key", "", 401},
		{"/v1/export", "free_refused-key", "", 401},
	} {
		body := &watchedBody{}
		r := httptest.NewRequest(http.MethodPost, tc.path, body)
		if tc.session != "" {
			r.Header.Set("Authorization", "Bearer "+tc.session)
		}
		if tc.credential != "" {
			r.Header.Set(auth.InferenceKeyHeader, tc.credential)
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
			h.useAuth(testAuth(cp))
			response := postRoute(h, "/v1/threads/turn", localInput("", api.Object{}), "")
			var refused struct {
				Error api.Error `json:"error"`
			}
			json.Unmarshal(response.Body.Bytes(), &refused)
			threads, runs, requests := liveCount(h)
			if response.Code != tc.want || refused.Error.Code != tc.code || response.Header().Get("Retry-After") != tc.retryAfter || modelCalls.Load() != 0 || rows.calls != 0 || threads+runs+requests != 0 {
				t.Fatalf("validation did not refuse work: HTTP %d %s, models=%d, rows=%d, live=%d/%d/%d", response.Code, response.Body.String(), modelCalls.Load(), rows.calls, threads, runs, requests)
			}
			if tc.status == 429 {
				session := postRoute(h, "/v1/session", api.Object{}, "")
				var body api.Object
				json.Unmarshal(session.Body.Bytes(), &body)
				if session.Code != 200 || api.Str(api.Obj(body["rateLimit"])["kind"]) != "free_daily" || api.Int(api.Obj(body["rateLimit"])["remaining"]) != 0 {
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
	run, err := h.engine.AcceptTurn(context.Background(), &auth.Principal{Anonymous: true, AnonymousID: api.HashHex([]byte("free_anonymous-key")), InferenceKey: "free_anonymous-key"}, auth.Credential{Key: "free_anonymous-key"}, auth.ContentKey{}, requestOf[engine.TurnRequest](localInput("", api.Object{})))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.Log, "TEXT_MESSAGE_CHUNK")
	for _, tc := range []struct {
		key, ip string
		want    int
	}{
		{"free_anonymous-key", "198.51.100.9:1234", 200},
		{"free_other-client", "192.0.2.1:1234", 404},
		{"", "192.0.2.1:1234", 401},
	} {
		body := api.Raw(api.Object{"threadId": run.Thread.ID, "runId": run.Log.ID})
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

// A signed-out caller's free key expires at the end of its day, and its
// client fetches another. The expired key still follows and stops the run
// it started, without the control plane being asked about it, but starts no
// new turn.
func TestAnExpiredFreeKeyStillReachesTheRunItStarted(t *testing.T) {
	model, release := heldModel()
	h, _, _ := testChatHarness(t, model)
	t.Cleanup(release)
	var expired atomic.Bool
	var validations atomic.Int32
	cp := httptest.NewServer(unflagged(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/shim/validate-key" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		validations.Add(1)
		if expired.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"error":{"code":"invalid_api_key"}}`)
			return
		}
		io.WriteString(w, "OK")
	}))
	t.Cleanup(cp.Close)
	h.useAuth(testAuth(cp))
	const freeKey = "free_yesterdays-key"
	request := func(path string, in api.Object) *http.Request {
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(api.Raw(in)))
		r.Header.Set("Authorization", "Bearer "+freeKey)
		return r
	}
	server := httptest.NewServer(h.routes())
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	turn, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/threads/turn", bytes.NewReader(api.Raw(localInput("", api.Object{}))))
	turn.Header.Set("Authorization", "Bearer "+freeKey)
	streamed, err := server.Client().Do(turn)
	if err != nil {
		t.Fatal(err)
	}
	defer streamed.Body.Close()
	frames := bufio.NewScanner(streamed.Body)
	next := func() (e event) {
		for frames.Scan() {
			if data, ok := strings.CutPrefix(frames.Text(), "data: "); ok {
				if err := json.Unmarshal([]byte(data), &e); err != nil {
					t.Fatal(err)
				}
				return e
			}
		}
		return e
	}
	started := next()
	if streamed.StatusCode != 200 || started.Type != "RUN_STARTED" || validations.Load() != 1 {
		t.Fatalf("the turn answered HTTP %d with %+v after %d validations", streamed.StatusCode, started, validations.Load())
	}
	// A fresh authenticator stands in for one that has since forgotten it
	// validated the key.
	expired.Store(true)
	h.useAuth(testAuth(cp))
	run := api.Object{"threadId": started.ThreadID, "runId": started.RunID}
	if events := eventsIn(t, replayOf(h, request("/v1/threads/follow", run))); events[0].RunID != started.RunID {
		t.Fatalf("following the run replayed %+v", events[0])
	}
	stopped := httptest.NewRecorder()
	h.routes().ServeHTTP(stopped, request("/v1/threads/cancel", run))
	if stopped.Code != 200 {
		t.Fatalf("stopping the run: HTTP %d %s", stopped.Code, stopped.Body.String())
	}
	var last event
	for e := next(); e.Type != ""; e = next() {
		last = e
	}
	var metadata domain.RunMetadata
	if last.Type != "RUN_FINISHED" || json.Unmarshal(last.Metadata, &metadata) != nil || !domain.ValueOr(metadata.Cancelled, false) || validations.Load() != 1 {
		t.Fatalf("the run ended with %+v after %d validations", last, validations.Load())
	}
	refused := httptest.NewRecorder()
	h.routes().ServeHTTP(refused, request("/v1/threads/turn", localInput("", api.Object{})))
	if refused.Code != 401 || validations.Load() != 2 {
		t.Fatalf("a turn with the expired key: HTTP %d after %d validations", refused.Code, validations.Load())
	}
}

func TestAnonymousRunThatExhaustsItsQuotaReportsNoneLeft(t *testing.T) {
	h, _, _ := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"message":"Daily request limit reached.","type":"insufficient_quota","code":"insufficient_quota"}}`)
	})
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("", api.Object{}), ""))
	var limit json.RawMessage
	for _, e := range events {
		if e.Type == "STATE_SNAPSHOT" {
			limit = api.Raw(api.Obj(e.Snapshot)["rateLimit"])
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
		Error api.Error `json:"error"`
	}
	json.Unmarshal(w.Body.Bytes(), &refused)
	if w.Code != http.StatusRequestEntityTooLarge || refused.Error.Code != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("an oversized body: HTTP %d %s", w.Code, w.Body.String())
	}
}

// A chat is screened when its credential is the Tinfoil Chat token the
// control plane mints for subscribers, or an API key, never a free key.
func TestOnlyChatTokenAndAPIKeyChatsAreScreened(t *testing.T) {
	for name, tc := range map[string]struct {
		sign     func(*http.Request)
		local    bool
		screened string // the credential the sidecar is sent, if any
	}{
		"subscriber": {func(r *http.Request) { signIn(r, "alice") }, false, chatTokenOf("alice")},
		"signed-in free key": {func(r *http.Request) {
			signIn(r, "alice")
			r.Header.Set(auth.InferenceKeyHeader, "free_anonymous-key")
		}, false, ""},
		"signed-out free key": {func(r *http.Request) { signIn(r, "") }, true, ""},
		"api key":             {func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+testAPIKey) }, true, testAPIKey},
	} {
		t.Run(name, func(t *testing.T) {
			h, _, key := testChatHarness(t, nil)
			drained := screenedBy(t, h)
			in := sendInput(key)
			if tc.local {
				in = localInput("", nil)
			}
			r := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(api.Raw(in)))
			tc.sign(r)
			w := httptest.NewRecorder()
			h.routes().ServeHTTP(w, r)
			if events := eventsIn(t, w); events[len(events)-1].Type != "RUN_FINISHED" {
				t.Fatalf("the run ended with %+v", events[len(events)-1])
			}
			got := drained()
			if tc.screened == "" && len(got) != 0 || tc.screened != "" && (len(got) != 1 || got[0].Credential != tc.screened || got[0].ConversationID != api.Str(in["threadId"])) {
				t.Fatalf("the sidecar received %+v", got)
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
	forged, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"iss": testChatIssuer, "sub": "alice", "aud": auth.ChatTokenAudience, "client_id": auth.ChatTokenClient, "scope": auth.ChatTokenScope,
		"product": auth.ChatTokenProduct, "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(), "jti": "forged"}).SignedString(forger)
	turn := func(credential string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(api.Raw(sendInput(key))))
		r.Header.Set("Authorization", "Bearer clerk-alice")
		r.Header.Set(auth.InferenceKeyHeader, credential)
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
		RateLimit api.Object  `json:"rateLimit"`
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
	resets := api.Now().Add(5 * time.Hour).Format(time.RFC3339)
	cp := httptest.NewServer(unflagged(func(w http.ResponseWriter, r *http.Request) {
		var in api.Object
		json.NewDecoder(r.Body).Decode(&in)
		if in["include_rate_limit"] != true {
			io.WriteString(w, "OK")
			return
		}
		json.NewEncoder(w).Encode(api.Object{"ok": true, "rate_limit": api.Object{"max_requests": 10, "remaining": 7, "max_input_tokens": 1000, "input_tokens_used": 100, "input_tokens_remaining": 900,
			"max_output_tokens": 500, "output_tokens_used": 0, "output_tokens_remaining": 500, "resets_at": resets}})
	}))
	defer cp.Close()
	for _, harness := range []*harness{h, router} {
		harness.useAuth(testAuth(cp))
	}
	var session struct {
		RateLimit json.RawMessage `json:"rateLimit"`
	}
	json.Unmarshal(postRoute(h, "/v1/session", api.Object{}, "").Body.Bytes(), &session)
	if want := `{"kind":"free_daily","maxRequests":10,"remaining":7,"inputTokens":{"max":1000,"used":100,"remaining":900},"outputTokens":{"max":500,"used":0,"remaining":500},"resetsAt":"` + resets + `"}`; string(session.RateLimit) != want {
		t.Fatalf("the session reports %s, want %s", session.RateLimit, want)
	}
	reported := func(e event) api.RateLimit {
		var metadata domain.RunMetadata
		json.Unmarshal(e.Metadata, &metadata)
		return domain.ValueOr(metadata.RateLimit, api.RateLimit{})
	}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("", api.Object{}), ""))
	want := `{"kind":"free_daily","maxRequests":10,"remaining":6,"inputTokens":{"max":1000,"used":112,"remaining":888},"outputTokens":{"max":500,"used":3,"remaining":497},"resetsAt":"` + resets + `"}`
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" || string(api.Raw(reported(last))) != want {
		t.Fatalf("the run ended %s reporting %s, want %s", last.Type, api.Raw(reported(last)), want)
	}
	refuse.Store(true)
	events = eventsIn(t, postRoute(h, "/v1/threads/turn", localInput("", api.Object{}), ""))
	spent := `{"kind":"free_daily","maxRequests":10,"remaining":0,"inputTokens":{"max":1000,"used":100,"remaining":900},"outputTokens":{"max":500,"used":0,"remaining":500},"resetsAt":"` + resets + `"}`
	if last := events[len(events)-1]; last.Code != "QUOTA_EXHAUSTED" || string(api.Raw(reported(last))) != spent {
		t.Fatalf("the refused run ended %s reporting %s", last.Code, api.Raw(reported(last)))
	}
	// A refusal before a run starts, as when a model is asked to describe an
	// image, or from another service reports the same quota.
	var photo bytes.Buffer
	png.Encode(&photo, image.NewRGBA(image.Rect(0, 0, 4, 4)))
	h.models.Offer.Set(append(h.models.Offer.Get(), &inference.Model{Name: "gemma4-31b", Vision: true, Endpoint: h.models.Offer.Get()[0].Endpoint, Client: h.models.Offer.Get()[0].Client}))
	described := localInput("", api.Object{})
	described["attachments"] = []any{api.Object{"id": "att_photo", "kind": "image", "fileName": "photo.png", "mimeType": "image/png", "base64": base64.StdEncoding.EncodeToString(photo.Bytes())}}
	for _, w := range []*httptest.ResponseRecorder{postRoute(h, "/v1/threads/turn", described, ""), uploadRoute(router, "/v1/transcriptions", "memo.mp3", []byte("ID3 audio"), nil, "")} {
		var refused struct {
			Error api.Error `json:"error"`
		}
		json.Unmarshal(w.Body.Bytes(), &refused)
		if w.Code != 429 || string(api.Raw(refused.Error.RateLimit)) != spent {
			t.Fatalf("a refusal: HTTP %d %s", w.Code, w.Body.String())
		}
	}
}
