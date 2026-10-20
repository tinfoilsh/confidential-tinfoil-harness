package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAnonymousInferenceValidatesTheClientKey(t *testing.T) {
	var calls atomic.Int32
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body object
		if r.Method != http.MethodPost || r.URL.Path != "/api/shim/validate-key" || json.NewDecoder(r.Body).Decode(&body) != nil || str(body["api_key"]) != "free_client-key" {
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
	if calls.Load() != 2 || len(a.credentials) != 0 {
		t.Fatal("client keys were cached as server-issued credentials")
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

type watchedBody struct{ read atomic.Bool }

func (b *watchedBody) Read([]byte) (int, error) { b.read.Store(true); return 0, io.EOF }

// A turn or an import may be tens of mebibytes; only a caller who signed in
// gets them read.
func TestLargeBodiesAreReadOnlyAfterAuthentication(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	for _, path := range []string{"/v1/threads/turn", "/v1/threads/import", "/v1/threads/upgrade"} {
		body := &watchedBody{}
		r := httptest.NewRequest(http.MethodPost, path, body)
		r.Header.Set("Authorization", "Bearer forged-session")
		w := httptest.NewRecorder()
		h.routes().ServeHTTP(w, r)
		if w.Code != 401 || body.read.Load() {
			t.Fatalf("%s: HTTP %d, body read: %v", path, w.Code, body.read.Load())
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
	run, err := h.acceptTurn(context.Background(), &principal{Anonymous: true, AnonymousID: hashHex([]byte("free_anonymous-key")), InferenceKey: "free_anonymous-key"}, contentKey{}, localInput("", object{}))
	if err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, run.log, "TEXT_MESSAGE_CHUNK")
	for _, tc := range []struct {
		key, ip string
		want    int
	}{
		{"free_anonymous-key", "198.51.100.9:1234", 200},
		{"free_other-client", "192.0.2.1:1234", 404},
		{"", "192.0.2.1:1234", 404},
	} {
		body := raw(object{"threadId": run.thread.id, "runId": run.log.id})
		req := httptest.NewRequest(http.MethodPost, "/v1/threads/follow", bytes.NewReader(body))
		req.RemoteAddr = tc.ip
		if tc.key != "" {
			req.Header.Set("Authorization", "Bearer "+tc.key)
		}
		response := replayOf(h, req)
		if response.Code != tc.want {
			t.Fatalf("incorrect reconnect authorization: want %d, got %d", tc.want, response.Code)
		}
		if strings.Contains(response.Body.String(), "free_anonymous-key") {
			t.Fatal("credential leaked into run events")
		}
	}
}

func subscriberHarness(t *testing.T, controlplane http.HandlerFunc) (*harness, contentKey) {
	t.Helper()
	h, _, key := testChatHarness(t, nil)
	cp := httptest.NewServer(controlplane)
	t.Cleanup(cp.Close)
	h.auth.client, h.auth.controlplane = cp.Client(), cp.URL
	delete(h.auth.credentials, "user:alice")
	h.syncAPI = &syncClient{URL: "https://sync.example", Client: &http.Client{Transport: syncTestTransport(func(*http.Request) (*http.Response, error) {
		return syncTestResponse(404, object{"ok": false, "code": "NOT_FOUND"})
	})}}
	return h, key
}

func TestSubscribersHaveTokenBudgetsButNoRequestQuota(t *testing.T) {
	resets := nowUTC().Add(30 * time.Minute).Format(time.RFC3339)
	h, key := subscriberHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat/token" || r.Header.Get("Authorization") != "Bearer clerk-alice" {
			t.Errorf("unexpected exchange %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(object{"key": "subscriber-jwt", "expires_at": nowUTC().Add(15 * time.Minute).Format(time.RFC3339), "is_free_tier": false,
			"rate_limit": object{"max_input_tokens": 1000000, "input_tokens_used": 250000, "input_tokens_remaining": 750000, "max_output_tokens": 100000, "output_tokens_used": 100000, "output_tokens_remaining": 0, "resets_at": resets}})
	})
	var session struct {
		RateLimit json.RawMessage `json:"rateLimit"`
	}
	response := postRoute(h, "/v1/session", object{}, "alice")
	json.Unmarshal(response.Body.Bytes(), &session)
	want := `{"kind":"hourly","inputTokens":{"max":1000000,"used":250000,"remaining":750000},"outputTokens":{"max":100000,"used":100000,"remaining":0},"resetsAt":"` + resets + `"}`
	if response.Code != 200 || string(session.RateLimit) != want {
		t.Fatalf("HTTP %d rateLimit %s, want %s", response.Code, session.RateLimit, want)
	}
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", sendInput(key), "alice"))
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" {
		t.Fatalf("a subscriber without a request quota was refused: %+v", last)
	}
	if limit := h.auth.currentLimit(&principal{ID: "alice"}); limit.MaxRequests != nil || limit.Remaining != nil {
		t.Fatal("a finished run invented a request quota")
	}
}

func TestOnlyAMissingSubscriptionFallsBackToAFreeKey(t *testing.T) {
	for _, tc := range []struct {
		name, body, retryAfter string
		status, http, wait     int
		code                   string
	}{
		{"suspended", `{"error":"account is suspended","code":"FORBIDDEN"}`, "", 403, 403, 0, "UPSTREAM_REFUSED"},
		{"provisioning", `{"error":"account provisioning is in progress","code":"ACCOUNT_PROVISIONING"}`, "5", 503, 503, 5, "UPSTREAM_REFUSED"},
		{"exchange rate limit", `{"error":"rate_limited","error_description":"too many requests, please slow down"}`, "30", 429, 429, 30, "RATE_LIMITED"},
		{"server error", `{"error":"failed to verify subscription","code":"INTERNAL_ERROR"}`, "", 500, 502, 0, "UPSTREAM_REFUSED"},
		{"malformed credential", `{"expires_at":"2099-01-01T00:00:00Z"}`, "", 200, 502, 0, "UPSTREAM_REFUSED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var freeKeys atomic.Int32
			h, _ := subscriberHarness(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/keys/chat" {
					freeKeys.Add(1)
				}
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			_, err := h.auth.inference(context.Background(), &principal{ID: "alice", JWT: "clerk-alice"})
			e := asAPIError(err)
			if err == nil || e.HTTP != tc.http || e.Code != tc.code || e.RetryAfter != tc.wait || freeKeys.Load() != 0 {
				t.Fatalf("got %v (HTTP %d %s retry %d), free key exchanges %d", err, e.HTTP, e.Code, e.RetryAfter, freeKeys.Load())
			}
		})
	}
	resets := nowUTC().Add(5 * time.Hour).Format(time.RFC3339)
	h, _ := subscriberHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer clerk-alice" {
			t.Error("exchanged without the Clerk session")
		}
		if r.URL.Path == "/api/chat/token" {
			w.WriteHeader(http.StatusPaymentRequired)
			io.WriteString(w, `{"error":"an active Tinfoil Chat subscription is required","code":"PAYMENT_REQUIRED"}`)
			return
		}
		json.NewEncoder(w).Encode(object{"key": "free_key", "expires_at": resets, "is_free_tier": true,
			"rate_limit": object{"max_requests": 10, "remaining": 7, "max_input_tokens": 500000, "input_tokens_used": 1000, "input_tokens_remaining": 499000, "max_output_tokens": 0, "output_tokens_used": 0, "output_tokens_remaining": 0, "resets_at": resets}})
	})
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	credential, err := h.auth.inference(context.Background(), p)
	want := rateLimit{Kind: "free_daily", MaxRequests: new(10), Remaining: new(7), InputTokens: &tokenBudget{500000, 1000, 499000}, ResetsAt: resets}
	if err != nil || credential.Key != "free_key" || !reflect.DeepEqual(credential.Limit, want) {
		t.Fatalf("free key %q limit %+v, err %v", credential.Key, credential.Limit, err)
	}
	if limit := h.auth.consumed(p); *limit.Remaining != 6 || *credential.Limit.Remaining != 7 {
		t.Fatal("a consumed request was not counted once")
	}
}

func TestHourlyCapIsRememberedUntilItResets(t *testing.T) {
	resets := nowUTC().Add(30 * time.Minute).Truncate(time.Second).Format(time.RFC3339)
	var exchanges atomic.Int32
	h, key := subscriberHarness(t, func(w http.ResponseWriter, r *http.Request) {
		exchanges.Add(1)
		if r.URL.Path != "/api/chat/token" {
			t.Errorf("a capped subscriber was downgraded to %s", r.URL.Path)
		}
		w.Header().Set("Retry-After", "1800")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(object{"error": "You've reached your hourly usage limit. Please try again later.", "code": "HOURLY_LIMIT_REACHED", "limit": "output", "resets_at": resets,
			"rate_limit": object{"max_input_tokens": 1000000, "input_tokens_used": 1000, "input_tokens_remaining": 999000, "max_output_tokens": 100000, "output_tokens_used": 100000, "output_tokens_remaining": 0, "resets_at": resets}})
	})
	p := &principal{ID: "alice", JWT: "clerk-alice"}
	for range 2 {
		_, err := h.auth.inference(context.Background(), p)
		if e := asAPIError(err); e.HTTP != 429 || e.Code != "QUOTA_EXHAUSTED" || e.Kind != "hourly" || e.ResetsAt != resets || e.RetryAfter < 1790 || e.RetryAfter > 1800 || e.Message != "You've reached your hourly usage limit. Please try again later." {
			t.Fatalf("hourly cap: %+v", e)
		}
	}
	turn := postRoute(h, "/v1/threads/turn", sendInput(key), "alice")
	var session struct {
		RateLimit json.RawMessage `json:"rateLimit"`
	}
	response := postRoute(h, "/v1/session", object{}, "alice")
	json.Unmarshal(response.Body.Bytes(), &session)
	want := `{"kind":"hourly","maxRequests":0,"remaining":0,"inputTokens":{"max":1000000,"used":1000,"remaining":999000},"outputTokens":{"max":100000,"used":100000,"remaining":0},"resetsAt":"` + resets + `"}`
	if turn.Code != 429 || turn.Header().Get("Retry-After") == "" || response.Code != 200 || string(session.RateLimit) != want || exchanges.Load() != 1 {
		t.Fatalf("turn HTTP %d, session HTTP %d %s, exchanges %d", turn.Code, response.Code, session.RateLimit, exchanges.Load())
	}
	h.auth.mu.Lock()
	capped := h.auth.capped[p.scope()]
	capped.until = nowUTC().Add(-time.Second)
	h.auth.capped[p.scope()] = capped
	h.auth.mu.Unlock()
	h.auth.inference(context.Background(), p)
	if exchanges.Load() != 2 {
		t.Fatal("the cap was remembered past its reset")
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
	r.Header.Set("Authorization", "Bearer clerk-alice")
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
			h, key := subscriberHarness(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == chatTokenPath && !subscribed {
					w.WriteHeader(http.StatusPaymentRequired)
					return
				}
				json.NewEncoder(w).Encode(object{"key": "opaque-" + r.URL.Path, "expires_at": nowUTC().Add(time.Hour).Format(time.RFC3339)})
			})
			delete(h.auth.credentials, "user:alice")
			drained := screenedBy(t, h)
			delete(h.auth.credentials, "user:alice")
			finishedRun(t, h, "/v1/threads/turn", sendInput(key))
			got := drained()
			if subscribed && (len(got) != 1 || got[0].Credential != "opaque-"+chatTokenPath) || !subscribed && len(got) != 0 {
				t.Fatalf("subscribed %v: the sidecar received %+v", subscribed, got)
			}
		})
	}
}
