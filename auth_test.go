package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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

func TestAnonymousKeyValidationRefusesWorkBeforeAllocatingThreads(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   int
	}{
		{"invalid or expired", 401, `{"error":{"code":"invalid_api_key"}}`, 401},
		{"disabled", 402, `{"error":{"code":"insufficient_quota"}}`, 403},
		{"exhausted", 429, `{"error":{"code":"insufficient_quota"}}`, 429},
		{"validation unavailable", 503, `{}`, 502},
		{"unexpected success body", 200, `{}`, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var modelCalls atomic.Int32
			h, rows, key := testChatHarness(t, func(http.ResponseWriter, *http.Request) { modelCalls.Add(1) })
			cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer cp.Close()
			h.auth.client, h.auth.controlplane = cp.Client(), cp.URL
			in := sendInput(key)
			delete(in, "key")
			in["ephemeral"] = true
			response := postRoute(h, "/v1/threads/turn", in, "")
			if response.Code != tc.want || modelCalls.Load() != 0 || rows.calls != 0 || len(h.threads) != 0 {
				t.Fatalf("validation did not refuse work: HTTP %d, models=%d, rows=%d, threads=%d", response.Code, modelCalls.Load(), rows.calls, len(h.threads))
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
	h, _, key := testChatHarness(t, nil)
	in := sendInput(key)
	delete(in, "key")
	in["ephemeral"] = true
	started := postRoute(h, "/v1/threads/turn", in, "")
	events := eventsIn(t, started)
	for _, tc := range []struct {
		key, ip string
		want    int
	}{
		{"free_anonymous-key", "198.51.100.9:1234", 200},
		{"free_other-client", "192.0.2.1:1234", 404},
		{"", "192.0.2.1:1234", 404},
	} {
		body := raw(object{"threadId": events[0].ThreadID, "runId": events[0].RunID})
		req := httptest.NewRequest(http.MethodPost, "/v1/threads/follow", bytes.NewReader(body))
		req.RemoteAddr = tc.ip
		if tc.key != "" {
			req.Header.Set("Authorization", "Bearer "+tc.key)
		}
		response := httptest.NewRecorder()
		h.routes().ServeHTTP(response, req)
		if response.Code != tc.want {
			t.Fatalf("incorrect reconnect authorization: want %d, got %d", tc.want, response.Code)
		}
		if strings.Contains(response.Body.String(), "free_anonymous-key") {
			t.Fatal("credential leaked into run events")
		}
	}
}
