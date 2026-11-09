package inference

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// testAccount is an account whose model answers with respond, and which
// keeps each request it receives.
func testAccount(t *testing.T, respond http.HandlerFunc) (Account, *[]map[string]any) {
	t.Helper()
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		requests = append(requests, body)
		respond(w, r)
	}))
	t.Cleanup(server.Close)
	client := openai.NewClient(option.WithBaseURL(server.URL), option.WithAPIKey("k"), option.WithMaxRetries(0))
	return &modelAccount{completions: &client.Chat.Completions}, &requests
}

// A completion sends the model, its prompt and bounds, the request members
// the catalog gives the model, and its cache scope, and reads back the text,
// why it ended and what it cost.
func TestACompletionAsksTheModelAsItsCallerSays(t *testing.T) {
	account, requests := testAccount(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"c","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"length","message":{"role":"assistant","content":"cut"}}],"usage":{"prompt_tokens":12,"completion_tokens":3,"total_tokens":15}}`)
	})
	done, err := account.Complete(context.Background(), Completion{Task: "test", Model: "long-model", System: "be brief", User: "the text", MaxTokens: 64, Temperature: new(0.2),
		Params: api.Object{"chat_template_kwargs": api.Object{"enable_thinking": false}}, CacheScope: "scope"})
	if err != nil || done.Text != "cut" || done.FinishReason != "length" || done.Usage != (api.Usage{Prompt: 12, Completion: 3, Total: 15}) {
		t.Fatalf("the completion read %+v, %v", done, err)
	}
	sent := (*requests)[0]
	prompt := []any{map[string]any{"role": "system", "content": "be brief"}, map[string]any{"role": "user", "content": "the text"}}
	if sent["model"] != "long-model" || sent["max_completion_tokens"] != 64.0 || sent["temperature"] != 0.2 || sent["user_cache_secret"] != "scope" ||
		!reflect.DeepEqual(sent["chat_template_kwargs"], map[string]any{"enable_thinking": false}) || !reflect.DeepEqual(sent["messages"], prompt) {
		t.Fatalf("the request was %v", sent)
	}
	if _, err := account.Complete(context.Background(), Completion{Model: "m", User: "x", MaxTokens: 1}); err != nil {
		t.Fatal(err)
	}
	if _, set := (*requests)[1]["temperature"]; set {
		t.Fatalf("a completion with no temperature of its own set one: %v", (*requests)[1])
	}
}

// A refusal says its status and how long the model asked to wait.
func TestARefusedCompletionSaysWhenToTryAgain(t *testing.T) {
	account, _ := testAccount(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"message":"busy"}}`)
	})
	_, err := account.Complete(context.Background(), Completion{Model: "m", User: "x", MaxTokens: 1})
	var refused *Refused
	if !errors.As(err, &refused) || refused.Status != http.StatusTooManyRequests || refused.RetryAfter != 7*time.Second {
		t.Fatalf("the refusal read as %v", err)
	}
}

// A completion its caller pays for goes through the router signed with the
// caller's key, under the run its usage names; one the harness pays for
// carries the harness's own key and no usage.
func TestACompletionItsCallerPaysForCarriesTheCallersKey(t *testing.T) {
	type received struct {
		path, key string
		usage     usagereporting.Context
		signed    bool
	}
	requests := make(chan received, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		usage, signed, err := usagereporting.FromHeaders(r.Header, "usage-secret", time.Now(), time.Minute)
		if err != nil {
			t.Error(err)
		}
		requests <- received{r.URL.Path, r.Header.Get("Authorization"), usage, signed}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"c","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"done"}}]}`)
	}))
	t.Cleanup(server.Close)
	own := openai.NewClient(option.WithBaseURL(server.URL+"/v1"), option.WithAPIKey("harness-key"), option.WithMaxRetries(0))
	router := &Service{URL: server.URL, Client: &http.Client{Transport: &auth.CallerTransport{Inner: http.DefaultTransport, UsageSecret: "usage-secret"}}}
	account := &modelAccount{completions: &own.Chat.Completions, callers: callerCompletions(router)}
	run := usagereporting.Context{ContextID: "run", RootRequestID: "run", ParentService: "harness", APIKeyHash: usagereporting.HashAPIKey("caller-key"), Depth: 1}
	ctx := auth.WithUsage(auth.WithCallerKey(context.Background(), "caller-key"), run)

	if _, err := account.Complete(ctx, Completion{Model: "m", User: "x", MaxTokens: 1, CallerPays: true}); err != nil {
		t.Fatal(err)
	}
	if got := <-requests; got.path != chatCompletionsPath || got.key != "Bearer caller-key" || !got.signed || got.usage.ContextID != "run" || got.usage.BillCustomerRequest ||
		!usagereporting.VerifyAPIKeyHash("caller-key", got.usage.APIKeyHash) {
		t.Fatalf("the caller's completion reached the router as %+v", got)
	}
	if _, err := account.Complete(ctx, Completion{Model: "m", User: "x", MaxTokens: 1}); err != nil {
		t.Fatal(err)
	}
	if got := <-requests; got.key != "Bearer harness-key" || got.signed {
		t.Fatalf("the harness's completion was sent as %+v", got)
	}
}

// A completion its caller pays for carries the chat token the caller's run
// holds as it is sent. One that has expired is renewed first, and the usage
// is signed as the renewed token's. A token that could not be renewed fails
// the completion with that refusal, without reaching the model, so the run
// asking for it ends saying why.
func TestACallersCompletionCarriesTheirRenewedChatAccess(t *testing.T) {
	type received struct {
		key   string
		usage usagereporting.Context
	}
	requests := make(chan received, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		usage, _, err := usagereporting.FromHeaders(r.Header, "usage-secret", time.Now(), time.Minute)
		if err != nil {
			t.Error(err)
		}
		requests <- received{r.Header.Get("Authorization"), usage}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"c","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"done"}}]}`)
	}))
	t.Cleanup(server.Close)
	router := &Service{URL: server.URL, Client: &http.Client{Transport: &auth.CallerTransport{Inner: http.DefaultTransport, UsageSecret: "usage-secret"}}}
	account := &modelAccount{callers: callerCompletions(router)}
	expired := auth.Credential{Key: "expired-token", Expires: time.Now().Add(-time.Minute)}
	run := usagereporting.Context{ContextID: "run", RootRequestID: "run", ParentService: "harness", Depth: 1}
	paid := Completion{Model: "m", User: "x", MaxTokens: 1, CallerPays: true}

	renewed := auth.NewAccess(expired, func(_ context.Context, held auth.Secret) (auth.Credential, error) {
		if held != expired.Key {
			t.Errorf("asked to renew %q", held)
		}
		return auth.Credential{Key: "renewed-token", Expires: time.Now().Add(15 * time.Minute)}, nil
	})
	if _, err := account.Complete(auth.WithUsage(auth.WithCaller(context.Background(), renewed), run), paid); err != nil {
		t.Fatal(err)
	}
	if got := <-requests; got.key != "Bearer renewed-token" || got.usage.ContextID != "run" || got.usage.APIKeyHash != usagereporting.HashAPIKey("renewed-token") {
		t.Fatalf("the caller's completion reached the router as %+v", got)
	}

	suspended := api.Err(403, "UPSTREAM_REFUSED", "This account is suspended").WithAction(api.ActionStop)
	refused := auth.NewAccess(expired, func(context.Context, auth.Secret) (auth.Credential, error) {
		return auth.Credential{}, suspended
	})
	_, err := account.Complete(auth.WithUsage(auth.WithCaller(context.Background(), refused), run), paid)
	var refusal *api.Error
	if !errors.As(err, &refusal) || refusal != suspended || len(requests) != 0 {
		t.Fatalf("the completion failed with %v, after %d more requests reached the model", err, len(requests))
	}
}
