package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// renewal is a chat token the control plane was asked to renew, and for whom.
type renewal struct{ user, held string }

// renewing has the tests' control plane answer every renewal of a run's
// chat token as answer says, and returns the renewals it was asked for.
func renewing(rows *memoryRows, answer func() (token string, refused int)) (asked func() []renewal) {
	var mu sync.Mutex
	var renewals []renewal
	rows.cp.Mu.Lock()
	rows.cp.RenewChatToken = func(user, held string) (string, int) {
		mu.Lock()
		renewals = append(renewals, renewal{user, held})
		mu.Unlock()
		return answer()
	}
	rows.cp.Mu.Unlock()
	return func() []renewal {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(renewals)
	}
}

// chatTokenExpiring is a chat token alice's client minted that expires in.
func chatTokenExpiring(in time.Duration) string {
	return mintChatToken("alice", func(_ map[string]any, claims jwt.MapClaims) { claims["exp"] = time.Now().Add(in).Unix() })
}

// turnWith sends a turn as alice with token as the chat access her client minted.
func turnWith(h *harness, in api.Object, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/threads/turn", bytes.NewReader(api.Raw(in)))
	signIn(r, "alice")
	r.Header.Set(auth.InferenceKeyHeader, token)
	w := httptest.NewRecorder()
	h.routes().ServeHTTP(w, r)
	return w
}

// A run whose chat token has expired, as a message that waited in a queue
// starts with, has the control plane renew it before it asks the model,
// which never sees the expired one.
func TestARunRenewsAnExpiredChatTokenBeforeItAsksTheModel(t *testing.T) {
	expired, renewed := chatTokenExpiring(-10*time.Second), mintChatToken("alice", nil)
	var mu sync.Mutex
	var presented []string
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		presented = append(presented, r.Header.Get("Authorization"))
		mu.Unlock()
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\ndata: [DONE]\n\n")
	})
	asked := renewing(rows, func() (string, int) { return renewed, 0 })
	events := eventsIn(t, turnWith(h, sendInput(key), expired))
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" {
		t.Fatalf("the run ended with %+v", last)
	}
	if !reflect.DeepEqual(presented, []string{"Bearer " + renewed}) {
		t.Fatalf("the model was asked %d times, not once with the renewed token", len(presented))
	}
	if got := asked(); !reflect.DeepEqual(got, []renewal{{"alice", expired}}) {
		t.Fatalf("the control plane was asked for %d renewals, not one of the run's own token for its user", len(got))
	}
}

// A model that refuses a run's chat token is asked once more, with the
// token the control plane renews it into and the same body. A token refused
// as it was issued ends the run as a failure to try again, not as a caller
// who must sign in.
func TestARefusedChatTokenIsRenewedAndTheTurnAskedAgain(t *testing.T) {
	for _, rejectAgain := range []bool{false, true} {
		t.Run(strconv.FormatBool(rejectAgain), func(t *testing.T) {
			held, renewed := chatTokenExpiring(10*time.Minute), mintChatToken("alice", nil)
			var modelCalls atomic.Int32
			var original []byte
			h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				call := modelCalls.Add(1)
				if call == 1 {
					original = body
				} else if !bytes.Equal(original, body) || r.Header.Get("Authorization") != "Bearer "+renewed {
					t.Error("the turn asked again changed its body or did not carry the renewed token")
				}
				if call == 1 || rejectAgain {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\ndata: [DONE]\n\n")
			})
			asked := renewing(rows, func() (string, int) { return renewed, 0 })
			events := eventsIn(t, turnWith(h, sendInput(key), held))
			if modelCalls.Load() != 2 {
				t.Fatalf("the model was asked %d times", modelCalls.Load())
			}
			if got := asked(); !reflect.DeepEqual(got, []renewal{{"alice", held}}) {
				t.Fatalf("the control plane was asked for %d renewals", len(got))
			}
			last := events[len(events)-1]
			if rejectAgain && (last.Code != "UPSTREAM_REFUSED" || last.Action != api.ActionRetry) || !rejectAgain && last.Type != "RUN_FINISHED" {
				t.Fatalf("the run ended with %+v", last)
			}
			row, _ := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", events[0].ThreadID)
			if !rejectAgain && len(mustRead(domain.ReadThreadRow(row)).Messages) != 2 {
				t.Fatal("asking the turn again duplicated it")
			}
		})
	}
}

// A run whose chat access the control plane will not renew ends saying why:
// an account that may no longer chat is told to stop, and a control plane
// that could not answer is worth another try. None asks the caller to sign
// in, which would change nothing.
func TestARunWhoseChatAccessIsNotRenewedSaysWhy(t *testing.T) {
	for _, tc := range []struct {
		refused         int
		message, action string
	}{
		{http.StatusPaymentRequired, "The Tinfoil Chat subscription has ended", api.ActionStop},
		{http.StatusForbidden, "This account is suspended", api.ActionStop},
		{http.StatusInternalServerError, "Chat access is temporarily unavailable", api.ActionRetry},
	} {
		t.Run(strconv.Itoa(tc.refused), func(t *testing.T) {
			var modelCalls atomic.Int32
			h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
				modelCalls.Add(1)
				w.WriteHeader(http.StatusUnauthorized)
			})
			renewing(rows, func() (string, int) { return "", tc.refused })
			events := eventsIn(t, turnWith(h, sendInput(key), chatTokenExpiring(10*time.Minute)))
			if last := events[len(events)-1]; last.Type != "RUN_ERROR" || last.Code != "UPSTREAM_REFUSED" || last.Message != tc.message || last.Action != tc.action {
				t.Fatalf("the run ended with %s %s %q, to %s", last.Type, last.Code, last.Message, last.Action)
			}
			if modelCalls.Load() != 1 {
				t.Fatalf("the model was asked %d times", modelCalls.Load())
			}
		})
	}
}

// A search the enclave refuses for a token that expired while the model was
// writing its turn is sent again with the renewed one, so the model reads
// its results and the run's search session goes on.
func TestASearchRefusedForAnExpiredTokenIsSentAgainWithTheNextOne(t *testing.T) {
	held, renewed := chatTokenExpiring(10*time.Minute), mintChatToken("alice", nil)
	var expired atomic.Bool
	enclave := serveWebsearchBehind(t, func(enclave http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if expired.Load() && r.Header.Get("Authorization") == "Bearer "+held {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			enclave.ServeHTTP(w, r)
		})
	}, func(string, map[string]any) (*mcp.CallToolResult, any) {
		return nil, api.Object{"results": []api.Object{{"title": "Tides", "url": "https://tides.test", "content": "High tide is at noon."}}}
	})
	sent := make(chan []any, 1)
	answer := toolTurns(toolCallChunk(0, "search-call", "web_search", `{"query":"tides"}`), sent)
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		answer(w, r)
		expired.Store(true)
	})
	asked := renewing(rows, func() (string, int) { return renewed, 0 })
	events := eventsIn(t, turnWith(h, searchInput(key), held))
	if last := events[len(events)-1]; last.Type != "RUN_FINISHED" {
		t.Fatalf("the run ended with %+v", last)
	}
	if read := toolMessage(<-sent, "search-call"); !strings.Contains(read, "High tide is at noon.") {
		t.Fatalf("the model read %q of its search", read)
	}
	calls := enclave.list()
	if len(calls) != 1 || calls[0].header.Get("Authorization") != "Bearer "+renewed {
		t.Fatalf("the enclave ran %d searches, not one under the renewed token", len(calls))
	}
	if got := asked(); !reflect.DeepEqual(got, []renewal{{"alice", held}}) {
		t.Fatalf("the control plane was asked for %d renewals", len(got))
	}
}

// A chat token is renewed once it nears its expiry and is still the one sent
// until the next arrives. A control plane that refuses is asked again only
// after a pause, the token held serving until it expires; after that a
// request fails with the refusal, and the run recovers once the control
// plane answers.
func TestAChatTokenIsRenewedAheadOfItsExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var asked []auth.Secret
		var refusal error
		lifetime := 15 * time.Minute
		access := auth.NewAccess(auth.Credential{Key: "first", Expires: time.Now().Add(lifetime)}, func(_ context.Context, held auth.Secret) (auth.Credential, error) {
			mu.Lock()
			defer mu.Unlock()
			asked = append(asked, held)
			if refusal != nil {
				return auth.Credential{}, refusal
			}
			return auth.Credential{Key: held + "+", Expires: time.Now().Add(lifetime)}, nil
		})
		// sent is the key a request sent now carries, or why it was not
		// sent, and how many renewals the control plane has been asked for
		// once it has answered.
		sent := func() (string, int) {
			key, err := access.Key(t.Context())
			synctest.Wait()
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				return err.Error(), len(asked)
			}
			return string(key), len(asked)
		}
		refuse := func(err error) {
			mu.Lock()
			refusal = err
			mu.Unlock()
		}
		expect := func(when, key string, renewals int) {
			t.Helper()
			if got, count := sent(); got != key || count != renewals {
				t.Fatalf("%s: sent %q after %d renewals, want %q after %d", when, got, count, key, renewals)
			}
		}

		expect("a token with most of its life left", "first", 0)
		time.Sleep(11 * time.Minute)
		expect("a token nearing its expiry", "first", 1)
		expect("the request after its renewal", "first+", 1)

		refuse(errors.New("unavailable"))
		time.Sleep(11 * time.Minute)
		expect("a refused renewal of a token that still lasts", "first+", 2)
		expect("a request within the pause", "first+", 2)
		time.Sleep(31 * time.Second)
		expect("a request after the pause", "first+", 3)

		time.Sleep(5 * time.Minute)
		expect("an expired token the control plane will not renew", "unavailable", 4)
		expect("the next request within the pause", "unavailable", 4)

		refuse(nil)
		time.Sleep(31 * time.Second)
		expect("an expired token once the control plane answers", "first++", 5)
	})
}
