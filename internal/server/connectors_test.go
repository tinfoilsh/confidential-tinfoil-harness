package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/connectors"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
)

const (
	connectorURL          = "https://mcp.example.test/mcp"
	connectorIssuer       = "https://auth.example.test"
	testConnectorClientID = "https://chat.example.test/oauth/client-metadata.json"
	testConnectorRedirect = "https://chat.example.test/connectors/callback"
	registeredClient      = "registered-client"
	consentCode           = "code-from-consent"
)

// fakeConnector is a remote MCP server and its authorization server, both
// served at public names: an MCP server at connectorURL offering a
// read-only lookup and a create_page that changes things, which with signIn
// refuses callers without a token its authorization server issued.
type fakeConnector struct {
	server           *httptest.Server
	signIn           bool
	metadataDocument bool
	// edit changes the authorization server's metadata, as a server the
	// harness cannot sign in to publishes it.
	edit func(api.Object)
	// refuse has the token endpoint refuse every refresh.
	refuse bool

	mu         sync.Mutex
	registered int
	challenge  string
	access     map[string]bool
	refresh    string
	issued     int
	refreshes  int
	calls      []recordedCall
	revoked    []string
}

func serveConnector(t *testing.T, f *fakeConnector) *fakeConnector {
	t.Helper()
	f.access = map[string]bool{}
	type lookupArgs struct {
		Query string `json:"query"`
	}
	type pageArgs struct {
		Title string `json:"title"`
	}
	record := func(req *mcp.CallToolRequest) map[string]any {
		var args map[string]any
		json.Unmarshal(req.Params.Arguments, &args)
		f.mu.Lock()
		f.calls = append(f.calls, recordedCall{req.Params.Name, args, req.Extra.Header.Clone()})
		f.mu.Unlock()
		return args
	}
	tools := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		s := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "test"}, nil)
		mcp.AddTool(s, &mcp.Tool{Name: "lookup", Description: "Look a page up.", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}}, func(_ context.Context, req *mcp.CallToolRequest, in lookupArgs) (*mcp.CallToolResult, any, error) {
			record(req)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "found " + in.Query}}}, nil, nil
		})
		mcp.AddTool(s, &mcp.Tool{Name: "create_page", Title: "Create a page", Description: "Create a page."}, func(_ context.Context, req *mcp.CallToolRequest, in pageArgs) (*mcp.CallToolResult, any, error) {
			record(req)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "created " + in.Title}}}, nil, nil
		})
		return s
	}, &mcp.StreamableHTTPOptions{Stateless: true, DisableLocalhostProtection: true})
	resourceMetadata := "https://mcp.example.test/.well-known/oauth-protected-resource/mcp"
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Host + r.URL.Path {
		case "mcp.example.test/mcp":
			if f.signIn && !f.admits(r) {
				w.Header().Set("WWW-Authenticate", `Bearer resource_metadata="`+resourceMetadata+`", scope="pages"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			tools.ServeHTTP(w, r)
		case "mcp.example.test/.well-known/oauth-protected-resource/mcp":
			if !f.signIn {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, api.Object{"resource": connectorURL, "authorization_servers": []string{connectorIssuer}, "scopes_supported": []string{"pages"}})
		case "auth.example.test/.well-known/oauth-authorization-server":
			metadata := api.Object{
				"issuer":                           connectorIssuer,
				"authorization_endpoint":           connectorIssuer + "/authorize",
				"token_endpoint":                   connectorIssuer + "/token",
				"registration_endpoint":            connectorIssuer + "/register",
				"revocation_endpoint":              connectorIssuer + "/revoke",
				"code_challenge_methods_supported": []string{"S256"},
				"response_types_supported":         []string{"code"},
				"scopes_supported":                 []string{"pages", "offline_access"},
			}
			if f.metadataDocument {
				metadata["client_id_metadata_document_supported"] = true
			}
			if f.edit != nil {
				f.edit(metadata)
			}
			writeJSON(w, metadata)
		case "auth.example.test/register":
			f.mu.Lock()
			f.registered++
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(api.Object{"client_id": registeredClient})
		case "auth.example.test/token":
			f.token(w, r)
		case "auth.example.test/revoke":
			r.ParseForm()
			f.mu.Lock()
			f.revoked = append(f.revoked, r.Form.Get("token"))
			f.mu.Unlock()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// errorCode is the code of the error a route answered with.
func errorCode(w *httptest.ResponseRecorder) any {
	return api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["error"])["code"]
}

func (f *fakeConnector) admits(r *http.Request) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && f.access[token]
}

// token answers the token endpoint: a code from the consent, with the
// verifier of its challenge, or the latest refresh token, each for new
// tokens that replace the ones before.
func (f *fakeConnector) token(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	f.mu.Lock()
	defer f.mu.Unlock()
	refuse := func() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(api.Object{"error": "invalid_grant"})
	}
	switch r.Form.Get("grant_type") {
	case "authorization_code":
		verified := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if r.Form.Get("code") != consentCode || base64.RawURLEncoding.EncodeToString(verified[:]) != f.challenge ||
			r.Form.Get("redirect_uri") != testConnectorRedirect || r.Form.Get("resource") != connectorURL {
			refuse()
			return
		}
	case "refresh_token":
		if f.refuse || r.Form.Get("refresh_token") != f.refresh {
			refuse()
			return
		}
		f.refreshes++
	default:
		refuse()
		return
	}
	f.issued++
	access := fmt.Sprintf("access-%d", f.issued)
	f.access = map[string]bool{access: true}
	f.refresh = fmt.Sprintf("refresh-%d", f.issued)
	writeJSON(w, api.Object{"access_token": access, "refresh_token": f.refresh, "token_type": "Bearer", "expires_in": 3600})
}

// consent is the user at the authorization server's page: it checks the page
// asks for a code by PKCE for the harness's client, and answers as the
// server would once they agree.
func (f *fakeConnector) consent(t *testing.T, page, clientID string) (code, state string) {
	t.Helper()
	u, err := url.Parse(page)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if u.Host != "auth.example.test" || u.Path != "/authorize" || q.Get("client_id") != clientID || q.Get("redirect_uri") != testConnectorRedirect ||
		q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || q.Get("resource") != connectorURL ||
		!slices.Equal(strings.Fields(q.Get("scope")), []string{"pages", "offline_access"}) || q.Get("state") == "" {
		t.Fatalf("the sign-in page asks %s", page)
	}
	f.mu.Lock()
	f.challenge = q.Get("code_challenge")
	f.mu.Unlock()
	return consentCode, q.Get("state")
}

func (f *fakeConnector) called() []recordedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// connectorHarness is testChatHarness reaching f's servers by their public
// names, as connectors.New reaches servers on the internet.
func connectorHarness(t *testing.T, upstream http.HandlerFunc, f *fakeConnector) (*harness, *memoryRows, auth.ContentKey) {
	t.Helper()
	h, rows, key := testChatHarness(t, upstream)
	transport := f.server.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.ServerName = f.server.Certificate().DNSNames[0]
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, f.server.Listener.Addr().String())
	}
	t.Cleanup(transport.CloseIdleConnections)
	client := connectors.NewWithTransport(testConnectorClientID, testConnectorRedirect, transport)
	h.connectors, h.engine.Connectors = client, client
	return h, rows, key
}

// connectorRoute posts to a connector route as alice and reads its answer.
func connectorRoute(t *testing.T, h *harness, key auth.ContentKey, path string, in api.Object) api.Object {
	t.Helper()
	in["key"] = keyBase64(key)
	w := postRoute(h, path, in, "alice")
	if w.Code != 200 {
		t.Fatalf("%s: HTTP %d: %s", path, w.Code, w.Body)
	}
	return api.Obj(api.Parsed(w.Body.Bytes()))
}

// connect adds f's server as the connector Fake, and signs alice in to it
// when it asks.
func connect(t *testing.T, h *harness, key auth.ContentKey, f *fakeConnector) string {
	t.Helper()
	added := connectorRoute(t, h, key, "/v1/connectors/add", api.Object{"name": "Fake", "url": connectorURL})
	id := api.Str(api.Obj(added["connector"])["id"])
	if page := api.Str(added["authorizationUrl"]); page != "" {
		code, state := f.consent(t, page, registeredClient)
		connectorRoute(t, h, key, "/v1/connectors/complete", api.Object{"id": id, "code": code, "state": state})
	}
	return id
}

func storedConnector(t *testing.T, h *harness, key auth.ContentKey, id string) domain.Connector {
	t.Helper()
	c, err := h.store.LoadConnector(context.Background(), &auth.Principal{ID: "alice"}, key, id)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAnOpenServerConnectsWithItsToolsAskingBeforeThoseThatChangeThings(t *testing.T) {
	f := serveConnector(t, &fakeConnector{})
	h, _, key := connectorHarness(t, nil, f)
	added := connectorRoute(t, h, key, "/v1/connectors/add", api.Object{"name": " Fake ", "url": connectorURL})
	if _, ok := added["authorizationUrl"]; ok {
		t.Fatalf("an open server was given a sign-in: %v", added)
	}
	want := []domain.ConnectorTool{
		{Name: "lookup", Description: "Look a page up.", ReadOnly: true, Permission: domain.ToolAllow},
		{Name: "create_page", Title: "Create a page", Description: "Create a page.", Permission: domain.ToolAsk},
	}
	listed := connectorRoute(t, h, key, "/v1/connectors/list", api.Object{})
	var got struct {
		Connectors []domain.ConnectorView `json:"connectors"`
	}
	json.Unmarshal(api.Raw(listed), &got)
	if len(got.Connectors) != 1 {
		t.Fatalf("listed %v", listed)
	}
	c := got.Connectors[0]
	slices.SortFunc(c.Tools, func(a, b domain.ConnectorTool) int { return strings.Compare(b.Name, a.Name) })
	if c.Name != "Fake" || c.URL != connectorURL || !c.Enabled || c.Status != domain.ConnectorConnected || c.SignIn || !slices.Equal(c.Tools, want) {
		t.Fatalf("listed %+v, want an enabled, connected Fake with %+v", c, want)
	}
}

func TestSigningInToAConnectorKeepsItsTokensInTheEnclave(t *testing.T) {
	f := serveConnector(t, &fakeConnector{signIn: true})
	h, _, key := connectorHarness(t, nil, f)
	added := connectorRoute(t, h, key, "/v1/connectors/add", api.Object{"name": "Fake", "url": connectorURL})
	connector := api.Obj(added["connector"])
	id := api.Str(connector["id"])
	if connector["status"] != domain.ConnectorSignedOut || connector["signIn"] != true || f.registered != 1 {
		t.Fatalf("added %v after %d registrations, want a signed-out connector registered once", added, f.registered)
	}
	code, state := f.consent(t, api.Str(added["authorizationUrl"]), registeredClient)

	refused := postRoute(h, "/v1/connectors/complete", api.Object{"key": keyBase64(key), "id": id, "code": code, "state": state + "x"}, "alice")
	if refused.Code != 400 || errorCode(refused) != "CONNECTOR_SIGN_IN_FAILED" || f.issued != 0 {
		t.Fatalf("a sign-in with another state: HTTP %d %s, %d tokens issued", refused.Code, refused.Body, f.issued)
	}
	w := postRoute(h, "/v1/connectors/complete", api.Object{"key": keyBase64(key), "id": id, "code": code, "state": state}, "alice")
	completed := api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["connector"])
	if w.Code != 200 || completed["status"] != domain.ConnectorConnected || len(api.Arr(completed["tools"])) != 2 {
		t.Fatalf("complete: HTTP %d %s, want a connected connector with the tools its token lists", w.Code, w.Body)
	}
	listed := postRoute(h, "/v1/connectors/list", api.Object{"key": keyBase64(key)}, "alice")
	for _, body := range []string{string(api.Raw(added)), w.Body.String(), listed.Body.String()} {
		if strings.Contains(body, "access-1") || strings.Contains(body, "refresh-1") {
			t.Fatalf("a client was sent what the connector signs in with: %s", body)
		}
	}
	stored := storedConnector(t, h, key, id)
	if stored.Auth == nil || stored.Auth.RefreshToken != "refresh-1" || stored.Auth.ClientID != registeredClient || stored.Pending != nil {
		t.Fatalf("stored %+v, want the tokens kept and the sign-in done", stored)
	}
	if again := postRoute(h, "/v1/connectors/complete", api.Object{"key": keyBase64(key), "id": id, "code": code, "state": state}, "alice"); again.Code != 400 || f.issued != 1 {
		t.Fatalf("a finished sign-in completed again: HTTP %d, %d tokens issued", again.Code, f.issued)
	}
}

func TestAServerThatReadsClientMetadataDocumentsIsNotRegisteredWith(t *testing.T) {
	f := serveConnector(t, &fakeConnector{signIn: true, metadataDocument: true})
	h, _, key := connectorHarness(t, nil, f)
	added := connectorRoute(t, h, key, "/v1/connectors/add", api.Object{"name": "Fake", "url": connectorURL})
	f.consent(t, api.Str(added["authorizationUrl"]), testConnectorClientID)
	if f.registered != 0 {
		t.Fatalf("the harness registered %d clients with a server that reads its metadata document", f.registered)
	}
}

func TestConnectorsRefuseWhatTheyCannotSignInTo(t *testing.T) {
	for name, edit := range map[string]func(api.Object){
		"no PKCE":               func(m api.Object) { delete(m, "code_challenge_methods_supported") },
		"another issuer":        func(m api.Object) { m["issuer"] = "https://elsewhere.example.test" },
		"no way to register":    func(m api.Object) { delete(m, "registration_endpoint") },
		"a private token route": func(m api.Object) { m["token_endpoint"] = "https://10.0.0.1/token" },
	} {
		t.Run(name, func(t *testing.T) {
			f := serveConnector(t, &fakeConnector{signIn: true, edit: edit})
			h, _, key := connectorHarness(t, nil, f)
			w := postRoute(h, "/v1/connectors/add", api.Object{"key": keyBase64(key), "name": "Fake", "url": connectorURL}, "alice")
			if w.Code != 422 || errorCode(w) != "CONNECTOR_UNSUPPORTED" {
				t.Fatalf("HTTP %d: %s", w.Code, w.Body)
			}
		})
	}
	f := serveConnector(t, &fakeConnector{})
	h, _, key := connectorHarness(t, nil, f)
	for _, address := range []string{"http://mcp.example.test/mcp", "https://127.0.0.1/mcp", "https://mcp.example.test:8443/mcp", "https://localhost/mcp"} {
		if w := postRoute(h, "/v1/connectors/add", api.Object{"key": keyBase64(key), "name": "Fake", "url": address}, "alice"); w.Code != 400 {
			t.Errorf("%s: HTTP %d, want it refused", address, w.Code)
		}
	}
}

// connectorTurn answers the first model turn by calling a tool, and every
// later one with a plain answer; it reports the tools the first was offered
// and what each later one read.
func connectorTurn(call, args string, offered chan<- []string, sent chan<- []any) http.HandlerFunc {
	var turns sync.Mutex
	first := true
	return func(w http.ResponseWriter, r *http.Request) {
		turns.Lock()
		opening := first
		first = false
		turns.Unlock()
		if opening {
			var body api.Object
			json.NewDecoder(r.Body).Decode(&body)
			var names []string
			for _, tool := range api.Arr(body["tools"]) {
				names = append(names, api.Str(api.Obj(api.Obj(tool)["function"])["name"]))
			}
			offered <- names
			io.WriteString(w, toolCallChunk(0, "app-call", call, args)+"data: [DONE]\n\n")
			return
		}
		sent <- lastRequest(r)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\ndata: [DONE]\n\n")
	}
}

func TestAReplyCallsAConnectorsToolsTheUserLeftOn(t *testing.T) {
	f := serveConnector(t, &fakeConnector{signIn: true})
	offered, sent := make(chan []string, 1), make(chan []any, 1)
	h, rows, key := connectorHarness(t, connectorTurn("fake__lookup", `{"query":"cats"}`, offered, sent), f)
	id := connect(t, h, key, f)
	connectorRoute(t, h, key, "/v1/connectors/update", api.Object{"id": id, "tools": api.Object{"create_page": domain.ToolOff}})
	if w := postRoute(h, "/v1/connectors/update", api.Object{"key": keyBase64(key), "id": id, "tools": api.Object{"delete_everything": domain.ToolAllow}}, "alice"); w.Code != 409 {
		t.Fatalf("a permission for a tool the server does not offer: HTTP %d", w.Code)
	}
	events := finishedRun(t, h, "/v1/threads/turn", sendInput(key))
	if names := <-offered; !slices.Equal(names, []string{"fake__lookup"}) {
		t.Fatalf("the model was offered %v, want the lookup the user left on alone", names)
	}
	calls := f.called()
	if len(calls) != 1 || calls[0].name != "lookup" || calls[0].args["query"] != "cats" || calls[0].header.Get("Authorization") != "Bearer access-1" {
		t.Fatalf("the server was called with %+v", calls)
	}
	if read := toolMessage(<-sent, "app-call"); read != "found cats" {
		t.Fatalf("the model read %q", read)
	}
	if shown := toolResult(events, "app-call"); shown["output"] != "found cats" {
		t.Fatalf("the client was shown %v", shown)
	}
	row, err := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", events[0].ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	block := mustRead(domain.ReadThreadRow(row)).Messages[1].Timeline[0]
	if d := block.Display; d == nil || d.Status != "completed" || d.Connector == nil || d.Connector.ID != id || d.Connector.Name != "Fake" || d.Connector.Tool != "lookup" || d.Connector.Approval != nil {
		t.Fatalf("the chat keeps the call as %+v", block)
	}
}

// awaitApproval waits for a run to ask the user to approve a call, and
// returns the id they answer it under.
func awaitApproval(t *testing.T, log *engine.Log) string {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		_, frames, _, wake, closed, done := log.Frames.Read(0)
		for _, frame := range frames {
			var e event
			json.Unmarshal(frame, &e)
			if e.Type == "ACTIVITY_DELTA" && e.Display != nil && e.Display.Connector != nil && e.Display.Connector.Approval != nil &&
				e.Display.Connector.Approval.Status == domain.ApprovalPending {
				done()
				return e.Display.Connector.Approval.ID
			}
		}
		done()
		if closed != nil {
			t.Fatal("the run ended without asking for approval")
		}
		select {
		case <-wake:
		case <-deadline:
			t.Fatal("the run did not ask for approval")
		}
	}
}

func TestACallThatNeedsApprovalRunsOnlyOnceTheUserApproves(t *testing.T) {
	for _, approved := range []bool{false, true} {
		t.Run(fmt.Sprint(approved), func(t *testing.T) {
			f := serveConnector(t, &fakeConnector{})
			sent := make(chan []any, 1)
			h, rows, key := connectorHarness(t, connectorTurn("fake__create_page", `{"title":"Plans"}`, make(chan []string, 1), sent), f)
			id := connect(t, h, key, f)
			p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
			run, err := h.engine.AcceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[engine.TurnRequest](sendInput(key)))
			if err != nil {
				t.Fatal(err)
			}
			defer run.Log.Frames.Detach()
			approval := awaitApproval(t, run.Log)
			if calls := f.called(); len(calls) != 0 {
				t.Fatalf("the call ran before the user answered: %+v", calls)
			}
			answer := api.Object{"key": keyBase64(key), "threadId": run.Thread.ID, "runId": run.Log.ID, "approvalId": approval, "approved": approved}
			if w := postRoute(h, "/v1/threads/approve", answer, "bob"); w.Code != 404 {
				t.Fatalf("another account answered: HTTP %d", w.Code)
			}
			if w := postRoute(h, "/v1/threads/approve", answer, "alice"); w.Code != 200 {
				t.Fatalf("answer refused: HTTP %d %s", w.Code, w.Body)
			}
			awaitFrame(t, run.Log, "RUN_FINISHED")
			<-run.Finished
			read := toolMessage(<-sent, "app-call")
			calls, status := f.called(), domain.ApprovalDeclined
			if approved {
				status = domain.ApprovalApproved
				if len(calls) != 1 || calls[0].name != "create_page" || read != "created Plans" {
					t.Fatalf("approved, the server got %+v and the model read %q", calls, read)
				}
			} else if len(calls) != 0 || !strings.Contains(read, "declined") {
				t.Fatalf("declined, the server got %+v and the model read %q", calls, read)
			}
			row, err := rows.Load(context.Background(), p, key, "chat", run.Thread.ID)
			if err != nil {
				t.Fatal(err)
			}
			block := mustRead(domain.ReadThreadRow(row)).Messages[1].Timeline[0]
			if d := block.Display; d == nil || d.Connector == nil || d.Connector.ID != id || d.Connector.Approval == nil || d.Connector.Approval.Status != status {
				t.Fatalf("the chat keeps the call as %+v, want its approval %s", block.Display, status)
			}
		})
	}
}

func TestARunKeepsTheTokensItRefreshes(t *testing.T) {
	f := serveConnector(t, &fakeConnector{signIn: true})
	sent := make(chan []any, 1)
	h, _, key := connectorHarness(t, connectorTurn("fake__lookup", `{"query":"cats"}`, make(chan []string, 1), sent), f)
	id := connect(t, h, key, f)
	expire := func(c *domain.Connector, _ bool) error {
		c.Auth.Expiry = api.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
		return nil
	}
	if _, err := h.store.MutateConnector(context.Background(), &auth.Principal{ID: "alice"}, key, id, false, expire); err != nil {
		t.Fatal(err)
	}
	finishedRun(t, h, "/v1/threads/turn", sendInput(key))
	if read := toolMessage(<-sent, "app-call"); read != "found cats" {
		t.Fatalf("the model read %q", read)
	}
	if calls := f.called(); len(calls) != 1 || calls[0].header.Get("Authorization") != "Bearer access-2" {
		t.Fatalf("the server was called with %+v, want the refreshed token", calls)
	}
	if stored := storedConnector(t, h, key, id); stored.Auth.RefreshToken != "refresh-2" || stored.Auth.AccessToken != "access-2" || f.refreshes != 1 {
		t.Fatalf("stored %+v after %d refreshes, want the rotated tokens kept", stored.Auth, f.refreshes)
	}
}

func TestARefusedRefreshSignsTheConnectorOut(t *testing.T) {
	f := serveConnector(t, &fakeConnector{signIn: true})
	offered, sent := make(chan []string, 1), make(chan []any, 1)
	h, _, key := connectorHarness(t, connectorTurn("fake__lookup", `{"query":"cats"}`, offered, sent), f)
	id := connect(t, h, key, f)
	f.mu.Lock()
	f.refuse = true
	f.mu.Unlock()
	expire := func(c *domain.Connector, _ bool) error {
		c.Auth.Expiry = api.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
		return nil
	}
	if _, err := h.store.MutateConnector(context.Background(), &auth.Principal{ID: "alice"}, key, id, false, expire); err != nil {
		t.Fatal(err)
	}
	finishedRun(t, h, "/v1/threads/turn", sendInput(key))
	if names := <-offered; len(names) != 0 {
		t.Fatalf("the model was offered %v from a connector signed out", names)
	}
	stored := storedConnector(t, h, key, id)
	if view := stored.View(); view.Status != domain.ConnectorSignedOut || stored.Auth.RefreshToken != "" {
		t.Fatalf("stored %+v, want the refused tokens forgotten", stored.Auth)
	}
}

func TestDeletingAConnectorRevokesItsTokens(t *testing.T) {
	f := serveConnector(t, &fakeConnector{signIn: true})
	h, _, key := connectorHarness(t, nil, f)
	id := connect(t, h, key, f)
	connectorRoute(t, h, key, "/v1/connectors/delete", api.Object{"id": id})
	if !slices.Equal(f.revoked, []string{"refresh-1"}) {
		t.Fatalf("revoked %v, want the refresh token", f.revoked)
	}
	if listed := connectorRoute(t, h, key, "/v1/connectors/list", api.Object{}); len(api.Arr(listed["connectors"])) != 0 {
		t.Fatalf("listed %v after the delete", listed)
	}
}

// A call to a connector's tool that asks the user first shows the chat the
// harness keeps: the call waiting, approved, and answered from.
func TestAnApprovedConnectorCallShowsTheChatTheHarnessKeeps(t *testing.T) {
	f := serveConnector(t, &fakeConnector{})
	h, rows, key := connectorHarness(t, toolTurns(toolCallChunk(0, "app-call", "fake__create_page", `{"title":"Plans"}`), nil), f)
	connect(t, h, key, f)
	p := &auth.Principal{ID: "alice", JWT: "clerk-alice"}
	run, err := h.engine.AcceptTurn(context.Background(), p, chatCredential("alice"), key, requestOf[engine.TurnRequest](sendInput(key)))
	if err != nil {
		t.Fatal(err)
	}
	followed := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		followed <- postRoute(h, "/v1/threads/follow", api.Object{"key": keyBase64(key), "threadId": run.Thread.ID, "runId": run.Log.ID}, "alice")
	}()
	approval := awaitApproval(t, run.Log)
	if w := postRoute(h, "/v1/threads/approve", api.Object{"key": keyBase64(key), "threadId": run.Thread.ID, "runId": run.Log.ID, "approvalId": approval, "approved": true}, "alice"); w.Code != 200 {
		t.Fatalf("answer refused: HTTP %d %s", w.Code, w.Body)
	}
	w := <-followed
	<-run.Finished
	events := eventsIn(t, w)
	if last := events[len(events)-1].Type; last != "RUN_FINISHED" {
		t.Fatalf("the run ended with %s", last)
	}
	kept := storedChat(t, rows, key, run.Thread.ID)
	conformance(t, "connector-approval", "A reply that calls a connected app's tool, which waits for the user to approve it, then answers from what the tool returned.", framesOf(t, w.Body.Bytes()), &kept)
}
