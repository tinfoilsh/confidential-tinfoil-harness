package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
)

// modelTurns is a model that answers its turns in order, the last of them
// again for every turn after, and keeps what each turn was sent.
type modelTurns struct {
	mu     sync.Mutex
	turns  []string
	bodies []api.Object
}

func scripted(turns ...string) *modelTurns { return &modelTurns{turns: turns} }

func (m *modelTurns) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body api.Object
	json.NewDecoder(r.Body).Decode(&body)
	m.mu.Lock()
	m.bodies = append(m.bodies, body)
	answer := m.turns[min(len(m.bodies), len(m.turns))-1]
	m.mu.Unlock()
	io.WriteString(w, answer+"data: [DONE]\n\n")
}

// sent is what each turn the model took was sent.
func (m *modelTurns) sent() []api.Object {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.bodies)
}

// contentChunk is one streamed chunk of a model's text.
func contentChunk(text string) string {
	return "data: " + string(api.Raw(api.Object{"choices": []api.Object{{"delta": api.Object{"content": text}}}})) + "\n\n"
}

// offered are the names of the tools a model turn was offered.
func offered(body api.Object) []string {
	names := []string{}
	for _, tool := range api.Arr(body["tools"]) {
		names = append(names, api.Str(api.Obj(api.Obj(tool)["function"])["name"]))
	}
	return names
}

// lastMessage is the text of the last message a model turn was sent.
func lastMessage(body api.Object) string {
	messages := api.Arr(body["messages"])
	return api.Str(api.Obj(messages[len(messages)-1])["content"])
}

// researchInput is the first turn of a new chat, researched.
func researchInput(key auth.ContentKey) api.Object {
	in := sendInput(key)
	api.Obj(in["options"])["research"] = true
	return in
}

// lighthouses is a web search enclave whose every search finds the page
// about the query at https://lights.test/<query>.
func lighthouses(t *testing.T, content string) {
	serveWebsearch(t, func(_ string, args map[string]any) (*mcp.CallToolResult, any) {
		query := api.Str(args["query"])
		return nil, api.Object{"results": []api.Object{{"title": "Lighthouses of " + query, "url": "https://lights.test/" + query, "content": query + " " + content}}}
	})
}

// researchStarted is when alice last started a research, as the control
// plane counts them.
func researchStarted(rows *memoryRows) (time.Time, bool) {
	rows.cp.Mu.Lock()
	defer rows.cp.Mu.Unlock()
	started, ok := rows.cp.ResearchStarts["alice"]
	return started, ok
}

// A research plans and searches with what it writes shown as its thinking,
// is asked once to check what it found, then writes its report without
// tools: the reply's only text, citing the page it read. The reply records
// the research, and the caller's hour counts it.
func TestAResearchShowsItsWorkAsThinkingAndAnswersWithACitedReport(t *testing.T) {
	lighthouses(t, "Maine has 65 lighthouses.")
	model := scripted(
		contentChunk("Plan: count the lighthouses.")+toolCallChunk(0, "s1", "web_search", `{"query":"maine"}`),
		contentChunk("Found the count."),
		contentChunk(" Covered."),
		contentChunk("Maine has 65 lighthouses [Lighthouses of maine](https://lights.test/maine)."),
	)
	h, rows, key := testChatHarness(t, model.ServeHTTP)
	events := finishedRun(t, h, "/v1/threads/turn", researchInput(key))
	reply := storedChat(t, rows, key, events[0].ThreadID).Messages[1]
	report := "Maine has 65 lighthouses [Lighthouses of maine](https://lights.test/maine)."
	steps := []string{}
	for _, block := range reply.Timeline {
		steps = append(steps, block.Type+":"+block.Content+block.Name)
	}
	if want := []string{"thinking:Plan: count the lighthouses.", "tool_call:web_search", "thinking:Found the count. Covered.", "content:" + report}; reply.Content != report || !reflect.DeepEqual(steps, want) {
		t.Fatalf("the reply holds %q with the steps %q", reply.Content, steps)
	}
	if reply.Research == nil || reply.Research.Version != 1 || len(reply.Annotations) != 1 || reply.Annotations[0].URLCitation.URL != "https://lights.test/maine" {
		t.Fatalf("the reply records research %+v and annotations %+v", reply.Research, reply.Annotations)
	}
	var metadata domain.RunMetadata
	if json.Unmarshal(events[len(events)-1].Metadata, &metadata) != nil || len(metadata.Messages) != 1 || metadata.Messages[0].Research == nil {
		t.Fatalf("the run committed %s", events[len(events)-1].Metadata)
	}
	sent := model.sent()
	if len(sent) != 4 {
		t.Fatalf("the model took %d turns", len(sent))
	}
	system := api.Str(api.Obj(api.Arr(sent[0]["messages"])[0])["content"])
	if !strings.Contains(system, "deep research") || strings.Contains(system, "Avoid repeating equivalent searches") || !reflect.DeepEqual(offered(sent[0]), []string{"web_search", "web_fetch"}) {
		t.Fatalf("the research was offered %v under the system prompt %q", offered(sent[0]), system)
	}
	if !strings.Contains(lastMessage(sent[2]), "Before you write the report") || !reflect.DeepEqual(offered(sent[2]), []string{"web_search", "web_fetch"}) {
		t.Fatalf("the third turn was told %q", lastMessage(sent[2]))
	}
	if !strings.Contains(lastMessage(sent[3]), "Write the report now") || sent[3]["tools"] != nil {
		t.Fatalf("the report was asked for with %q and the tools %v", lastMessage(sent[3]), offered(sent[3]))
	}
	if _, counted := researchStarted(rows); !counted {
		t.Fatal("the research was not counted")
	}
}

// A research may first ask the user what it needs to know, which ends its
// run without counting it as the hour's research. The turn that sends the
// answers researches without being offered the questions again.
func TestAResearchMayAskFirstAndStartsOnceAnswered(t *testing.T) {
	lighthouses(t, "Maine has 65 lighthouses.")
	questions := `{"title":"Scope","questions":[{"id":"q1","prompt":"Which state?","selection":"single","options":[{"id":"me","label":"Maine"},{"id":"ma","label":"Massachusetts"}]}]}`
	model := scripted(
		toolCallChunk(0, "ask", domain.QuestionsWidget, questions),
		toolCallChunk(0, "s1", "web_search", `{"query":"maine"}`),
		contentChunk("Covered."),
		contentChunk("Covered."),
		contentChunk("Maine has 65 lighthouses."),
	)
	h, rows, key := testChatHarness(t, model.ServeHTTP)
	h.models.Catalog.Get().EnabledWidgets = []string{domain.QuestionsWidget}
	first := researchInput(key)
	first["widgets"] = []any{domain.QuestionsWidget}
	id := finishedRun(t, h, "/v1/threads/turn", first)[0].ThreadID
	asked := storedChat(t, rows, key, id).Messages[1]
	if sent := model.sent(); len(sent) != 1 || !slices.Contains(offered(sent[0]), domain.QuestionsWidget) || asked.Research != nil || len(asked.Timeline) != 1 || asked.Timeline[0].Name != domain.QuestionsWidget {
		t.Fatalf("asking took %d turns and left the reply %+v", len(sent), asked)
	}
	if _, counted := researchStarted(rows); counted {
		t.Fatal("asking counted as the hour's research")
	}
	if w := postRoute(h, "/v1/threads/messages/answer", api.Object{"key": keyBase64(key), "threadId": id, "messageId": asked.ID, "toolCallId": "ask", "answers": api.Object{"q1": []any{"me"}}}, "alice"); w.Code != 200 {
		t.Fatalf("answering: HTTP %d %s", w.Code, w.Body.String())
	}
	answered := researchInput(key)
	answered["threadId"], answered["widgets"] = id, []any{domain.QuestionsWidget}
	delete(answered, "content")
	finishedRun(t, h, "/v1/threads/turn", answered)
	sent := model.sent()
	if slices.Contains(offered(sent[1]), domain.QuestionsWidget) || !strings.Contains(string(api.Raw(sent[1]["messages"])), "[me] Maine") {
		t.Fatalf("the answered research was offered %v and sent %v", offered(sent[1]), sent[1]["messages"])
	}
	if reply := storedChat(t, rows, key, id).Messages[3]; reply.Research == nil || reply.Content != "Maine has 65 lighthouses." {
		t.Fatalf("the answered research left the reply %+v", reply)
	}
	if _, counted := researchStarted(rows); !counted {
		t.Fatal("the answered research was not counted")
	}
}

// A research its caller may not start is refused before its turn is
// applied: within an hour of their last, with when the next may start, and
// to a caller without a subscription, whose session does not offer it.
func TestAResearchIsRefusedWithinTheHourAndWithoutASubscription(t *testing.T) {
	lighthouses(t, "Maine has 65 lighthouses.")
	model := scripted(contentChunk("never"))
	h, rows, key := testChatHarness(t, model.ServeHTTP)
	started := time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second)
	rows.cp.Mu.Lock()
	rows.cp.ResearchStarts["alice"] = started
	rows.cp.Mu.Unlock()
	in := researchInput(key)
	w := postRoute(h, "/v1/threads/turn", in, "alice")
	refusal := api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["error"])
	if w.Code != 429 || refusal["code"] != "RESEARCH_LIMIT_REACHED" || refusal["action"] != "wait" || refusal["resetsAt"] != started.Add(time.Hour).Format(time.RFC3339) {
		t.Fatalf("a research within the hour: HTTP %d %s", w.Code, w.Body.String())
	}
	if _, err := rows.Load(context.Background(), &auth.Principal{ID: "alice"}, key, "chat", api.Str(in["threadId"])); err == nil || len(model.sent()) != 0 {
		t.Fatalf("the refused research stored its chat (%v) or asked the model %d times", err, len(model.sent()))
	}
	anonymous := localInput("", priorThread())
	api.Obj(anonymous["options"])["research"] = true
	if w := postRoute(h, "/v1/threads/turn", anonymous, ""); w.Code != 403 || api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["error"])["code"] != "TOOL_UNAVAILABLE" {
		t.Fatalf("a signed-out research: HTTP %d %s", w.Code, w.Body.String())
	}
	offers := func(user string) any {
		return api.Obj(api.Obj(api.Parsed(postRoute(h, "/v1/session", api.Object{}, user).Body.Bytes()))["features"])["research"]
	}
	if subscriber, visitor := offers("alice"), offers(""); subscriber != true || visitor != false {
		t.Fatalf("research is offered to a subscriber: %v, to a visitor: %v", subscriber, visitor)
	}
}

// A research request sent again once its run ended is the turn already
// applied, not a second research the hour refuses.
func TestAResearchSentAgainIsAlreadyApplied(t *testing.T) {
	lighthouses(t, "Maine has 65 lighthouses.")
	h, _, key := testChatHarness(t, scripted(
		toolCallChunk(0, "s1", "web_search", `{"query":"maine"}`),
		contentChunk("Covered."),
		contentChunk("Covered."),
		contentChunk("Maine has 65 lighthouses."),
	).ServeHTTP)
	in := researchInput(key)
	finishedRun(t, h, "/v1/threads/turn", in)
	if w := postRoute(h, "/v1/threads/turn", in, "alice"); w.Code != 409 || api.Obj(api.Obj(api.Parsed(w.Body.Bytes()))["error"])["code"] != "ALREADY_APPLIED" {
		t.Fatalf("the research sent again: HTTP %d %s", w.Code, w.Body.String())
	}
}

// A research counts as the hour's once it first searches: one that answers
// without searching is not counted, and its reply is an ordinary one.
func TestAResearchThatNeverSearchesIsNotCounted(t *testing.T) {
	lighthouses(t, "Maine has 65 lighthouses.")
	model := scripted(contentChunk("Plan: recall it."), contentChunk("I know this."), contentChunk("Maine has 65 lighthouses."))
	h, rows, key := testChatHarness(t, model.ServeHTTP)
	events := finishedRun(t, h, "/v1/threads/turn", researchInput(key))
	reply := storedChat(t, rows, key, events[0].ThreadID).Messages[1]
	if _, counted := researchStarted(rows); counted || reply.Research != nil || reply.Content != "Maine has 65 lighthouses." || len(model.sent()) != 3 {
		t.Fatalf("counted %v, after %d turns, a reply %+v", counted, len(model.sent()), reply)
	}
}

// A research that fails before its report keeps its steps for later turns
// to read, rather than being read back as an empty report.
func TestAResearchThatFailsBeforeItsReportIsNotMarkedAsOne(t *testing.T) {
	lighthouses(t, "Maine has 65 lighthouses.")
	searchThenFail := scripted(toolCallChunk(0, "s1", "web_search", `{"query":"maine"}`))
	h, rows, key := testChatHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if len(searchThenFail.sent()) > 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		searchThenFail.ServeHTTP(w, r)
	})
	in := researchInput(key)
	events := eventsIn(t, postRoute(h, "/v1/threads/turn", in, "alice"))
	if last := events[len(events)-1]; last.Type != "RUN_ERROR" {
		t.Fatalf("the run ended with %+v", last)
	}
	reply := storedChat(t, rows, key, api.Str(in["threadId"])).Messages[1]
	if reply.Research != nil || len(reply.Timeline) != 1 || reply.Timeline[0].Name != "web_search" {
		t.Fatalf("the failed research left the reply %+v", reply)
	}
}

// notesOf is research notes in the form their task asks for, whose findings
// are findings.
func notesOf(findings string) string {
	var b strings.Builder
	for _, section := range summarize.ResearchNotes.Sections {
		body := "None"
		if section.Heading == "Findings" {
			body = findings
		}
		fmt.Fprintf(&b, "## %s\n%s\n\n", section.Heading, body)
	}
	return b.String()
}

// A research whose reading outgrows the window folds it into notes, which
// the model goes on from in its place, and the report still cites a page
// read before the fold.
func TestAResearchFoldsWhatItReadIntoNotes(t *testing.T) {
	lighthouses(t, filler(120))
	model := scripted(
		toolCallChunk(0, "s1", "web_search", `{"query":"maine"}`),
		toolCallChunk(0, "s2", "web_search", `{"query":"vermont"}`),
		toolCallChunk(0, "s3", "web_search", `{"query":"ohio"}`),
		contentChunk("Covered."),
		contentChunk("Covered."),
		contentChunk("Maine has 65 [Lighthouses of maine](https://lights.test/maine)."),
	)
	h, rows, key := testChatHarness(t, model.ServeHTTP)
	var read []string
	var mu sync.Mutex
	h.compactOn(accountFunc(func(_ context.Context, c inference.Completion) (inference.Completed, error) {
		if c.Task != summarize.ResearchNotes.Name {
			return inference.Completed{Text: "Lighthouses", FinishReason: "stop"}, nil
		}
		mu.Lock()
		read = append(read, c.User)
		mu.Unlock()
		return inference.Completed{Text: notesOf("Maine has 65 lighthouses [Lighthouses of maine](https://lights.test/maine)."), FinishReason: "stop"}, nil
	}))
	events := finishedRun(t, h, "/v1/threads/turn", researchInput(key))
	mu.Lock()
	defer mu.Unlock()
	if len(read) != 1 || !strings.Contains(read[0], `<result tool="web_search" query="maine">`) || !strings.Contains(read[0], "maine 0 1 2") {
		t.Fatalf("the notes read %q", read)
	}
	sent := model.sent()
	var after strings.Builder
	for _, m := range api.Arr(sent[3]["messages"]) {
		after.WriteString(api.Str(api.Obj(m)["content"]) + "\n")
	}
	if text := after.String(); !strings.Contains(text, "<research_notes>") || !strings.Contains(text, "Maine has 65 lighthouses") || strings.Contains(text, "maine 0 1 2") {
		t.Fatalf("the turn after the fold was sent %s", text)
	}
	reply := storedChat(t, rows, key, events[0].ThreadID).Messages[1]
	if len(reply.Annotations) != 1 || reply.Annotations[0].URLCitation.URL != "https://lights.test/maine" {
		t.Fatalf("the report cites %+v", reply.Annotations)
	}
}

// A research that keeps searching is asked for its report once its turns
// are used up, and gets no tools to answer with.
func TestAResearchThatKeepsSearchingWritesItsReportWhenItsTurnsRunOut(t *testing.T) {
	lighthouses(t, "A lighthouse.")
	var turns []string
	for i := range 40 {
		turns = append(turns, toolCallChunk(0, fmt.Sprintf("s%d", i), "web_search", fmt.Sprintf(`{"query":"coast %d"}`, i)))
	}
	model := scripted(append(turns, contentChunk("The report."))...)
	h, rows, key := testChatHarness(t, model.ServeHTTP)
	events := finishedRun(t, h, "/v1/threads/turn", researchInput(key))
	sent := model.sent()
	if last := sent[len(sent)-1]; len(sent) != 41 || last["tools"] != nil || !strings.Contains(lastMessage(last), "The time for this research is up") {
		t.Fatalf("the research took %d turns, the last sent %q", len(sent), lastMessage(sent[len(sent)-1]))
	}
	if reply := storedChat(t, rows, key, events[0].ThreadID).Messages[1]; reply.Content != "The report." {
		t.Fatalf("the reply holds %q", reply.Content)
	}
}

// The model reads a research's reply back as its report alone, without the
// steps that led to it.
func TestLaterTurnsReadAResearchAsItsReport(t *testing.T) {
	var prompt string
	h, rows, key := testChatHarness(t, capturePrompt(&prompt))
	rows.rows[rowMapKey(&auth.Principal{ID: "alice"}, "chat", "chat-1")] = domain.StoredRow{ID: "chat-1", ETag: "1", Data: api.Raw(api.Object{"id": "chat-1", "title": "Lighthouses", "titleState": "generated",
		"model": "kimi-k3", "presetId": nil, "pinned": false, "webSearchEnabled": false, "recordVersion": 2, "createdAt": "2026-01-01T00:00:00Z", "messages": []any{
			api.Object{"id": "m1", "role": "user", "createdAt": "2026-01-01T00:00:00Z", "content": "How many lighthouses has Maine?"},
			api.Object{"id": "m2", "role": "assistant", "createdAt": "2026-01-01T00:00:01Z", "content": "Maine has 65.", "research": api.Object{"version": 1}, "timeline": []any{
				api.Object{"id": "t1", "type": "thinking", "content": "Plan: count them."},
				api.Object{"id": "s1", "toolCallId": "s1", "type": "tool_call", "name": "web_search", "arguments": `{"query":"maine"}`, "complete": true, "result": api.Object{"query": "maine", "results": []any{}}},
				api.Object{"id": "c1", "type": "content", "content": "Maine has 65."},
			}},
		}})}
	finishedRun(t, h, "/v1/threads/turn", continueInput(key, "chat-1"))
	var sent struct {
		Messages []api.Object `json:"messages"`
	}
	json.Unmarshal([]byte(prompt), &sent)
	if len(sent.Messages) != 5 || !reflect.DeepEqual(sent.Messages[2], api.Object{"role": "assistant", "content": "Maine has 65."}) {
		t.Fatalf("the model was sent %v", sent.Messages)
	}
	if stored := storedChat(t, rows, key, "chat-1").Messages[1]; stored.Research == nil || stored.Research.Version != 1 {
		t.Fatalf("the research reply was stored back as %+v", stored)
	}
}
