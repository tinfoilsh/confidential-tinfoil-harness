# Layout

Package `main` starts `internal/server`, which holds the routes, wiring, and integration tests; each layer under `internal/` depends only on the ones above it. `api` is what every route shares; `auth` is who a request is from; `domain` is what a chat is made of; `inference` is the models the harness serves and how it reaches them; `jobs` runs one background job per caller at a time and keeps its last report for a while; `sync` seals and stores what a caller keeps, in the control plane's database and the buckets sidecar, and knows nothing of the harness; `store` reads and writes it for a caller; `attach` is what a chat carries besides text; `importer` reads other assistants' exports; `archives` moves chats in and out whole; `engine` carries out turns and holds the live runs. A change goes in the lowest layer that has what it needs, and nothing reaches back up.

# Protocol

`protocol/openapi.yaml` is the contract with every client. A change to what a route accepts or returns, or to a run's events or error codes, changes it in the same commit, following the rules in its description. The tests check every request, response and event against it, so the harness cannot send a member it does not name.

`protocol/fixtures` holds recorded runs that clients replay through their reducers. A change to what a run sends records them again in the same commit: `RECORD_FIXTURES=1 GOWORK=off go test ./...`.

# Testing

A test earns its place by failing on a plausible wrong implementation that no other test catches. Before adding or keeping one, name that bug. If the test still passes when the production code is replaced with a no-op or the obvious wrong branch, delete it.

Run what CI runs: `GOWORK=off go test -race -count=1 -shuffle=on ./...`, plus `gofmt -l .` and `go vet ./...`. Timing-sensitive tests should also pass with `-cpu=1`.

Prefer an end-to-end test through `postRoute`/`eventsIn` or the handler over a unit test of a helper the same route already exercises. Several rows in the coercion, quirk and widget tables were deleted because a chat test failed on exactly the same mutation.

## Shared fixtures

Use these instead of building new fakes:

- `testChatHarness`: a harness wired to a fake model server, a stub of the Tinfoil API whose storage routes go to `cpstub`, a `bucketstub`, and `memoryRows`.
- `memoryRows`: the in-memory sync row store. Like sync, it refuses to load or push a row with a key other than the one that sealed it (409 `KEY_MISMATCH`), and deletes without checking the key. Rows a test seeds directly count as sealed with the harness's test key. Its `cp` and `bk` are the control plane and buckets stubs the harness stores attachments through.
- `storedThread`, `seedAttachment` and `attachmentBytes`: stored chats, and attachments held as the harness stores them (bytes in `bucketstub`, index in `cpstub`).
- `newFakeSync`: the sync enclave's import jobs, checking a native cloud import as sync does before storing it in `memoryRows`.
- `archiveRoute`, `importOf` and the archives in `internal/server/testdata`: multipart archive requests, an import's NDJSON lines, backups and exports written by the web app's own code (`web-*`, `tinfoil-export.zip`), and small exports in ChatGPT's and Claude's formats (`chatgpt-export.zip`, `claude-export.zip`, `claude-projects.json`).
- `routerHarness`, `serveWebsearch`, `toolTurns`, `screenedBy`: the model router, the websearch enclave, a scripted tool turn, and the safeguards sidecar.

## Patterns that were removed

- Asserting what a fake copied or returned instead of what the harness built from it (fork and share tests once re-read sync's copy).
- Assertions inside a loop over a result that can be empty, and status-only checks (`w.Code == 200`, `err == nil` and nothing else).
- Table rows that take the same branch as another row, and rows whose input already satisfies the expected output (sorted keys, values already of the right type).
- Unit tests of a check whose caller is never tested, such as screening conditions without a turn that proves a failed chat is not screened.

## Things that only fail in CI

- Waiting on a fake's flush instead of on the harness: a cancel test that waited for the fake model to write its chunk failed every run with `-cpu=1`. Wait for the run log to contain the frame you need.
- A recorded run in `protocol/fixtures` whose frames can come in more than one order: the harness runs tool calls concurrently, so a run with two of them is not recorded. Check a new fixture with `-count=20 -cpu=1,4`.
