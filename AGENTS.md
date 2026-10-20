# Testing

A test earns its place by failing on a plausible wrong implementation that no other test catches. Before adding or keeping one, name that bug. If the test still passes when the production code is replaced with a no-op or the obvious wrong branch, delete it.

Run what CI runs: `GOWORK=off go test -race -count=1 -shuffle=on ./...`, plus `gofmt -l .` and `go vet ./...`. Timing-sensitive tests should also pass with `-cpu=1`.

Prefer an end-to-end test through `postRoute`/`eventsIn` or the handler over a unit test of a helper the same route already exercises. Several rows in the coercion, quirk and widget tables were deleted because a chat test failed on exactly the same mutation.

## Shared fixtures

Use these instead of building new fakes:

- `testChatHarness`: a harness wired to a fake model server, a control plane stub and `memoryRows`.
- `memoryRows`: the in-memory sync row store. Like sync, it refuses to load or push a row with a key other than the one that sealed it (409 `KEY_MISMATCH`), and deletes without checking the key. Rows a test seeds directly count as sealed with the harness's test key.
- `newFakeSync` and `storedThread`: attachment blobs and stored chats.
- `routerHarness`, `serveWebsearch`, `toolTurns`, `screenedBy`: the model router, the websearch enclave, a scripted tool turn, and the safeguards sidecar.

## Patterns that were removed

- Asserting what a fake copied or returned instead of what the harness built from it (fork and share tests once re-read sync's copy).
- Assertions inside a loop over a result that can be empty, and status-only checks (`w.Code == 200`, `err == nil` and nothing else).
- Table rows that take the same branch as another row, and rows whose input already satisfies the expected output (sorted keys, values already of the right type).
- Unit tests of a check whose caller is never tested, such as screening conditions without a turn that proves a failed chat is not screened.

## Things that only fail in CI

- Waiting on a fake's flush instead of on the harness: a cancel test that waited for the fake model to write its chunk failed every run with `-cpu=1`. Wait for the run log to contain the frame you need.
- The reference checks in `compatibility_test.go` use the web client in `TINFOIL_WEBAPP` or `../tinfoil-webapp`, with its harness reducer and `node_modules`, and skip without it, as in CI. Keep each test's Go assertions before its skip.
