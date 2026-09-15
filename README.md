# confidential-tinfoil-harness

An attested Go service that owns chat prompts, turns, tools, persistence, and
run recovery. The `/v1` API accepts a Clerk session and, for stored data, a
base64 32-byte content encryption key (CEK) in the encrypted request body.
[DESIGN.md](DESIGN.md) describes the intended client migration.
[IMPLEMENTATION.md](IMPLEMENTATION.md) records the implemented scope and the
remaining rollout and contract gaps. The webapp source now uses the harness
on all chat routes while preserving its UI. Deployment and the iOS cutover
remain separate work.

All application routes are POST. JSON requests are limited to 8 MiB. Uploads
and imports use bounded multipart bodies. `GET /healthz` is public. The
transitional [AG-UI API](LEGACY.md) remains at `/agui` for comparison with
existing clients.

Code execution is temporarily disabled. The session API reports it unavailable,
and the harness does not connect to or advertise its tools.
The deployment allows unrestricted outbound traffic; the SDK still verifies
downstream enclave identities against the pinned repositories.

## Requests and events

Use the Tinfoil SDK's attested fetch transport to this service. The client must
retain custody of the CEK; it sends neither a constructed prompt nor tool
schemas. For example, a first stored turn has this JSON body:

```json
{
  "key": "<base64 CEK>",
  "threadId": null,
  "clientRequestId": "<UUID nonce>",
  "kind": "send",
  "content": "Explain the attached document",
  "attachments": [],
  "widgets": ["render_chart"],
  "options": {"model": "kimi-k3", "timezone": "America/Los_Angeles"}
}
```

`Authorization: Bearer <Clerk JWT>` authenticates the caller. The harness
exchanges it for an inference credential, but forwards the original Clerk
JWT to sync. A model request rejected with HTTP 401 refreshes the inference
credential and retries its unchanged body once under the same run ID.
Anonymous clients obtain a `free_` key directly from controlplane's existing
`GET /api/keys/chat` endpoint, then send `Authorization: Bearer <free key>` to
the harness. The harness validates it with `/api/shim/validate-key` before
starting work; downstream services continue enforcing validity and quotas.
It accepts no client-provided expiry or quota metadata. Anonymous turns
require `ephemeral: true`; temporary uploads also use `ephemeral=true`.

The response is SSE with monotonically increasing numeric IDs per run:
`RUN_STARTED`, message/state snapshots, text/reasoning/tool events, and a
terminal `RUN_FINISHED` or `RUN_ERROR`. The harness mints thread, message,
run, attachment, and queue IDs. A `clientRequestId` retry uses the original
thread and run, including after a process restart for stored threads.

`POST /v1/threads/follow` takes `{key, threadId, runId}` and optionally
`Last-Event-ID`. It sends an unnumbered message snapshot of the cursor prefix
before replaying subsequent numbered frames. A client replaces its messages
with that snapshot; it must not append it. Closing a connection does not cancel
a run. `POST /v1/threads/cancel` commits partial output as interrupted and
preserves the queue. SSE comments keep idle connections alive.

The other route families cover session/catalog display, thread CRUD/search,
projects/documents/memory, profile settings, attachments/transcription,
sharing, native v2 and Claude project exports, staged imports, key passthrough,
and downstream verification. The complete route table is in `api.go` and
`records.go`; bodies and response shapes are specified in DESIGN §5.

## Keys, persistence, and compatibility

The CEK is never logged or written to a local file. Code execution derives the
same HKDF keys as the web and iOS clients. Managed run logs use
`HKDF-SHA256(CEK, "confidential-tinfoil-harness run log v2:" + runId)` and
AES-GCM with frame indices authenticated as associated data. Stored runs spill
ciphertext from their start, including while the client is attached. Their
recovery authorization is independent of inference credential rotation.

Stored rows are read and written through confidential-sync's actual v2 wire
contract, including base64 plaintext, `if_match`, and `X-Sync-Protocol: 2`.
CAS retries reapply changes to freshly pulled rows, preserve unknown fields,
and maintain iOS edit clocks and profile field clocks. Legacy recovery fields
are carried through. Favorites are mirrored into the legacy profile field
while iOS still writes it.

Completed stored runs discard their CEK, JWT, input, and assembled plaintext.
Their sealed logs and run decryption context remain available in RAM for the
30-minute recovery window. Temporary conversations and their keys remain in
RAM for that window and are never spilled. `ask` reads a stored source thread
but runs a temporary side conversation with that transcript hidden from the
returned message window.

Images and document pages use sync attachment references in stored rows. The
harness makes thumbnails, extracts documents, transcribes audio, and caches
image descriptions in the thread. Export/import staging uses temporary files
containing only AES-GCM ciphertext; chunk indices are authenticated. Temporary
archive keys are random and discarded when the request ends.

## Build and checks

Go 1.26.6 or later is required. From this directory:

```sh
GOWORK=off go test -race -timeout 60s ./...
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

If `../tinfoil-webapp/node_modules` is installed, the tests also run frozen reference
validators from `scripts/fixtures/legacy-webapp/` against Go-produced
artifacts. Without that checkout or Node, those reference checks explicitly
skip. The remaining Go tests have no live service dependency.

The companion web client is in `../tinfoil-webapp/src/services/harness/`.
Configure `NEXT_PUBLIC_HARNESS_ENCLAVE_URL` with the deployed HTTPS origin
and open the main chat page to exercise the API. See the webapp's `HARNESS.md` for
its scope and tests. Go-produced event fixtures also run through that reducer.

Embedded widget declarations, presets, and memory schemas are maintained here.
Check the web renderer schemas against those declarations with:

```sh
node scripts/check-widgets.cjs ../tinfoil-webapp
```

The Docker build includes those embedded JSON files and a timezone database.
The SDK remains pinned to the existing gateway transport commit in `go.mod`.
No local module replacements are required.

## Deployment

`tinfoil-config.yml` exposes `/v1/*` through the shim and leaves Clerk
verification to Go. Only legacy `/agui` requires shim API-key authentication.
Anonymous requests use a browser-issued inference key. The harness does not
forward client IPs to controlplane.

Required configuration:

- `USAGE_CONTEXT_SECRET`: shared with the gateway and downstream usage
  reporters, so one model/tool/derived-call tree counts as one customer run.
- `CLERK_ISSUER`, and optionally `CLERK_AUDIENCE`, must match sync and the
  configured Clerk instance.
- Gateway/controlplane URLs and downstream enclave hosts can be changed with
  the `TINFOIL_*` variables shown in the measured configuration.

Sync and the Auto router are required at boot. Optional tool, document,
metadata, summary, and audio services are advertised only after attestation.
Each attestation attempt has a 20-second deadline, and each model or audio
replica pool has a 45-second budget to find a verified replica. Startup skips
unavailable pools and fails if no pinned chat model verifies. SDK metadata
requests have a 15-second HTTP timeout; streaming inference keeps its own
request context. At most eight SDK verification calls run concurrently. The
SDK cannot cancel verification directly, so timed-out calls retain their slots
until they finish and their late results are discarded.

The controlplane model endpoint returns a JSON array. The harness validates
each active chat model against its pinned repository and advertises only models
with an attested gateway replica. Pins include DeepSeek v4.1 Flash, GLM 5.3,
and GLM 5.3 Flash. Adding a pin does not create a gateway pool.

Memory extraction is disabled unless `TINFOIL_MEMORY_ENABLED=true`;
individual projects may opt out with `memoryEnabled=false`. Sandbox tools are
unavailable because the referenced sandbox service exposes a different API.

The upstream network uses `egress: open`, so replica changes do not require
updating a network allowlist. Downstream hosts must still resolve and pass SDK
attestation. The zero image digest is replaced by the existing release workflow
before measurement and publication.

The registry is bounded to 128 accepted runs/threads, with at most 32 queued
turns per thread. Run logs are capped at 8 MiB. Temporary attachments share a
128 MiB RAM budget. Four uploads and one archive operation can run concurrently.
Archive input is capped at 512 MiB; native exports also cap total entry bytes
and entry count. These limits should be sized against the deployment workload.

### Anonymous chat setup

Set the webapp's `NEXT_PUBLIC_API_BASE_URL` to `https://api.tinfoil.sh` and
`NEXT_PUBLIC_HARNESS_ENCLAVE_URL` to `https://chat-api.tinfoil.sh`. The browser
calls controlplane directly, so existing IP-based key issuance applies.
The key stays in memory and travels to the harness through its attested SDK
transport. The browser refreshes it on expiry or one HTTP 401, and discards it
on account changes. Issuance quota metadata is used only for the browser UI.

Deploy the updated harness and webapp together. This flow uses existing
controlplane endpoints and requires no controlplane release, migration, or
additional shared secret. Keep `USAGE_CONTEXT_SECRET` attached to the harness.

The existing `/api/shim/validate-key` endpoint accepts calls from registered
Tinfoil host IPs or configured `HOST_IPS`. Verify this access from the deployed
harness; a local harness process outside those hosts cannot use that endpoint.
The webapp can run locally against the deployed harness.

Anonymous ephemeral state is scoped to a hash of the presented key. Reconnects
with that key survive client IP changes. A different key starts a separate
scope, including when controlplane rotates the daily key. No anonymous
transcripts are persisted.
