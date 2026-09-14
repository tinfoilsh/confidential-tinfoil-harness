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
Public anonymous turns require `ephemeral: true`; temporary
uploads also use the multipart field `ephemeral=true`.

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
Trusted proxy defaults include the shim bridge's `172.31.255.1/32` and
loopback. Set `TINFOIL_TRUSTED_PROXIES` for a different ingress topology;
forwarded addresses are accepted only from those proxies.

Required configuration:

- `USAGE_CONTEXT_SECRET`: shared with the gateway and downstream usage
  reporters, so one model/tool/derived-call tree counts as one customer run.
- `HARNESS_IDENTITY_SECRET`: a separate secret shared with controlplane.
  The companion controlplane change verifies signed, short-lived source-IP
  assertions for `/api/keys/chat`. Anonymous key exchange fails if the
  assertion is not acknowledged. Existing browser key exchange is unchanged.
- `CLERK_ISSUER`, and optionally `CLERK_AUDIENCE`, must match sync and the
  configured Clerk instance. The issuer's JWKS host must be allowed in egress.
- Gateway/controlplane URLs and downstream enclave hosts can be changed with
  the `TINFOIL_*` variables shown in the measured configuration.

Sync and the Auto router are required at boot. Optional tool, document,
metadata, summary, and audio services are advertised only after attestation.
Memory extraction is disabled unless `TINFOIL_MEMORY_ENABLED=true`;
individual projects may opt out with `memoryEnabled=false`. Sandbox tools are
unavailable because the referenced sandbox service exposes a different API.

The egress allowlist includes the previously documented model replicas and
the new services. Before release, reconcile it with the actual gateway catalog,
including Voxtral replica hosts; these hosts are not present in the reference
checkouts. DNS failures fail allowlist setup closed. The zero image digest is
replaced by the existing release workflow before measurement and publication.

The registry is bounded to 128 accepted runs/threads, with at most 32 queued
turns per thread. Run logs are capped at 8 MiB. Temporary attachments share a
128 MiB RAM budget. Four uploads and one archive operation can run concurrently.
Archive input is capped at 512 MiB; native exports also cap total entry bytes
and entry count. These limits should be sized against the deployment workload.
