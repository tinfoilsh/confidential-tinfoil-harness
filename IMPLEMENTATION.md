# Implementation and rollout status

The server and webapp source now use the harness for the full chat flow. The existing web UI and browser key custody remain. Anonymous key issuance stays in the browser and uses the existing controlplane endpoint. Deployment prerequisites, iOS cutover, and protocol differences below still apply. No service deployment is part of the webapp refactor.

| Responsibility | Implementation | Verification |
| --- | --- | --- |
| Clerk verification, inference exchange, anonymous credentials | `auth.go`; webapp `services/harness/anonymous-credential.ts` | Existing key validation, invalid/expired/disabled/quota refusal, direct issuance, bounded refresh, account changes, reconnect ownership |
| Catalog, prompts, reasoning, BPE budgeting | `catalog.go`, `context.go`, generated presets/widgets | Escaping, prompt ordering, whole-message budgeting, reasoning/tool history, schema validation |
| Stored turns, edits, regeneration, resolution, retries | `thread.go`, `sync.go`, `records.go`, `widgets.go` | Fake model/sync/controlplane integration; CAS with concurrent iOS fields; widget replacement |
| Queue, cancellation, temporary/ask conversations | `thread.go`, `shutdown.go` | Partial cancellation, queue retention, anonymous operation without sync |
| Events and recovery | `agui.go`, `timeline.go`, `recovery.go`, `resume.go` | Cursor snapshots, cold nonce replay, cross-user denial, corrupted interior frame rejection |
| Keys and row compatibility | `derive.go`, `legacy.go`, `pins.go` | WebCrypto HKDF vectors, actual web stored-row validators, unknown-field preservation |
| Attachments, document pages, audio | `attach.go` | Resize dimensions, transparent backgrounds, pixel limits, reference-based archive fixtures; no live converter/audio test |
| Projects, settings, memory, titles | `records.go`, `derived.go`, `memory.go` | Title starts before the answer finishes; isolated fact operations; project archive fixture |
| Sharing, export, import, search, key relay | `export.go`, `import.go`, `records.go` | Real native v2 archive validator; encrypted temporary-file and chunk-boundary tests; native-to-cloud import conversion |
| Ingress and image build | `tinfoil-config.yml`, `Dockerfile` | Go build/vet; embedded assets; local configuration validation |
| Web migration client | Webapp `services/harness/`, `pages/harness.tsx` | Transport/SSE/reducer tests, Go-event parity, account-change and input/render tests |

The webapp's main chat routes use `services/harness/` with the existing shell, widget renderers, settings, project panels, import/export controls, and key recovery dialogs. The old browser inference, transcript storage, replication, recovery, prompt assembly, and archive parser modules have been removed. The shared API supports bulk deletion in `records.go`, favicon lookup in `metadata.go`, normalized public shares in `export.go`, and queue priority sends in `thread.go`. First turns carry project/preset selection, and context usage comes from the server. See `../tinfoil-webapp/HARNESS.md` for configuration and local-data cutover limitations.

## Differences from the proposed contracts

1. **Auto routing.** The checked-out gateway accepts concrete model names and
   has no `auto` route. Auto therefore uses an additional attested connection
   to `confidential-model-router`, while named models use the gateway's existing
   attested 421 transport. This makes the router's model selection/trust policy
   part of the Auto trust boundary. Achieving DESIGN §7.4's exact topology
   requires an Auto-aware gateway transport or moving Auto selection into Go.
   The harness rejects configured, active chat models outside its pinned set.
   The pins now include DeepSeek v4.1 Flash, GLM 5.3, and GLM 5.3 Flash from
   the controlplane catalog. Catalog loading accepts its JSON array response.
   Models without an attested gateway replica are omitted. Attestation has a
   20-second deadline per attempt and a 45-second budget per replica pool.
   A failed refresh retains the last validated catalog; it does not authorize
   new pins.

2. **Anonymous credentials.** The browser obtains its free key directly from
   `/api/keys/chat` and sends it as a bearer credential to the harness. The
   harness validates it through the existing `/api/shim/validate-key` endpoint
   before starting work. Downstream services enforce validity and quotas on
   their calls. Browser expiry and quota metadata never authorize server work.
   Temporary state belongs to the key hash, so client IP changes do not change
   ownership; daily key rotation starts a new scope. The custom forwarding
   protocol and its controlplane changes have been removed. See the
   [setup steps](README.md#anonymous-chat-setup) for the existing host access
   requirement on key validation.

3. **Clerk token lifetime.** Sync checks the original Clerk JWT on every call;
   ordinary session tokens live about a minute. A detached 30-minute run can
   therefore finish inference after its token expires. Its encrypted run log
   remains recoverable, and a follow with a fresh JWT performs a pending final
   CAS. During a live run, follow also refreshes the JWT used at finalization.
   Unconditional final storage without a returning client requires a suitable
   longer-lived Clerk token or an explicit delegated sync credential. No
   authentication bypass or unverified JWT is used here.

4. **Replica affinity.** Queues and temporary conversations are local to one
   harness instance. A cold, incomplete log with an unexpired active-run lease
   is not declared abandoned: the other instance might still be running it.
   It returns `RUN_IN_FLIGHT` with retry advice. Keep live requests on their
   original instance; after the maximum run lifetime, another instance can
   recover interrupted output. A distributed lease/forwarding protocol is not
   part of this change.

5. **Queued SSE.** DESIGN §§5.4 and 5.5 conflict about frame zero for queued
   requests. Here, a queued POST starts with its queue delta and waits for its
   own run, whose `RUN_STARTED` retains that run's identity. The active stream
   also receives queue changes. The queued response does not multiplex the
   active run's numbered frames. Client reducers must follow that convention.

6. **Native backup.** Sync accepts `tinfoil-native-cloud-import` v1 with source
   `tinfoil_backup`, not the native-backup v2 ZIP unchanged. The harness checks
   paths, sizes, hashes, and inventory, then repackages entities and image blobs.
   Old local chats become cloud chats. Native v2 cannot express typed results
   directly on `tool_call`: built-in results use its existing `code_exec`
   output representation and are upgraded back on read. Old clients may display
   those results as code output. Tool progress and transport metadata do not
   survive the native v2 export. Core messages, tool arguments/results, widget
   resolutions, images, and document pages do.

7. **Verification display.** `/v1/verify` returns verified downstream repos,
   hosts, roles, and timestamps. Its own `measurement` is null with
   `verifiedBy: "client"`; the client must use its SDK's actual attestation
   document for the harness measurement. The service does not invent one.

8. **Optional features.** Memory extraction is implemented but off by default.
   The referenced sandbox workload exposes enrollment/SSH operations, not a
   compatible model tool family, so sandbox is explicitly unavailable. Live
   thinking summaries and anonymous continuity beyond 30 minutes remain the
   product decisions called out in DESIGN §13.

## Validation limits and next rollout steps

The harness tests use local HTTP fixtures, not live attested infrastructure.
Reference tests use frozen legacy validators with the webapp's installed dependencies. Browser tests cover the existing UI components and key flows, the attested transport, event reducer, controller reconnects, and account/key invalidation. The web typecheck, lint, widget schema comparison, and Go race suite can run without a development server.
Before moving client traffic, deploy the updated harness and webapp, retain
the existing usage secret, confirm key validation from the Tinfoil host, and
choose the long-run JWT policy. No controlplane release is required for
anonymous key issuance. The image digest is filled
by the existing release pipeline; no release was published during this work.

Exercise the existing web UI against the deployed harness before moving traffic. Validate uploads, key recovery, project first turns, reconnects, and archive import/export with a test account. Browser-only chats must be exported with the previous client before cutover. iOS remains a concurrent writer and has not been migrated; the Android implementation also remains outstanding.
