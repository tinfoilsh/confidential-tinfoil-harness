# Chat harness: a client with no chat logic

This is the design for extending `confidential-tinfoil-harness` from an agent loop
into the service that owns a conversation end to end, so that `tinfoil-webapp`
(and the iOS and Android clients after it) keep only three things: custody of
the user's key, a reducer from server events to a view model, and rendering.

Everything below is grounded in the current `tinfoil-webapp` source (94k lines of
TS/TSX under `src/`, 205 test files), the current `tinfoil-ios` source (67k lines
of Swift in the app target, 13k in tests) and the current harness (2,181 lines of
Go). Where the doc says a file is deleted, that file was read and its
responsibility has a named home in the harness or has ceased to exist.

The organising rule, applied to every item in §9 and §10:

> **A line of code lives in the client only if it needs something the harness
> cannot have: the raw key, a pixel, or a finger.**

Everything else -- anything that reads two messages, anything that knows a model
name, anything that decides when to save, retry, title, budget, sync, resume, or
what a system prompt says -- is harness code.

---

## 0. What changes, in one screen

| | Today | After |
|---|---|---|
| Client talks to | Clerk, controlplane (`/api/config/*`, `/api/keys/chat`, `/api/chat/token`, `/api/shares`, `/recovery/*`), inference gateway (via model router), sync enclave (`/v1/sync/*`, `/v1/key/*`, `/v1/attachment/*`, `/v1/search/*`, `/v1/share/*`, `/v1/import/*`, `/v1/blobs/*`), summarizer enclave, opengraph-metadata enclave, docling enclave | Clerk, controlplane for account operations and anonymous key issuance, **harness** |
| Enclaves the client attests | model router (or harness), sync, summarizer, metadata, docling | **harness** |
| Where the prompt is built | `chat-query-builder.ts` in the browser | `context.go` |
| Where the thread is stored | IndexedDB (7 object stores) + sessionStorage, mirrored to the sync enclave with Lamport clocks, ETags, coalescers, tombstones | sync enclave, written by the harness only; client has no store |
| Where a turn runs | browser tab; `chat-recovery*.ts` (2,000 lines) rescues it when the tab dies | harness run log; client reattaches by id |
| Model catalog, reasoning params, context windows, the Auto intelligence scale | `config/models.ts` (615 lines) in the client | `catalog.go`; client gets display fields only |
| Title, memory, summaries, transcription, doc extraction, image resize, link metadata | client calls five enclaves | harness calls them; client sees the result |
| Client code | web ~94k lines, iOS ~67k | rendering + key custody: web ~42k (§9 sums to ~52k removed, ~550 added), iOS ~32k (§10 sums to ~36k removed, ~550 added) |

---

## 1. Trust and keys

This section comes first because everything else depends on it being acceptable.

### 1.1 What the client already does

The client today hands its 32-byte content encryption key (CEK) to the sync
enclave **on every push, pull, delete, search and share call** (`sync-api.ts`:
"supplying the user's CEK on every push/pull/delete"). The enclave unseals rows
server-side and returns plaintext. The client trusts this because it attested
the enclave against `tinfoilsh/confidential-sync`.

So the trust step "an attested enclave holds my key and my plaintext in RAM for
the duration of a request" is already taken. The harness design does not add a
new kind of trust; it changes *which* attested enclave the client gives the key
to, and lets that enclave do more with it.

### 1.2 What crosses, and where it stops

```
client                         harness (CVM)                 sync enclave (CVM)   controlplane
──────                         ─────────────                 ──────────────────   ────────────
passkey PRF ─► KEK ─► CEK      CEK in RAM per request        CEK in RAM per call  ciphertext only
(WebAuthn, never leaves)       plaintext thread in RAM       seals/unseals rows   ETags, key_ids
                               HKDF(CEK) → code-exec tokens,                      sealed run logs
        CEK in request body ─► sandbox secret, run-log key
                               attests sync, models, tools
                               never logs, never spills
                               plaintext
```

- **Client** holds the CEK in memory (as `encryptionService` does now) and the
  passkey-wrapped bundles. Nothing else about the thread lives on the client.
- **Harness** receives the CEK in the body of every authenticated request (see
  §5.1 for why the body). It holds it for the life of that request or run,
  derives from it, and discards it. Plaintext exists only in RAM. The harness
  never writes plaintext anywhere: the thread goes back to the sync enclave
  sealed by the sync enclave; the run log is sealed under an HKDF of the CEK
  before it leaves the process.
- **Sync enclave** is unchanged. It gains one new caller (the harness) and, over
  the transition, loses its old ones.
- **Controlplane** keeps seeing exactly what it sees today: ciphertext rows with
  ETags, and the sealed run logs `recovery.go` already spills there.

### 1.3 What the harness derives from the CEK

Every derivation `exec-snapshot/key-derivation.ts` does today moves verbatim:

| Purpose | Derivation | Today | After |
|---|---|---|---|
| Code-exec encryption key | `HKDF(CEK, "tinfoil-code-execution-encryption-key-v1")` | client, sent in `forwardedProps` | harness, `derive.go` |
| Code-exec container auth token | `HKDF(CEK, "tinfoil-code-execution-v1:" + chatId)` | client | harness |
| Sandbox cache secret | `HKDF(CEK, "tinfoil-agent-sandbox-cache-secret-v1")` | client | harness |
| Code-exec access token | random 32 bytes at chat creation, stored in chat | client | harness, stored in thread row |
| Run-log key | `HKDF(recoveryToken, hash(apiKey))` | harness | `HKDF(CEK, "confidential-tinfoil-harness run log v2:" + runId)` |

The last row is the one real change to the harness's own crypto. Today a run log
is opened by a client-minted `sessionId` + `recoveryToken`. After, it is opened
by the CEK, which every device of the user has, so cross-device resume falls out
for free and the client mints nothing.

### 1.4 What the harness's own README currently promises and what changes

`README.md` says "The harness never reads [the code-exec tokens]." After this
design it derives them. It says "a `sessionId` names nothing… the store cannot
tell which conversation a log belongs to." After this design the run log id is
the run id, which the thread row names. Both are deliberate: the harness has
become the thing that owns the thread, and hiding the thread from itself buys
nothing once it holds the plaintext anyway. The store still cannot read either.

---

## 2. Topology

```
   web            iOS           Android
    │              │               │
    └──────────────┼───────────────┘
                   │  one attested endpoint
                   │  JSON over POST, SSE for runs
                   │  Clerk JWT or free key; CEK in body
                   ▼
        ┌──────────────────────────────────┐
        │   HARNESS  (CVM)                 │
        │   auth · session · catalog       │
        │   threads · turns · queue        │
        │   context · loop · runlog        │
        │   attachments · derived          │
        └─┬─────┬──────┬──────┬──────┬─────┘
          │     │      │      │      │        every hop attested by the harness
          ▼     ▼      ▼      ▼      ▼        against a pinned repo, as main.go
        model  tools  sync   summ-  doc/      does for models and tools today
        encl.  encl.  encl.  arizer meta/
        (gw)   ws/ce         audio
                        │
                        ▼
                  controlplane
                  ciphertext rows, run logs, config, auth
```

The client's chat network surface is one host. The harness's egress allowlist
(`tinfoil-config.yml`) grows by: `sync.tinfoil.sh`, `summarizer.tinfoil.sh`,
`opengraph-metadata.tinfoil.sh`, the docling enclave, the audio model's replicas,
and Clerk's JWKS host.

---

## 3. Harness internals

Flat repo, one package, as now. New files beside the five that exist:

```
main.go        boot: attest everything, mount routes            (exists, grows)
agui.go        SSE framing, event types, run follow/resume       (exists; +STATE_* events)
agent.go       the tool loop                                     (exists, unchanged in shape)
tools.go       tool families table                               (exists, unchanged)
recovery.go    run log, seal, spill, rehydrate                   (exists; key derivation changes)

api.go         route table, request decoding, error envelope, CEK extraction
auth.go        Clerk JWT verify (JWKS), signed-in chat-key exchange,
               rate-limit cache per user, validation of client-issued free keys
catalog.go     models (controlplane /api/config/models ∩ gateway /catalog ∩ pinned repos),
               the Auto intelligence scale (§7.4), widgets (name, description, schema,
               hint, surface), presets, upload accept list
sync.go        client of confidential-sync: push/pull/list/delete, attachment put/get,
               search, share seal/open, import, key/* passthrough
thread.go      thread model, load/commit with ETag CAS + retry, list, queue, cancel,
               ephemeral threads, active-run marker
context.go     prompt assembly (§7.1), budgeting (§7.2), model quirks (§7.3)
derive.go      every HKDF in §1.3
attach.go      upload pipeline: sniff → text | image resize+thumbnail | docling | audio
derived.go     after-turn work: title (summarizer), memory facts (flagged), context usage
export.go      export archives (native backup v2, Claude projects), share payload
verify.go      attestation report for the client (what this harness verified, when)
```

Roughly 5–6k lines of Go replacing roughly 52k lines of TS (§9) and 36k of Swift
(§10). Most of the TS was written three times over (IndexedDB, sync, recovery
each re-derive the same message shape), then all of it once more in Swift; the
Go is written once.

---

## 4. Conventions on the wire

### 4.1 Transport

- **All requests are `POST` with a JSON body**, including reads. This mirrors the
  sync enclave (`sync-api.ts`: "All endpoints are POST with a JSON body") and
  for the same reason: the CEK travels in the body, and the SDK's encrypted-body
  transport seals bodies, not headers. The only exceptions are `GET /healthz`
  and the SSE responses to turn/resume calls.
- Run responses are `text/event-stream`, framed exactly as `agui.go` frames them
  today: `id: <n>\ndata: <json>\n\n`, ids monotonic from zero per run.
- Request bodies are capped at 8 MiB as today (`maxRequestBody`), except
  attachment upload and import which are streamed multipart with their own caps.

### 4.2 Authentication

Two states, one header:

| Caller | `Authorization` | Harness does |
|---|---|---|
| signed in | `Bearer <Clerk session JWT>` | verifies against Clerk JWKS (cached); exchanges for an inference key via controlplane `GET /api/chat/token` (subscriber) falling back to `GET /api/keys/chat` (free tier), caching per user until `expires_at`; forwards the inference key upstream exactly as `callerAuth` does today |
| anonymous | `Bearer <free_ key>` | client obtains the key directly from `GET /api/keys/chat`; harness validates it with the existing `/api/shim/validate-key` endpoint before starting work and scopes temporary state by key hash |

This deletes `tinfoil-client.ts` (768 lines: generation counters, cache
invalidation, JWT-vs-opaque fallback, hourly-limit surfacing, optimistic
decrement). A small browser credential cache remains for anonymous issuance,
expiry, and one retry after HTTP 401. Anonymous quota metadata comes directly
from controlplane for display; server validation and downstream enforcement
remain authoritative. Other rate limit state comes from the harness.

**Infra change required:** the shim currently has `authenticated: true`, meaning
it expects a Tinfoil API key. Either the shim learns Clerk JWTs, or the new
routes run with `authenticated: false` and the harness validates. This doc
assumes the latter; `auth.go` owns it.

### 4.3 The key field

Every request that touches a stored thread, profile, project, attachment or the
search index carries:

```json
{ "key": "<base64 raw 32 bytes>", ... }
```

Absent on a request that needs it → `401 KEY_REQUIRED`. Present but the sync
enclave answers `409 STALE_KEY` / `EXISTING_DATA_UNDER_OTHER_KEY` →
`409 KEY_MISMATCH` with the enclave's `key_id` echoed, so the passkey recovery UI
can open. The harness never persists this field and redacts it in logs (as
`secret` does today via `LogValue`).

### 4.4 Identifiers

The harness mints every id: thread ids, message ids, run ids, attachment ids,
queue ids. The client mints nothing. This deletes `utils/reverse-id.ts`,
`generateRecoverySessionId`, `generateCodeExecutionAccessToken`,
`newIdempotencyKey` callers, `generateQueuedId`, and the "blank chat has no id
until first message" state machine in `chat-operations.ts` /
`use-chat-storage.ts`.

Thread, project and document ids keep today's shape, `<13-digit reverse
millisecond timestamp>_<uuid>`, which is what `utils/reverse-id.ts` and the
controlplane's `/api/chats/generate-id` mint now, so new rows sort among old
ones in the sync enclave's lists and iOS keeps reading them during §12.2. The
three `generate-id` helpers on the controlplane lose their callers.

Idempotency: turn requests carry a client-generated `clientRequestId` (UUID) so a
retried POST after a dropped connection does not append the user message twice.
That is the one id the client generates, and it is only a nonce.

### 4.5 Error envelope

```json
{ "error": { "code": "QUOTA_EXHAUSTED", "message": "…", "kind": "free_daily",
             "resetsAt": "2026-09-05T00:00:00Z", "retryAfter": 30 } }
```

`code` is the only field control flow may branch on (webapp `AGENTS.md`: never
string-match error messages). Full list in §5.12. Inside a stream the same shape
appears as `RUN_ERROR.code`.

---

## 5. The wire surface, exhaustively

Every endpoint the webapp will call. Nothing else is exposed to it.

### 5.1 Health

`GET /healthz` → `{"ok":true}`. Unauthenticated. Already exists.

### 5.2 Session

`POST /v1/session` `{}` → everything the client needs to draw before the user
does anything. Replaces `/api/config/models`, `/api/config/system-prompt`
(the client no longer sees the prompt, only the widget allow-list),
`/api/config/rate-limits`, `/v1/key/current`, and the client-side model
catalog.

```json
{
  "user": { "id": "user_…", "anonymous": false },
  "rateLimit": { "kind": "free_daily" | "hourly" | null,
                 "maxRequests": 20, "remaining": 17, "resetsAt": "…" },
  "key": { "registered": true, "keyId": "<32 hex>" | null,
           "bundles": [{ "credentialId": "…", "createdAt": "…" }] },
  "models": [{
     "id": "kimi-k3", "name": "Kimi K3", "nameShort": "Kimi K3",
     "description": "…", "descriptionShort": "Best for coding and visual tasks",
     "image": "moonshot.png", "multimodal": true,
     "reasoning": { "toggle": true, "effort": false, "defaultEnabled": true } | null,
     "attributes": ["smart"], "paid": true,
     "experimental": false, "deprecated": false, "deprecationDate": null
  }],
  "auto": { "multimodal": true, "default": "high",
            "levels": [{ "id": "instant", "label": "Instant" }, { "id": "low", "label": "Low" },
                       { "id": "medium", "label": "Med" }, { "id": "high", "label": "High" },
                       { "id": "extra", "label": "Extra" }, { "id": "max", "label": "Max" }] } | null,
  "defaultModel": "auto",
  "widgets": ["render_stat_cards", "render_timeline", "render_chart", "…"],
  "presets": [{ "id": "builtin:tutor", "name": "Tutor",
                "description": "Patient teacher who explains step by step",
                "icon": "student", "builtIn": true }],
  "features": { "webSearch": true, "codeExecution": true, "sandbox": false,
                "piiCheck": true, "genUI": true, "transcription": true },
  "upload": { "accept": [".pdf", ".docx", ".png", "…"],
              "maxBytes": 34603008, "maxTextBytes": 52428800 }
}
```

Every field is display-only. `models[]` carries none of
`chatConfig.contextWindowTokens`, `chatConfig.intelligence`,
`reasoningConfig.params`, `effortMap`, `reasoningHistoryPolicy`, `requestParams`,
`endpoint`, `toolCalling`: those decide requests, and requests are built in the
harness. `auto.levels` carries labels only; where a level sits on the router's
0–100 scale is `catalog.go`'s business (§7.4). Deprecated models stay in the list
so a thread pinned to one still has a name and a badge; the picker hides them as
it does today.

### 5.3 Threads

| Call | Body | Returns |
|---|---|---|
| `POST /v1/threads/list` | `{key, cursor?, limit?, projectId?}` | `{threads: [ThreadSummary], nextCursor?}` |
| `POST /v1/threads/get` | `{key, id, before?, limit?}` | `Thread` (§6.1) with the newest `limit` messages before `before`, plus `hasOlder` |
| `POST /v1/threads/update` | `{key, id, title?, model?, webSearchEnabled?, projectId?, presetId?, pinned?}` | `ThreadSummary` |
| `POST /v1/threads/delete` | `{key, id}` | `{}` |
| `POST /v1/threads/search` | `{key, query, limit?}` | `{results: [{id, title, score, updatedAt}], indexing: bool}` |

`ThreadSummary`:

```json
{ "id": "…", "title": "…", "titleState": "placeholder"|"generated"|"manual",
  "createdAt": "…", "updatedAt": "…", "projectId": null, "model": "kimi-k3",
  "webSearchEnabled": true, "pinned": false, "messageCount": 12,
  "activeRun": { "runId": "…", "lastEventId": 41 } | null }
```

There is no "create thread" call. A turn with `threadId: null` creates one and
`RUN_STARTED` names it (§5.4). This deletes `createBlankChat`,
`createTemporaryChat`, `ensureAtLeastOneChat`, `isBlankChat`, `pendingSave`, and
the URL-swap-on-first-message dance in `use-chat-router.ts` /
`use-chat-storage.ts`; the client just navigates to the id it is told.

`pinned` replaces `pinnedChatIds` in the profile blob plus `use-pinned-chats.ts`,
`pinned-chats.ts`, `pinned-chat-hydration.ts`. Favorites become a filter on the
list (`POST /v1/threads/list {pinned: true}`).

`search` wraps the sync enclave's `/v1/search/query` and its
`needs_reindex → /v1/search/reindex → poll` dance from `chat-search.ts`. The
client sends a string and gets rows.

### 5.4 Turns

`POST /v1/threads/turn` → `text/event-stream`

```json
{
  "key": "…",                       // absent only when ephemeral: true and anonymous
  "clientRequestId": "<uuid>",      // idempotency nonce, the one id the client mints
  "threadId": "…" | null,           // null creates a thread
  "kind": "send" | "edit" | "regenerate" | "resolve" | "retryToolCall" | "ask",
  "content": "…",                   // send, edit
  "quote": "…",                     // send (reply-to), ask (the highlighted text)
  "attachments": ["att_…"],         // send, edit — ids from /v1/attachments/upload
  "messageId": "…",                 // edit: the user message to replace from
                                    // regenerate: the assistant message to redo
  "toolCallId": "…",                // resolve, retryToolCall
  "resolution": { "text": "…", "data": {} },   // resolve
  "widgets": ["render_chart", "…"], // which enabled widgets this client can draw
  "ephemeral": true,                // never stored; anonymous and "temporary chat" both use this
  "options": {                      // all optional; absent = thread default = profile default
    "model": "auto" | "kimi-k3",
    "autoIntelligence": "instant"|"low"|"medium"|"high"|"extra"|"max",
    "timezone": "Europe/Zurich",    // the one fact about the user the harness cannot know (§7.1)
    "reasoningEffort": "low"|"medium"|"high",
    "thinking": true,
    "webSearch": true,
    "codeExecution": true,
    "sandbox": false, "sandboxReset": false,
    "piiCheck": false,
    "genUI": true
  }
}
```

`kind` semantics, each of which is a client hook today:

| kind | Replaces | Harness does |
|---|---|---|
| `send` | `handleQuery` | append user message, run |
| `edit` | `editMessage` (truncate + resubmit) | truncate thread after `messageId`, append new user message with the original's attachments unless `attachments` given, run |
| `regenerate` | `regenerateMessage` | truncate from the assistant `messageId`, run again from the preceding user message |
| `resolve` | `resolveInputToolCall` | mark the `tool_call` block resolved with `resolution`, append `resolution.text` as a user message, run |
| `retryToolCall` | `genui/retry.ts` (285 lines of context selection + structured completion) | re-ask the model for that widget's arguments under its schema, emit `TOOL_CALL_START/ARGS/END` for the **same** `toolCallId`; client replaces |
| `ask` | `use-sidebar-chat.ts` | ephemeral run with `threadId`'s transcript as hidden context and `quote` as the question; nothing stored |

If a run is already in flight on the thread, the turn is **queued** (§7.7)
rather than refused. The response is still an SSE stream; its first frame is
`STATE_DELTA` adding the queue entry, and it then follows the live run. This
deletes `use-message-queue.ts` (588) and `message-queue-identity.ts`.

Anonymous callers may only send `ephemeral: true` turns (there is no key to
store under). See §12.3 for what that means for anonymous continuity.

#### Events

The stream carries the eleven AG-UI event types the harness emits today plus
three standard ones. Every frame has `type`, and `threadId`/`runId` on the
run-level ones.

| Event | Fields | Client reducer does |
|---|---|---|
| `RUN_STARTED` | `threadId`, `runId`, `queued: bool` | set thread id (navigate if it was null); mark streaming |
| `STATE_SNAPSHOT` | `snapshot: {thread: ThreadSummary, queue: [QueueItem], rateLimit}` | replace thread meta, queue, rate limit |
| `STATE_DELTA` | `delta: [RFC 6902 ops]` on the same object | apply patch (title arrives this way after the run; `remaining` decrements here) |
| `MESSAGES_SNAPSHOT` | `messages: [Message]`, `hasOlder` | replace messages (sent on resume and on `edit`/`regenerate` truncation); the same window `threads/get` serves, so a phone never holds a whole thread it is not showing |
| `TEXT_MESSAGE_CHUNK` | `messageId`, `role: "assistant"`, `delta` | append to content block of `messageId` |
| `REASONING_MESSAGE_CHUNK` | `messageId` (`<id>-reasoning`), `delta` | append to thinking block |
| `TOOL_CALL_START` | `toolCallId`, `toolCallName`, `parentMessageId` | open tool_call block (widget or built-in) |
| `TOOL_CALL_ARGS` | `toolCallId`, `delta` | append to arguments; widgets re-render from partial JSON |
| `TOOL_CALL_END` | `toolCallId` | mark arguments complete |
| `TOOL_CALL_RESULT` | `toolCallId`, `messageId`, `role: "tool"`, `content`, `metadata?` | attach result to the block (shape per tool in §7.5) |
| `ACTIVITY_SNAPSHOT` | `messageId: act_<toolCallId>`, `activityType: "TOOL"`, `content: {toolCallId, tool, progress, output: []}` | open progress panel |
| `ACTIVITY_DELTA` | `messageId`, `patch: [ops]` | patch progress panel |
| `RUN_FINISHED` | `threadId`, `runId`, `metadata: {usage, rateLimit}` | mark idle; update rate limit |
| `RUN_ERROR` | `threadId`, `runId`, `message`, `code` (§5.12), `retryAfter?` | mark error on the last assistant message; banner by `code` |

The `thinking_start/delta/end/tail`, `web_search`, `url_fetch`, `annotation`,
`search_reasoning`, `code_exec_tool_call`, `<tinfoil-event>` marker parsing,
reasoning-vs-content boundary heuristics, and "late reasoning tail" recovery in
`event-normalizer.ts` (390), `tinfoil-events.ts` (278), `content-preprocessor.ts`,
`timeline-builder.ts` (312), `message-assembler.ts` (131),
`rich-stream-session.ts` (268) and `interrupted-message.ts` cease to exist on the
client. The harness already consumes the raw upstream stream in `agent.go/turn`
and emits clean events; the boundaries are decided once, in Go.

#### Cancel

`POST /v1/threads/cancel` `{key, threadId, runId}` → `{}`. Stops the run; the
stream ends with `RUN_FINISHED` carrying `metadata.cancelled: true` and whatever
partial assistant message exists is committed as-is (`isInterrupted: true` on
the message). This replaces the client's `AbortController` plumbing in
`use-chat-streams.ts` and the `persistInterruptedAssistant` path -- and works
from a device other than the one that started the run.

`POST /v1/threads/queue/remove` `{key, threadId, queueId}` → `{}` removes a
queued turn; `STATE_DELTA` follows on any attached stream.

### 5.5 Resume

`POST /v1/threads/follow` `{key, threadId, runId}` with header
`Last-Event-ID: <n>` → `text/event-stream` replaying frames after `n`, live if
the run is still going, from the stored log otherwise. A client that opens a
thread and sees `activeRun` in `ThreadSummary` calls this; `agui.go/follow`
already implements the mechanics.

Frame 0 is always `RUN_STARTED`, and `MESSAGES_SNAPSHOT` is sent first on any
follow so a device that never saw the run has the whole thread before deltas
arrive.

This deletes `chat-recovery.ts` (1018), `chat-recovery-sync.ts` (506),
`chat-recovery-crypto.ts` (342), `chat-recovery-client.ts` (156),
`chat-recovery-drafts.ts` (130), `use-chat-recovery-drafts.ts`,
`types/chat-recovery.ts`, `utils/chat-recovery-envelope.ts`, the
`pendingRecoveries` field and its zod schema, `streaming-tracker.ts` (141),
`use-streaming-chats.ts`, and the recovery-token capture in
`inference-client.ts`. The problem they solve -- a turn that lives in a browser
tab -- stops existing.

### 5.6 Attachments and transcription

`POST /v1/attachments/upload` -- multipart: `key`, `file`, optional
`threadId` (for the attachment's storage association) →

```json
{ "id": "att_…", "kind": "image" | "document" | "audio",
  "fileName": "report.pdf", "mimeType": "application/pdf", "size": 812331,
  "thumbnail": "<base64 jpeg, ≤ 256px>" | null,
  "pages": 12 | null, "status": "ready" }
```

The harness does what `document-uploader.tsx` (394), `preprocessing/media.ts`
(207) and `file-types.ts` (273) do now: sniff the type against the accept list;
read plain-text formats directly; scale images to 1536px and emit a thumbnail;
send PDFs/DOCX/PPTX/XLSX to the docling enclave (the `type: "document"` model
in the catalog) with the 10-minute timeout; store the bytes through the sync
enclave's `/v1/attachment/put` under a per-attachment key it keeps in the
thread row. A description for non-vision models is generated **at turn time**,
only if the chosen model lacks vision (today it is generated at upload time
regardless).

`POST /v1/attachments/get` `{key, id}` → bytes with the original `Content-Type`
(lightbox, download). `POST /v1/attachments/delete` `{key, id}` → `{}`.

`POST /v1/transcriptions` -- multipart: `file` (webm/wav/m4a) → `{text}`. Wraps
the gateway's audio model (`voxtral-small-24b` today) exactly as
`chat-input.tsx/sendAudioForTranscription` does; the client keeps only
`MediaRecorder`.

### 5.7 Projects

| Call | Body | Returns |
|---|---|---|
| `POST /v1/projects/list` | `{key, cursor?}` | `{projects: [Project], nextCursor?}` |
| `POST /v1/projects/get` | `{key, id}` | `Project` with `documents[]` and `contextUsage` |
| `POST /v1/projects/create` | `{key, name, description?, systemInstructions?, color?}` | `Project` |
| `POST /v1/projects/update` | `{key, id, name?, description?, systemInstructions?, color?}` | `Project` |
| `POST /v1/projects/delete` | `{key, id}` | `{}` |
| `POST /v1/projects/documents/upload` | multipart `key, projectId, file` | `ProjectDocument` |
| `POST /v1/projects/documents/delete` | `{key, projectId, documentId}` | `{}` |
| `POST /v1/projects/memory/get` | `{key, projectId}` | `{facts: [Fact]}` |
| `POST /v1/projects/memory/update` | `{key, projectId, facts: [Fact]}` | `{facts}` |

`contextUsage` (`{systemInstructions, documents[{filename, tokens}], memory,
totalUsed, modelLimit, availableForChat}`) is computed by the harness with a
real tokenizer against the thread's model; `project-context.tsx/estimateTokenCount`
and `ProjectContextUsage` client math go.

Project memory is commented out in the client today (`buildProjectContext`,
`use-chat-messaging.ts:1105`). The harness carries the extractor behind a flag
(§7.6); the endpoints exist so the settings UI can show and edit facts.

This deletes `project-storage.ts` (811), `project-provider.tsx` (695 →
~120 of fetch + context), `project-cache.ts`, `project-document-hydration.ts`,
`project-load-validity.ts`, `use-projects.ts` (321 → ~60), `use-memory.ts`,
`fact-extractor.ts`, `types/memory.ts` (schema only, keep the `Fact` type).

### 5.8 Profile

`POST /v1/profile/get` `{key}` → `Profile` (§6.2).
`POST /v1/profile/update` `{key, patch: {…subset of Profile…}}` → `Profile`.

The harness does the read-modify-write against the sync enclave's `profile`
row with ETag CAS and retry. There is one writer per user per request; the
three-way merge, per-field Lamport clocks, dirty flags, baselines and
"unknown fields" preservation in `profile-sync.ts` (489), `profile-merge.ts`
(372), `profile-settings-serializer.ts` (596), `profile-sync-state.ts`,
`profile-sync-coordinator.ts`, `use-lossless-profile-sync.ts` (469),
`edit-clock.ts` (170) exist to reconcile two clients writing the same blob
from localStorage mirrors. With no localStorage mirror there is nothing to
reconcile. (Transition caveat for iOS in §12.2.)

`Profile` carries every user-editable setting the settings modal shows,
including the ones that today live only in localStorage and never synced
(`themeMode`, `chatFont`, `selectedModel`, `autoIntelligence`,
`pixelateSidebarChatTitles`, `browserTabChatTitle`, `enterToNewline`,
`hasSeenOnboarding`, dismissed-warning flags). The client may mirror
`themeMode` and `chatFont` in localStorage to avoid a flash on load; that mirror
is a cache of a server value, not a source of truth.

### 5.9 Share, export, import

| Call | Body | Returns | Replaces |
|---|---|---|---|
| `POST /v1/threads/share` | `{key, threadId}` | `{url}` | `share-modal.tsx` payload building, `utils/share-payload.ts`, `share-api.ts`, `shareSeal` |
| `POST /v1/shares/open` | `{shareId, fragment}` (unauthenticated) | `SharedThread` (a `Thread` minus ids and keys, attachments as `{id, attKey}` for `attachments/get-public`) | `pages/share/[[...slug]].tsx` fetch + `shareOpen` |
| `POST /v1/attachments/get-public` | `{id, attKey}` | bytes | `attachmentGetPublic` |
| `POST /v1/export` | `{key, format: "tinfoil-backup" \| "claude-projects", threadIds?, projectId?}` | `tinfoil-backup`: the `tinfoil-native-backup` v2 zip (`manifest.json`, `projects/`, `project_documents/`, `cloud_chats/`, `relationships/`, `images/`; `local_chats/` always empty) as a stream; `claude-projects`: the Claude-compatible projects JSON | `services/native-backup/*` (3,801), `export-archive.ts` (271), `project-export/claude-project-export.ts` (304), `chat-import-parsers.ts` for the Tinfoil format |
| `POST /v1/import` | multipart `key, source: chatgpt|claude|tinfoil, file` | `{jobId}` | `off-device-import.ts`, `local-tinfoil-import*.ts` (worker + parser), `chat-import/constants.ts`, `native-backup/restore.ts`, `native-backup/orchestrate.ts` |
| `POST /v1/import/status` | `{jobId}` | `ImportStatusResponse`, `failureReason` ∈ `timeout, invalid_archive, limit_exceeded, key_mismatch, internal` | polling in `native-backup/orchestrate.ts` and the settings modal; `import-failure-copy.ts` keeps rendering the reason |

The harness builds the share payload from the plaintext it already holds, calls
the sync enclave's `/v1/share/seal`, PUTs the ciphertext to controlplane
`/api/shares/{id}` with the user's inference key, and returns
`https://…/share/{id}#v2:{shareKeyHex}`. The share page sends the fragment to
`shares/open`. Only `v2:` fragments exist: the webapp dropped legacy share links
in #550, so `compression.ts` and `share-encryption.ts` are already gone.

A backup restore is an import. `native-backup/restore.ts` validates the archive
in the browser and then hands it, unchanged, to the sync enclave's `/v1/import`,
so the enclave already accepts the format; the harness streams the file through
and the browser-side validation (`schemas.ts`, `sanitize.ts`, 529 lines of zod)
becomes whatever the enclave answers. `local_chats` in an old archive restore as
cloud chats, since there is no local store to put them in.

### 5.10 Keys

Verbatim passthroughs to the sync enclave so the client attests one enclave.
The harness reads nothing from these bodies but the route.

| Harness | Sync enclave |
|---|---|
| `POST /v1/keys/current` | `/v1/key/current` |
| `POST /v1/keys/register` | `/v1/key/register` |
| `POST /v1/keys/add-bundle` | `/v1/key/add-bundle` |
| `POST /v1/keys/remove-bundle` | `/v1/key/remove-bundle` |
| `POST /v1/keys/migrate` | `/v1/blobs/migrate` |
| `POST /v1/keys/migrate-all` | `/v1/blobs/migrate-all` |
| `POST /v1/keys/migrate-status` | `/v1/blobs/migrate-status` |

The passkey ceremony itself (`@tinfoilsh/passkey-kit`, WebAuthn PRF, wrap/unwrap)
stays in the client. It is the one piece of real logic the client keeps, because
it is the thing that produces the key.

### 5.11 Verification

`POST /v1/verify` `{}` →

```json
{ "harness": { "repo": "tinfoilsh/confidential-tinfoil-harness", "measurement": "…" },
  "verified": [
    { "role": "model", "name": "kimi-k3", "repo": "…", "enclave": "…", "at": "…" },
    { "role": "tool",  "name": "webSearch", "repo": "…", "enclave": "…", "at": "…" },
    { "role": "sync",  "repo": "tinfoilsh/confidential-sync", "enclave": "sync.tinfoil.sh", "at": "…" },
    { "role": "summarizer", … }, { "role": "document", … }, { "role": "metadata", … }
  ] }
```

The client's own `SecureClient` verifies the harness; `verification-sidebar.tsx`
renders that document plus this list. The client no longer holds a
`VerificationDocument` per downstream enclave because it no longer talks to
them.

### 5.12 Error codes

| Code | HTTP | Meaning | Client does |
|---|---|---|---|
| `UNAUTHENTICATED` | 401 | JWT missing/invalid where required | sign-in |
| `KEY_REQUIRED` | 401 | stored-data call without `key` | open key setup |
| `KEY_MISMATCH` | 409 | enclave has data under another `keyId` | open passkey recovery |
| `QUOTA_EXHAUSTED` | 429 | `kind: free_daily` or `hourly`, `resetsAt` | upgrade prompt / banner |
| `RUN_IN_FLIGHT` | 409 | only for `cancel`/`follow` on a run that isn't this thread's | refresh |
| `THREAD_NOT_FOUND` | 404 | | navigate away |
| `REVISION_CONFLICT` | 409 | CAS lost after retries (another writer) | refetch thread |
| `MODEL_UNAVAILABLE` | 404 | pinned model not attested / not in catalog | pick another |
| `CONTEXT_TOO_LARGE` | 413 | a single message exceeds every model's window | trim attachment |
| `ATTACHMENT_UNSUPPORTED` / `ATTACHMENT_TOO_LARGE` | 415 / 413 | | toast |
| `TOOL_UNAVAILABLE` | 503 | family enclave did not verify at startup | disable toggle |
| `UPSTREAM_REFUSED` | 502 | gateway non-200; `status` echoed | retry banner |
| `PII_BLOCKED` | 422 | PII screen refused the run | in-chat notice |
| `IMPORT_INVALID` | 400 | | toast |

`RUN_ERROR.code` uses the same set plus `MAX_TURNS` (16 tool turns without an
answer) and `RUN_LOG_FULL` (8 MiB cap).

---

## 6. Data model

### 6.1 Thread

Stored as the sync enclave's `chat` scope plaintext. **The stored shape stays
`RemoteChatPlaintextSchema` v2** so iOS keeps reading and writing the same rows
during transition (§12.2), with three changes: `pendingRecoveries` is never
written by the harness (and carried through untouched while iOS still writes it,
§12.2), `activeRun` is added, and `messages[].attachments[]` always carry
`{id, encryptionKey}` references (never inline base64 -- bytes live behind
`/v1/attachments/get`).

What the client receives from `threads/get`:

```json
{
  "id": "…", "title": "…", "titleState": "generated",
  "createdAt": "…", "updatedAt": "…", "revision": "17",
  "projectId": null, "presetId": null, "model": "kimi-k3",
  "webSearchEnabled": true, "pinned": false,
  "activeRun": null,
  "messages": [{
    "id": "msg_…", "role": "user", "createdAt": "…",
    "content": "…", "quote": "…" | null,
    "attachments": [{ "id": "att_…", "kind": "image", "fileName": "…",
                      "mimeType": "…", "size": 1, "thumbnail": "…" }]
  }, {
    "id": "msg_…", "role": "assistant", "createdAt": "…",
    "model": "kimi-k3", "modelDisplayName": "Kimi K3",
    "timeline": [
      { "type": "thinking",  "id": "…", "content": "…", "duration": 4.2 },
      { "type": "tool_call", "id": "…", "toolCallId": "call_…", "name": "web_search",
        "arguments": "{…}", "result": {…}, "progress": ["…"],
        "resolvedAt": null, "resolution": null },
      { "type": "content",   "id": "…", "content": "…" }
    ],
    "isError": false, "isInterrupted": false,
    "usage": { "promptTokens": 1, "completionTokens": 1 }
  }]
}
```

`timeline` is the single source of truth for an assistant message, as
`message-assembler.ts` already says it is; the flat `content`, `thoughts`,
`webSearch`, `urlFetches`, `annotations`, `codeExecCalls`, `toolCalls`,
`searchReasoning`, `webSearchBeforeThinking`, `documentContent`,
`multimodalText`, `documents`, `imageData` fields on `Message` are gone. Web
search and code execution are `tool_call` blocks whose `name` says which. The
harness upgrades legacy stored messages to this shape on read
(`ensure-timeline.ts` and `attachment-helpers.ts` legacy paths move to Go and
run once per row).

### 6.2 Profile

Stored as the sync enclave's `profile` row. Fields are the union of
`ProfileData` and the localStorage-only settings:

```
themeMode, chatFont, language, nickname, profession, traits[], additionalContext,
isUsingPersonalization, isUsingCustomPrompt, customSystemPrompt,
customPromptPresets[{id, name, description, systemPrompt, createdAt, updatedAt}],
favoritePromptPresetIds[], selectedModel, autoIntelligence, reasoningEffort,
thinkingEnabled, webSearchEnabled, codeExecutionEnabled, sandboxEnabled,
piiCheckEnabled, genUIEnabled, pixelateSidebarChatTitlesEnabled,
browserTabChatTitleEnabled, enterToNewlineEnabled,
hasSeenOnboarding, hasSeenWebSearchIntro, dismissed: {passkeyRecovery,
passkeyFirstTime, manualRecovery, backupWarning, passkeySetupWarning}
```

`pinnedChatIds` leaves the profile for `thread.pinned`. `isDarkMode` is derived
from `themeMode` on the client. `webSearchAvailable` was a cached feature flag and
is `session.features.webSearch` now. A `selectedModel` of `auto-smart` or
`auto-fast`, written by clients before the slider, reads as `auto`.

### 6.3 Project, ProjectDocument

Unchanged from `types/project.ts`; stored as today in the `project` and
`project_document` scopes. `syncVersion` is not exposed to the client.

### 6.4 Run log

Unchanged from `recovery.go` except the key (§1.3) and the id (the run id). The
thread row's `activeRun` is written when a run starts and cleared when its
final commit lands, so a device can always find the log for a thread.

---

## 7. Harness behaviour

What the harness does with what it's given. Each subsection names the client
code it replaces.

### 7.1 Prompt assembly (`context.go`) -- replaces `chat-query-builder.ts`, `use-custom-system-prompt.ts`, `use-project-system-prompt.ts`, `genui/system-prompt.ts`, `time-reminder.ts`, `prompt-escaping.ts`

Order, per turn:

1. **Base prompt.** Precedence, as `useCustomSystemPrompt` has it: thread
   `presetId` → its `systemPrompt` (built-in from `catalog.go`, custom from the
   profile) ; else `profile.isUsingCustomPrompt` → `profile.customSystemPrompt`
   ; else the controlplane's `/api/config/system-prompt`. Rules from the same
   response are appended unless a custom prompt is empty (today's edge case).
2. **Placeholders.** `{MODEL_NAME}`, `{LANGUAGE}` (profile, default English),
   `{TIMEZONE}` (sent by the client in `options.timezone`; the one thing about
   the user the harness cannot know), `{USER_PREFERENCES}` → the
   `<user_preferences>` XML from nickname/profession/traits/additionalContext,
   empty unless `isUsingPersonalization`. User-controlled text inside
   `<user_preferences>` and `<project_context>` has `&`, `<` and `>` escaped, as
   `escapePromptContent` does, so a closing tag in a nickname or a document
   cannot end the block it sits in.
3. **Project context.** If `projectId`: `<project_context>` with name,
   description, instructions, every document's content, as
   `buildProjectContext` renders it.
4. **Tool family prompts.** One `system` message per dialled family, from
   `tools.go`, as `toolset.system()` does now.
5. **Widget hint.** Header from controlplane `genUI.header`, one line per widget
   in the intersection of (controlplane `enabledWidgets`, request `widgets[]`,
   catalog). Empty intersection means GenUI is off for this run.
6. **History** after budgeting (§7.2), each message rendered per §7.3.
7. **Time reminder** as the last user message, minute granularity, never
   stored, so the cached prefix is stable across turns.

Personalization, custom prompt text and project documents are read from the
profile/project rows the harness already has; the client sends none of them.
`systemPromptOverride` (a `handleQuery` parameter) has no wire equivalent; its
only callers were internal.

### 7.2 Budgeting -- replaces `token-estimation.ts`, `reasoning-history.ts`, `CONTEXT_WINDOW_USAGE_RATIO`, `estimateTokenCount` in three places

- Window per model from the gateway `/catalog` (`context`), or the smallest
  across auto candidates.
- Budget = 80% of window minus the rendered system prompt, tool definitions and
  family prompts, using a real tokenizer (tiktoken-compatible BPE is enough for
  budgeting across these models; the chars/4 estimate goes).
- Walk history newest-first, include whole messages until the budget is met;
  attachments count at their rendered size; images at a fixed 1500 tokens as
  `agui.go` does.
- Reasoning history policy per model (`none` | `tool-call-only` | `all`, the
  strongest across auto candidates) decides whether a prior assistant message's
  thinking block is rendered as `reasoning_content`.
- A single user message that alone exceeds every candidate's budget →
  `CONTEXT_TOO_LARGE` before any upstream call.

### 7.3 Model quirks -- replaces `buildModelBodyParams`, `substituteEffort`, `RESERVED_MODEL_BODY_PARAMS`, `AUTO_MODEL_OPTIONS_FIELD`

- The system prompt is one leading `system` message for every model, as the
  webapp has sent it since #566 dropped the per-model system-role quirks.
- `reasoningConfig.params["/v1/chat/completions"].enable|disable` merged into
  the body with `$EFFORT` substituted through `effortMap`; `requestParams`
  merged minus the reserved keys. For `auto` none of this is sent: the router
  picks the model and applies its own params (§7.4).
- User attachments: documents inline as text with the `Document title:` framing
  for text-only, as `[Attached file]` + per-page text/image parts for vision
  models with paged docs; images as `image_url` data URLs for vision models,
  as a generated description otherwise (generated here, cached on the
  attachment).
- `quote` framed as `In reply to:\n> …`.
- Prior widget calls get a synthetic `tool` result `"executed"` so the history is
  well-formed, as today.

The catalog data driving this comes from controlplane `/api/config/models`,
which the harness fetches at boot and refreshes; the client never sees these
fields.

### 7.4 Model selection -- replaces `resolveModelSelection`, `getAutoModel`, `getDefaultModelId`, `AUTO_INTELLIGENCE_LEVELS`, `use-model-management.ts`, `use-auto-intelligence.ts`

`options.model` → thread `model` → `profile.selectedModel` → `auto` (whenever
the catalog has a chat model). `auto-smart` and `auto-fast`, saved by clients
before the slider, read as `auto`.

`auto` is the router's sentinel. The harness sends `model: "auto"` and
`auto_model_options: {intelligence: <0–100>}`, mapping the level id from
`options.autoIntelligence` → thread → `profile.autoIntelligence` → `high`
through the scale in `catalog.go` (instant 0, low 20, medium 40, high 60, extra
80, max 100 today). The router picks the concrete model and effort from the
per-model `chatConfig.intelligence` scores and applies that model's reasoning
params itself; the harness sends no candidates and no per-model params, which is
what `applyAutoRouting` in `inference-client.ts` does today. Budgeting for
`auto` uses the smallest `contextWindowTokens` across chat models and the
strongest `reasoningHistoryPolicy` among them, as `getAutoModel` and
`getReasoningHistoryPolicy` do.

What stays the harness's own is the attestation-side check. A named model must
be in the catalog pinned at boot (`MODEL_UNAVAILABLE` otherwise), and for `auto`
the replica the gateway's 421 names is attested against the pinned repos before
a byte is sent, as `main.go` does today. A model the router may pick that the
harness has not pinned is a deployment error, surfaced at boot by diffing
`/api/config/models` against the pinned set, not a runtime fallback.

### 7.5 Tools and widgets -- extends `tools.go`, `agent.go`; replaces `genui/registry.ts` schema half, `zod-to-json-schema`, `ROUTER_AUTO_CONTINUE_FLAG`

Built-in families are unchanged. Widgets move from "the caller sends full JSON
schemas" to "the caller sends names": `catalog.go` holds each widget's
`{name, description, schema, promptHint, surface}`, ported from the
`defineGenUIWidget` calls in `genui/widgets/*.tsx`. The advertised order is
fixed (families first, then widgets in catalog order) so the prompt cache holds.

`TOOL_CALL_RESULT.content` per built-in, so the client renders from data rather
than parsing:

| tool | content |
|---|---|
| `web_search` | `{query, results: [{url, title, snippet}]}` |
| `web_fetch` | `{url, title, text}` (clipped) |
| `bash`, `view`, `str_replace`, `create`, `insert` | `{output}` |
| `present` | `{filename, language, content}` |
| `render_link_preview` | `{url, title, description, siteName, image}` -- the harness fetches these from the metadata enclave at call time, which is what `metadata-client.ts` did on the client |
| any widget | `"[rendered for the user]"` |

Citations: sources of a `web_search` result are the citations. There is no
separate `annotations` channel.

### 7.6 Derived work (`derived.go`) -- replaces `title.ts`, `summary-client.ts`, `getTitleContent`, `TITLE_GENERATION_WORD_THRESHOLD`, `fact-extractor.ts`, `use-memory.ts`

After the first user message of a thread commits, in parallel with the run:
first 100 words of the message (or of its attachments' text if the message is
empty, as `getTitleContent` falls back) → summarizer enclave `title_summary` →
`title`, `titleState: generated`, `STATE_DELTA /title`. Fails silently to
"New Chat".

Memory: after a run settles in a project with memory enabled, the fact
extractor runs against messages since `lastProcessedTimestamp` with the
controlplane's `/api/config/memory-prompt` and `FACT_OPERATIONS_SCHEMA` as a
structured completion, applies the operations, writes `project.memory`. Off by
default, matching today.

Both are billed under the run's usage context (`usagereporting.Context`,
`Depth: 1`) as tool turns are today, so metering is unchanged.

### 7.7 Queue, cancel, ephemeral (`thread.go`)

- One run per thread at a time. A turn arriving during a run is appended to the
  thread's in-memory queue (`{queueId, kind, content, attachments, quote,
  createdAt}`), announced by `STATE_DELTA`, and started when the run finishes.
  The queue lives with the run in RAM and is spilled with the log; it does not
  survive the harness losing the run.
- Cancel stops the run context; partial assistant output is committed with
  `isInterrupted`; the queue is *not* drained (the client shows it, the user
  sends the next or removes it) -- today's `cancelGenerationAndResumeQueue`
  semantics.
- `ephemeral: true`: the harness never calls the sync enclave. The thread exists
  only as a run log for `runTimeout` (30 min) after the last run, so a reload
  within that window can `follow` it. Used for "temporary chat" and for all
  anonymous traffic.

### 7.8 Consistency (`thread.go`, `sync.go`)

Every commit is a `push` with `ifMatch: <etag>`. On `412 STALE_BLOB` the harness
re-pulls, re-applies its append (append-only, so it always applies), and pushes
again, up to three times, then `REVISION_CONFLICT`. Deletions use the enclave's
tombstone. There are no client-side tombstones (`deleted-chats-tracker.ts`),
no outbox (`SYNC_OUTBOX_STORE`), no coalescer (`upload-coalescer.ts`), no
revision ledger replay (`chat-revision-sync.ts`) -- the client never has a copy
that could be stale.

---

## 8. The client after

### 8.1 Responsibilities

```
┌──────────────────────────────────────────────────────────────┐
│ key custody      WebAuthn PRF, wrap/unwrap CEK, bundles,      │  real logic; stays
│                  key-setup / recovery UI, hold CEK in memory  │
├──────────────────────────────────────────────────────────────┤
│ transport        SecureClient(harness) + fetch + SSE reader   │  SDK
│ reducer          events → {threads[], thread, queue,          │  ~200 lines, spec'd in §5.4
│                  rateLimit, streaming flags}                   │
├──────────────────────────────────────────────────────────────┤
│ render           messages, timeline blocks, widgets, markdown,│  platform-native
│                  katex, mermaid, code blocks, lightbox, print  │
│ input            textarea, file picker, drag/drop, recorder,  │
│                  quote selection, keyboard                    │
│ shell            sidebar, modals, settings forms, routing,    │
│                  theme, toasts                                │
└──────────────────────────────────────────────────────────────┘
```

Things the client no longer knows: any model id's capabilities, any prompt
text, any token count, any storage schema, any sync state, any recovery state,
whether it is "signed in with cloud sync" versus "local only" (there is a key or
there isn't), what a `<tinfoil-event>` is, what `reasoning_content` is.

### 8.2 The reducer, as a contract for three platforms

State:

```
session:   Session (from /v1/session)
threads:   Map<id, ThreadSummary>        + cursor
thread:    Thread | null                 (the one on screen)
queue:     QueueItem[]
run:       {runId, status: idle|queued|streaming|error, error?} per threadId
draft:     string, attachments: UploadResult[]   (input area)
key:       CEK | null, keyState
```

Transitions are exactly the table in §5.4 plus: `threads/list` replaces the
map page; `threads/update` replaces one summary; `threads/delete` removes it;
`attachments/upload` appends to `draft.attachments`. Nothing else mutates
state. A platform implementation is a `switch` on `event.type` and a JSON-patch
library.

### 8.3 What still hits the controlplane directly

Account management only: subscription status (via Clerk metadata), upgrade
checkout, account deletion, MapKit JS token. None of it is chat.

---

## 9. Deletion table for `tinfoil-webapp/src`

Legend: **D** deleted · **S(n)** shrinks to roughly *n* lines · **K** kept as is
· **K*** kept, data source changes. Line counts are today's.

### 9.1 `services/`

| File | Lines | Fate | Where it went |
|---|---|---|---|
| `services/auth/auth-token-manager.ts` | 173 | S(40) | only supplies the Clerk JWT to the harness client |
| `services/auth/index.ts` | 5 | K | |
| `services/chat-export/export-archive.ts` | 271 | D | `export.go` |
| `services/chat-import/constants.ts` | 1 | D | |
| `services/chat-import/import-failure-copy.ts` | 49 | K* | copy per `failureReason` from `import/status` |
| `services/chat-import/local-tinfoil-import-parser.ts` | 70 | D | harness `import` |
| `services/chat-import/local-tinfoil-import.ts` | 356 | D | |
| `services/chat-import/local-tinfoil-import.worker.ts` | 40 | D | |
| `services/chat-import/off-device-import.ts` | 112 | D | `POST /v1/import` |
| `services/cloud/account-operation.ts` | 5 | D | |
| `services/cloud/backup-read-error.ts` | 132 | D | harness classifies unreadable rows |
| `services/cloud/cek-encoding.ts` | 166 | S(20) | base64 of the CEK for the `key` field |
| `services/cloud/chat-codec.ts` | 249 | D | harness reads rows |
| `services/cloud/chat-health.ts` | 53 | D | |
| `services/cloud/chat-ingestion.ts` | 149 | D | |
| `services/cloud/chat-revision-sync.ts` | 439 | D | no client replica to sync |
| `services/cloud/chat-search.ts` | 282 | D | `POST /v1/threads/search` |
| `services/cloud/cloud-key-authorization.ts` | 270 | S(40) | `KEY_MISMATCH` handling in the key UI |
| `services/cloud/cloud-key-preflight.ts` | 167 | D | `/v1/session.key` |
| `services/cloud/cloud-storage.ts` | 959 | D | `sync.go` |
| `services/cloud/cloud-sync.ts` | 1260 | D | nothing to sync |
| `services/cloud/edit-clock.ts` | 170 | D | single writer |
| `services/cloud/ensure-current-key.ts` | 386 | D | |
| `services/cloud/legacy-blob-migration.ts` | 288 | D | harness runs migration on first login if needed |
| `services/cloud/legacy-chat-eviction.ts` | 90 | D | no local chats to evict |
| `services/cloud/manual-cloud-sync.ts` | 28 | D | |
| `services/cloud/profile-merge.ts` | 387 | D | §5.8 |
| `services/cloud/profile-settings-serializer.ts` | 650 | D | profile is fetched, not assembled from localStorage |
| `services/cloud/profile-sync-coordinator.ts` | 58 | D | |
| `services/cloud/profile-sync-state.ts` | 58 | D | |
| `services/cloud/profile-sync.ts` | 491 | D | |
| `services/cloud/project-storage.ts` | 1077 | D | §5.7 |
| `services/cloud/schemas.ts` | 193 | D | harness validates rows |
| `services/cloud/streaming-tracker.ts` | 141 | D | `run` status in reducer |
| `services/cloud/sync-health.ts` | 212 | D | |
| `services/cloud/sync-predicates.ts` | 223 | D | |
| `services/cloud/upload-coalescer.ts` | 488 | D | |
| `services/encryption/encryption-service.ts` | 625 | S(150) | hold CEK + fallback keys in memory/localStorage; drop everything that encrypts (the client encrypts nothing) |
| `services/exec-snapshot/access-token.ts` | 9 | D | `derive.go` |
| `services/exec-snapshot/index.ts` | 6 | D | |
| `services/exec-snapshot/key-derivation.ts` | 61 | D | |
| `services/exec-snapshot/use-exec-snapshot.ts` | 114 | D | |
| `services/inference/chat-query-builder.ts` | 291 | D | `context.go` |
| `services/inference/chat-recovery-client.ts` | 156 | D | `follow` |
| `services/inference/chat-recovery-crypto.ts` | 342 | D | |
| `services/inference/chat-recovery-drafts.ts` | 130 | D | |
| `services/inference/chat-recovery-sync.ts` | 506 | D | |
| `services/inference/chat-recovery.ts` | 1018 | D | |
| `services/inference/chat-stream.ts` | 96 | D | SSE reader is ~30 lines in the harness client |
| `services/inference/constants.ts` | 2 | D | |
| `services/inference/inference-client.ts` | 849 | D | `context.go`, `auth.go`, retry in harness |
| `services/inference/metadata-client.ts` | 265 | D | §7.5 `render_link_preview` |
| `services/inference/summary-client.ts` | 57 | D | `derived.go` |
| `services/inference/tinfoil-client.ts` | 816 | D | `auth.go`; one `SecureClient` for the harness in a new `services/harness/client.ts` (~150) |
| `services/inference/title.ts` | 67 | D | `derived.go` |
| `services/mapkit-token.ts` | 61 | K | account/controlplane, not chat |
| `services/memory/fact-extractor.ts` | 251 | D | `derived.go` |
| `services/native-backup/*` (11 files) | 3801 | D | `export.go` writes the archive; restore is `POST /v1/import` (§5.9) |
| `services/passkey/*` (6 files) | 1448 | S(1250) | key custody; `passkey-key-storage.ts` (1084) calls `keys/*` on the harness instead of the sync enclave |
| `services/project-export/claude-project-export.ts` | 304 | D | `POST /v1/export {format: "claude-projects"}` |
| `services/project/project-deletion.ts` | 17 | D | |
| `services/project/project-events.ts` | 68 | D | |
| `services/share-api.ts` | 78 | D | `threads/share`, `shares/open` |
| `services/storage/chat-events.ts` | 59 | D | |
| `services/storage/chat-storage.ts` | 541 | D | |
| `services/storage/deleted-chats-tracker.ts` | 150 | D | |
| `services/storage/indexed-db.ts` | 3646 | D | no client store |
| `services/storage/pinned-chat-hydration.ts` | 49 | D | `thread.pinned` |
| `services/storage/pinned-chats.ts` | 92 | D | |
| `services/storage/project-cache.ts` | 81 | D | |
| `services/storage/session-storage.ts` | 214 | D | ephemeral threads live in the harness; see §12.3 |
| `services/sync-enclave/enclave-error-classification.ts` | 215 | D | harness classifies |
| `services/sync-enclave/enclave-error-recovery.ts` | 172 | D | |
| `services/sync-enclave/index.ts` | 59 | D | |
| `services/sync-enclave/passkey-events.ts` | 48 | K | |
| `services/sync-enclave/retry-policy.ts` | 132 | D | |
| `services/sync-enclave/sync-api.ts` | 1329 | S(80) | only the `keys/*` request types |
| `services/sync-enclave/sync-enclave-client.ts` | 459 | D | replaced by the harness client |
| `services/sync-enclave/tinfoil-key-id.ts` | 33 | K | `HKDF(CEK, "tinfoil-key-id-v1")` for the key UI |
| `services/sync-enclave/wire-contract.ts` | 96 | D | |

New: `services/harness/client.ts` (~150: `SecureClient`, JSON POST, SSE
iterator, error envelope → typed error), `services/harness/reducer.ts` (~200,
§8.2), `services/harness/types.ts` (wire types).

### 9.2 `hooks/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `hooks/use-chat-print.ts` | 227 | K | rendering |
| `hooks/use-chat-recovery-drafts.ts` | 40 | D | |
| `hooks/use-chat-router.ts` | 104 | S(50) | navigate to ids the harness names; no blank-chat URL swap |
| `hooks/use-chat-search.ts` | 122 | S(40) | debounce + one call |
| `hooks/use-cloud-pagination.ts` | 214 | S(40) | cursor over `threads/list` |
| `hooks/use-cloud-sync.ts` | 564 | D | |
| `hooks/use-lossless-profile-sync.ts` | 473 | D | |
| `hooks/use-memory.ts` | 149 | D | |
| `hooks/use-passkey-backup.ts` | 1894 | S(900) | same state machine minus every branch that reasons about sync state, migration sweeps, and local-vs-remote data (`inspectRemoteEncryptedState`, `validateCurrentPrimaryKey`, exhausted-keyset fingerprints) |
| `hooks/use-pinned-chats.ts` | 95 | D | |
| `hooks/use-profile-sync.ts` | 1 | D | |
| `hooks/use-projects.ts` | 369 | S(60) | fetch + context |
| `hooks/use-streaming-chats.ts` | 17 | D | |
| `hooks/use-subscription-status.ts` | 205 | K | Clerk |
| `hooks/use-sync-enclave-session.ts` | 188 | D | |
| `hooks/use-sync-health.ts` | 56 | D | |
| `hooks/use-toast.ts` | 216 | K | |
| `hooks/use-upgrade-to-pro.ts` | 43 | K | |

### 9.3 `components/chat/hooks/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `chat-operations.ts` | 244 | D | ids, blank chats, `loadChats`, `sortChats` (server sorts) |
| `chat-persistence-manager.ts` | 95 | D | |
| `chat-persistence.ts` | 200 | D | |
| `streaming/animation-frame-publisher.ts` | 120 | K | rAF batching of reducer output is rendering |
| `streaming/content-preprocessor.ts` | 44 | D | |
| `streaming/event-normalizer.ts` | 390 | D | |
| `streaming/index.ts` | 7 | D | |
| `streaming/interrupted-message.ts` | 102 | D | |
| `streaming/message-assembler.ts` | 131 | D | |
| `streaming/process-stream.ts` | 82 | D | |
| `streaming/rich-response-parser.ts` | 30 | D | |
| `streaming/rich-stream-session.ts` | 268 | D | |
| `streaming/timeline-builder.ts` | 312 | D | |
| `streaming/types.ts` | 111 | D | |
| `use-auto-intelligence.ts` | 65 | S(20) | level → `profile/update`; no localStorage, no cross-tab event |
| `use-browser-tab-chat-title.ts` | 47 | S(15) | reads `profile.browserTabChatTitleEnabled` |
| `use-chat-collection.ts` | 213 | D | |
| `use-chat-font.ts` | 66 | S(30) | reads `profile.chatFont` |
| `use-chat-messaging.ts` | 2074 | D | `thread.go` + reducer |
| `use-chat-state.ts` | 400 | S(120) | composes reducer + UI state |
| `use-chat-storage.ts` | 1109 | D | |
| `use-chat-streams.ts` | 168 | D | `run` in reducer |
| `use-custom-system-prompt.ts` | 269 | D | `context.go` |
| `use-enter-to-newline.ts` | 38 | S(15) | reads `profile.enterToNewlineEnabled` |
| `use-message-queue.ts` | 588 | D | §7.7 |
| `use-message-renderer.ts` | 32 | K | |
| `use-model-management.ts` | 198 | S(30) | selected id → `profile/update` |
| `use-prompt-library.ts` | 303 | S(80) | presets from session + profile |
| `use-reasoning-effort.ts` | 131 | S(30) | → `profile/update` |
| `use-sidebar-chat.ts` | 310 | S(60) | `kind: "ask"` + reducer instance |
| `use-ui-state.ts` | 223 | K | |

### 9.4 `components/chat/` (non-hook)

| File | Lines | Fate | Notes |
|---|---|---|---|
| `artifact-sidebar.tsx` | 207 | K | |
| `ask-sidebar.tsx` | 154 | K | |
| `attachment-helpers.ts` | 103 | D | no legacy shapes reach the client |
| `chat-announcer.tsx` | 62 | K | |
| `chat-input.tsx` | 1470 | S(1000) | drop transcription client, token-budget checks on paste, doc-uploader wiring; keep recorder, textarea, drag/drop, widget input surface |
| `chat-interface.tsx` | 4630 | S(1500) | drop every sync/recovery/storage/key-authorization effect, model resolution, prompt composition, rate-limit math, favorite hydration; keep layout, modals, routing glue |
| `chat-list-item.tsx` | 965 | K | |
| `chat-list.tsx` | 287 | K | |
| `chat-list-utils.ts` | 41 | K | |
| `chat-messages.tsx` | 784 | K* | reads `timeline` only |
| `chat-scroll.ts` | 33 | K | |
| `chat-sidebar.tsx` | 2636 | S(1800) | drop sync status, health, manual sync, cloud pagination internals |
| `chat-utils.ts` | 63 | S(25) | error type only |
| `cloud-sync-health-card.tsx` | 191 | D | |
| `components/*` (4) | 366 | K | `context-usage-indicator` reads `contextUsage` from the harness |
| `constants.ts` | 84 | S(60) | drop `MAX_MESSAGE_LENGTH`, `MAX_DOCUMENT_SIZE_*`, `MAX_IMAGE_DIMENSION_PX`, `TITLE_GENERATION_WORD_THRESHOLD`, `DOCUMENT_PROCESSING_TIMEOUT_MS`, `MESSAGE_SEND_MAX_RETRIES`, `DEFAULT_AUDIO_MODEL` |
| `DataFlowDiagram.tsx` | 213 | K* | draws the new topology |
| `delete-confirmation.tsx` | 83 | K | |
| `document-content.ts` | 15 | D | |
| `document-uploader.tsx` | 395 | S(80) | pick file → `attachments/upload` → show result |
| `drag-context.tsx` | 102 | K | |
| `ensure-timeline.ts` | 63 | D | harness normalises |
| `favorite-drag.ts`, `favorite-navigation.ts`, `use-favorite-drop-target.ts` | 180 | K | drop → `threads/update {pinned}` |
| `file-upload-routing.ts` | 42 | K | project-vs-chat destination is a UI question |
| `genui/config.ts`, `enabled-widgets.ts` | 67 | D | `/v1/session.widgets` |
| `genui/GenUIInputAreaRenderer.tsx` | 46 | K | |
| `genui/GenUIToolCallRenderer.tsx` | 427 | K | |
| `genui/input-coercion.ts` | 58 | K | |
| `genui/partial-json.ts` | 116 | K | progressive rendering of streamed args |
| `genui/pending-input-tool-call.ts` | 97 | S(50) | `surface` comes from the widget module, schema validation goes |
| `genui/registry.ts` | 96 | S(30) | name → renderer only |
| `genui/render.tsx` | 61 | K | |
| `genui/retry.ts` | 288 | D | `kind: "retryToolCall"` |
| `genui/system-prompt.ts` | 34 | D | `context.go` |
| `genui/types.ts` | 134 | S(60) | drop `schema`, `description`, `promptHint`; keep `render*`, `surface` |
| `genui/widgets/*.tsx` (11 widgets + polyfills + 2 views) | 4022 | K* | each loses its `schema` and `promptHint` (moved to `catalog.go`); `LinkPreview` renders from `TOOL_CALL_RESULT` instead of fetching |
| `image-gallery-context.tsx` | 124 | K | |
| `index.ts`, `lazy-chat-features.tsx` | 30 | K | |
| `keyboard-utils.ts` | 15 | K | |
| `message-queue-identity.ts` | 10 | D | |
| `message-queue.tsx` | 103 | K* | reads `queue` from reducer |
| `mfa-settings-card.tsx` | 474 | K | Clerk |
| `model-lifecycle-badges.tsx`, `model-selector-trigger-label.tsx` | 136 | K* | read `experimental`/`deprecated`/`deprecationDate` and `session.auto` |
| `model-selector.tsx` | 635 | K* | reads `session.models` and `session.auto` |
| `native-backup-export.tsx` | 133 | S(60) | one call, progress, download |
| `native-backup-restore.tsx` | 157 | S(80) | file picker → `import` → `import/status` |
| `PrintableChat.tsx` | 277 | K | |
| `project-navigation.ts` | 19 | K | |
| `prompt-library-modal.tsx` | 641 | K* | |
| `prompts/built-in-presets.ts` | 275 | S(40) | icons only; prompt bodies live in `catalog.go` |
| `prompts/preset-editor.tsx`, `prompts/types.ts` | 139 | K | |
| `quote-selection-popover.tsx` | 179 | K | |
| `rate-limit-banner.tsx` | 71 | K* | reads `rateLimit` |
| `renderers/**` (22 files) | 4424 | K* | `WebSearchProcess`, `URLFetchProcess`, `CodeExecProcess`, `SourcesButton`, `ThoughtProcess` read `tool_call`/`thinking` blocks; no `webSearch`/`urlFetches`/`annotations` props |
| `settings-modal.tsx` | 4803 | S(3000) | drop export/import implementations, cloud-sync sections, key preflight, local-only mode; keep forms bound to `profile/update`, key UI |
| `shared-chat-view.tsx` | 160 | K | |
| `settings-project-policy.ts` | 32 | S(10) | export filtering is the harness's; the two premium gates stay |
| `share-modal.tsx` | 504 | S(250) | one call, show the link |
| `sidebar-pagination.ts` | 14 | D | no local-vs-remote page |
| `sidebar-sync-button.tsx` | 165 | D | |
| `sidebar-upsell-state.ts` | 18 | K | |
| `stream-error-banner.tsx` | 277 | K* | branches on `RUN_ERROR.code` |
| `types.ts` | 262 | S(100) | the §6.1 shapes; drop legacy fields, `PendingRecoveryEnvelope`, `LoadingState` internals |
| `typing-animation.tsx` | 89 | K | |
| `verification-status-display.tsx` | 338 | K* | §5.11 |
| `WelcomeScreen.tsx` | 530 | K | |

### 9.5 `components/` (other)

| File | Lines | Fate | Notes |
|---|---|---|---|
| `auth-cleanup-handler.tsx` | 335 | S(60) | on user change: clear CEK, reset reducer |
| `code-block.tsx`, `copy-button.tsx`, `icons/*`, `link.tsx`, `loading-dots.tsx`, `logo.tsx`, `preview/*`, `texture-grid.tsx`, `ui/*`, `user-avatar.tsx`, `signout-progress-overlay.tsx` | | K | `signout-progress-overlay` becomes trivial |
| `modals/add-to-project-context-modal.tsx` | 104 | K | |
| `modals/cloud-sync-setup-modal.tsx` | 1164 | S(500) | becomes the key-setup modal; no sync modes |
| `modals/cloud-sync-setup-mode.ts` | 75 | D | |
| `modals/first-login-key-modal.tsx` | 313 | K | |
| `modals/signout-confirmation-modal.tsx`, `subscribe-prompt-modal.tsx` | 190 | K | |
| `onboarding/onboarding-view.tsx` | 332 | K* | `hasSeenOnboarding` in profile |
| `project/hooks/use-project-system-prompt.ts` | 41 | D | |
| `project/project-context.tsx` | 124 | S(50) | drop `buildProjectContext`, `estimateTokenCount` |
| `project/project-document-hydration.ts`, `project-load-validity.ts` | 57 | D | |
| `project/project-provider.tsx` | 754 | S(120) | |
| `project/*` (rest: header, indicator, selector, settings, sidebar, upload, wrapper, index) | 3710 | K* | |
| `url-hash-message-handler.tsx`, `url-hash-settings-handler.tsx` | 248 | K | |
| `verification-sidebar.tsx` | 251 | K* | |

### 9.6 `utils/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `auth-signout-intent.ts` | 49 | K | |
| `binary-codec.ts` | 150 | S(20) | base64 only |
| `chat-import-parsers.ts` | 293 | D | |
| `chat-recovery-envelope.ts` | 111 | D | |
| `chat-timestamps.ts` | 41 | D | |
| `clerk-errors.ts` | 37 | K | |
| `cloud-sync-settings.ts` | 81 | D | |
| `dev-simulator.ts` | 569 | D | point the client at a local harness |
| `dev-stream-logger.ts` | 54 | D | |
| `error-handling.ts` | 114 | K | |
| `file-types.ts` | 273 | S(20) | icon-by-extension only; accept list from session |
| `interaction-lock.ts` | 69 | K | |
| `latex-processing.ts`, `markdown-preprocessing.ts`, `markdown-pdf-export.ts` | 774 | K | rendering |
| `navigation.ts`, `redirect-url.ts` | 63 | K | |
| `performance-metrics.ts` | 26 | S(15) | drop the inference timers |
| `personalization-settings.ts` | 24 | K | form helper |
| `preprocessing/index.ts`, `preprocessing/media.ts` | 212 | D | `attach.go` |
| `prompt-escaping.ts` | 15 | D | `context.go` |
| `response-language.ts` | 111 | K | the picker's list; `{LANGUAGE}` is filled in by the harness |
| `reasoning-history.ts` | 42 | D | |
| `reverse-id.ts` | 15 | D | |
| `share-payload.ts` | 93 | D | `export.go` |
| `signout-cleanup.ts` | 261 | S(40) | |
| `signout-progress.ts` | 105 | D | |
| `storage-migration.ts` | 131 | D | |
| `time-reminder.ts` | 35 | D | |
| `tinfoil-events.ts` | 278 | D | |
| `token-estimation.ts` | 134 | D | |
| `token-validation.ts` | 43 | D | |

### 9.7 `config/`, `constants/`, `types/`, `pages/`, `dev/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `config/models.ts` | 615 | D | `session.models` |
| `config.ts` | 61 | S(25) | `HARNESS_ENCLAVE_URL`, `HARNESS_REPO`, `API_BASE_URL`, `IS_DEV`; drop `SYNC_ENCLAVE_*`, `CLOUD_SYNC`, `PAGINATION` |
| `constants/auth-events.ts`, `chat-events.ts`, `chat.ts`, `settings-events.ts`, `project-colors.ts` | 90 | K | |
| `constants/storage-keys.ts` | 201 | S(25) | key material, active user id, theme/font mirror, sidebar UI state, anonymous `{threadId, runId}`; every `SYNC_*`, `MIGRATION_*`, `SETTINGS_CLOUD_SYNC_*`, `SETTINGS_LOCAL_ONLY_*`, `USER_PREFS_*`, `MESSAGE_QUEUE_PREFIX` goes, and every other `SETTINGS_*` is a `Profile` field |
| `constants/rate-limits.ts` | 1 | K | |
| `types/chat-recovery.ts` | 58 | D | |
| `types/memory.ts` | 93 | S(15) | `Fact` only |
| `types/project.ts` | 123 | S(60) | drop list/sync response types |
| `pages/*` | | K* | `share/[[...slug]].tsx` 303 → ~120 (one call); `chat/local/[chatId].tsx` D (§12.3); `dev/cloud-sync-flows.tsx` 506 D; `dev/index.tsx`, `dev/onboarding-flow.tsx` K |
| `theme/`, `styles/`, `fonts/`, `lib/` | | K | |

### 9.8 `tests/`

Of 205 test files, the directories `tests/services/cloud` (27),
`tests/services/inference` (17), `tests/services/storage` (12),
`tests/services/native-backup` (10), `tests/components/chat/streaming` (9),
`tests/services/sync-enclave` (8), `tests/services/chat-import` (5),
`tests/config` (3), `tests/services/chat-export`, `exec-snapshot`, `encryption`,
`project-export` (1 each) go with their subjects: 95 files. `tests/services/passkey`
(9) stays. Their coverage moves to Go
tests against `context.go`, `thread.go`, `attach.go`, `auth.go`, and to one
client test suite for the reducer.

---

## 10. Deletion table for `tinfoil-ios`

Same legend as §9. Line counts are today's, over the app target
(`TinfoilChat/`, `TinfoilShared/`, `TinfoilShareExtension/`: 67,415 lines in
169 files; `TinfoilChatTests/` is another 13,380 in 61 files, §10.9).

### 10.1 What is different about iOS

The responsibilities are the webapp's, written a second time in Swift, so most
rows below have a twin in §9. Where iOS differs:

- **Five attestation handshakes become one.** The app holds a `SecureClient` for
  the inference router (`TinfoilAI.create`), the sync enclave, the summarizer,
  the metadata fetcher and the docling enclave, and a bare one to fetch the
  inference enclave's HPKE key for recovery. After, it attests the harness.
- **Recovery is the biggest single deletion, and the reason is the platform.**
  iOS grants about 30 seconds of background execution
  (`beginBackgroundTask` around the stream, `ChatViewModel.swift:3874`), so a
  long reasoning turn dies with the app. The client compensates by running the
  completion over EHBP with an `X-Session-Id`, sealing the EHBP session token
  into a `pendingRecoveries` envelope stored *inside the chat row*, and polling
  the controlplane's `/recovery/<built-in function id>/status` to fetch the rest later. That is
  five services (3,456 lines), ~1,200 lines of `ChatViewModel`, a
  `PendingResponseRecoveryView`, and a field on every stored chat. `follow`
  with `Last-Event-ID` replaces all of it: foreground → reopen the stream,
  background → close the socket and let the run continue.
- **There is no SSE reader today.** Progress rides *inside* the content stream
  as `<tinfoil-event>` markers (`TinfoilEventParser`, 211 lines) because a
  second channel did not exist. The reducer needs ~100 lines on
  `URLSession.bytes` (§11).
- **Rate limits come from the token mint**, and the client decrements
  optimistically and reconciles stale echoes (`SessionTokenManager`,
  `ChatModels.swift:1662–1805`). `STATE_DELTA /rateLimit` replaces the guess.
- **The thread window is a memory constraint, not a nicety.** A phone cannot hold
  every decrypted chat, which is why `ChatListSummary` and materialised `Chat`
  are split and inactive chats are evicted. `threads/get {before, limit}` and a
  windowed `MESSAGES_SNAPSHOT` (§5.3, §5.4) keep that property without a store.
- **Thinking summaries** (`ThinkingSummaryService`) are iOS-only and a second
  model call per run. Product call in §13.11.
- **Verification today shows one enclave**, the inference router, in three
  drawers. The user can verify the machine that reads the prompt but not the one
  that stores the history. `VerifierViewController` renders `/v1/verify` after
  and is the one file in this table that grows.
- **UIKit bridges are load-bearing.** `MessageTableView` (1,214), the
  `CustomTextEditor`/`PastingTextView` in `MessageInputView`, the UIKit chat
  list: these are the purest pixel-and-finger files in the repo and the
  migration does not touch them.
- **iOS-only surfaces stay whole:** Sign in with Apple and Clerk MFA/TOTP with QR
  enrolment, StoreKit through RevenueCat, camera and photo capture, the QR
  scanner for key import, Siri/Shortcuts intents, home-screen quick actions,
  the share extension and its app-group inbox, haptics, VoiceOver
  announcements. None of it reads a message.

What still hits the controlplane directly: `/api/config/mobile` for
`minSupportedVersion` only (its `chatConfig.systemPrompt`/`rules` half is the
harness's), Clerk, RevenueCat, `dash.tinfoil.sh` links.

### 10.2 `TinfoilChat/Services/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `Services/AttachmentProcessingStore.swift` | 100 | D | generation fence for local async processing |
| `Services/AudioRecordingService.swift` | 191 | S(90) | keep `AVAudioRecorder` + hold-to-record; `transcribe` → `POST /v1/transcriptions` |
| `Services/ChatEncryptor.swift` | 61 | K | AES-GCM helper the key bundle wrap uses |
| `Services/ChatLoadingService.swift` | 650 | D | load/save/delete of local chats and their fences |
| `Services/ChatRecoveryClient.swift` | 428 | D | EHBP run with `X-Session-Id`, controlplane `/recovery/*`; `follow` |
| `Services/ChatRecoveryCoordinator.swift` | 1838 | D | scan/begin/complete/cancel, merge a recovered turn by `turnId` |
| `Services/ChatRecoveryCrypto.swift` | 372 | D | HKDF+AES-GCM envelope for the EHBP session token |
| `Services/ChatRecoveryDraftStore.swift` | 261 | D | the in-flight message is the reducer's |
| `Services/ChatRecoverySync.swift` | 557 | D | `pendingRecoveries` on the row |
| `Services/CloudKeyAuthorizationStore.swift` | 273 | S(80) | which key fingerprint the user consented to; drop the rollback coupling |
| `Services/CloudKeyPreflightValidator.swift` | 138 | D | `/v1/session.key`, `KEY_MISMATCH` |
| `Services/CloudStorageService.swift` | 695 | D | `sync.go`; the `/api/chats/generate-id` caller (§4.4) |
| `Services/CloudSyncService.swift` | 2719 | D | `UploadCoalescer`, revision replay, tombstones, conflict pulls |
| `Services/DeviceEncryptionService.swift` | 113 | D | device key for local-only chats; goes with §12.3 |
| `Services/DocumentConversionService.swift` | 176 | D | docling client; `attach.go` |
| `Services/DocumentProcessingService.swift` | 118 | D | PDFKit extraction; `attach.go` |
| `Services/EditClock.swift` | 216 | D | single writer |
| `Services/EncryptedFileStorage.swift` | 1345 | D | no client store |
| `Services/EncryptionService.swift` | 605 | S(200) | CEK custody: Keychain, in-memory key, staged primary, key history for the one `keys/migrate-all` call |
| `Services/ImageProcessingService.swift` | 105 | S(45) | downscale + thumbnail for the composer preview only |
| `Services/LinkMetadataService.swift` | 196 | D | `render_link_preview` result (§7.5) |
| `Services/ManagedFileBatchProcessor.swift` | 103 | D | local batch processing |
| `Services/ManagedFileStore.swift` | 191 | S(90) | stage a picked file before upload |
| `Services/PersonalizationPromptBuilder.swift` | 60 | D | `context.go` |
| `Services/ProfileManager.swift` | 1077 | S(150) | observable profile the settings bind to; no Keychain copy, baseline, dirty flags or timers |
| `Services/ProfileMerge.swift` | 346 | D | §5.8 |
| `Services/ProfileSyncService.swift` | 371 | D |  |
| `Services/ProjectStorageService.swift` | 499 | D | §5.7 |
| `Services/ResponseLanguageResolver.swift` | 30 | D | client sends its locale; `context.go` resolves |
| `Services/RevisionSyncState.swift` | 332 | D | sync policy |
| `Services/ShareAPIService.swift` | 84 | D | `threads/share` |
| `Services/SharedImportCoordinator.swift` | 76 | K* | drains the share-sheet inbox into the composer |
| `Services/SharePayloadBuilder.swift` | 198 | D | `export.go` |
| `Services/StreamingMarkdownChunker.swift` | 364 | K | layout chunking |
| `Services/StreamingResponseProcessor.swift` | 820 | S(250) | becomes the reducer; marker stripping, the reasoning state machine, tool-call delta merging and citation collection go |
| `Services/SummarizerService.swift` | 123 | D | `derived.go` |
| `Services/SyncHealthStore.swift` | 128 | D | nothing syncs, so nothing has health |
| `Services/ThinkingSummaryService.swift` | 103 | D | §13.11 |
| `Services/ThinkingTextChunker.swift` | 136 | K | layout chunking |

### 10.3 `Services/Passkey/`, `Services/SyncEnclave/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `Services/Passkey/LegacyPasskeyCredentials.swift` | 96 | D | controlplane `/api/passkey-credentials/`, a migration artifact |
| `Services/Passkey/PasskeyDiagnostics.swift` | 89 | K |  |
| `Services/Passkey/PasskeyManager.swift` | 1681 | S(400) | setup/recovery/backup state machine; drop `/v1/key/*` orchestration, the 30 s poll, `dismissedKeyId` |
| `Services/Passkey/PasskeyService.swift` | 213 | K | `TinfoilPasskeyKit` façade |
| `Services/Passkey/TinfoilPasskeyAdapters.swift` | 191 | K | rpId, PRF salt, HKDF info |
| `Services/Passkey/TinfoilPasskeyPresentationAnchorProvider.swift` | 37 | K |  |
| `Services/SyncEnclave/CEKEncoding.swift` | 59 | S(20) | base64 of the CEK for `key` |
| `Services/SyncEnclave/ChatSearchService.swift` | 320 | D | `threads/search` |
| `Services/SyncEnclave/EnclaveErrorRecovery.swift` | 268 | D | harness classifies |
| `Services/SyncEnclave/LegacyBlobMigration.swift` | 591 | D | `keys/migrate-all` is one passthrough call |
| `Services/SyncEnclave/LegacyChatEviction.swift` | 95 | D |  |
| `Services/SyncEnclave/PasskeyKeyFlow.swift` | 834 | S(250) | PRF→KEK, wrap/unwrap, `key_id` check; HTTP orchestration → `keys/*` |
| `Services/SyncEnclave/SyncEnclaveAPI.swift` | 1076 | D | 22 endpoints → the harness client |
| `Services/SyncEnclave/SyncEnclaveClient.swift` | 544 | S(200) | becomes the harness client: `SecureClient`, Clerk JWT, error envelope |
| `Services/SyncEnclave/SyncEnclaveKeyBundle.swift` | 173 | K | wire shape of a wrapped CEK |
| `Services/SyncEnclave/SyncEnclaveProjectStore.swift` | 205 | D |  |
| `Services/SyncEnclave/SyncEnclaveWireContract.swift` | 76 | D |  |

### 10.4 `Models/`, `Utilities/`, `Config/`, `Extensions/`, `Intents/`, root

| File | Lines | Fate | Notes |
|---|---|---|---|
| `Models/AttachmentModels.swift` | 124 | S(60) | compose/render model; drop `base64`, `encryptionKey`, `processingState` |
| `Models/ChatFavorites.swift` | 86 | D | `thread.pinned` |
| `Models/ChatIndexEntry.swift` | 75 | D | sync bookkeeping |
| `Models/ChatListSummary.swift` | 45 | K* | `ThreadSummary` |
| `Models/ChatModels.swift` | 1814 | S(600) | `Message`, segments, tool calls, `QueuedMessage`, `RateLimitInfo`; drop sync fields, `pendingRecoveries`, `reasoningContentForHistory`, `buildSyncTimeline`, `SessionTokenManager` (→ `auth.go`) |
| `Models/CloudKeyRecoveryModes.swift` | 9 | K |  |
| `Models/CloudSyncModels.swift` | 696 | D | `StoredChat` and friends become Go structs in `thread.go` |
| `Models/PersonalizationDraft.swift` | 104 | S(50) | editor state; drop field diffing |
| `Models/ProjectModels.swift` | 167 | K* | DTOs minus `syncVersion` |
| `Models/PromptPreset.swift` | 360 | D | prompt bodies → `catalog.go`; icons stay in the view |
| `Models/VerificationModels.swift` | 34 | K |  |
| `Utilities/AccessibilityAnnouncer.swift` | 27 | K |  |
| `Utilities/AccountOperationFence.swift` | 79 | S(30) | one generation fence for sign-out |
| `Utilities/ChatQueryBuilder.swift` | 306 | D | `context.go`; this file is the prompt builder |
| `Utilities/KeychainHelper.swift` | 132 | K |  |
| `Utilities/NetworkMonitor.swift` | 29 | K |  |
| `Utilities/PerformanceInstrumentation.swift` | 141 | S(90) | sync and storage signposts go |
| `Utilities/PremiumProjectPolicy.swift` | 31 | D | harness authorises |
| `Utilities/ProjectContextBuilder.swift` | 58 | D | `context.go` |
| `Utilities/TimeReminder.swift` | 34 | D | `context.go` |
| `Utilities/TinfoilEventParser.swift` | 211 | D | `agent.go` reads the upstream stream |
| `Utilities/TokenEstimation.swift` | 106 | D | `context.go` |
| `Utilities/URLErrorClassifier.swift` | 37 | K | offline banner |
| `Config/AppConfig.swift` | 645 | S(150) | display catalog from `session`, selected model id, version gate |
| `Config/Constants.swift` | 658 | S(300) | UI constants; every `API`, `Sync`, `SyncEnclave`, `Summarizer`, `Metadata`, `DocumentProcessing`, `TitleGeneration`, `Context`, `Share`, `ChatRecovery` block goes |
| `Config/GenUIConfigService.swift` | 66 | D | `session.widgets` |
| `Config/Theme.swift` | 63 | K |  |
| `Extensions/Color+Hex.swift` | 77 | K |  |
| `Extensions/Color.swift` | 108 | K |  |
| `Extensions/Model+Tinfoil.swift` | 33 | D | the client names no model on the wire |
| `Extensions/View+Accessibility.swift` | 18 | K |  |
| `Intents/AppIntentCoordinator.swift` | 41 | K |  |
| `Intents/TinfoilAppIntents.swift` | 97 | K* | intents dispatch turns |
| `TinfoilChatApp.swift` | 241 | S(120) | drop storage migration, sync init, enclave pre-warm, profile sync |
| `ContentView.swift` | 313 | S(220) | drop the key → sync bootstrap chain |
| `HomeScreenQuickActionSceneDelegate.swift` | 82 | K | premium gate reads the session |

### 10.5 `ViewModels/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `ViewModels/AuthManager.swift` | 530 | S(300) | Clerk session + token; drop teardown orchestration and the entitlement timer |
| `ViewModels/AuthenticatorMFAViewModel.swift` | 292 | K | Clerk MFA |
| `ViewModels/ChatSearchController.swift` | 116 | D | a debounce stays in the view |
| `ViewModels/ChatViewModel.swift` | 7780 | S(450) | §10.6 |
| `ViewModels/RevenueCatManager.swift` | 183 | K | StoreKit |

### 10.6 `ChatViewModel.swift`, by section

7,780 lines. The verdict per region, from a `// MARK:` and `func` map of the file:

| Lines | What | Fate |
|---|---|---|
| 81–150 | GenUI retry engine: a literal repair system prompt, history sanitiser, finish-reason classifier, second inference request | D, `kind: "retryToolCall"` |
| 229–630 | ~70 `@Published`: chats/summaries/hydration, sync flags, pagination, rate limit, queue, projects, recovery timers, stream throttles | D except sheets, scroll targets, focus, image viewer, encryption/passkey flags, audio (~80 lines) |
| 630–960 | favorites hydration, pagination persisted in `UserDefaults` | D, `thread.pinned`, `threads/list` |
| 1119–1537 | observers for remote delete / recovery / profile push; auto-sync and recovery-scan timers; recovery scanning | D |
| 1538–1620 | attested inference client + `onVerification` | S(40), verifies the harness |
| 1747–2421 | projects: load, create, enter, documents (docling → markdown → upload), move chats | D, §5.7 |
| 2559–2779 | chat selection with hydration fences and lazy image fetch | S(20), select = `threads/get` + `follow` |
| 2827–3192 | `deleteChat` (178 lines), summary reconciliation, eviction | D |
| 3302–3479 | `sendMessage` gates, optimistic rate-limit decrement, the message queue | S(15) |
| 3480–3803 | attachments: extract, resize, size limits, fences | S(80), pick → `attachments/upload` → hold an id |
| 3804–4766 | `generateResponse`: model resolution, prompt assembly with `{MODEL_NAME}`/`{LANGUAGE}`/`{USER_PREFERENCES}`/`{TIMEZONE}`, `ChatQueryBuilder`, recovery session, `<tinfoil-event>` handling, chunk loop, title generation, save, 401 retry, error → flags | D, all 963 lines |
| 4767–4820 | `applyStreamSnapshot` | the one function with a reducer analogue, S(30) |
| 4821–5015 | error classification by string matching | D, `RUN_ERROR.code` |
| 5021–5220 | cancel, recovery abandon, stream drain, queue drain on finish | S(5), `threads/cancel` |
| 5221–5448 | GenUI retry driver | D |
| 5449–5583 | regenerate, edit, save edit (truncate + resend) | S(20), three `kind`s |
| 5619–5938 | end-stream backup, normalise/dedup chats, blank chat at top, persistence, `changeModel` | D except `changeModel` (5) |
| 5939–6972 | account fences, legacy migration, sign-in/out with anonymous-chat re-encryption, delete-all, `initializeCloudSync` | S(90), sign-out = forget key |
| 6973–7368 | pagination (323 lines), `performFullSync` | D |
| 7369–7660 | key custody: set/reload key, passkey retry, start fresh, backup, re-encrypt-and-upload | K except `reencryptAndUploadChats`, S(220) |
| 7662–7780 | audio recording | K |

What it becomes: `Harness/Reducer.swift` (~200, §8.2), `ChatStore.swift` (~120,
subscriptions and intents), `UIState.swift` (~80), the key-custody block moved
next to `EncryptionService`, and audio on its own. About 610 lines for the
7,780.

### 10.7 `Views/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `Views/AnimatedConfidentialTitle.swift` | 97 | K |  |
| `Views/AttachmentPreviewBar.swift` | 151 | K |  |
| `Views/AuthenticationView.swift` | 26 | K |  |
| `Views/AuthenticatorMFASettingsView.swift` | 794 | K |  |
| `Views/AutoIntelligenceSelector.swift` | 137 | K* | `session.auto.levels` |
| `Views/CameraPickerView.swift` | 42 | K |  |
| `Views/ChatListView.swift` | 284 | S(230) | drop archived-index math and recovery-draft pruning |
| `Views/ChatSidebar.swift` | 1152 | S(750) | drop local/cloud tabs, sync spinners, pull-to-sync, search policy |
| `Views/ChatView.swift` | 1204 | S(1050) | model tabs read `session.models` |
| `Views/CloudSyncOnboardingView.swift` | 885 | S(550) | key generate/restore/display steps stay; the sync toggle and its intro go |
| `Views/CloudSyncSettingsView.swift` | 885 | S(350) | key + passkey sections; sync status, local-only, bundle inventory go |
| `Views/ContextUsageIndicator.swift` | 76 | K* | `contextUsage` from the harness |
| `Views/DocumentPickerView.swift` | 223 | K |  |
| `Views/EncryptionKeyInputView.swift` | 302 | K | the raw key |
| `Views/GatedPaywallView.swift` | 116 | K |  |
| `Views/GhostIcon.swift` | 104 | K |  |
| `Views/GridTexture.swift` | 43 | K |  |
| `Views/InitializationFailedView.swift` | 82 | K |  |
| `Views/InlineSubscriptionLoadingView.swift` | 141 | K |  |
| `Views/LaTeXMarkdownView.swift` | 1421 | K |  |
| `Views/MessageAttachmentIndicator.swift` | 446 | K* |  |
| `Views/MessageInputView.swift` | 2064 | S(1820) | drop `contextUsage`, rate-limit math, catalog reads |
| `Views/MessageQueueView.swift` | 163 | K* | `queue` from the reducer |
| `Views/MessageTableView.swift` | 1214 | K | UIKit list; untouchable |
| `Views/MessageView.swift` | 2303 | S(1750) | one `ForEach` over timeline blocks; coverage checks, `<think>` parsing, error-kind copy go |
| `Views/NoInternetView.swift` | 67 | K |  |
| `Views/OnboardingView.swift` | 640 | S(580) | model page reads `session.models` |
| `Views/OptimizedChatListView.swift` | 524 | S(480) | drop archived-index math |
| `Views/PasskeyRecoveryChoiceView.swift` | 218 | K |  |
| `Views/PersonalizationView.swift` | 275 | K* | writes `profile/update` |
| `Views/PixelAvatarView.swift` | 73 | K |  |
| `Views/ProjectFolderIcon.swift` | 21 | K |  |
| `Views/ProjectPage.swift` | 517 | S(430) | intents + rendering |
| `Views/PromptLibraryView.swift` | 523 | S(450) | drop `<system>` tag wrapping |
| `Views/QRCodeScannerView.swift` | 211 | K |  |
| `Views/ReasoningEffortSelector.swift` | 159 | K* | two booleans from `session.models[].reasoning` |
| `Views/SettingsView.swift` | 1382 | S(600) | `SettingsManager` goes; forms bind to `profile/update` |
| `Views/ShareChatView.swift` | 282 | S(120) | one call |
| `Views/StartFreshConfirmation.swift` | 4 | K |  |
| `Views/UpdateRequiredView.swift` | 84 | K | `minSupportedVersion` still from `/api/config/mobile` |
| `Views/URLFetchBox.swift` | 170 | K* | reads a `tool_call` block |
| `Views/VerifierViewController.swift` | 793 | K* | renders `/v1/verify`; the one file that grows |
| `Views/WebSearchBox.swift` | 429 | K* | reads a `tool_call` block |
| `Views/Authentication/AuthenticationHelpers.swift` | 193 | K | Clerk errors |
| `Views/Authentication/DisableKeyboardExtension.swift` | 19 | K |  |
| `Views/Authentication/ForgotPasswordView.swift` | 307 | K |  |
| `Views/Authentication/ModularAuthenticationView.swift` | 399 | K |  |
| `Views/Authentication/SignInView.swift` | 445 | K |  |
| `Views/Authentication/SignUpView.swift` | 355 | K |  |
| `Views/GenUI/GenUIInputAreaView.swift` | 81 | S(50) | mounts the `.input` surface |
| `Views/GenUI/GenUIRegistry.swift` | 98 | S(35) | name → renderer; `buildToolParams`, `buildPromptHint`, the allow-list go |
| `Views/GenUI/GenUIStyle.swift` | 141 | K |  |
| `Views/GenUI/GenUIToolCallView.swift` | 191 | S(120) | placeholder, failure card, retry button; JSON-parse-to-guess-streaming goes |
| `Views/GenUI/GenUIWidget.swift` | 331 | S(120) | drop `description`, `schema`, `promptHint` and the `GenUISchema` DSL; keep surface, contexts, typed `Args` |
| `Views/GenUI/TimelineToolCalls.swift` | 157 | D | storage-format helpers |
| `Views/GenUI/Widgets/ArtifactPreviewWidget.swift` | 231 | S(195) |  |
| `Views/GenUI/Widgets/ChartWidget.swift` | 396 | S(350) |  |
| `Views/GenUI/Widgets/ClockWidget.swift` | 682 | S(640) |  |
| `Views/GenUI/Widgets/ImagePreviewView.swift` | 221 | K |  |
| `Views/GenUI/Widgets/ImageWidget.swift` | 279 | S(245) |  |
| `Views/GenUI/Widgets/LinkPreviewWidget.swift` | 126 | S(100) | renders `TOOL_CALL_RESULT` |
| `Views/GenUI/Widgets/MapWidget.swift` | 514 | S(470) |  |
| `Views/GenUI/Widgets/MessageComposeWidget.swift` | 275 | S(245) |  |
| `Views/GenUI/Widgets/RecipeCardWidget.swift` | 598 | S(555) |  |
| `Views/GenUI/Widgets/SportsDataWidget.swift` | 435 | S(390) |  |
| `Views/GenUI/Widgets/StatCardsWidget.swift` | 134 | S(105) |  |
| `Views/GenUI/Widgets/TimelineWidget.swift` | 111 | S(85) |  |

### 10.8 `TinfoilShared/`, `TinfoilShareExtension/`

| File | Lines | Fate | Notes |
|---|---|---|---|
| `TinfoilShared/BoundedFileIO.swift` | 244 | K |  |
| `TinfoilShared/SharedImportContract.swift` | 118 | K | accept list stays compiled; §13.12 |
| `TinfoilShared/SharedImportRepresentationLoader.swift` | 34 | K |  |
| `TinfoilShared/SharedImportStore.swift` | 509 | K |  |
| `TinfoilShareExtension/ShareViewController.swift` | 270 | K | knows nothing about chats |

### 10.9 `TinfoilChatTests/`

Of 61 files (13,380 lines), these go with their subjects: `ChatRecovery*` (5),
`CloudConflictPullSafety`, `CloudSyncRecoveryValidation`, `CloudSyncUploadRetry`,
`RevisionSync`, `LazyChatHydration`, `SyncAuthenticationRecovery`,
`EnclaveErrorRecovery`, `ProfileMerge`, `ProjectSchemaFidelity`,
`ProjectChatEncoding`, `AtomicProjectDeletion`, `AccountOperationFence`,
`ChatQueryBuilderReasoning`, `ProjectContextBuilder`, `PersonalizationPrompt`,
`PromptResolver`, `ResponseLanguageResolver`, `TimeReminder`, `TokenEstimation`,
`CitationRegex`, `MessageSegmentDecode`, `ChatSearchService`,
`ChatSearchController`, `ShareV2Contract`, `GenUIRetry`, `GenUIConfig`,
`AttachmentProcessingStore`, `AttachmentProcessing`, `AttachmentLegacyKeyDecode`,
`BoundedDocumentStaging`, `BoundedImageDownload`, `DocumentPickerBatch`,
`ModelAvailability`, `ChatFavorites`, `ErrorExplanation`,
`PendingResponseRecoveryPresentation`, `PremiumProjectPolicy`: 42 files, about
10,300 lines. `PasskeyComposition` (1,220), `SharedImportStore`,
`SharedImportCoordinator`, `StreamingMarkdownChunker`, `AuthenticatorMFAViewModel`
and the layout tests stay. `StreamingResponseProcessorTests` shrinks to the
reducer's cases.

### 10.10 Sums

| | Lines |
|---|---|
| D | 17,361 |
| S, before → after | 35,936 → 17,010 |
| K | 11,085 |
| K* | 3,033 |
| removed | ~36,287 |
| added (`Harness/Client.swift`, `Reducer.swift`, `Types.swift`) | ~550 |
| app target after | ~31,678 |

---

## 11. Dependencies the clients drop

`fflate`, `@zip.js/zip.js`, `zod-to-json-schema`, `@noble/hashes`, `uuid`
(`openai` and `pako` are already gone).
`zod` survives only if a widget renderer still wants it for coercion;
`input-coercion.ts` can do without. `tinfoil` stays (SecureClient),
`@tinfoilsh/passkey-kit` stays.

The iOS app drops `encrypted-http-body-protocol` (EHBP served only the recovery
path) and the OpenAI query types `TinfoilAI` wraps; it keeps `tinfoil-swift`
(`SecureClient`), `tinfoil-passkey-kit`, `clerk-ios`, `purchases-ios`,
`SwiftMath`, `textual` and `sentry-cocoa`. It gains an SSE reader on
`URLSession.bytes` (~100 lines, `Last-Event-ID` included) and a JSON-patch
applier; it has neither today, because its progress events ride inside the
content stream as `<tinfoil-event>` markers.

The harness gains: a Clerk JWKS verifier, a BPE tokenizer, `golang.org/x/image`
for resize/webp, a zip writer (stdlib), and the tinfoil-go client it already has
pointed at four more repos.

---

## 12. Transition

### 12.1 Order

1. **`context.go` + `catalog.go`, shadow mode.** Port the builder; the harness
   accepts today's `RunAgentInput` and also a `threads/turn` that renders the
   same prompt. Diff the two builders on captured threads until they agree.
2. **Web streams through `threads/turn` with `ephemeral: true` and the client
   still storing.** Deletes §9.3 streaming, `inference-client.ts`,
   `tinfoil-client.ts`, `config/models.ts`. Reversible: `HARNESS_ENCLAVE_URL`
   already gates the transport.
3. **Harness owns the thread.** `sync.go`, `thread.go`, `auth.go`; client sends
   `key`. Deletes IndexedDB, cloud sync, profile sync, projects storage,
   recovery, queue. This is the step that removes `use-chat-messaging.ts`; do
   not leave it dormant behind a flag.
4. **Attachments, transcription, share, export (native backup, Claude
   projects), import, search.** Deletes the remaining `services/`.
5. **iOS**, against a protocol three steps of web traffic have shaken out, in
   the same three steps: (a) harness client + reducer + SSE reader behind a
   flag, streaming `ephemeral: true` turns while `EncryptedFileStorage` still
   stores -- deletes `ChatQueryBuilder`, `TinfoilEventParser`, the parser half
   of `StreamingResponseProcessor`, `GenUIRegistry.buildToolParams`, and all of
   recovery; (b) the harness owns the thread -- deletes `CloudSyncService`,
   `EncryptedFileStorage`, `ChatLoadingService`, `ProfileMerge`,
   `ProfileSyncService`, `ProjectStorageService` and the ~6,000 lines of
   `ChatViewModel` that drive them; (c) attachments, transcription, share,
   search, and export, which iOS gets for the first time (today it opens the
   webapp's settings page in Safari). Android follows the same three steps.

### 12.2 iOS as a second writer

iOS today reads and writes the same sync-enclave rows the harness will. Until
iOS moves to the harness:

- The harness writes the v2 plaintext shape (§6.1) and stamps
  `clock`/`writer`/`clockVersion` on chat rows and `fieldClocks` on the profile
  row, so iOS's arbitration keeps working. It reads them but never needs them.
- The harness's CAS-and-retry (§7.8) handles iOS writing concurrently. Content
  conflicts resolve last-writer-wins by ETag, which is what the enclave does
  today for two web tabs.
- iOS also stores `pendingRecoveries` envelopes on the row (its recovery
  design, §10.1) and a per-chat `modelType` it marks as not synced. The harness
  preserves any field it does not know on a CAS rewrite and reads neither.
- Once iOS is on the harness, the clock fields are dropped from the writer and
  `pendingRecoveries` with them.

### 12.3 Anonymous users and "local only"

Two features today depend on client storage in a way nothing server-side can
replace:

- **Anonymous continuity.** Anonymous chats live in `sessionStorage` and survive
  a reload within the tab. Under this design anonymous traffic is ephemeral:
  the harness holds it for 30 minutes after the last run and a reload can
  `follow` it if the client remembers `{threadId, runId}` -- which it can, in
  `sessionStorage`, as two strings. Beyond 30 minutes it is gone. **This is a
  small regression** relative to today's tab-lifetime persistence and is the
  price of having no client store; a 50-line `sessionStorage` cache of the
  reducer's `thread` (pure serialisation, no logic) closes the gap if the
  product wants it.
- **Local-only mode** (`SETTINGS_LOCAL_ONLY_MODE_ENABLED`, `isLocalOnly`
  chats in IndexedDB, never uploaded; on iOS `isLocalOnlyModeEnabled`, chats
  under `EncryptedFileStorage.local` sealed with the device key from
  `DeviceEncryptionService`). This mode *is* client storage; it has no harness
  equivalent. The design drops it. A user who does not want a thread
  stored uses a temporary (ephemeral) chat. If local-only must survive, it
  survives as a separate, deliberately thin storage adapter, and this doc's
  deletion counts for `indexed-db.ts` no longer hold.

Both are product calls, flagged here rather than decided.

### 12.4 Things this needs from other repos

- **Shim / tinfoild:** `/v1/*` leaves authentication to the harness. The harness
  validates Clerk JWTs and client-issued free keys. Anonymous key validation
  uses the existing controlplane endpoint restricted to registered Tinfoil hosts.
- **Controlplane:** none required. `/api/chat/token`, `/api/keys/chat`,
  `/api/config/*`, `/api/shares/{id}`, `/recovery/*` are called as they
  exist today. The browser keeps anonymous `/api/keys/chat` issuance; the
  harness uses `/api/shim/validate-key`. `/api/chats/generate-id`,
  `/api/projects/generate-id`, `/api/projects/{id}/documents/generate-id` and
  `/api/passkey-credentials/` lose their last callers and can go later.
- **confidential-sync:** none required. The harness is one more client.
- **tinfoil-config.yml:** egress allowlist additions listed in §2.

---

## 13. Decisions taken here

Stated so they can be argued with rather than rediscovered.

1. The harness holds the CEK per request and derives from it. Justified in §1
   by the sync enclave already doing so.
2. The thread store is the existing sync enclave, not a new store. Keeps iOS,
   search, share, import and attachments working through the transition with
   zero controlplane changes.
3. All reads are POST with the key in the body. Same reason the sync enclave
   does it.
4. The client authenticates with the Clerk JWT alone; the harness exchanges it.
   Removes the largest single piece of fragile client code that is not
   rendering.
5. Widgets are advertised by name; schemas live in the harness. The client
   cannot be the source of truth for what the model is told.
6. The harness mints all ids. Removes the blank-chat state machine.
7. Queueing is server-side. Removes 600 lines and makes the queue visible from
   any device.
8. Cancel is a call, not an abort. A run the harness owns cannot be stopped by
   closing a socket.
9. Attachment preprocessing, including image resize and transcription, is
   server-side. The earlier instinct to keep it on the client for "platform
   codecs" does not survive contact with the goal: Go decodes JPEG/PNG/GIF/WebP,
   the PDF path was already an enclave, and three clients would otherwise each
   own a resize pipeline.
10. Local-only mode is dropped; anonymous is ephemeral. See §12.3.
11. Live thinking summaries -- iOS's `ThinkingSummaryService` makes a second
    model call every few seconds while the model reasons and shows the result
    above the thinking box -- are not in the protocol. If the product keeps
    them, `derived.go` emits `STATE_DELTA /run/thinkingSummary` on the same
    cadence, billed like the title; otherwise iOS loses the feature. Flagged,
    not decided.
12. The iOS share extension keeps a compiled accept list and size caps. It runs
    in its own process with no session, so it cannot ask `/v1/session`; the
    harness's `ATTACHMENT_UNSUPPORTED` at upload is the real gate.
13. Model icons stay bundled in each app. `session.models[].image` is a key
    into that bundle, never a URL a client fetches.
