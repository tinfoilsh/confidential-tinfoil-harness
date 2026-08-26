# confidential-tinfoil-harness

Minimal [AG-UI](https://ag-ui.com) agent in front of Tinfoil's inference gateway,
for confidential tool loops.

    POST   /agui      RunAgentInput in, AG-UI events out
    DELETE /agui      drop a stored run log
    GET    /healthz

## Protocol

The body is a `RunAgentInput`. The harness reads `threadId`, `runId` and
`messages`, and ignores `state`, `context` and `parentRunId`.

`tools` declares widgets the caller draws. They are advertised to the model and
answered on the caller's behalf, never dialled, so the loop only executes what it
attests. A widget that claims a name the run itself serves is rejected instead of
allowed to shadow it. AG-UI has no model field, so `forwardedProps.model` names a
model or `auto`. `forwardedProps.piiCheck: true` asks the model enclave to screen
the run for PII, and absent means off.

### Tool families

A family is one enclave the loop dials and the tools it advertises out of it.
`tools.go` holds them in one table. A row names the family, its pinned repo and
host, its tools, the system prompt the model reads for them, and one function
that reads `forwardedProps` to decide whether the run dials that enclave at all
and what its calls carry. Two are served:

    webSearch       web_search, web_fetch
    codeExecution   bash, view, str_replace, create, insert, present

Tool names live in the table instead of being read off the enclave, which buys
three things. The harness can reject a shadowing widget before the run starts,
the advertised order stays fixed so a caller keeps hitting the same prompt cache,
and an enclave that grows a seventh tool does not widen what this agent offers.
Only the families a run dials reserve their names, and the sandbox's are ordinary
words, so a widget called `view` or `create` is the caller's own on every run
that does not ask for code execution.

Each family the run dials also prepends its system prompt, which counts against
the model's context the same as the caller's messages do.

A tool enclave that does not verify at startup is logged and skipped, not fatal.
Runs that ask for that family are refused, everything else is served, and the
next restart is the next chance to pin it.

`forwardedProps.webSearch: false` withholds web search for that run, and absent
means on. Code execution works the other way round, since its sandbox cannot be
dialled without the caller's tokens:

    "forwardedProps": {
      "codeExecution": {
        "accessToken": ..., "encryptionKey": ..., "containerAuthToken": ...,
        "uploads": [{"fileAccessToken": ..., "filename": ..., "sha256": ...}]
      }
    }

All three tokens are required, `uploads` is optional, and a block missing one
answers `400`. The harness never reads them. They cross to the enclave as call
metadata, which is what lets a workspace outlive the run. `accessToken` names the
container, so a caller that sends the same one comes back to the same shell, the
same files and the same installed packages. A family that holds a shell takes its
calls one at a time, in the order the model made them. Everything else still runs
concurrently.

`present` renders a file for the user. Its output comes back as that call's
`TOOL_CALL_RESULT`, like any other tool's, and the prompt tells the model the
user is already looking at it. A client that offers code execution has to draw
it. Results are truncated for both the model and the caller, but the caller's
limit is much the larger of the two, because the model is told not to repeat what
it presented.

Eleven event types come back, framed as SSE:

    RUN_STARTED  RUN_FINISHED  RUN_ERROR
    TEXT_MESSAGE_CHUNK  REASONING_MESSAGE_CHUNK
    TOOL_CALL_START  TOOL_CALL_ARGS  TOOL_CALL_END  TOOL_CALL_RESULT
    ACTIVITY_SNAPSHOT  ACTIVITY_DELTA

Tool calls run concurrently and are addressed by id, so their events may
interleave. Text and reasoning use chunk events, one message per turn. A call
made in a turn that also produced text carries that message as its
`parentMessageId`. `TOOL_CALL_RESULT` carries the tool's output, truncated only
if it outgrew what a frame may hold. A call that failed carries
`{"error": ...}`, which is what the model reads too.

### Tool output as it happens

A tool that reports progress streams it. Each call opens an activity of type
`TOOL`, addressed as `act_<toolCallId>`, and every MCP progress notification from
the enclave patches it:

    ACTIVITY_SNAPSHOT  {"toolCallId": ..., "tool": "web_search", "progress": 0, "output": []}
    ACTIVITY_DELTA     [{"op":"replace","path":"/progress","value":0.5},
                        {"op":"add","path":"/output/-","value":"..."}]

`output` is a list because RFC 6902 cannot grow a string; a client joins it. The
result itself still arrives whole, since MCP has no partial tool result, so a
long-running tool is legible only as far as it reports progress.

A run answers within 16 tool-calling turns or ends with `RUN_ERROR`, and widget
calls count against that too. `RUN_FINISHED` carries the run's summed usage
across every turn. A request that fails before the run opens answers with an HTTP
status instead, so a gateway refusal reaches the caller as itself.

### Coming back to a run

Every frame carries an `id:`, monotonic from zero. A caller that wants to be able
to come back generates a `sessionId` and a `recoveryToken`, both 128 random bits
as hex, and sends them with the run. It comes back by posting the same pair with
`resume: true` and a `Last-Event-ID`, and is served everything after that id,
from memory if this harness is still running it and from the stored log
otherwise.

Being able to open the log is the whole of the authorization. A secret that does
not open it and a log that is not there are refused identically, and a run too
young to have framed anything cannot be authorized either way, so it answers
`503` with a `Retry-After` instead of a refusal. `DELETE /agui` with the same
pair drops the log once the caller has the answer, authorized by opening it like
everything else, and answers `502` rather than `204` if the store did not drop
it. A log nobody drops expires with the store.

A run outlives the connection that asked for it. Nothing is written anywhere
while a caller is attached. Once the caller disconnects, the harness seals off
what it has to the store and keeps writing there until the run ends. Frames are
sealed as they are produced, under a key derived from the caller's secret, so the
spill is a byte copy and the store holds ciphertext it has no way to read. When
the run ends the key and the frames go with it, and a caller arriving after that
reads the stored log instead. A log with no terminal event and no harness still
running it belongs to a run that died, and replays as one. A run holds its whole
log in memory while it lives, so there is a ceiling on both: a run that reaches
it ends with `RUN_ERROR` and stops, rather than billing turns into a log nothing
can read.

Two things follow. A run that finishes with its caller attached is never written
down at all, so a caller that loses the answer between the last byte and its own
storage has nothing to come back to. And a `sessionId` names nothing. It is not
the thread, not the run, and not derived from either, so the store cannot tell
which conversation a log belongs to.

Image parts must be inline, either `source.type: "data"` or a `data:` URL. A
remote URL is refused because the model enclave, not the harness, would fetch it,
off the attested path and outside its egress allowlist.

## Deployment

The harness runs as the single container of a Tinfoil CVM. `tinfoil-config.yml`
is the measured description of that CVM: a client attesting this enclave is told
exactly this file, so the container is pinned by digest and every host the
harness may reach is enumerated.

    tinctl inspect tinfoilsh/confidential-tinfoil-harness   # resolve the config
    tinctl deploy  tinfoilsh/confidential-tinfoil-harness   # launch it

Ingress is the shim alone: TLS on 443, forwarded to the harness on 8081, with
`/agui` and `/healthz` the only paths that reach it. The shim validates the
caller's Tinfoil key and passes it through, which is the same key the harness
forwards upstream. It has none of its own.

### Egress

`networks.upstream.allow` is resolved to IPs and enforced with nftables, and a
name that does not resolve fails the whole set closed. It covers four things:

- the gateway and one host per tool family, which are the hosts it dials;
- every enclave in `main.go`, because attestation is fetched from the host
  itself, including a replica the gateway names in a 422 and the SDK re-seals to;
- what verifying those attestations needs: `github-proxy` for the pinned repo's
  release and Sigstore bundle, `tuf-repo-cdn.sigstore.dev` for the trust root,
  and the AMD and Intel collateral proxies;
- the controlplane, which is the one host here the harness does not attest. A
  detached run spills its sealed log there and reads it back; see above for what
  that host can and cannot see.

Adding a model to `main.go`, or a tool family to `tools.go`, means adding its
host here too.

### Releasing

1. `Image` builds the container and prints the digest to push into
   `tinfoil-config.yml`.
2. Tagging `v*` runs `Release`, which measures the CVM this config describes and
   publishes `tinfoil.hash` with a Sigstore bundle. That release is what the
   Tinfoil SDKs verify a running instance against.

### Two things this is waiting on

- `go.mod` still replaces `tinfoil-go` with a sibling checkout, and the
  seal-following transport it needs is not in a tagged release. Until it is, the
  image builds only against a local `../tinfoil-go`:

      docker build --build-context sdk=../tinfoil-go -t confidential-tinfoil-harness .

  The `Image` workflow checks out `tinfoilsh/tinfoil-go@main` for the same reason
  and will fail until the transport lands there.

- `gateway.tinfoil.sh`, the compiled-in default and the name used in the
  allowlist, is NXDOMAIN today. The deployment fails closed until the gateway has
  a hostname; substitute the real one in both `env` and `allow`.
