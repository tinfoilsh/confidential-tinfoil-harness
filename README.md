# confidential-tinfoil-harness

Minimal [AG-UI](https://ag-ui.com) agent in front of Tinfoil's inference gateway
used for confidential tool loops.

    POST   /agui      RunAgentInput in, AG-UI events out
    DELETE /agui      drop a stored run log
    GET    /healthz

## Protocol

The body is a `RunAgentInput`. `threadId`, `runId` and `messages` are read;
`state`, `context` and `parentRunId` are ignored. `tools` declares
widgets the caller draws: they are advertised to the model and answered on the
caller's behalf when it calls one, never dialled, so the loop still executes
only what it attests. A tool naming `web_search` or `web_fetch` is refused
rather than allowed to shadow one. `forwardedProps.model` names a model or
`auto`, since AG-UI has no model field; `forwardedProps.webSearch: false`
withholds the attested tools for that run, and absent means on;
`forwardedProps.piiCheck: true` asks the model enclave to screen the run for
PII, and absent means off.

Eleven event types come back, framed as SSE:

    RUN_STARTED  RUN_FINISHED  RUN_ERROR
    TEXT_MESSAGE_CHUNK  REASONING_MESSAGE_CHUNK
    TOOL_CALL_START  TOOL_CALL_ARGS  TOOL_CALL_END  TOOL_CALL_RESULT
    ACTIVITY_SNAPSHOT  ACTIVITY_DELTA

Tool calls run concurrently and are addressed by id, so their events may
interleave; text and reasoning use chunk events, one message per turn. A call
made in a turn that also spoke carries that message as its `parentMessageId`.
`TOOL_CALL_RESULT` carries what the model read, truncation included; a call
that failed carries `{"error": ...}`, which is what the model reads too.

### Tool output as it happens

A tool that reports progress streams it. Each call opens an activity of type
`TOOL`, addressed as `act_<toolCallId>`, and every MCP progress notification
from the enclave patches it:

    ACTIVITY_SNAPSHOT  {"toolCallId": ..., "tool": "web_search", "progress": 0, "output": []}
    ACTIVITY_DELTA     [{"op":"replace","path":"/progress","value":0.5},
                        {"op":"add","path":"/output/-","value":"..."}]

`output` is a list because RFC 6902 cannot grow a string; a client joins it.
The result itself still arrives whole -- MCP has no partial tool result -- so a
long-running tool is legible only as far as it reports progress.

`RUN_FINISHED` carries the run's summed usage, every turn of it. A request that
fails before the run opens answers with a status instead, so a gateway refusal
reaches the caller as itself.

### Coming back to a run

Every frame carries an `id:`, monotonic from zero. A caller that wants to be
able to come back generates a `sessionId` and a `recoveryToken`, both 128 random
bits as hex, and sends them with the run. It comes back by posting the same
pair with `resume: true` and a `Last-Event-ID`, and is served everything after
that id -- from memory if this harness is still running it, otherwise from the
stored log. Being able to open the log is the whole of the authorization: a
secret that does not open it and a log that is not there are refused
identically, and a run too young to have framed anything cannot be authorized
either way, so it answers `503` with a `Retry-After` instead of a refusal.
`DELETE /agui` with the same pair drops the log once the caller has the answer
-- authorized by opening it, like everything else, and answering `502` rather
than `204` if the store did not drop it. A log nobody drops expires with the
store.

A run outlives the connection that asked for it. While a caller is attached
nothing is written anywhere: only when it disconnects does the harness seal off
what it has to the store and keep writing there until the run ends. Frames are
sealed as they are produced, under a key derived from the caller's secret, so
the spill is a byte copy and the store holds ciphertext it has no way to read.
When the run ends the key and the frames go with it, and a caller arriving
after that reads the stored log instead. A log with no terminal event and no
harness still running it belongs to a run that died, and replays as one. A run
holds its whole log in memory while it lives, so there is a ceiling on both: a
run that reaches it ends with `RUN_ERROR` and stops, rather than billing turns
into a log nothing can read.

Two things follow from this. A run that finishes with its caller attached is
never written down at all, so a caller that loses the answer between the last
byte and its own storage has nothing to come back to. And a `sessionId` names
nothing: it is not the thread, not the run, and not derived from either, so the
store cannot tell which conversation a log belongs to.

Image parts must be inline: `source.type: "data"`, or a `data:` URL. A remote
URL is refused because the model enclave, not the harness, would fetch it --
off the attested path and outside its egress allowlist.

## Deployment

The harness runs as the single container of a Tinfoil CVM. `tinfoil-config.yml`
is the measured description of that CVM: a client attesting this enclave is
told exactly this file, so the container is pinned by digest and every host the
harness may reach is enumerated.

    tinctl inspect tinfoilsh/confidential-tinfoil-harness   # resolve the config
    tinctl deploy  tinfoilsh/confidential-tinfoil-harness   # launch it

Ingress is the shim alone: TLS on 443, forwarded to the harness on 8081, with
`/agui` and `/healthz` the only paths that reach it. The shim validates the
caller's Tinfoil key and passes it through, which is the same key the harness
forwards upstream -- it has none of its own.

### Egress

`networks.upstream.allow` is resolved to IPs and enforced with nftables, and a
name that does not resolve fails the whole set closed. It covers four things:

- the gateway and the tools enclave, which are the two hosts the harness dials;
- every enclave in `main.go`, because attestation is fetched from the host
  itself -- including a replica the gateway names in a 422 and the SDK re-seals
  to;
- what verifying those attestations needs: `github-proxy` for the pinned repo's
  release and Sigstore bundle, `tuf-repo-cdn.sigstore.dev` for the trust root,
  and the AMD and Intel collateral proxies;
- the controlplane, which is the one host here the harness does not attest. A
  detached run spills its sealed log there and reads it back; see above for
  what that host can and cannot see.

Adding a model to `main.go` means adding its host here too.

### Releasing

1. `Image` builds the container and prints the digest to push into
   `tinfoil-config.yml`.
2. Tagging `v*` runs `Release`, which measures the CVM this config describes and
   publishes `tinfoil.hash` with a Sigstore bundle. That release is what the
   Tinfoil SDKs verify a running instance against.

### Two things this is waiting on

- `go.mod` still replaces `tinfoil-go` with a sibling checkout, and the
  seal-following transport it needs is not in a tagged release. Until it is,
  the image builds only against a local `../tinfoil-go`:

      docker build --build-context sdk=../tinfoil-go -t confidential-tinfoil-harness .

  The `Image` workflow checks out `tinfoilsh/tinfoil-go@main` for the same
  reason and will fail until the transport lands there.

- `gateway.tinfoil.sh`, the compiled-in default and the name used in the
  allowlist, is NXDOMAIN today. The deployment fails closed until the gateway
  has a hostname; substitute the real one in both `env` and `allow`.
