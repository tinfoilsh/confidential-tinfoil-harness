# Confidential Tinfoil Harness

Tinfoil's chat harness is the enclave that runs conversations for [Tinfoil Chat](https://docs.tinfoil.sh/chat/overview). It builds prompts and runs tool calls and model turns. Cloud chats are sealed under the account's content key and stored through the control plane and the buckets sidecar beside the harness; a chat kept on a device is sent with each turn and returned when the run ends. The harness never logs or persists a client's key, and it keeps no chat state once a run ends, apart from what the chat list shows of each chat (its title, counts and times), which it holds in memory for up to an hour so a listing reopens only the chats that changed, a stored chat's text and the key to its owner's search index, which it holds for up to a minute after the write until the index takes the chat, and the events of a device chat's run, which it holds for up to ten minutes after the run ends, until the client collects them or deletes the chat, so a client that lost its stream can collect the chat the run returns.

## Development

Go 1.27.2 or later is required.

```sh
GOWORK=off go test -race -timeout 60s ./...
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

[protocol/openapi.yaml](protocol/openapi.yaml) describes the API clients use, and the tests hold the harness to it. After a change to what runs send, record the runs in [protocol/fixtures](protocol/fixtures), which clients replay, with `RECORD_FIXTURES=1 GOWORK=off go test ./...`.

Anonymous chat works only from a deployed harness, because Tinfoil validates `free_` keys only for registered Tinfoil hosts.

## Deployment

[tinfoil-config.yml](tinfoil-config.yml) is the measured configuration and the only place the harness reads its hosts from. It provides:

- `USAGE_CONTEXT_SECRET`: required secret for usage accounting
- `TINFOIL_API_KEY`: secret for the safeguards sidecar, and for embedding chats and search queries and writing chat titles, reasoning labels and compaction summaries through the inference service
- `TINFOIL_CONTROLPLANE_URL` and `CLERK_ISSUER`: required
- `TINFOIL_ROUTER_ENCLAVE`: required, must attest at boot, and serves every chat model turn
- `TINFOIL_TOOLS_ENCLAVE`: the web search enclave, used once its attestation succeeds
- `SYNC_ENCLAVE_SECRET`, `BUCKETS_URL`, `CHAT_ATTACHMENTS_BUCKET`, `SEARCH_BUCKETS_URL`, `SEARCH_INDEXES_BUCKET` and `EMBEDDING_MODEL`: required; the control plane's service secret for its storage API, the buckets sidecar that holds attachments and search indexes, and the model that embeds chats for search
- `SUMMARY_MODEL`: required; the model that writes chat titles and reasoning labels
- `LONG_SUMMARY_MODEL`: required; the model that summarizes a chat's older messages when its prompt outgrows the chat model's window
- `CONTEXT_DEV_API_KEY`: required secret for reading link previews through [context.dev](https://docs.context.dev), which needs zero data retention enabled for its organization; site favicons come from DuckDuckGo by host
- `SAFEGUARDS_URL`: the [safeguards](https://github.com/tinfoilsh/confidential-safeguards) sidecar that classifies completed first-party chats

A chat model is offered only when Tinfoil's model catalog lists it, its repository is pinned in [internal/inference/inference.go](internal/inference/inference.go), and a replica serving it is available.

## Reporting Vulnerabilities

Please report security vulnerabilities by either:

- Emailing [security@tinfoil.sh](mailto:security@tinfoil.sh)
- Opening an issue on GitHub on this repository

We aim to respond to (legitimate) security reports within 24 hours.
