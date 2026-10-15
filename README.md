# Confidential Tinfoil Harness

Tinfoil's chat harness is the enclave that runs conversations for [Tinfoil Chat](https://chat.tinfoil.sh). It owns prompt construction, tool calls, model turns, and encrypted persistence so clients only need to send user input and hold their own encryption key.

## How it works

For each chat turn the harness:

1. Authenticates the caller's Clerk session and exchanges it for an inference credential
2. Assembles the prompt from the stored thread, project documents, memory, and attachments
3. Streams the model turn as server-sent events, running web search as the model requests it; uploaded documents are converted to text when they are attached
4. Encrypts the result with the client's content encryption key and stores it through the [sync enclave](https://github.com/tinfoilsh/confidential-sync)

The client keeps custody of its content encryption key and sends it inside the encrypted request body. The harness never logs or persists that key; completed runs discard it along with the plaintext transcript.

Signed-out clients obtain a `free_` key from the control plane's `GET /api/keys/chat` and send it as their bearer credential. The harness validates it with `/api/shim/validate-key` before starting work. Anonymous turns are temporary and are never persisted.

The API is `POST /v1/*`, with `GET /healthz` public. The full route table lives in [api.go](api.go) and [records.go](records.go).

## Development

Go 1.27.2 or later is required.

```sh
GOWORK=off go test -race -timeout 60s ./...
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

Tests have no live service dependency. When `../tinfoil-webapp/node_modules` is installed, they also check Go-produced rows and archives against the frozen web validators in [scripts/fixtures](scripts/fixtures). `node scripts/check-widgets.cjs ../tinfoil-webapp` checks the web widget renderers against [widgets.json](widgets.json).

## Deployment

`tinfoil-config.yml` is the measured configuration. It requires `USAGE_CONTEXT_SECRET` (shared with the gateway for usage accounting) and `CLERK_ISSUER`. Downstream enclave hosts and gateway URLs are set with the `TINFOIL_*` variables shown there; sync and the model router are required at boot, other services are advertised once their attestation succeeds. Chat models are advertised only when the control plane lists them, their repository is pinned in [main.go](main.go), and the gateway's catalog offers them; each request is sealed to a replica attested against that pin on first use. Deprecated models stay listed so older chats can show their names, but cannot be chosen.

Setting `SAFEGUARDS_URL` to a [safeguards](https://github.com/tinfoilsh/confidential-safeguards) sidecar in the same enclave submits completed first-party chats for acceptable-use classification, as the model router does for the chats it serves.

The control plane accepts `/api/shim/validate-key` calls only from registered Tinfoil hosts, so anonymous chat works only from a deployed harness.

## Architecture Overview

- **[api.go](api.go)**, **[records.go](records.go)**: HTTP routes and request handling
- **[auth.go](auth.go)**: Clerk sessions, anonymous keys, and inference credentials
- **[agent.go](agent.go)**, **[tools.go](tools.go)**: Model turn loop and tool execution
- **[thread.go](thread.go)**, **[sync.go](sync.go)**: Thread state and encrypted persistence through the sync enclave
- **[recovery.go](recovery.go)**, **[resume.go](resume.go)**: Run recovery and reconnection after disconnects
- **[export.go](export.go)**, **[import.go](import.go)**: Encrypted archive export and staged import
- **[verify.go](verify.go)**, **[attestation.go](attestation.go)**, **[pins.go](pins.go)**: Attestation of downstream enclaves

## Reporting Vulnerabilities

Please report security vulnerabilities by either:

- Emailing [security@tinfoil.sh](mailto:security@tinfoil.sh)
- Opening an issue on GitHub on this repository

We aim to respond to (legitimate) security reports within 24 hours.
