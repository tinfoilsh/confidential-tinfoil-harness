# Confidential Tinfoil Harness

Tinfoil's chat harness is the enclave that runs conversations for [Tinfoil Chat](https://chat.tinfoil.sh). It owns prompt construction, tool calls, model turns, and encrypted persistence so clients only need to send user input and hold their own encryption key.

## How it works

For each chat turn the harness:

1. Authenticates the caller's Clerk session and exchanges it for an inference credential
2. Assembles the prompt from the stored thread, project documents, memory, and attachments
3. Streams the model turn as server-sent events, running tools (code execution, web search, document processing) as the model requests them
4. Encrypts the result with the client's content encryption key and stores it through the [sync enclave](https://github.com/tinfoilsh/confidential-sync)

The client keeps custody of its content encryption key and sends it inside the encrypted request body. The harness never logs or persists that key; completed runs discard it along with the plaintext transcript.

The API is `POST /v1/*`, with `GET /healthz` public. The full route table lives in [api.go](api.go) and [records.go](records.go).

## Development

Go 1.26.6 or later is required.

```sh
GOWORK=off go test -race -timeout 60s ./...
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

Tests have no live service dependency.

## Deployment

`tinfoil-config.yml` is the measured configuration. It requires `USAGE_CONTEXT_SECRET` (shared with the gateway for usage accounting), `HARNESS_IDENTITY_SECRET` (shared with the control plane), and `CLERK_ISSUER`. Downstream enclave hosts and gateway URLs are set with the `TINFOIL_*` variables shown there; sync and the model router are required at boot, other services are advertised once their attestation succeeds.

## Architecture Overview

- **[api.go](api.go)**, **[records.go](records.go)**: HTTP routes and request handling
- **[agent.go](agent.go)**, **[tools.go](tools.go)**: Model turn loop and tool execution
- **[thread.go](thread.go)**, **[sync.go](sync.go)**: Thread state and encrypted persistence through the sync enclave
- **[recovery.go](recovery.go)**, **[resume.go](resume.go)**: Run recovery and reconnection after disconnects
- **[export.go](export.go)**, **[import.go](import.go)**: Encrypted archive export and staged import
- **[verify.go](verify.go)**, **[pins.go](pins.go)**: Attestation of downstream enclaves

## Reporting Vulnerabilities

Please report security vulnerabilities by either:

- Emailing [security@tinfoil.sh](mailto:security@tinfoil.sh)
- Opening an issue on GitHub on this repository

We aim to respond to (legitimate) security reports within 24 hours.
