# Confidential Tinfoil Harness

Tinfoil's chat harness is the enclave that runs conversations for [Tinfoil Chat](https://chat.tinfoil.sh). It owns prompt construction, tool calls, model turns, and encrypted persistence through the [sync enclave](https://github.com/tinfoilsh/confidential-sync), so clients only need to send user input and hold their own encryption key. The harness never logs or persists that key and discards it with the plaintext transcript when a run completes. Anonymous turns are never persisted.

## Development

Go 1.27.2 or later is required.

```sh
GOWORK=off go test -race -timeout 60s ./...
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

With `../tinfoil-webapp/node_modules` installed, the tests also check Go-produced rows and archives against the web validators in [scripts/fixtures](scripts/fixtures). `node scripts/check-widgets.cjs ../tinfoil-webapp` checks the web widget renderers against [widgets.json](widgets.json).

Anonymous chat works only from a deployed harness, because the control plane accepts `free_` key validation only from registered Tinfoil hosts.

## Deployment

[tinfoil-config.yml](tinfoil-config.yml) is the measured configuration. It provides:

- `USAGE_CONTEXT_SECRET`: required secret, shared with the gateway for usage accounting
- `TINFOIL_API_KEY`: secret for the safeguards sidecar
- `CLERK_ISSUER`: issuer for signed-in Clerk sessions
- `TINFOIL_SYNC_ENCLAVE` and `TINFOIL_ROUTER_ENCLAVE`: must attest at boot
- The other `TINFOIL_*_ENCLAVE` hosts: used once their attestation succeeds
- `SAFEGUARDS_URL`: the [safeguards](https://github.com/tinfoilsh/confidential-safeguards) sidecar that classifies completed first-party chats

A chat model is offered only when the control plane lists it, its repository is pinned in [main.go](main.go), and the gateway's catalog offers it.

## Reporting Vulnerabilities

Please report security vulnerabilities by either:

- Emailing [security@tinfoil.sh](mailto:security@tinfoil.sh)
- Opening an issue on GitHub on this repository

We aim to respond to (legitimate) security reports within 24 hours.
