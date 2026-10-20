# Confidential Tinfoil Harness

Tinfoil's chat harness is the enclave that runs conversations for [Tinfoil Chat](https://docs.tinfoil.sh/chat/overview). It builds prompts and runs tool calls and model turns. Cloud chats are stored encrypted through the [sync enclave](https://github.com/tinfoilsh/confidential-sync); a chat kept on a device is sent with each turn and returned when the run ends. The harness never logs or persists a client's key, and it keeps no chat state once a run ends.

## Development

Go 1.27.2 or later is required.

```sh
GOWORK=off go test -race -timeout 60s ./...
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

With a web client checkout and its `node_modules` in `TINFOIL_WEBAPP` (or `../tinfoil-webapp`), the tests also replay run events through its harness reducer and read attachments through its chat view.

Anonymous chat works only from a deployed harness, because Tinfoil validates `free_` keys only for registered Tinfoil hosts.

## Deployment

[tinfoil-config.yml](tinfoil-config.yml) is the measured configuration and the only place the harness reads its hosts from. It provides:

- `USAGE_CONTEXT_SECRET`: required secret for usage accounting
- `TINFOIL_API_KEY`: secret for the safeguards sidecar
- `TINFOIL_GATEWAY_URL`, `TINFOIL_CONTROLPLANE_URL` and `CLERK_ISSUER`: required
- `TINFOIL_SYNC_ENCLAVE` and `TINFOIL_ROUTER_ENCLAVE`: required, and must attest at boot
- The other `TINFOIL_*_ENCLAVE` hosts: used once their attestation succeeds
- `SAFEGUARDS_URL`: the [safeguards](https://github.com/tinfoilsh/confidential-safeguards) sidecar that classifies completed first-party chats

A chat model is offered only when Tinfoil's model catalog lists it, its repository is pinned in [main.go](main.go), and a replica serving it is available.

## Reporting Vulnerabilities

Please report security vulnerabilities by either:

- Emailing [security@tinfoil.sh](mailto:security@tinfoil.sh)
- Opening an issue on GitHub on this repository

We aim to respond to (legitimate) security reports within 24 hours.
