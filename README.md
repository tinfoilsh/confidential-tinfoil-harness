# confidential-tinfoil-harness

Minimal streaming agent endpoint in front of Tinfoil's inference gateway used for confidential tool loops.

    POST /v1/chat/completions      OpenAI-shaped, streaming only
    GET  /healthz

## Deployment

The harness runs as the single container of a Tinfoil CVM. `tinfoil-config.yml`
is the measured description of that CVM: a client attesting this enclave is
told exactly this file, so the container is pinned by digest and every host the
harness may reach is enumerated.

    tinctl inspect tinfoilsh/confidential-tinfoil-harness   # resolve the config
    tinctl deploy  tinfoilsh/confidential-tinfoil-harness   # launch it

Ingress is the shim alone: TLS on 443, forwarded to the harness on 8081, with
`/v1/chat/completions` and `/healthz` the only paths that reach it. The shim
validates the caller's Tinfoil key and passes it through, which is the same key
the harness forwards upstream -- it has none of its own.

### Egress

`networks.upstream.allow` is resolved to IPs and enforced with nftables, and a
name that does not resolve fails the whole set closed. It covers three things:

- the gateway and the tools enclave, which are the two hosts the harness dials;
- every enclave in `enclave.go`, because attestation is fetched from the host
  itself -- including a replica the gateway names in a 422 and the SDK re-seals
  to;
- what verifying those attestations needs: `github-proxy` for the pinned repo's
  release and Sigstore bundle, `tuf-repo-cdn.sigstore.dev` for the trust root,
  and the AMD and Intel collateral proxies.

Adding a model to `enclave.go` means adding its host here too.

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
