# Deployment audit

Checked 2026-09-14T21:59:27.814926+00:00. Target: `chat-api.tinfoil.sh`, container `chat-harness`, release `v0.1.12`.

The deployed release is stopped at firewall setup. The controlplane reports the VM as `started`, but the workload container and HTTPS shim are still pending. Its recorded error is:

```text
populating egress allowlists: network "upstream": resolving code-execution.tinfoil.sh: lookup code-execution.tinfoil.sh on 1.1.1.1:53: no such host
```

The working tree now disables code execution and uses `egress: open`. The configuration passes the Tinfoil config validator. The startup fixes below are also implemented locally. These changes have not been released or deployed.

## Fixed locally

| Finding | Fix and validation |
| --- | --- |
| Model catalog response decoding failed | `refreshCatalog` now decodes the JSON array returned by `/api/config/models`. Regression tests cover the response contract and retention of the last valid catalog after an invalid refresh. |
| Active models were missing from the pinned catalog | Added explicit repository pins for `deepseek-v4-1-flash`, `glm-5-3`, and `glm-5-3-flash`. Repository mismatches still fail validation. Models without verified gateway replicas are omitted. |
| Startup could wait indefinitely for attestation | Each attempt has a 20-second deadline, each replica pool has a 45-second budget, and SDK metadata HTTP requests time out after 15 seconds. Timed-out SDK work remains counted against the eight-call concurrency limit until it finishes. Tests cover fallback, cancellation, late results, and unavailable pools. |
| Anonymous sessions depended on a new identity secret and controlplane change | The browser now calls the existing key issuance endpoint directly and presents the issued key to the harness. The forwarding protocol and controlplane patch were removed. Tests cover key validation, quota refusal, refresh, account changes, and reconnect ownership. |

A live check of the updated model attestation and catalog loading completed in 45.926 seconds, with `glm-5-3` and `gemma4-31b` available. Other model pools failed DNS, timed out, or were absent and did not block the catalog. This was a read-only check from the workspace, with no inference requests or running application server. Harness race tests and `go vet` pass.

## Remaining findings

| Finding | Evidence | Effect |
| --- | --- | --- |
| Key validation needs confirmation from the deployed harness | Anonymous requests now use the existing `/api/shim/validate-key` endpoint, which restricts callers to registered Tinfoil host IPs or configured `HOST_IPS`. Local tests cover its response contract. | Verify the harness's egress IP is accepted during rollout. Failed validation refuses work. No new secret or controlplane code is needed. |
| New model pools are absent | The gateway does not advertise pools for `deepseek-v4-1-flash` or `glm-5-3-flash`. | The harness now pins these repositories but cannot offer these models until the gateway supplies a verified replica. |
| Configured document endpoint does not resolve | `docling.tinfoil.sh` returns NXDOMAIN from the VM's resolver, `1.1.1.1`. | Open egress prevents this from blocking VM startup, but document conversion remains unavailable. |
| Kimi has no resolving replica in the gateway catalog | Both advertised Kimi hosts return NXDOMAIN. | Startup cannot attest Kimi; it will be omitted from the available models. |
| Several replica attestation requests time out | DeepSeek v4, all three resolving GPT-OSS replicas, Llama, and Voxtral failed the bounded probes below. | Affects model availability and audio transcription. The updated harness bounds attestation and skips unavailable pools. These timeouts were observed from this workspace, not inside the VM. |
| Gateway contains stale replica DNS entries | GPT-OSS inf6-0 and Gemma inf10-6 also return NXDOMAIN. | GPT-OSS must fall back to other replicas; routing or attestation to the stale Gemma replica can fail. Open egress removes the old firewall startup dependency on these names. |

The original model decoding failure, pinned-model rejection, and missing-secret HTTP 503 were reproduced before the fixes using an isolated test overlay and snapshots of the live configuration. The model failures are covered by passing regression tests, and anonymous issuance no longer uses the missing secret. See [anonymous chat setup](README.md#anonymous-chat-setup) for rollout checks.

## DNS failures

All names below returned NXDOMAIN from `1.1.1.1`. The old firewall treated an unresolved allowlist hostname as a boot failure. Open egress removes that behavior.

| Host | Relevance after open egress |
| --- | --- |
| `code-execution.tinfoil.sh` | Code execution is disabled in the working tree. Still referenced by deployed v0.1.12. |
| `docling.tinfoil.sh` | Configured document conversion service. |
| `kimi-k3-inf14-2.tinfoil.containers.tinfoil.dev` | Advertised Kimi replica. |
| `kimi-k3-inf17.tinfoil.containers.tinfoil.dev` | Advertised Kimi replica. |
| `kimi-k3-inf18.tinfoil.containers.tinfoil.dev` | Retired allowlist entry; absent from the current gateway catalog. |
| `gpt-oss-120b-inf6-0.tinfoil.containers.tinfoil.dev` | Advertised GPT-OSS replica. |
| `gemma4-31b-inf10-6.tinfoil.containers.tinfoil.dev` | Advertised Gemma replica, outside the old allowlist. |

## Attestation results

These read-only probes use the exact `tinfoil-go` version pinned by the harness, with a 15-second HTTP timeout and a 30-second process limit. They perform startup attestation and do not send inference requests or user data. A timeout establishes failure within that probe window, not permanent service failure.

| Dependency | Host | Result |
| --- | --- | --- |
| audio | `voxtral-small-24b-inf12.tinfoil.containers.tinfoil.dev` | Attestation endpoint timed out |
| auto router | `inference.tinfoil.sh` | Verified |
| deepseek | `deepseek-v4-flash-inf15.tinfoil.containers.tinfoil.dev` | Attestation endpoint timed out |
| gemma | `gemma4-31b-inf6-0.tinfoil.containers.tinfoil.dev` | Verified |
| gpt-oss | `gpt-oss-120b-inf12-0.tinfoil.containers.tinfoil.dev` | Attestation endpoint timed out |
| gpt-oss fallback | `gpt-oss-120b-inf12-1.tinfoil.containers.tinfoil.dev` | Attestation endpoint timed out |
| gpt-oss fallback | `gpt-oss-120b-inf12-2.tinfoil.containers.tinfoil.dev` | Attestation endpoint timed out |
| llama | `llama3-3-70b-inf12.tinfoil.containers.tinfoil.dev` | Attestation endpoint timed out |
| metadata | `opengraph-metadata.tinfoil.sh` | Verified |
| summarizer | `summarizer.tinfoil.sh` | Verified |
| sync | `sync.tinfoil.sh` | Verified |
| web search | `websearch.tinfoil.sh` | Verified |

Kimi and document attestation were not attempted because their configured hosts do not resolve. Gemma attestation was tested against the first replica used at startup; the audit did not attest every Gemma replica.

## Checks that passed and limits

The gateway catalog, controlplane model/system/memory configuration endpoints, and Clerk JWKS endpoint returned HTTP 200. `USAGE_CONTEXT_SECRET` exists and is attached to the container. Sync, Auto routing, web search, summarization, metadata, and the first Gemma replica passed attestation.

Code execution and project memory extraction are intentionally disabled. Sandbox tools are not implemented by the current harness integration. These are feature limitations, not new deployment failures.

This audit covers deployment configuration, DNS, public startup contracts, secret presence, and startup attestation. It does not establish that authenticated chat, storage writes, uploads, archive conversion, billing, anonymous key validation, or browser CORS work end to end. Those checks require a running new deployment and a test account.

Public configuration snapshots, DNS results, the isolated Go probe, and full attestation errors are saved under `/tmp/tinfoil-deployment-audit/`. That directory contains no secret values.
