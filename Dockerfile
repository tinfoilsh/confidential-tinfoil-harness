# syntax=docker/dockerfile:1

# The image the measured config pins by digest. Static, non-root, no shell: the
# harness holds no state and no credentials, so nothing else belongs in here.
#
# go.mod still replaces tinfoil-go with the sibling checkout, so the SDK arrives
# as a named build context rather than from the module proxy:
#
#     docker build --build-context sdk=../tinfoil-go -t confidential-tinfoil-harness .
#
# When the seal-following release lands, drop the replace, the COPY below, and
# the --build-context flag; nothing else changes.

FROM golang:1.26-alpine@sha256:28d89ee9cc0ff9fec75c82ca201e6bf7fdf9a679d4b7b24dfa04f2bb766bb468 AS build
COPY --from=sdk / /src/tinfoil-go/
COPY . /src/tinfoil-harness/
WORKDIR /src/tinfoil-harness
# Static and path-free, so the binary depends on nothing in the builder.
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /confidential-tinfoil-harness .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a
COPY --from=build /confidential-tinfoil-harness /confidential-tinfoil-harness
# Matches shim.upstream-port in tinfoil-config.yml.
EXPOSE 8081
ENTRYPOINT ["/confidential-tinfoil-harness"]
