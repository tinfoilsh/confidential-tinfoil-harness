# syntax=docker/dockerfile:1

FROM golang:1.27.2-alpine3.23@sha256:9e45f0eb4a63ed37ad6e604950407558b7c738e34e015ae77a5d4ad369cde079 AS build
COPY go.mod go.sum *.go *.json /src/tinfoil-harness/
WORKDIR /src/tinfoil-harness
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /confidential-tinfoil-harness .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a
COPY --from=build /confidential-tinfoil-harness /confidential-tinfoil-harness
EXPOSE 8081
ENTRYPOINT ["/confidential-tinfoil-harness"]
