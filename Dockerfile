# syntax=docker/dockerfile:1

FROM golang:1.27.4-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS build
COPY go.mod go.sum *.go /src/tinfoil-harness/
COPY internal/ /src/tinfoil-harness/internal/
WORKDIR /src/tinfoil-harness
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /confidential-tinfoil-harness .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a
COPY --from=build /confidential-tinfoil-harness /confidential-tinfoil-harness
EXPOSE 8081
ENTRYPOINT ["/confidential-tinfoil-harness"]
