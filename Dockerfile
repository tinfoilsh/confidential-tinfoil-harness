# syntax=docker/dockerfile:1

FROM golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd AS build
COPY go.mod go.sum *.go *.json /src/tinfoil-harness/
COPY internal/ /src/tinfoil-harness/internal/
WORKDIR /src/tinfoil-harness
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /confidential-tinfoil-harness .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a
COPY --from=build /confidential-tinfoil-harness /confidential-tinfoil-harness
EXPOSE 8081
ENTRYPOINT ["/confidential-tinfoil-harness"]
