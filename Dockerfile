# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_TIME=unknown

ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go test ./...
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -mod=readonly -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${REVISION} -X main.buildTime=${BUILD_TIME}" \
    -o /out/study-list-api ./cmd/study-list-api

FROM gcr.io/distroless/static-debian12:nonroot AS runtime

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.title="study-list-api" \
      org.opencontainers.image.description="Study List Go API" \
      org.opencontainers.image.source="https://github.com/Luminet2023/hifumi-backend" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/study-list-api /app/study-list-api

USER 65532:65532
EXPOSE 8080
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/app/study-list-api", "healthcheck"]

ENTRYPOINT ["/app/study-list-api"]
CMD ["serve"]
