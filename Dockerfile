# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM golang:1.24-alpine AS build

WORKDIR /src

# Resolve dependencies first so editing Go sources doesn't invalidate the
# download layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

# CGO_ENABLED=0 gives fully static binaries: cachekv's tree (badger, ristretto,
# klauspost/compress) is pure Go, so the runtime stage needs no libc at all.
# -mod=mod ignores any vendor/ directory that slipped into the build context.
# ./cmd/... also builds cachekv-cli, which the container uses to health-check
# itself over gRPC without shipping a separate probe binary.
ARG TARGETOS=linux
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -mod=mod -trimpath -ldflags='-s -w' -o /out/ ./cmd/...

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM alpine:3.22

# tzdata is not needed; ca-certificates is only pulled in because badger's
# temp-dir probe and future TLS work both benefit, and it costs ~700kB.
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 cachekv \
    && adduser -S -u 10001 -G cachekv -H -h /data cachekv \
    && mkdir -p /data \
    && chown cachekv:cachekv /data \
    && chmod 0750 /data

COPY --from=build /out/cachekv-server /out/cachekv-cli /usr/local/bin/

# /data is the only writable path the server needs. Both the databases
# (/data/store) and the keyring that decrypts them (/data/keys) live under it,
# so a single volume mounted at /data keeps them together — secure databases are
# unrecoverable without their keyring. Startup() creates both subdirectories.
ENV CACHEKV_ADDR=127.0.0.1:50051

USER cachekv:cachekv
WORKDIR /data

EXPOSE 50051

# The server installs its own SIGTERM handler, so it is safe as PID 1.
STOPSIGNAL SIGTERM

# Flags live in CMD so `docker run <image> --listen :6000` overrides them.
ENTRYPOINT ["/usr/local/bin/cachekv-server"]
CMD ["--listen", ":50051", "--store-path", "/data/store", "--key-path", "/data/keys"]
