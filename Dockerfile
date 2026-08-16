# Multi-stage build for the pg-tide relay binary.
# Final image: Alpine-based, ~20 MB. Supports linux/amd64 and linux/arm64.

# ── Build stage ────────────────────────────────────────────────────────────
FROM --platform=$BUILDPLATFORM rust:1.91.1-alpine AS builder

ARG TARGETARCH
ARG CARGO_FEATURES=core

RUN apk add --no-cache bash musl-dev pkgconfig openssl-dev openssl-libs-static git

# Map Docker's TARGETARCH to the Rust musl target triple.
RUN case "$TARGETARCH" in \
      amd64) echo "x86_64-unknown-linux-musl"   > /rust_target ;; \
      arm64) echo "aarch64-unknown-linux-musl"  > /rust_target ;; \
      *)     echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac

RUN rustup target add "$(cat /rust_target)"

WORKDIR /src
COPY . .

LABEL org.opencontainers.image.pg-tide.features="$CARGO_FEATURES"

# On Alpine, gcc is the native musl gcc. cc-rs (used by ring's build.rs) defaults to
# looking for "<triple>-gcc" even on native musl targets. Also set AR for link-time tools.
RUN CC_x86_64_unknown_linux_musl=gcc \
    AR_x86_64_unknown_linux_musl=ar \
    CC_aarch64_unknown_linux_musl=gcc \
    AR_aarch64_unknown_linux_musl=ar \
    cargo build --package pg-tide-relay --release --target "$(cat /rust_target)" \
    --no-default-features --features "$CARGO_FEATURES" \
    && strip "target/$(cat /rust_target)/release/pg-tide" \
    && cp "target/$(cat /rust_target)/release/pg-tide" /pg-tide

# ── Runtime stage ──────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /pg-tide /usr/local/bin/pg-tide

# v0.19.0: Bake example TOML into the image so operators can
# `docker cp` a working starting config without consulting external docs.
RUN mkdir -p /etc/pg-tide
COPY pg-tide.example.toml /etc/pg-tide/pg-tide.example.toml

# Metrics + health endpoint.
EXPOSE 9090

# Non-root user for security.
RUN adduser -D -u 1000 pgtide
USER pgtide

ENTRYPOINT ["pg-tide"]
