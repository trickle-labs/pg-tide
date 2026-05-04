# Multi-stage build for the pg-tide relay binary.
# Final image: Alpine-based, ~20 MB. Supports linux/amd64 and linux/arm64.

# ── Build stage ────────────────────────────────────────────────────────────
FROM --platform=$BUILDPLATFORM rust:1.87-alpine AS builder

ARG TARGETARCH

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

# Map Docker's TARGETARCH to the Rust musl target triple.
RUN case "$TARGETARCH" in \
      amd64) echo "x86_64-unknown-linux-musl"   > /rust_target ;; \
      arm64) echo "aarch64-unknown-linux-musl"  > /rust_target ;; \
      *)     echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac

RUN rustup target add "$(cat /rust_target)"

WORKDIR /src
COPY . .

RUN cargo build --package pg-tide-relay --release --target "$(cat /rust_target)" \
    && strip "target/$(cat /rust_target)/release/pg-tide" \
    && cp "target/$(cat /rust_target)/release/pg-tide" /pg-tide

# ── Runtime stage ──────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /pg-tide /usr/local/bin/pg-tide

# Metrics + health endpoint.
EXPOSE 9090

# Non-root user for security.
RUN adduser -D -u 1000 pgtide
USER pgtide

ENTRYPOINT ["pg-tide"]
