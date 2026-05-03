# Multi-stage build for the pg-tide relay binary.
# Final image: Alpine-based, ~20 MB.

# ── Build stage ────────────────────────────────────────────────────────────
FROM rust:1.87-alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

WORKDIR /src
COPY . .

RUN cargo build --package pg-tide-relay --release --target x86_64-unknown-linux-musl \
    && strip target/x86_64-unknown-linux-musl/release/pg-tide

# ── Runtime stage ──────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/pg-tide /usr/local/bin/pg-tide

# Metrics + health endpoint.
EXPOSE 9090

# Non-root user for security.
RUN adduser -D -u 1000 pgtide
USER pgtide

ENTRYPOINT ["pg-tide"]
