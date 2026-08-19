FROM postgres:18.0-bookworm

ARG RUST_VERSION=1.97.1
ARG CARGO_PGRX_VERSION=0.18.0

ENV RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    PATH=/opt/cargo/bin:/opt/rustup/toolchains/${RUST_VERSION}-x86_64-unknown-linux-gnu/bin:${PATH}

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        build-essential ca-certificates clang curl git libssl-dev llvm-dev pkg-config python3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash pgtide \
    && mkdir -p /opt/rustup /opt/cargo \
    && chown -R pgtide:pgtide /opt/rustup /opt/cargo

RUN curl --proto '=https' --tlsv1.2 --fail --silent --show-error https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain ${RUST_VERSION} \
    && cargo install cargo-pgrx --version "=${CARGO_PGRX_VERSION}" --locked

RUN apt-get update \
    && apt-get install --no-install-recommends --yes postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*

RUN chown -R pgtide:pgtide /opt/rustup /opt/cargo

RUN chown -R pgtide:pgtide \
    /var/lib/postgresql \
    /var/run/postgresql \
    /usr/share/postgresql/18/extension \
    /usr/lib/postgresql/18/lib

ENV RUSTUP_TOOLCHAIN=${RUST_VERSION} \
    RUSTUP_OFFLINE=1 \
    RUST_TEST_THREADS=1 \
    USER=pgtide

USER pgtide

RUN cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config

WORKDIR /workspace
COPY --chown=pgtide:pgtide . /workspace
COPY --chown=pgtide:pgtide tests/extension-cleanroom/entrypoint.sh /usr/local/bin/extension-cleanroom

RUN chmod 0755 /usr/local/bin/extension-cleanroom

ENTRYPOINT ["/usr/local/bin/extension-cleanroom"]