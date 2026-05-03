# Installation

pg_tide has two components to install: the **PostgreSQL extension** (SQL functions and catalog tables) and the **relay binary** (the `pg-tide` process that bridges messages to external systems).

---

## Prerequisites

- PostgreSQL 18 or later
- Superuser or `CREATE EXTENSION` privileges on your target database

---

## Installing the Extension

### From Source (pgrx)

```bash
# Install cargo-pgrx if you haven't already
cargo install cargo-pgrx --version "=0.18.0" --locked
cargo pgrx init --pg18 $(which pg_config)

# Build and install
cd pg-tide-ext
cargo pgrx install --release
```

### Enable the Extension

```sql
CREATE EXTENSION pg_tide;
```

This creates the `tide` schema with all catalog tables, views, and functions.

---

## Installing the Relay Binary

### From GitHub Releases

Download the latest release for your platform from the
[releases page](https://github.com/trickle-labs/pg-tide/releases):

```bash
# Linux (amd64)
curl -LO https://github.com/trickle-labs/pg-tide/releases/latest/download/pg-tide-x86_64-unknown-linux-gnu.tar.gz
tar xzf pg-tide-x86_64-unknown-linux-gnu.tar.gz
sudo mv pg-tide /usr/local/bin/

# macOS (Apple Silicon)
curl -LO https://github.com/trickle-labs/pg-tide/releases/latest/download/pg-tide-aarch64-apple-darwin.tar.gz
tar xzf pg-tide-aarch64-apple-darwin.tar.gz
sudo mv pg-tide /usr/local/bin/
```

### From Source (Cargo)

```bash
cargo install --git https://github.com/trickle-labs/pg-tide pg-tide-relay
```

### Docker

```bash
docker pull ghcr.io/trickle-labs/pg-tide:latest
```

---

## Verify Installation

```bash
# Check relay version
pg-tide --version

# Check extension is installed
psql -c "SELECT * FROM pg_extension WHERE extname = 'pg_tide';"
```

---

## Next Steps

- [Quickstart →](quickstart.md) — publish your first message in 5 minutes
- [Tutorial →](tutorial.md) — set up a complete outbox-to-sink pipeline
