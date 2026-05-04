//! pg_tide — Transactional Outbox, Idempotent Inbox & Relay Catalog
//!
//! Extracted from pg_trickle v0.46.0 into a standalone extension.
//! Works with any PostgreSQL 18+ database — pg_trickle is NOT required.
//!
//! Schema: `tide`
//! Catalog: `tide.tide_outbox_config`, `tide.tide_inbox_config`,
//!           `tide.relay_outbox_config`, `tide.relay_inbox_config`

use pgrx::prelude::*;

mod error;
mod inbox;
mod outbox;
mod relay;

pgrx::pg_module_magic!();

// Install the full catalog schema (tables, indexes, views, triggers, etc.)
// when `CREATE EXTENSION pg_tide` runs. The plpgsql utility functions
// (grant_publish, inbox_truncate_processed, etc.) also come from this file.
// outbox_truncate_delivered is implemented as a #[pg_extern] above and is
// therefore NOT in the SQL file.
pgrx::extension_sql_file!(
    "../sql/pg_tide--0.1.0.sql",
    name = "pg_tide_tables",
    bootstrap
);

/// Extension initialization — runs once when the extension is loaded.
#[pg_guard]
extern "C-unwind" fn _PG_init() {
    // pg_tide has no shared memory or background workers.
    // All state lives in catalog tables in the `tide` schema.
}

// Re-export all pg_extern functions so pgrx discovers them.
#[allow(unused_imports)]
use crate::inbox::*;
#[allow(unused_imports)]
use crate::outbox::*;
#[allow(unused_imports)]
use crate::relay::*;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
