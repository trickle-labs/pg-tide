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
mod outbox;
mod inbox;
mod relay;

pgrx::pg_module_magic!();

/// Extension initialization — runs once when the extension is loaded.
#[pg_guard]
extern "C-unwind" fn _PG_init() {
    // pg_tide has no shared memory or background workers.
    // All state lives in catalog tables in the `tide` schema.
}

// Re-export all pg_extern functions so pgrx discovers them.
use crate::outbox::*;
use crate::inbox::*;
use crate::relay::*;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
