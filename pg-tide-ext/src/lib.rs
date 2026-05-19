//! pg_tide — Transactional Outbox, Idempotent Inbox & Relay Catalog
//!
//! Extracted from pg_trickle v0.46.0 into a standalone extension.
//! Works with any PostgreSQL 18+ database — pg_trickle is NOT required.
//!
//! Schema: `tide`
//! Catalog: `tide.tide_outbox_config`, `tide.tide_inbox_config`,
//!           `tide.relay_outbox_config`, `tide.relay_inbox_config`

use pgrx::prelude::*;

mod backfill;
mod error;
mod inbox;
mod outbox;
mod relay;
pub(crate) mod validation;

pgrx::pg_module_magic!();

// Declare the `tide` schema so pgrx's SQL generator knows to emit
// CREATE SCHEMA IF NOT EXISTS tide before defining extension functions.
#[pgrx::pg_schema]
pub mod tide {}

// Install the full catalog schema (tables, indexes, views, triggers, etc.)
// when `CREATE EXTENSION pg_tide` runs.  The base file is the bootstrap for
// every migration that follows — each migration loads in version order so that
// `CREATE EXTENSION pg_tide` (fresh install) and `ALTER EXTENSION pg_tide
// UPDATE` (upgrade chain) produce identical catalog schemas.
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.1.0.sql",
    name = "pg_tide_tables",
    bootstrap
);

// v0.17.0: Complete ordered migration chain so fresh installs pick up every
// schema change that has been added across upgrade scripts.  Each file uses
// CREATE … IF NOT EXISTS / CREATE OR REPLACE, making the chain idempotent on
// a fresh install while still serving as an upgrade script for existing ones.
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.1.0--0.2.0.sql",
    name = "pg_tide_m_0_2",
    requires = ["pg_tide_tables"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.2.0--0.3.0.sql",
    name = "pg_tide_m_0_3",
    requires = ["pg_tide_m_0_2"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.3.0--0.4.0.sql",
    name = "pg_tide_m_0_4",
    requires = ["pg_tide_m_0_3"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.4.0--0.5.0.sql",
    name = "pg_tide_m_0_5",
    requires = ["pg_tide_m_0_4"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.5.0--0.6.0.sql",
    name = "pg_tide_m_0_6",
    requires = ["pg_tide_m_0_5"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.6.0--0.7.0.sql",
    name = "pg_tide_m_0_7",
    requires = ["pg_tide_m_0_6"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.7.0--0.8.0.sql",
    name = "pg_tide_m_0_8",
    requires = ["pg_tide_m_0_7"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.8.0--0.9.0.sql",
    name = "pg_tide_m_0_9",
    requires = ["pg_tide_m_0_8"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.9.0--0.10.0.sql",
    name = "pg_tide_m_0_10",
    requires = ["pg_tide_m_0_9"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.10.0--0.11.0.sql",
    name = "pg_tide_m_0_11",
    requires = ["pg_tide_m_0_10"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.11.0--0.12.0.sql",
    name = "pg_tide_m_0_12",
    requires = ["pg_tide_m_0_11"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.12.0--0.13.0.sql",
    name = "pg_tide_m_0_13",
    requires = ["pg_tide_m_0_12"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.13.0--0.14.0.sql",
    name = "pg_tide_tables_0_14",
    requires = ["pg_tide_m_0_13"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.14.0--0.15.0.sql",
    name = "pg_tide_m_0_15",
    requires = ["pg_tide_tables_0_14"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.15.0--0.16.0.sql",
    name = "pg_tide_m_0_16",
    requires = ["pg_tide_m_0_15"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.16.0--0.17.0.sql",
    name = "pg_tide_m_0_17",
    requires = ["pg_tide_m_0_16"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.17.0--0.18.0.sql",
    name = "pg_tide_m_0_18",
    requires = ["pg_tide_m_0_17"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.18.0--0.19.0.sql",
    name = "pg_tide_m_0_19",
    requires = ["pg_tide_m_0_18"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.19.0--0.20.0.sql",
    name = "pg_tide_m_0_20",
    requires = ["pg_tide_m_0_19"]
);

/// Extension initialization — runs once when the extension is loaded.
#[pg_guard]
extern "C-unwind" fn _PG_init() {
    // pg_tide has no shared memory or background workers.
    // All state lives in catalog tables in the `tide` schema.
}

// Re-export all pg_extern functions so pgrx discovers them.
#[allow(unused_imports)]
use crate::backfill::*;
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
