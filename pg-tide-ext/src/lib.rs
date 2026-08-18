//! pg_tide — Transactional Outbox, Idempotent Inbox & Relay Catalog
//!
//! Extracted from pg_trickle v0.46.0 into a standalone extension.
//! Works with PostgreSQL 18 — pg_trickle is NOT required.
//!
//! Schema: `tide`
//! Catalog: `tide.tide_outbox_config`, `tide.tide_inbox_config`,
//!           `tide.relay_outbox_config`, `tide.relay_inbox_config`

use pgrx::prelude::*;

mod backfill;
mod error;
mod fanin;
mod inbox;
mod lifecycle;
mod outbox;
mod relay;
mod template;
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
// NOTE: pg_tide--0.17.0--0.18.0.sql is intentionally excluded from this chain.
// It only redefines relay_enable, relay_disable, and relay_set_outbox_v2 using
// CREATE OR REPLACE FUNCTION (plpgsql).  Those same functions are also generated
// by pgrx from their #[pg_extern] Rust implementations using plain CREATE FUNCTION
// (not OR REPLACE).  Including the migration file causes SQLSTATE 42723
// ("already exists with same argument types") on fresh installs because the
// plpgsql version is created first, then pgrx's CREATE FUNCTION fails.
// The migration file is still used by ALTER EXTENSION pg_tide UPDATE for upgrades.
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.18.0--0.19.0.sql",
    name = "pg_tide_m_0_19",
    requires = ["pg_tide_m_0_17"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.19.0--0.20.0.sql",
    name = "pg_tide_m_0_20",
    requires = ["pg_tide_m_0_19"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.20.0--0.21.0.sql",
    name = "pg_tide_m_0_21",
    requires = ["pg_tide_m_0_20"]
);
// v0.23.0: Add missing entry for 0.21.0→0.22.0.  Without this, a fresh
// CREATE EXTENSION pg_tide at v0.22.0 was silently missing
// tide.ducklake_source_config, tide.ducklake_replicate(), and
// tide.ducklake_source_last_snapshot() even though ALTER EXTENSION UPDATE
// chains worked correctly.
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.21.0--0.22.0.sql",
    name = "pg_tide_m_0_22",
    requires = ["pg_tide_m_0_21"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.22.0--0.23.0.sql",
    name = "pg_tide_m_0_23",
    requires = ["pg_tide_m_0_22"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.23.0--0.24.0.sql",
    name = "pg_tide_m_0_24",
    requires = ["pg_tide_m_0_23"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.24.0--0.25.0.sql",
    name = "pg_tide_m_0_25",
    requires = ["pg_tide_m_0_24"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.25.0--0.26.0.sql",
    name = "pg_tide_m_0_26",
    requires = ["pg_tide_m_0_25"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.26.0--0.27.0.sql",
    name = "pg_tide_m_0_27",
    requires = ["pg_tide_m_0_26"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.27.0--0.28.0.sql",
    name = "pg_tide_m_0_28",
    requires = ["pg_tide_m_0_27"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.28.0--0.29.0.sql",
    name = "pg_tide_m_0_29",
    requires = ["pg_tide_m_0_28"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.29.0--0.30.0.sql",
    name = "pg_tide_m_0_30",
    requires = ["pg_tide_m_0_29"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.30.0--0.31.0.sql",
    name = "pg_tide_m_0_31",
    requires = ["pg_tide_m_0_30"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.31.0--0.32.0.sql",
    name = "pg_tide_m_0_32",
    requires = ["pg_tide_m_0_31"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.32.0--0.33.0.sql",
    name = "pg_tide_m_0_33",
    requires = ["pg_tide_m_0_32"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.33.0--0.34.0.sql",
    name = "pg_tide_m_0_34",
    requires = ["pg_tide_m_0_33"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.34.0--0.35.0.sql",
    name = "pg_tide_m_0_35",
    requires = ["pg_tide_m_0_34"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.35.0--0.36.0.sql",
    name = "pg_tide_m_0_36",
    requires = ["pg_tide_m_0_35"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.36.0--0.37.0.sql",
    name = "pg_tide_m_0_37",
    requires = ["pg_tide_m_0_36"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.37.0--0.38.0.sql",
    name = "pg_tide_m_0_38",
    requires = ["pg_tide_m_0_37"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.38.0--0.39.0.sql",
    name = "pg_tide_m_0_39",
    requires = ["pg_tide_m_0_38"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.39.0--0.40.0.sql",
    name = "pg_tide_m_0_40",
    requires = ["pg_tide_m_0_39"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.40.0--0.41.0.sql",
    name = "pg_tide_m_0_41",
    requires = ["pg_tide_m_0_40"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.41.0--0.42.0.sql",
    name = "pg_tide_m_0_42",
    requires = ["pg_tide_m_0_41"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.42.0--0.43.0.sql",
    name = "pg_tide_m_0_43",
    requires = ["pg_tide_m_0_42"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.43.0--0.44.0.sql",
    name = "pg_tide_m_0_44",
    requires = ["pg_tide_m_0_43"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.44.0--0.45.0.sql",
    name = "pg_tide_m_0_45",
    requires = ["pg_tide_m_0_44"]
);
pgrx::extension_sql_file!(
    "../../sql/pg_tide--0.45.0--0.46.0.sql",
    name = "pg_tide_m_0_46",
    requires = ["pg_tide_m_0_45"]
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
use crate::fanin::*;
#[allow(unused_imports)]
use crate::inbox::*;
#[allow(unused_imports)]
use crate::lifecycle::*;
#[allow(unused_imports)]
use crate::outbox::*;
#[allow(unused_imports)]
use crate::relay::*;
#[allow(unused_imports)]
use crate::template::*;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
