#![no_main]

//! Fuzz the production pipeline JSON parser with bounded arbitrary input.
//! Invalid JSON and unsupported configurations are expected errors.

use libfuzzer_sys::fuzz_target;
use pg_tide_relay::config::schema_support::PipelineDocument;

fuzz_target!(|data: &[u8]| {
    // Parsing arbitrary bytes as JSON keeps this target focused on the real
    // parser without inventing a second config format or unbounded input.
    let Ok(raw) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    let _ = PipelineDocument::parse_runtime("fuzz", &raw);
});
