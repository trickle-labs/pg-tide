#![no_main]

//! Fuzz target for all wire format decoders.
//!
//! Exercises `WireFormat::decode()` for every supported format with arbitrary
//! bytes in the message payload.  The decoder must not panic or produce UB
//! regardless of input — it should return an `Err(WireError)` or `Ok(None)`
//! for malformed input.
//!
//! Supported formats exercised:
//!   - native (pg_tide native JSON)
//!   - debezium-json
//!   - maxwell
//!   - canal
//!   - cloudevents
//!   - cdc-json
//!   - claim-check

use libfuzzer_sys::fuzz_target;
use pg_tide_relay::wire_format::{from_config, RawMessage};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Use the first byte to select the wire format under test.
    let format_selector = data[0] % 7;
    let payload = &data[1..];

    let format_name = match format_selector {
        0 => "native",
        1 => "debezium-json",
        2 => "maxwell",
        3 => "canal",
        4 => "cloudevents",
        5 => "cdc-json",
        _ => "claim-check",
    };

    let config = serde_json::json!({ "wire_format": format_name });
    let decoder = from_config(&config);

    let raw = RawMessage::new("fuzz-topic", None, Some(payload.to_vec()));

    // Decoder must never panic — error results are acceptable.
    let _ = decoder.decode(&raw);
});
