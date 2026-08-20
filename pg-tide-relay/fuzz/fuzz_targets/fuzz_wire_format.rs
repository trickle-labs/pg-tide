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
//!   - cloudevents

use libfuzzer_sys::fuzz_target;
use pg_tide_relay::wire_format::{from_config, RawMessage};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Use the first byte to select the wire format under test.
    let format_selector = data[0] % 2;
    let payload = &data[1..];

    let format_name = match format_selector {
        0 => "native",
        _ => "cloudevents",
    };

    let config = serde_json::json!({ "wire_format": format_name });
    let Ok(decoder) = from_config(&config) else {
        return;
    };

    let raw = RawMessage::new("fuzz-topic", None, Some(payload.to_vec()));

    // Decoder must never panic — error results are acceptable.
    let _ = decoder.decode(&raw);
});
