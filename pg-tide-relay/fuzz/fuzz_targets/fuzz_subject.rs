#![no_main]

//! Fuzz NATS subject validation and template expansion through production APIs.

use libfuzzer_sys::fuzz_target;
use pg_tide_relay::envelope::RelayMessage;
use pg_tide_relay::sink::nats::SubjectSpec;

fuzz_target!(|data: &[u8]| {
    let Ok(subject) = std::str::from_utf8(data) else {
        return;
    };
    let spec = SubjectSpec::from_config(Some(subject), None);
    let _ = spec.validate();
    let _ = spec.render(&RelayMessage::new_reverse(
        "fuzz-dedup",
        "event",
        serde_json::Value::Null,
    ));
});
