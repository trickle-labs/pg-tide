/// Subject/topic template helper retained for supported outbound sinks.
///
/// Re-exports `render_subject` from envelope for convenience.
pub use crate::envelope::render_subject;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_subject_stream_table() {
        let result = render_subject(
            "pgtrickle.{stream_table}.events",
            "orders",
            "insert",
            1,
            None,
        );
        assert_eq!(result, "pgtrickle.orders.events");
    }

    #[test]
    fn test_render_subject_op() {
        let result = render_subject("{stream_table}.{op}", "products", "delete", 5, None);
        assert_eq!(result, "products.delete");
    }

    #[test]
    fn test_render_subject_outbox_id() {
        let result = render_subject("events-{outbox_id}", "orders", "insert", 42, None);
        assert_eq!(result, "events-42");
    }
}
