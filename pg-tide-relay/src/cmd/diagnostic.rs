use super::output::Diagnostic;

pub fn from_error(component: &str, _error: impl std::fmt::Display) -> Diagnostic {
    Diagnostic {
        code: "operator.failure".into(),
        component: component.into(),
        message: "The requested operation could not be completed.".into(),
        likely_cause: "The relay dependency or catalog state is unavailable.".into(),
        next_action: "Run `pg-tide doctor` and inspect the relay logs.".into(),
    }
}
