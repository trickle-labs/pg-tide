//! Pure executable model for the relay delivery state machine.
#![allow(dead_code)]

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryState {
    Polled,
    Encoded,
    PublishStarted,
    SinkAccepted,
    SinkAcknowledged,
    DryRunObserved,
    IntentionallyFiltered,
    PublishFailed,
    DlqPersisted,
    CheckpointCommitted,
    CleanupEligible,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryEvent {
    Encoded,
    PublishStarted,
    SinkAccepted,
    SinkAcknowledged { frontier: u64 },
    DryRunObserved,
    IntentionallyFiltered,
    PublishFailed { transient: bool },
    DlqPersisted,
    CheckpointCommitted,
    CleanupAllowed,
    NextBatch { checkpoint: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryFacts {
    pub state: DeliveryState,
    pub sink_acknowledged_frontier: Option<u64>,
    pub source_checkpoint: Option<u64>,
    pub source_visible: bool,
    pub duplicate_possible: bool,
    pub cleanup_allowed: bool,
    pub failure_transient: bool,
}

impl DeliveryFacts {
    pub fn polled(checkpoint: u64) -> Self {
        Self {
            state: DeliveryState::Polled,
            sink_acknowledged_frontier: None,
            source_checkpoint: Some(checkpoint),
            source_visible: true,
            duplicate_possible: false,
            cleanup_allowed: false,
            failure_transient: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransitionError {
    pub state: DeliveryState,
    pub event: DeliveryEvent,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "event {:?} is invalid from state {:?}",
            self.event, self.state
        )
    }
}

impl std::error::Error for TransitionError {}

pub fn transition(
    mut facts: DeliveryFacts,
    event: DeliveryEvent,
) -> Result<DeliveryFacts, TransitionError> {
    let state = facts.state;
    let valid = match (state, event) {
        (DeliveryState::Polled, DeliveryEvent::Encoded) => {
            facts.state = DeliveryState::Encoded;
            true
        }
        (DeliveryState::Encoded, DeliveryEvent::PublishStarted) => {
            facts.state = DeliveryState::PublishStarted;
            true
        }
        (DeliveryState::PublishStarted, DeliveryEvent::SinkAccepted) => {
            facts.state = DeliveryState::SinkAccepted;
            facts.duplicate_possible = true;
            true
        }
        (DeliveryState::PublishStarted, DeliveryEvent::SinkAcknowledged { frontier })
        | (DeliveryState::SinkAccepted, DeliveryEvent::SinkAcknowledged { frontier }) => {
            if facts.source_checkpoint == Some(frontier) {
                facts.state = DeliveryState::SinkAcknowledged;
                facts.sink_acknowledged_frontier = Some(frontier);
                true
            } else {
                false
            }
        }
        (DeliveryState::Encoded, DeliveryEvent::DryRunObserved) => {
            facts.state = DeliveryState::DryRunObserved;
            true
        }
        (DeliveryState::Encoded, DeliveryEvent::IntentionallyFiltered) => {
            facts.state = DeliveryState::IntentionallyFiltered;
            true
        }
        (DeliveryState::PublishStarted, DeliveryEvent::PublishFailed { transient })
        | (DeliveryState::SinkAccepted, DeliveryEvent::PublishFailed { transient }) => {
            facts.state = DeliveryState::PublishFailed;
            facts.failure_transient = transient;
            true
        }
        (DeliveryState::PublishFailed, DeliveryEvent::DlqPersisted) => {
            facts.state = DeliveryState::DlqPersisted;
            true
        }
        (
            DeliveryState::SinkAcknowledged
            | DeliveryState::DryRunObserved
            | DeliveryState::IntentionallyFiltered
            | DeliveryState::DlqPersisted,
            DeliveryEvent::CheckpointCommitted,
        ) => {
            facts.state = DeliveryState::CheckpointCommitted;
            facts.source_visible = false;
            true
        }
        (DeliveryState::CheckpointCommitted, DeliveryEvent::CleanupAllowed) => {
            facts.state = DeliveryState::CleanupEligible;
            facts.cleanup_allowed = true;
            true
        }
        (
            DeliveryState::CheckpointCommitted | DeliveryState::CleanupEligible,
            DeliveryEvent::NextBatch { checkpoint },
        ) => {
            return Ok(DeliveryFacts::polled(checkpoint));
        }
        _ => false,
    };

    valid
        .then_some(facts)
        .ok_or(TransitionError { state, event })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn apply(facts: DeliveryFacts, events: &[DeliveryEvent]) -> DeliveryFacts {
        events.iter().fold(facts, |facts, event| {
            transition(facts, *event).expect("legal delivery transition")
        })
    }

    #[test]
    fn successful_delivery_reaches_cleanup() {
        let facts = apply(
            DeliveryFacts::polled(7),
            &[
                DeliveryEvent::Encoded,
                DeliveryEvent::PublishStarted,
                DeliveryEvent::SinkAcknowledged { frontier: 7 },
                DeliveryEvent::CheckpointCommitted,
                DeliveryEvent::CleanupAllowed,
            ],
        );

        assert_eq!(facts.state, DeliveryState::CleanupEligible);
        assert_eq!(facts.sink_acknowledged_frontier, Some(7));
        assert_eq!(facts.source_checkpoint, Some(7));
        assert!(!facts.source_visible);
        assert!(facts.cleanup_allowed);
    }

    #[test]
    fn ambiguous_publish_marks_duplicate_risk() {
        let facts = apply(
            DeliveryFacts::polled(3),
            &[
                DeliveryEvent::Encoded,
                DeliveryEvent::PublishStarted,
                DeliveryEvent::SinkAccepted,
            ],
        );

        assert_eq!(facts.state, DeliveryState::SinkAccepted);
        assert!(facts.duplicate_possible);
        assert_eq!(facts.source_checkpoint, Some(3));
    }

    #[test]
    fn alternative_terminal_paths_commit_the_original_checkpoint() {
        for terminal in [
            DeliveryEvent::DryRunObserved,
            DeliveryEvent::IntentionallyFiltered,
        ] {
            let facts = apply(
                DeliveryFacts::polled(11),
                &[
                    DeliveryEvent::Encoded,
                    terminal,
                    DeliveryEvent::CheckpointCommitted,
                ],
            );
            assert_eq!(facts.source_checkpoint, Some(11));
            assert!(!facts.source_visible);
        }

        let facts = apply(
            DeliveryFacts::polled(13),
            &[
                DeliveryEvent::Encoded,
                DeliveryEvent::PublishStarted,
                DeliveryEvent::PublishFailed { transient: false },
                DeliveryEvent::DlqPersisted,
                DeliveryEvent::CheckpointCommitted,
            ],
        );
        assert!(!facts.failure_transient);
        assert!(!facts.source_visible);
    }

    #[test]
    fn transient_failure_remains_retryable_until_terminal() {
        let facts = apply(
            DeliveryFacts::polled(17),
            &[
                DeliveryEvent::Encoded,
                DeliveryEvent::PublishStarted,
                DeliveryEvent::PublishFailed { transient: true },
            ],
        );

        assert_eq!(facts.state, DeliveryState::PublishFailed);
        assert!(facts.failure_transient);
        assert!(facts.source_visible);
        assert!(transition(facts, DeliveryEvent::CheckpointCommitted).is_err());
    }

    #[test]
    fn illegal_transitions_return_errors() {
        let facts = DeliveryFacts::polled(19);
        let error = transition(facts, DeliveryEvent::CheckpointCommitted).unwrap_err();
        assert_eq!(error.state, DeliveryState::Polled);
    }

    #[test]
    fn next_batch_resets_all_delivery_facts() {
        let facts = apply(
            DeliveryFacts::polled(23),
            &[
                DeliveryEvent::Encoded,
                DeliveryEvent::PublishStarted,
                DeliveryEvent::SinkAccepted,
                DeliveryEvent::SinkAcknowledged { frontier: 23 },
                DeliveryEvent::CheckpointCommitted,
                DeliveryEvent::NextBatch { checkpoint: 29 },
            ],
        );

        assert_eq!(facts, DeliveryFacts::polled(29));
    }

    proptest! {
        #[test]
        fn checkpoint_never_exceeds_acknowledged_frontier(checkpoint in 0_u64..u64::MAX) {
            let facts = apply(
                DeliveryFacts::polled(checkpoint),
                &[
                    DeliveryEvent::Encoded,
                    DeliveryEvent::PublishStarted,
                    DeliveryEvent::SinkAcknowledged { frontier: checkpoint },
                    DeliveryEvent::CheckpointCommitted,
                ],
            );
            prop_assert_eq!(facts.source_checkpoint, facts.sink_acknowledged_frontier);
        }
    }
}
