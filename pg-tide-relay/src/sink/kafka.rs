/// Apache Kafka producer sink (RELAY-8).
/// Feature-gated: only compiled with `--features kafka`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "kafka")]
use rdkafka::{
    error::{KafkaError, RDKafkaErrorCode},
    producer::{FutureProducer, FutureRecord, Producer},
    ClientConfig,
};

#[cfg(feature = "kafka")]
fn kafka_error(error: &KafkaError) -> RelayError {
    use crate::error::{ConnectorFailureCode, RetryClass};

    let native_code = match error {
        KafkaError::MessageProduction(code)
        | KafkaError::Global(code)
        | KafkaError::Flush(code) => Some(*code),
        _ => None,
    };
    let (code, class, summary) = match native_code {
        Some(RDKafkaErrorCode::BrokerTransportFailure)
        | Some(RDKafkaErrorCode::RequestTimedOut)
        | Some(RDKafkaErrorCode::NotEnoughReplicas) => (
            ConnectorFailureCode::Unavailable,
            RetryClass::Transient,
            "Kafka broker is unavailable",
        ),
        Some(RDKafkaErrorCode::MessageSizeTooLarge) => (
            ConnectorFailureCode::MessageTooLarge,
            RetryClass::Permanent,
            "Kafka rejected an oversized message",
        ),
        Some(RDKafkaErrorCode::Authentication | RDKafkaErrorCode::SaslAuthenticationFailed) => (
            ConnectorFailureCode::Authentication,
            RetryClass::Permanent,
            "Kafka authentication rejected",
        ),
        Some(
            RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed,
        ) => (
            ConnectorFailureCode::Authorization,
            RetryClass::Permanent,
            "Kafka authorization rejected",
        ),
        Some(RDKafkaErrorCode::UnknownTopicOrPartition) => (
            ConnectorFailureCode::InvalidDestination,
            RetryClass::Permanent,
            "Kafka topic was not found",
        ),
        None => match error {
            KafkaError::ClientConfig(..) | KafkaError::ClientCreation(..) => (
                ConnectorFailureCode::InvalidDestination,
                RetryClass::Permanent,
                "Kafka producer configuration was rejected",
            ),
            _ => (
                ConnectorFailureCode::Unknown,
                RetryClass::Transient,
                "Kafka operation failed",
            ),
        },
        Some(_) => (
            ConnectorFailureCode::Unknown,
            RetryClass::Transient,
            "Kafka operation failed",
        ),
    };
    RelayError::connector_failure("kafka", code, class, summary)
}

#[cfg(feature = "kafka")]
pub struct KafkaSink {
    producer: FutureProducer,
    topic_template: String,
}

#[cfg(feature = "kafka")]
impl KafkaSink {
    pub fn new(brokers: &str, topic_template: impl Into<String>) -> Result<Self, RelayError> {
        let topic_template = topic_template.into();
        if topic_template.trim().is_empty()
            || topic_template
                .chars()
                .any(|character| character.is_control())
        {
            return Err(RelayError::connector_failure(
                "kafka",
                crate::error::ConnectorFailureCode::InvalidDestination,
                crate::error::RetryClass::Permanent,
                "Kafka topic is invalid",
            ));
        }
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("retries", "10")
            .set("max.in.flight.requests.per.connection", "5")
            .create()
            .map_err(|error| kafka_error(&error))?;

        Ok(Self {
            producer,
            topic_template,
        })
    }
}

#[cfg(feature = "kafka")]
#[async_trait::async_trait]
impl super::Sink for KafkaSink {
    fn name(&self) -> &str {
        "kafka"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        use std::time::Duration;

        for msg in messages {
            let payload = serde_json::to_string(msg).map_err(RelayError::Json)?;
            let key = msg.dedup_key.as_str();

            self.producer
                .send(
                    FutureRecord::to(&self.topic_template)
                        .key(key)
                        .payload(&payload),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(error, _)| kafka_error(&error))?;
        }
        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        self.producer
            .client()
            .fetch_metadata(None, std::time::Duration::from_secs(1))
            .is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        self.producer
            .flush(std::time::Duration::from_secs(5))
            .map_err(|error| kafka_error(&error))?;
        Ok(())
    }
}
