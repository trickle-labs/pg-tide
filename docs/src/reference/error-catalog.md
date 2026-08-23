# Operator error catalog

The relay emits the five frozen CLI diagnostic fields. Retryability,
runbook ownership, safe context, and exit metadata are defined here.

<!-- BEGIN GENERATED OPERATOR ERRORS -->
| Code | Surface | Summary | Retryable | Exit | Runbook |
| --- | --- | --- | --- | ---: | --- |
| `operator.failure` | cli | The requested operation could not be completed. | false | 1 | `relay-will-not-start` |
| `PGTIDE_EXTENSION_VERSION_INCOMPATIBLE` | cli | The installed pg_tide extension is outside the relay lifecycle policy. | false | 1 | `failed-upgrade` |
| `PGTIDE_POSTGRES_UNAVAILABLE` | cli | PostgreSQL is unavailable. | true | 1 | `relay-will-not-start` |
| `PGTIDE_POSTGRES_AUTHENTICATION` | cli | PostgreSQL authentication was rejected. | false | 1 | `relay-will-not-start` |
| `PGTIDE_POSTGRES_AUTHORIZATION` | cli | PostgreSQL denied the requested operation. | false | 1 | `relay-will-not-start` |
| `PGTIDE_CATALOG_MISSING` | cli | The pg_tide catalog is missing. | false | 1 | `failed-upgrade` |
| `PGTIDE_PIPELINE_NOT_FOUND` | cli | The requested pipeline was not found. | false | 1 | `pipeline-undiscovered` |
| `PGTIDE_PIPELINE_INVALID` | cli | The pipeline configuration is invalid. | false | 1 | `relay-will-not-start` |
| `PGTIDE_PIPELINE_NOT_DISCOVERED` | cli | The pipeline was not discovered by the relay. | false | 1 | `pipeline-undiscovered` |
| `PGTIDE_PIPELINE_NOT_OWNED` | cli | The relay does not own the requested pipeline. | true | 1 | `ownership-ambiguity` |
| `PGTIDE_CONNECTOR_UNAVAILABLE` | cli | The destination connector is unavailable. | true | 1 | `relay-will-not-start` |
| `PGTIDE_CONNECTOR_TIMEOUT` | cli | The destination connector timed out. | true | 1 | `relay-will-not-start` |
| `PGTIDE_CONNECTOR_THROTTLED` | cli | The destination connector throttled the request. | true | 1 | `relay-will-not-start` |
| `PGTIDE_CONNECTOR_AUTHENTICATION` | cli | The destination connector rejected authentication. | false | 1 | `relay-will-not-start` |
| `PGTIDE_CONNECTOR_AUTHORIZATION` | cli | The destination connector denied authorization. | false | 1 | `relay-will-not-start` |
| `PGTIDE_TLS_VERIFICATION_FAILED` | cli | TLS verification failed. | false | 1 | `relay-will-not-start` |
| `PGTIDE_WEBHOOK_SSRF_REJECTED` | cli | The webhook destination was rejected by SSRF protection. | false | 1 | `webhook-authentication-failure` |
| `PGTIDE_CONNECTOR_INVALID_DESTINATION` | cli | The destination address or topic is invalid. | false | 1 | `relay-will-not-start` |
| `PGTIDE_CONNECTOR_MESSAGE_TOO_LARGE` | cli | The connector rejected a message that exceeded its limit. | false | 1 | `relay-will-not-start` |
| `PGTIDE_CONNECTOR_PROTOCOL_REJECTION` | cli | The destination rejected the request protocol. | false | 1 | `relay-will-not-start` |
| `PGTIDE_CONNECTOR_INVALID_CONFIG` | cli | The connector configuration is invalid. | false | 1 | `relay-will-not-start` |
| `PGTIDE_CONNECTOR_UNKNOWN` | cli | The destination connector failed unexpectedly. | true | 1 | `relay-will-not-start` |
| `PGTIDE_REPLAY_INPUT_INVALID` | cli | The replay or DLQ input is invalid. | false | 1 | `dlq-growth` |
| `PGTIDE_MAINTENANCE_SWEEP_FAILED` | cli | The maintenance sweep could not be completed. | true | 1 | `cleanup-failure` |
| `PGTIDE_SHUTDOWN_FAILED` | cli | The relay could not complete graceful shutdown. | true | 1 | `ownership-ambiguity` |
| `PGTIDE_SUPPORT_BUNDLE_WRITE_FAILED` | cli | The support bundle could not be written. | false | 1 | `relay-will-not-start` |
| `PGTIDE_INTERNAL_FAILURE` | cli | The relay encountered an internal failure. | true | 1 | `relay-will-not-start` |
| `PGTIDE_CONFIG_UNSUPPORTED_SURFACE` | sql | The requested configuration surface is not supported. | false | — | `failed-upgrade` |
| `PGTIDE_OUTBOX_ALREADY_EXISTS` | sql | The outbox already exists. | false | — | `relay-will-not-start` |
| `PGTIDE_OUTBOX_NOT_FOUND` | sql | The outbox was not found. | false | — | `relay-will-not-start` |
| `PGTIDE_INBOX_ALREADY_EXISTS` | sql | The inbox already exists. | false | — | `relay-will-not-start` |
| `PGTIDE_INBOX_NOT_FOUND` | sql | The inbox was not found. | false | — | `relay-will-not-start` |
| `PGTIDE_RELAY_NOT_FOUND` | sql | The relay pipeline was not found. | false | — | `pipeline-undiscovered` |
| `PGTIDE_INVALID_ARGUMENT` | sql | A SQL function argument is invalid. | false | — | `relay-will-not-start` |
| `PGTIDE_SWEEP_FAILED` | sql | The outbox sweep failed. | true | — | `cleanup-failure` |
| `PGTIDE_PUBLISH_DENIED` | sql | Publishing to the outbox was denied. | false | — | `relay-will-not-start` |
| `PGTIDE_AUTHORIZATION_FAILED` | sql | The PostgreSQL authorization check failed. | false | — | `relay-will-not-start` |
| `PGTIDE_SPI_ERROR` | sql | The PostgreSQL catalog operation failed. | true | — | `relay-will-not-start` |
<!-- END GENERATED OPERATOR ERRORS -->
