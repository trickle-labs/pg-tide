# Integration: Terraform

This guide shows how to manage pg_tide infrastructure as code using Terraform. You can provision PostgreSQL extensions, create outboxes/inboxes, and configure relay pipelines declaratively.

## Provider Setup

Use the [cyrilgdn/postgresql](https://registry.terraform.io/providers/cyrilgdn/postgresql/) provider for managing PostgreSQL objects:

```hcl
terraform {
  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.22"
    }
  }
}

provider "postgresql" {
  host     = var.postgres_host
  port     = var.postgres_port
  database = var.postgres_database
  username = var.postgres_username
  password = var.postgres_password
  sslmode  = "require"
}
```

## Install the Extension

```hcl
resource "postgresql_extension" "pg_tide" {
  name    = "pg_tide"
  schema  = "tide"
  version = "0.11.0"
}
```

## Create Outboxes and Inboxes

```hcl
resource "postgresql_function" "create_outbox" {
  depends_on = [postgresql_extension.pg_tide]
  
  name     = "create_order_outbox"
  language = "sql"
  body     = "SELECT tide.outbox_create('order_events');"
  
  lifecycle {
    ignore_changes = [body]
  }
}
```

For a more maintainable approach, use `local-exec` provisioners:

```hcl
resource "null_resource" "outboxes" {
  depends_on = [postgresql_extension.pg_tide]

  for_each = toset(var.outbox_names)

  provisioner "local-exec" {
    command = <<-EOT
      psql "${var.postgres_url}" -c "SELECT tide.outbox_create('${each.value}');"
    EOT
  }
}

variable "outbox_names" {
  type    = list(string)
  default = ["order_events", "user_events", "notification_events"]
}
```

## Configure Relay Pipelines

```hcl
resource "null_resource" "pipeline_orders_to_kafka" {
  depends_on = [null_resource.outboxes]

  provisioner "local-exec" {
    command = <<-EOT
      psql "${var.postgres_url}" -c "
        SELECT tide.relay_set_outbox_v2(
          jsonb_build_object(
            'name', 'orders-to-kafka',
            'outbox', 'order_events',
            'sink_type', 'kafka',
            'config', '${jsonencode({
              brokers   = var.kafka_brokers
              topic     = "orders"
              wire_format = "debezium"
            })}'::jsonb
          )
        );
      "
    EOT
  }

  triggers = {
    config_hash = sha256(jsonencode({
      sink_type = "kafka"
      brokers   = var.kafka_brokers
      topic     = "orders"
    }))
  }
}
```

## Deploy the Relay (Kubernetes)

```hcl
resource "kubernetes_deployment" "pg_tide_relay" {
  metadata {
    name      = "pg-tide-relay"
    namespace = var.namespace
  }

  spec {
    replicas = var.relay_replicas

    selector {
      match_labels = {
        app = "pg-tide-relay"
      }
    }

    template {
      metadata {
        labels = {
          app = "pg-tide-relay"
        }
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "9090"
        }
      }

      spec {
        termination_grace_period_seconds = 60

        container {
          name  = "pg-tide"
          image = "${var.relay_image}:${var.relay_version}"

          args = [
            "--postgres-url", "$(DATABASE_URL)",
            "--relay-group-id", var.relay_group_id,
            "--shutdown-timeout", "45",
          ]

          port {
            container_port = 9090
            name           = "metrics"
          }

          env {
            name = "DATABASE_URL"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.pg_tide.metadata[0].name
                key  = "database-url"
              }
            }
          }

          liveness_probe {
            http_get {
              path = "/health"
              port = 9090
            }
            initial_delay_seconds = 10
            period_seconds        = 15
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "128Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_secret" "pg_tide" {
  metadata {
    name      = "pg-tide-secrets"
    namespace = var.namespace
  }

  data = {
    "database-url" = var.postgres_url
  }
}
```

## Variables

```hcl
variable "postgres_url" {
  type      = string
  sensitive = true
}

variable "kafka_brokers" {
  type    = string
  default = "kafka:9092"
}

variable "relay_replicas" {
  type    = number
  default = 2
}

variable "relay_group_id" {
  type    = string
  default = "production"
}

variable "relay_image" {
  type    = string
  default = "ghcr.io/your-org/pg-tide"
}

variable "relay_version" {
  type    = string
  default = "0.11.0"
}
```

## Further Reading

- [Deployment Architectures](../operations/deployment-architectures.md) — Choosing your topology
- [GitHub Actions Integration](github-actions.md) — CI/CD automation
