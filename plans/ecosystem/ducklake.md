# pg-tide × DuckLake: Ecosystem Integration Plan

> **Status:** Research & Proposal  
> **Date:** 2026-05-19  
> **Audience:** Engineering, product, and community stakeholders

---

## Executive Summary

DuckLake is a new open data lake format from the DuckDB team that stores table data as Parquet files on object storage while keeping all metadata — schemas, snapshots, statistics — in a standard SQL database like PostgreSQL. It reached production-readiness with its v1.0 release in April 2026 and is already being adopted by dozens of companies.

pg-tide is a PostgreSQL extension that provides transactional outbox, idempotent inbox, and relay pipelines. It captures business events atomically inside your existing transactions and streams them to 15+ backends.

These two projects share a remarkably deep foundation: both are PostgreSQL-native, both are built around the idea that a relational database is the correct place to manage metadata, and both speak Parquet as their data interchange format. This document explores the opportunities that arise when you bring them together — from features we can build, to tutorials we can write, to demos that can showcase the combined power of both systems.

---

## Background: How DuckLake Works

To understand the synergy, it helps to first understand what DuckLake actually is, even if you're not deeply technical.

Imagine you have a warehouse full of boxes (Parquet files on S3). Each box contains structured data — orders, sensor readings, whatever your business produces. Now imagine you have a librarian (a PostgreSQL database) who keeps a perfect catalog of every box: what's in it, when it arrived, how big it is, which boxes replace which older ones. This is DuckLake. The librarian (PostgreSQL) manages the catalog. The boxes (Parquet files) sit on cheap storage. Multiple people (DuckDB instances) can ask the librarian questions and get efficient answers without interfering with each other.

The key insight: DuckLake's catalog tables are just regular PostgreSQL tables — `ducklake_snapshot`, `ducklake_data_file`, `ducklake_table_stats`, etc. There are 28 tables in total, all sitting in a PostgreSQL schema. They're managed by simple SQL transactions. Any tool that can write correct SQL to those tables can participate in the lake.

DuckLake also supports a powerful feature called **data inlining**, where small writes (streaming-style single-row inserts) are stored directly in the catalog database rather than creating a Parquet file for each one. This avoids the "small files problem" that plagues traditional data lakes and makes DuckLake uniquely suited for streaming workloads — exactly the kind of workloads that pg-tide manages.

---

## The Natural Fit: Why pg-tide and DuckLake Belong Together

Both pg-tide and DuckLake make a philosophical bet on the same idea: PostgreSQL is the correct place to coordinate distributed data operations. pg-tide captures events inside your PostgreSQL transactions. DuckLake manages lakehouse metadata inside PostgreSQL transactions. The fact that they live in the same database isn't a coincidence — it's an architectural gift that enables capabilities neither system can achieve alone.

Here's what makes this special: in a typical data pipeline, capturing an event and writing it to a data lake are two separate distributed operations. You need complex exactly-once delivery guarantees, deduplication, checkpoint management, and error recovery. But when both systems share the same PostgreSQL instance, you can atomically write "event captured" and "lake snapshot committed" in a single database transaction. This eliminates an entire class of distributed systems problems.

The pg-tide relay already has a DuckLake sink (`pg-tide-relay/src/sink/ducklake.rs`). Today it writes its own custom catalog schema (`tide.ducklake_snapshots`). The opportunity is to evolve this sink to speak the real DuckLake v1.0 catalog protocol — writing to the actual `ducklake_data_file`, `ducklake_snapshot`, and `ducklake_file_column_stats` tables — so that DuckDB can query the resulting lake directly without any translation layer.

---

## Feature Opportunities

### 1. Native DuckLake v1.0 Catalog Sink (Priority: High)

**What it is:** Upgrade the existing DuckLake sink to write data using the official DuckLake v1.0 specification — creating real `ducklake_snapshot` entries, writing column statistics to `ducklake_file_column_stats`, and registering Parquet files in `ducklake_data_file`.

**Why it matters:** Today, the pg-tide DuckLake sink uses its own proprietary catalog format. That means DuckDB cannot query the resulting Parquet files without custom glue code. By writing to the real DuckLake catalog tables, any DuckDB instance can immediately `ATTACH` to the PostgreSQL catalog and query the data with full time-travel, filter pushdown, and schema evolution support — no extra work required.

**Technical approach:** The DuckLake write protocol is well-documented. For each batch of outbox messages, the relay would: (a) write a Parquet file to object storage, (b) within a single PostgreSQL transaction, insert into `ducklake_data_file`, update `ducklake_table_stats` and `ducklake_table_column_stats`, insert per-file column statistics, and create a new `ducklake_snapshot` entry. The DuckLake spec requires monotonically increasing snapshot IDs and proper `begin_snapshot`/`end_snapshot` lifecycle management — all achievable with PostgreSQL sequences and transactions.

**Bonus:** Because both the outbox consumer-offset advance and the DuckLake snapshot commit can happen in the same transaction, this gives us transactional exactly-once delivery from PostgreSQL to the data lake — a guarantee that no other pipeline tool can currently offer with DuckLake.

---

### 2. DuckLake Data Inlining Integration (Priority: High)

**What it is:** Instead of always writing Parquet files, let the relay use DuckLake's data inlining feature for small batches — writing rows directly into DuckLake's `ducklake_inlined_data_*` tables in the catalog database.

**Why it matters:** DuckLake's data inlining stores small inserts directly in PostgreSQL, avoiding the creation of thousands of tiny Parquet files. This is perfect for low-throughput outboxes or bursty event streams. Events trickle into the lake in real-time, queries against the DuckLake table always return fresh data, and the system only flushes to Parquet files when a threshold is reached. DuckLake's benchmarks show that inlining is 926× faster for queries and 105× faster for ingestion compared to Iceberg.

**How it works with pg-tide:** The relay sink could detect batch sizes. For batches below the inlining threshold (configurable, default 10 rows), it writes directly to the inlined data table. For larger flushes, it writes Parquet. This gives you the best of both worlds: sub-millisecond writes for single events, with eventual consolidation into efficient columnar storage.

---

### 3. Same-Transaction Atomicity Mode (Priority: High)

**What it is:** A special co-located mode where the relay sink commits both the outbox offset advance and the DuckLake catalog write in a single PostgreSQL transaction.

**Why it matters:** This is the holy grail of exactly-once delivery to a data lake. In a normal relay pipeline, the flow is: read messages → write Parquet → commit catalog → acknowledge messages. If the process crashes between "commit catalog" and "acknowledge," you get duplicates. By combining both commits into one PostgreSQL transaction, either everything succeeds or nothing does. No duplicates, no lost messages, no complex idempotency logic.

**When it applies:** This mode requires the relay to be connected to the same PostgreSQL instance that hosts both the pg-tide outbox and the DuckLake catalog. This is the recommended production deployment for users who want bulletproof event-to-lake delivery.

---

### 4. DuckLake Snapshot-Aware Consumer Offsets (Priority: Medium)

**What it is:** Map pg-tide consumer group offsets to DuckLake snapshot IDs, so that downstream consumers can ask "give me all events since my last checkpoint" using DuckLake's time-travel feature.

**Why it matters:** DuckLake supports querying tables `AT (VERSION => N)`. If each relay batch creates a new snapshot, and we record the mapping from consumer offset to snapshot ID, any analytics consumer can replay events by simply querying `FROM events AT (VERSION => 42)` through `AT (VERSION => 57)`. This turns your data lake into a replayable event log — queryable with SQL, without needing a message broker.

---

### 5. Reverse Relay: DuckLake → pg-tide Inbox (Priority: Medium)

**What it is:** A DuckLake source that monitors new DuckLake snapshots and relays incoming data into a pg-tide inbox for processing by application services.

**Why it matters:** Data doesn't only flow from applications to the lake. Analytics pipelines, ML model outputs, and enriched datasets often need to flow back into operational systems. By watching for new DuckLake snapshots (easily done by polling `max(snapshot_id)` or listening for NOTIFY events), the relay can pick up new lake data and deliver it into application inboxes with full deduplication guarantees.

---

### 6. Schema Evolution Bridge (Priority: Medium)

**What it is:** Automatically evolve the DuckLake table schema when new fields appear in outbox messages, using DuckLake's built-in schema evolution support.

**Why it matters:** In event-driven systems, payload schemas change over time. New fields get added, old ones become optional. DuckLake natively supports adding columns, removing columns, and changing types — all tracked in the `ducklake_column` table with snapshot-based versioning. The relay could detect new JSON keys in the message payload and issue `ALTER TABLE` equivalents (new `ducklake_column` entries) within the same catalog transaction. Consumers always see the latest schema, and historical queries with time-travel still work against older snapshots.

---

### 7. Partitioned Event Tables (Priority: Low)

**What it is:** Use DuckLake's partition support to automatically partition event tables by date, event type, or tenant — improving query performance for time-range and filtering queries.

**Why it matters:** DuckLake supports hidden partitioning (similar to Iceberg) and bucket partitioning. For a high-throughput outbox generating millions of events, partitioning by day or event type means that analytics queries like "show me all order.created events from last week" only scan the relevant Parquet files. The relay can set up partitioning automatically when it creates DuckLake tables for new outbox streams.

---

## Tutorial Ideas

### Tutorial 1: "From Transaction to Data Lake in 5 Minutes"

**Audience:** Backend developers who know PostgreSQL but haven't used a data lake before.

**Story arc:** You have a PostgreSQL-based e-commerce app. You want to build an analytics layer without adding Kafka, Spark, or any heavy infrastructure. Walk through: install pg_tide → create an outbox → publish order events → configure a DuckLake relay pipeline → query your events with DuckDB using time-travel.

**Why it's compelling:** It shows the absolute minimum path from "I have a PostgreSQL application" to "I have a queryable data lake with full history" — no Docker compose files full of services, no Kafka cluster, no Spark jobs.

---

### Tutorial 2: "Real-Time Analytics Dashboard with PostgreSQL, pg-tide, and DuckDB"

**Audience:** Data engineers building internal dashboards.

**Story arc:** Stream IoT sensor data through pg-tide into DuckLake. Use DuckDB to power a live Grafana dashboard. Show how data inlining gives sub-second query latency on fresh data, while older data lives efficiently in Parquet on S3. Demonstrate time-travel queries: "what was the sensor reading profile last Thursday at 3pm?"

**Why it's compelling:** It positions pg-tide as the real-time ingestion layer for DuckLake, replacing traditional Kafka → Flink → Iceberg pipelines with a simpler PostgreSQL-native approach.

---

### Tutorial 3: "Multi-Tenant Data Lake with Row-Level Security"

**Audience:** SaaS platform engineers.

**Story arc:** Build a multi-tenant system where each tenant's events flow into isolated DuckLake namespaces via pg-tide's per-outbox publisher ACLs. Demonstrate that tenants can only query their own data, using PostgreSQL's row-level security on both the outbox and the DuckLake catalog tables.

**Why it's compelling:** Multi-tenancy in data lakes is notoriously hard. This tutorial shows how PostgreSQL's security model extends naturally to protect both the event pipeline and the analytics layer — a single security model for everything.

---

### Tutorial 4: "Event Sourcing with DuckLake as the Event Store"

**Audience:** Architects considering event sourcing patterns.

**Story arc:** Use pg-tide's outbox as the write-side event log. Configure the relay to append events to a DuckLake table. Use DuckLake snapshots as "event versions." Show how to rebuild application state by replaying events with DuckDB queries. Demonstrate the consumer-offset-to-snapshot mapping for reliable replay.

**Why it's compelling:** Event sourcing typically requires dedicated event stores. This shows how PostgreSQL + DuckLake provides a production-grade event store with built-in time-travel, efficient storage, and SQL-based replay — using tools people already know.

---

### Tutorial 5: "Migrating from Kafka Connect to pg-tide + DuckLake"

**Audience:** Teams currently running Kafka Connect → Iceberg/Delta pipelines.

**Story arc:** Show a side-by-side comparison of the infrastructure needed for a Kafka-based CDC pipeline vs. a pg-tide + DuckLake approach. Walk through the migration step by step. Highlight: fewer moving parts, simpler operations, same query capabilities, better consistency guarantees.

**Why it's compelling:** Many teams are over-provisioned with streaming infrastructure. This gives them a concrete path to simplification.

---

## Demo Ideas

### Demo 1: "Zero to Data Lake" (5-minute lightning talk)

**Format:** Live coding demo at a meetup or conference.

**Script:**
1. Start with a fresh PostgreSQL database
2. `CREATE EXTENSION pg_tide;`
3. Create an outbox, publish 3 events inside a transaction
4. Configure a DuckLake pipeline: `SELECT tide.relay_set_outbox(...)`
5. Start the relay binary
6. Open DuckDB, attach to the same PostgreSQL catalog
7. `SELECT * FROM events;` — the events are there
8. `SELECT * FROM events AT (VERSION => 1);` — time travel works
9. Insert more events, watch them appear in real-time

**Punchline:** "You just built a production-grade event streaming pipeline to a data lake, with time-travel and exactly-once delivery, in under 5 minutes. No Kafka, no Spark, no Airflow, no Flink. Just PostgreSQL."

---

### Demo 2: "The Impossible Guarantee" (Conference talk demo)

**Format:** Technical deep-dive showing transactional exactly-once delivery.

**Script:**
1. Set up pg-tide + DuckLake on the same PostgreSQL instance
2. Start the relay in same-transaction mode
3. Publish events, show them appearing in DuckLake snapshots
4. Kill the relay mid-batch (simulate a crash)
5. Restart the relay — show that there are zero duplicate events in the lake
6. Explain why: single PostgreSQL transaction means the offset advance and snapshot commit are atomic
7. Contrast with Kafka → Iceberg, where you need complex idempotency logic

**Punchline:** "This is a guarantee that no other CDC-to-lakehouse pipeline can make today. And it works because both systems trust the same PostgreSQL transaction."

---

### Demo 3: "The Streaming Sensor Dashboard" (Interactive booth demo)

**Format:** Live interactive demo at a conference booth or webinar.

**Script:**
1. Simulate 10 IoT sensors publishing temperature readings every second
2. Events flow through pg-tide outbox → relay → DuckLake (with inlining)
3. DuckDB-powered dashboard updates in real-time
4. Audience members can trigger "anomalies" via a web button
5. Show how the anomaly events appear in the lake within milliseconds
6. Use time-travel to query "what was the temperature 30 seconds ago?"

**Why it's engaging:** People can interact with it, see live data flowing, and understand the value proposition viscerally.

---

### Demo 4: "Compliance Replay" (Enterprise-focused demo)

**Format:** Solution demo for financial services or healthcare audiences.

**Script:**
1. Show a banking application recording transactions via pg-tide
2. Events flow into an immutable DuckLake archive on S3
3. Regulator asks: "show me all transactions from Account X between March 1-15"
4. Use DuckDB time-travel: `SELECT * FROM transactions AT (TIMESTAMP => '2026-03-15') WHERE account_id = 'X'`
5. Show that nobody can tamper with the historical record — Parquet files are immutable, snapshots are append-only
6. Demonstrate audit trail through DuckLake's `ducklake_snapshot_changes`

**Punchline:** "Your compliance archive is just SQL queries away. No special tools, no data warehouse licenses, no waiting for batch jobs."

---

## Other Integration Opportunities

### pg-tide as a DuckLake Change Notification System

DuckLake currently has no built-in mechanism to notify external systems when new data arrives. pg-tide's LISTEN/NOTIFY integration could fill this gap: after writing a new DuckLake snapshot, fire a PostgreSQL NOTIFY event. External consumers subscribe and get near-real-time notifications of lake updates. This turns DuckLake into a reactive system.

### Shared PostgreSQL as Single Operational Database

For teams already running PostgreSQL, pg-tide + DuckLake means one database manages: (a) your application data, (b) your event outbox, (c) your data lake catalog, and (d) your pipeline configuration. There's no ZooKeeper, no additional Kafka cluster, no separate metadata service. This massively reduces operational complexity.

### DuckLake as a Dead Letter Queue Archive

pg-tide has a dead letter queue (DLQ) for messages that fail delivery. Instead of just storing them in PostgreSQL forever, expired DLQ messages could be archived to a DuckLake table. This keeps the operational DLQ small while providing unlimited, queryable history of failed messages for debugging and compliance.

### Cross-Lake Replication

Use pg-tide as the replication layer between DuckLake instances. When one DuckLake gets new snapshots, the reverse relay picks them up and publishes to another DuckLake in a different region or cloud. This enables multi-region data lake federation without complex ETL.

### Integration with the DuckLake Ecosystem

DuckLake already has client implementations for Apache DataFusion, Apache Spark, Trino, and Pandas. By writing data in the standard DuckLake format, pg-tide events become instantly queryable from all of these engines — not just DuckDB. This makes pg-tide relevant to the broader data engineering ecosystem, not just PostgreSQL shops.

---

## Positioning and Community Strategy

### Target Audiences

1. **PostgreSQL developers** who want analytics without adding infrastructure
2. **Data engineers** looking to simplify their CDC → lake pipelines
3. **DuckDB/DuckLake users** who need a reliable way to get data into their lake
4. **Platform teams** tired of managing Kafka + Flink + Iceberg stacks

### Messaging

The core message is: **"pg-tide is the fastest path from a PostgreSQL transaction to a production data lake."**

Secondary messages:
- "Same PostgreSQL transaction. Zero duplicates. It just works."
- "No Kafka, no Spark, no Airflow. Just PostgreSQL and Parquet."
- "Your events become a time-traveling data lake — queryable with DuckDB in minutes."

### Community Engagement Ideas

- Write a blog post for the DuckDB community blog or get mentioned in their "awesome-ducklake" repository
- Contribute a DuckLake guide: "Streaming data into DuckLake with pg-tide"
- Create a Docker Compose example: `docker compose up` gives you PostgreSQL + pg-tide + DuckLake + Grafana
- Present at DuckDB meetups (they have an active community)
- Open a DuckLake discussion thread proposing a NOTIFY-based change notification mechanism

---

## Implementation Roadmap

| Phase | Work Item | Effort | Impact |
|-------|-----------|--------|--------|
| **1** | Upgrade DuckLake sink to v1.0 spec (write real catalog tables) | 2-3 weeks | Unlocks DuckDB querying |
| **1** | Same-transaction atomicity mode | 1 week | Unique selling point |
| **2** | Data inlining support for small batches | 1-2 weeks | Streaming performance |
| **2** | Schema evolution bridge | 1 week | Production readiness |
| **3** | DuckLake → inbox reverse source | 2 weeks | Bidirectional flow |
| **3** | Snapshot-to-offset mapping | 1 week | Consumer experience |
| **4** | Partition auto-configuration | 1 week | Query performance |
| **4** | DLQ → DuckLake archive | 3 days | Operational hygiene |
| **∞** | Tutorials, demos, community content | Ongoing | Adoption |

---

## Conclusion

pg-tide and DuckLake are two projects built on the same philosophical foundation — that PostgreSQL is the right place to manage critical data operations. Bringing them together creates something greater than either can offer alone: a transactionally consistent, operationally simple path from business events to a queryable data lake. 

The opportunity is not just technical. The DuckLake community is growing fast, with production deployments at dozens of companies and an active ecosystem of clients across DuckDB, Spark, Trino, and DataFusion. By positioning pg-tide as the recommended ingestion layer for PostgreSQL-backed DuckLakes, we tap into that growing community while offering them something nobody else can: single-transaction exactly-once delivery from an OLTP database to a data lake.

The simplest possible data lake pipeline should be: write to PostgreSQL → events appear in your lake. With pg-tide + DuckLake, that's exactly what you get.
