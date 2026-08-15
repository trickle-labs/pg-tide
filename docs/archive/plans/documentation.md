# Documentation Improvement Plan

> Goal: Transform pg_tide's documentation from concise reference material into a rich,
> engaging guide that explains *why* things work the way they do, walks through real-world
> scenarios in depth, and reduces the cognitive overhead of navigating 45+ separate pages.

---

## 1. Current State Assessment

### What we have

- 45 substantive Markdown files across 10 categories
- Average file length: ~400 words (~1.5 printed pages)
- Total documentation: ~18,500 words
- Style: terse, bullet-heavy, assumes familiarity with messaging concepts
- Structure: many small files, most under 600 words

### Key problems

| Problem | Impact |
|---------|--------|
| Files are too short to explain concepts properly | Readers with less messaging experience get lost |
| Assumes prior knowledge of Kafka/NATS/outbox patterns | Narrows the audience significantly |
| Little narrative flow between sections | Feels like a reference manual, not a learning resource |
| Too many tiny pages break reading flow | Constant page-switching, repetitive headers |
| Tutorials jump to code without explaining *why* | Copy-paste without understanding |
| No real-world scenario framing | Hard to map features to actual business problems |
| Backend docs are thin config tables without context | Users can't evaluate which backend fits their needs |

### Target state

- Total documentation: ~55,000–65,000 words (3–3.5× current size)
- Average file length: 1,500–2,500 words per page
- Fewer pages overall (~28–32 instead of 45)
- Every concept explained from first principles before diving into API
- Real-world examples woven throughout, not isolated to a tutorials section
- Conversational, explanatory tone with clear problem→solution arcs

---

## 2. Structural Consolidation

The current structure has too many small files. Below is the proposed consolidation
that reduces page count while increasing depth per page.

### 2.1 Merge Plan

| Current Files | Merged Into | Rationale |
|---------------|-------------|-----------|
| `evaluate/when-to-use.md` + `evaluate/comparison.md` | **`evaluate/choosing-pg-tide.md`** | Both answer "should I use this?" — combine into one decision guide |
| `getting-started/quickstart.md` + `getting-started/tutorial.md` | **`getting-started/first-pipeline.md`** | The quickstart is a subset of the tutorial; merge into a single guided walkthrough with clear milestones |
| `concepts/transactional-outbox.md` + `concepts/idempotent-inbox.md` + `concepts/exactly-once-delivery.md` | **`concepts/message-guarantees.md`** | These three form one conceptual arc (publish → deliver → deduplicate). Telling the story end-to-end is more powerful than three separate short pages |
| `concepts/consumer-groups.md` + `concepts/relay-pipelines.md` | **`concepts/consumption-and-relay.md`** | Consumer groups only make sense in the context of relay pipelines; explain them together |
| `operations/deployment.md` + `operations/docker.md` + `operations/kubernetes.md` | **`operations/deployment.md`** | Deployment is one topic; consolidate bare-metal, Docker, and K8s into one page with clear sections |
| `operations/backup-and-restore.md` + `operations/upgrading.md` | **`operations/maintenance.md`** | Both are lifecycle operations; grouping them under maintenance is natural |
| `relay-guide/configuration.md` + `reference/configuration.md` | **`relay-guide/configuration.md`** | Having two config pages confuses users. Merge into one authoritative configuration reference |
| `relay-guide/error-handling.md` + `reference/errors.md` | **`relay-guide/error-handling.md`** | Error codes and error handling belong together |
| `reference/changelog.md` | Remove (link to top-level `CHANGELOG.md` instead) | Duplicate content with no added value |
| `tutorials/outbox-to-kafka.md` + `tutorials/inbox-from-nats.md` | **`tutorials/end-to-end-pipeline.md`** | Combine forward and reverse pipeline tutorials into one end-to-end walkthrough showing both directions |
| All 6 backend files in `relay-guide/backends/` | **`relay-guide/backends.md`** (single page) | Each backend page is only ~300 words. A single page with all backends side-by-side is more useful for comparison and scanning |

### 2.2 Proposed New Structure

```
docs/src/
├── introduction.md                          (expanded: ~2,000 words)
├── SUMMARY.md
│
├── evaluate/
│   ├── choosing-pg-tide.md                  (merged: when-to-use + comparison)
│   └── architecture.md                      (expanded with diagrams/narrative)
│
├── getting-started/
│   ├── installation.md                      (expanded: multi-platform details)
│   └── first-pipeline.md                    (merged: quickstart + tutorial)
│
├── concepts/
│   ├── message-guarantees.md                (merged: outbox + inbox + exactly-once)
│   └── consumption-and-relay.md             (merged: consumer-groups + relay-pipelines)
│
├── relay-guide/
│   ├── configuration.md                     (merged: both config pages)
│   ├── cli-reference.md                     (expanded with examples)
│   ├── backends.md                          (merged: all 6 backends + index)
│   ├── error-handling.md                    (merged: errors + error handling)
│   └── monitoring.md                        (expanded)
│
├── operations/
│   ├── deployment.md                        (merged: deployment + docker + k8s)
│   ├── scaling.md                           (expanded)
│   ├── maintenance.md                       (merged: backup + upgrading)
│   └── troubleshooting.md                   (expanded)
│
├── tutorials/
│   ├── end-to-end-pipeline.md              (merged: outbox-to-kafka + inbox-from-nats)
│   ├── bidirectional-sync.md               (expanded)
│   ├── fan-out-pattern.md                  (expanded)
│   ├── dead-letter-queue.md                (expanded)
│   └── real-world-scenarios.md             (NEW)
│
├── integrations/
│   ├── pg-trickle.md                        (as-is, expanded)
│   ├── dbt.md                               (expanded with full example)
│   ├── cloudnativepg.md                     (expanded)
│   └── pgbouncer.md                         (expanded)
│
├── sql-reference/
│   ├── outbox-api.md                        (expanded with more examples)
│   ├── inbox-api.md                         (expanded with more examples)
│   ├── relay-api.md                         (expanded with more examples)
│   ├── consumer-groups-api.md               (expanded with more examples)
│   └── catalog-tables.md                    (expanded with query cookbook)
│
└── reference/
    └── security.md                          (expanded)
```

**Result: 28 pages** (down from 45), each substantially longer and more self-contained.

---

## 3. Content Expansion Strategy

### 3.1 Writing Principles

1. **Start with the problem.** Every page should open by describing the real-world
   situation the reader is in. Don't lead with API signatures.

2. **Explain the "why" before the "how."** Before showing `SELECT tide.outbox_publish(...)`,
   explain what the function accomplishes, what happens under the hood, and why this
   approach is better than the alternatives.

3. **Use concrete scenarios.** Replace abstract "your application publishes events" with
   concrete examples: "Your e-commerce platform processes an order and needs to notify
   the warehouse service, update the analytics pipeline, and send a confirmation email."

4. **Annotate code blocks.** Every code example should have inline comments explaining
   non-obvious choices, and a paragraph after the block explaining what just happened.

5. **Cross-reference generously.** When a concept is explained elsewhere, link to it
   with a brief inline summary so the reader doesn't have to leave the page.

6. **Include "what could go wrong" sections.** For every operational guide, include
   common failure modes and their symptoms.

7. **Show before and after.** When explaining improvements (e.g., exactly-once delivery),
   show the broken version first, then the fixed version with pg_tide.

### 3.2 Per-Page Expansion Targets

#### Introduction (~2,000 words, currently ~400)

**Add:**
- Extended narrative about what messaging reliability means in practice
- A "day in the life" scenario: what happens when things go wrong without pg_tide
- Visual system diagram (Mermaid) showing all components
- Explicit audience definition (backend engineers, DBAs, platform teams)
- "What you'll learn" roadmap for the rest of the docs

#### Choosing pg_tide (~1,800 words, currently ~1,200 across 2 files)

**Add:**
- Detailed comparison table with feature-by-feature scoring
- Decision flowchart (Mermaid): "Should I use pg_tide?"
- Real cost analysis: operational overhead of pg_tide vs. running a Kafka cluster
- Migration stories: "Coming from Debezium," "Coming from pg_notify"
- Explicit anti-patterns section with deeper explanation

#### Architecture (~2,000 words, currently ~600)

**Add:**
- Detailed data flow diagram for forward and reverse pipelines
- Sequence diagram showing a message lifecycle end-to-end
- Explanation of how advisory locks prevent duplicate relay processing
- Deep dive on the notification mechanism (`pg_notify`)
- Explanation of what "inline_threshold" actually controls and why
- Comparison of topologies: single relay, multi-relay, multi-region

#### Installation (~1,500 words, currently ~300)

**Add:**
- Platform-specific instructions (Ubuntu/Debian, RHEL/Rocky, macOS, Docker)
- Verification steps with expected output
- Common installation problems and solutions
- Permissions and prerequisites explained in detail
- First-time health check SQL queries

#### First Pipeline (~3,000 words, currently ~900 across 2 files)

**Add:**
- Complete narrative walkthrough with a realistic scenario (order processing)
- "What's happening under the hood" callouts at each step
- Verification queries after each step
- Diagram showing message flow at each stage
- Explaining each parameter choice (not just listing them)
- "Try it yourself" challenges at the end of sections
- Link to Docker Compose setup for zero-install experimentation

#### Message Guarantees (~3,500 words, currently ~1,850 across 3 files)

**Add:**
- Extended explanation of the dual-write problem with timeline diagrams
- Comparison with other approaches (two-phase commit, saga pattern, change data capture)
- Deep dive into the inbox deduplication mechanism (UNIQUE constraint, TTL, cleanup)
- Exactly-once delivery explained with a visual timeline showing retry + dedup
- Edge cases and limitations explained honestly and in detail
- Performance implications of each guarantee level
- FAQ: "Is this really exactly-once?" (with the caveats industry experts raise)

#### Consumption and Relay (~2,500 words, currently ~1,150 across 2 files)

**Add:**
- Extended analogy: consumer groups as independent readers of a shared newspaper
- Visibility leases explained with a timeline diagram
- How advisory locks prevent duplicate relay processing (with failure scenarios)
- Hot reload mechanism explained in detail
- Multi-pipeline orchestration with practical examples
- Consumer group lifecycle: create, join, leave, rebalance
- Detailed "auto_offset_reset" explanation with scenarios for each mode

#### Configuration (~2,500 words, currently ~750 across 2 files)

**Add:**
- Complete annotated example TOML file (every option with explanation)
- Environment variable substitution patterns with real examples
- Configuration precedence rules explained with a priority table and examples
- SQL pipeline configuration deep dive with all parameter combinations
- Connection string patterns (direct, pooled, SSL, IAM auth)
- Config validation tips and common mistakes

#### CLI Reference (~1,200 words, currently ~300)

**Add:**
- Each flag explained with a use-case scenario
- Complete invocation examples for common deployment patterns
- Signal handling behavior explained in detail
- Systemd unit file example with annotations
- launchd plist example for macOS

#### Backends (~3,000 words, currently ~1,700 across 7 files)

**Add:**
- "Choosing a backend" decision matrix at the top
- For each backend: when to use it, when not, typical deployment alongside pg_tide
- Complete worked example for each backend (not just config snippets)
- Performance characteristics and tuning guidance per backend
- TLS/auth configuration explained thoroughly
- Subject/topic/queue naming strategies and best practices
- Failure mode documentation: what happens when each backend is unavailable

#### Error Handling (~2,000 words, currently ~750 across 2 files)

**Add:**
- Error taxonomy with flowchart: "My relay crashed — what do I do?"
- Each error code explained with cause, impact, and resolution steps
- Retry strategy deep dive: exponential backoff parameters and rationale
- DLQ management: when messages land there, how to investigate, how to replay
- Circuit breaker behavior explained with state diagram
- Graceful shutdown sequence explained step by step

#### Monitoring (~2,000 words, currently ~350)

**Add:**
- Complete Prometheus scrape configuration
- Every metric explained with its meaning, alert thresholds, and example queries
- Grafana dashboard JSON (or screenshot + import instructions)
- SQL monitoring queries cookbook (top 10 most useful queries)
- Alert rule examples: lag threshold, relay down, DLQ growing
- Integration with PagerDuty/Opsgenie alerting patterns
- Health check endpoint response format and interpretation

#### Deployment (~2,500 words, currently ~1,000 across 3 files)

**Add:**
- Complete Docker Compose development environment (PostgreSQL + relay + NATS)
- Complete Kubernetes manifests with annotations explained
- Helm chart usage explained with all configurable values
- Resource request/limit guidance based on throughput targets
- Health check and readiness probe configuration rationale
- Multi-region deployment patterns
- Rolling update strategy for relay binary upgrades

#### Scaling (~2,000 words, currently ~500)

**Add:**
- Capacity planning worksheet: "given X messages/sec, you need Y"
- Detailed benchmarks with methodology (not just numbers)
- PostgreSQL tuning parameters that impact outbox performance
- Partitioning strategy for very high volumes (with SQL examples)
- Connection pool sizing formula
- When to split outboxes vs. scale relay instances (decision tree)
- Cost modeling: PostgreSQL IOPS vs. messages published

#### Maintenance (~1,800 words, currently ~750 across 2 files)

**Add:**
- Upgrade procedure with zero-downtime strategy explained step by step
- Backup strategy for different deployment models (physical, logical, PITR)
- Retention policy tuning: how to choose the right retention window
- Message cleanup mechanics explained
- Disk usage monitoring and capacity planning
- Version compatibility matrix with clear support policy

#### Troubleshooting (~2,000 words, currently ~400)

**Add:**
- Structured diagnostic flowchart: symptom → probable cause → fix
- Every common problem gets its own section with: symptoms, diagnosis SQL,
  root cause explanation, fix steps, prevention tips
- "The relay connects but no messages flow" deep debug walkthrough
- "Messages are duplicated downstream" investigation procedure
- "Consumer lag is growing" systematic investigation
- "Extension upgrade failed" recovery procedure
- Log message glossary: what each WARNING/ERROR log line means

#### End-to-End Pipeline Tutorial (~3,000 words, currently ~700 across 2 files)

**Add:**
- Complete scenario: "Building an order fulfillment notification system"
- Forward pipeline: order events → Kafka → downstream consumers
- Reverse pipeline: payment confirmations from NATS → inbox → status update
- Docker Compose file for the complete environment
- Step-by-step with verification at every stage
- "What happens when things fail" section with simulated failures
- Cleanup and teardown instructions

#### Bidirectional Sync (~2,000 words, currently ~500)

**Add:**
- Extended use case: two microservices maintaining synchronized state
- Loop prevention explained with concrete examples and SQL
- Conflict resolution patterns
- Monitoring bidirectional flows for consistency
- Performance implications of bidirectional setups

#### Fan-out Pattern (~1,800 words, currently ~400)

**Add:**
- Complete example: one order event → Kafka + webhook + analytics inbox
- How independent consumer groups achieve independent progress
- Handling partial fan-out failures
- Monitoring per-destination lag independently
- Comparison with fan-out at the broker level

#### Dead-Letter Queue (~2,000 words, currently ~600)

**Add:**
- DLQ philosophy: why you need one, what goes there
- Investigation workflow: how to examine failed messages
- Replay strategies: single message, batch, filtered
- Automated DLQ processing patterns
- Integration with alerting (complete PagerDuty example)
- DLQ retention and cleanup policies

#### Real-World Scenarios (NEW — ~3,500 words)

This is a new page that demonstrates pg_tide in realistic business contexts.

**Sections:**
- **E-commerce order pipeline:** Payment processing, inventory updates, shipping notifications, email confirmations — all driven by outbox events
- **Multi-tenant SaaS webhook delivery:** Publishing tenant-specific webhooks with retry, DLQ, and per-tenant rate limiting considerations
- **Event-driven data warehouse loading:** Using the inbox to receive CDC events from upstream services and loading them into analytics tables
- **Microservice choreography:** Coordinating a multi-step business process across 3–4 services using outbox + inbox without a central orchestrator
- **Audit trail and compliance logging:** Using the outbox as an immutable audit log that's relayed to long-term storage

#### SQL Reference Pages (each expanded by ~400–600 words)

**Add to each API page:**
- "When to use this function" context paragraph
- Complete usage scenario (not just parameter tables)
- Common patterns and idioms
- Error cases and how to handle them
- Performance considerations for each function

**Add to Catalog Tables:**
- Query cookbook: 15–20 useful monitoring/debugging queries
- Index strategy explanation
- Retention and cleanup mechanics
- How tables relate to each other (with diagram)

#### Security (~1,800 words, currently ~450)

**Add:**
- Threat model overview: what attacks pg_tide protects against
- Role-based access control patterns (GRANT examples)
- Payload encryption strategies for sensitive data
- Network security architecture diagram
- Secrets management patterns (Vault, K8s secrets, env vars)
- Compliance considerations (GDPR, SOC2 implications for message storage)

---

## 4. New Content to Create

### 4.1 Real-World Scenarios Page

Already described above. This is the highest-priority new content as it bridges
the gap between "I understand the API" and "I know how to use this in my project."

### 4.2 Migration Guides (in appropriate existing pages)

Add migration sections within relevant pages:
- "Coming from pg_notify" → in concepts/message-guarantees.md
- "Coming from Debezium" → in evaluate/choosing-pg-tide.md
- "Coming from a DIY outbox table" → in getting-started/first-pipeline.md

### 4.3 Glossary (add to introduction.md)

Define key terms used throughout the docs:
- Outbox, Inbox, Relay, Pipeline, Consumer Group, Offset, Lease, DLQ, Sink, Source,
  Forward pipeline, Reverse pipeline, Advisory lock, Relay group ID, Batch, Envelope

---

## 5. Style and Tone Guidelines

### Current tone (too terse)

> "Consumer groups let you track which messages have been consumed."

### Target tone (explanatory, engaging)

> "A consumer group is like a bookmark in a shared book. Multiple services might be
> interested in the same stream of outbox messages — one service sends emails, another
> updates a search index, a third feeds an analytics pipeline. Each of these services
> needs to track its own progress independently. That's exactly what a consumer group
> provides: an independent offset that records how far a particular consumer has read
> through the outbox. If the email service crashes and restarts, it picks up right where
> it left off, without replaying messages that the analytics service has already processed."

### Specific style rules

1. **Lead with motivation.** Every section starts with the reader's problem.
2. **One concept per paragraph.** Don't pack multiple ideas into a wall of text.
3. **Use analogies.** Relate messaging concepts to familiar systems (libraries,
   post offices, assembly lines).
4. **Active voice.** "The relay polls the outbox" not "The outbox is polled by the relay."
5. **Second person.** "When you publish a message…" not "When one publishes a message…"
6. **Annotated code.** Every code block has at minimum a one-line comment per
   non-trivial line, plus a prose explanation following the block.
7. **Progressive disclosure.** Start simple, add complexity. Each section should be
   readable on its own but reveal more depth as you continue.
8. **Be honest about limitations.** If something isn't exactly-once under all
   conditions, say so clearly and explain the edge cases.

---

## 6. Visual Content

### Diagrams to add (Mermaid)

| Location | Diagram Type | Shows |
|----------|-------------|-------|
| Introduction | System overview | PostgreSQL ↔ Extension ↔ Relay ↔ Sinks |
| Architecture | Sequence diagram | Full message lifecycle (publish → relay → deliver → ack) |
| Architecture | Topology diagram | Single relay, multi-relay, multi-region |
| Message Guarantees | Timeline | Dual-write failure scenario vs. outbox solution |
| Message Guarantees | Sequence diagram | Exactly-once flow with inbox dedup |
| Consumption & Relay | State diagram | Consumer group states |
| Consumption & Relay | Timeline | Visibility lease expiry and redelivery |
| Error Handling | Flowchart | Error → retry → DLQ decision tree |
| Deployment | Component diagram | Full production deployment topology |
| Scaling | Graph | Throughput vs. batch size |
| Real-World Scenarios | Architecture diagrams | Per-scenario system overviews |

---

## 7. Implementation Phases

### Phase 1: Foundation (highest impact)

1. Consolidate file structure (merges from §2.1)
2. Rewrite Introduction (the first thing every reader sees)
3. Expand Getting Started → First Pipeline (the onboarding path)
4. Expand Message Guarantees (the core concept page)
5. Create Real-World Scenarios page

**Target: +20,000 words, -12 pages**

### Phase 2: Operational Depth

6. Expand Deployment (merged, with Docker Compose + K8s)
7. Expand Monitoring (with complete Grafana + alert examples)
8. Expand Troubleshooting (with diagnostic flowcharts)
9. Expand Error Handling (merged)
10. Expand Scaling (with capacity planning)

**Target: +12,000 words**

### Phase 3: Reference & Tutorials

11. Expand all SQL Reference pages (with cookbooks and scenarios)
12. Expand Backends page (merged, with decision matrix)
13. Expand End-to-End Pipeline tutorial
14. Expand remaining tutorials (bidirectional, fan-out, DLQ)
15. Expand Configuration (merged, with complete annotated examples)

**Target: +15,000 words**

### Phase 4: Polish

16. Add all Mermaid diagrams
17. Cross-reference audit (ensure all pages link appropriately)
18. Add glossary to introduction
19. Review and update SUMMARY.md navigation
20. Final tone/style pass for consistency across all pages

---

## 8. Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Total word count | ~18,500 | ~60,000 |
| Average words per page | ~400 | ~2,100 |
| Number of pages | 45 | ~28 |
| Code examples per page | ~2 | ~4–6 |
| Pages with diagrams | 0 | ~10 |
| Real-world scenarios documented | 0 | 5+ |
| New reader time-to-first-pipeline | ~15 min | ~10 min (despite more text, better flow) |

---

## 9. Pages Unchanged

These pages are already well-scoped and need only minor expansion:

- `integration/pgbouncer.md` — Already specific and practical (add ~200 words)
- `integration/cloudnativepg.md` — Platform-specific, adequate depth (add ~300 words)
- `integration/pg-trickle.md` — Migration-focused, clear (add ~200 words)
- `reference/security.md` — Solid coverage (expand to ~1,800 words)

---

## 10. Content That Could Be Removed

- `reference/changelog.md` — Pure duplicate of top-level CHANGELOG.md; just link to it
- `docs/src/SUMMARY.md` — Will be rewritten to match new structure (not "removed" but replaced)

---

## 11. Priorities Summary

**Most impactful improvements (do these first):**

1. Merge the three message-guarantee concept pages into one flowing narrative
2. Create the Real-World Scenarios page (bridges theory → practice)
3. Rewrite Introduction with system diagram and learning roadmap
4. Merge and expand the getting-started path into one cohesive tutorial
5. Consolidate backends into one comparison page with decision guidance

**Biggest audience expansion opportunities:**

- Explaining outbox pattern from scratch (for devs new to event-driven architecture)
- Complete Docker Compose "try it in 2 minutes" setup
- Migration guides from common starting points (pg_notify, DIY tables, Debezium)

**Biggest operational confidence builders:**

- Troubleshooting flowcharts with diagnostic SQL
- Complete monitoring setup with alert rules
- Capacity planning worksheet
