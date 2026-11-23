# Meaningful Projects

> Engineering projects focused on backend internals, distributed systems, architectural trade-offs, performance, fault tolerance, and production engineering.

These projects are intended to be built incrementally. Each begins with a functional implementation and evolves through increasingly complex architectural requirements.

The goal is to understand not only how to build software, but also how it behaves under concurrency, failures, load, network partitions, and horizontal scaling.

---

# 01. Rate Limiter & API Gateway

**Domain:** Networking · Backend Infrastructure · Distributed Systems

**Stack:** Java · Spring Boot · Spring WebFlux · Redis · PostgreSQL · Docker

### Problem Statement

Build a programmable API gateway with a distributed rate-limiting engine capable of enforcing traffic policies across multiple gateway instances.

The rate limiter should be designed as an independent component, with configurable algorithms and pluggable storage backends.

### Architecture

```text
                    Clients
                       │
                       ▼
               ┌───────────────┐
               │ API Gateway   │
               │               │
               │ Authentication│
               │ Rate Limiting│
               │ Routing       │
               │ Load Balancer │
               └───────┬───────┘
                       │
              ┌────────┴─────────┐
              │                  │
              ▼                  ▼
        Backend Service A  Backend Service B
              │                  │
              └────────┬─────────┘
                       │
                 ┌─────▼─────┐
                 │   Redis   │
                 │  Cluster  │
                 └───────────┘
```

### Implementation Scope

**Rate Limiting Engine**

Implement the following algorithms from scratch:

- Fixed Window Counter
- Sliding Window Log
- Sliding Window Counter
- Token Bucket
- Leaky Bucket

Each algorithm should implement a common interface, allowing runtime selection and benchmarking.

```java
public interface RateLimiter {
    RateLimitResult consume(RateLimitRequest request);
}
```

Investigate:

- Time complexity and memory complexity.
- Burst traffic handling.
- Clock precision and clock drift.
- Concurrent request behavior.
- Atomic state transitions.
- Distributed coordination.

**Distributed State Management**

- Redis-backed rate-limit state.
- Atomic execution using Lua scripts.
- TTL-based state expiration.
- Key design and cardinality.
- Atomic counter operations.
- Race conditions across gateway instances.
- Redis Cluster hash-slot considerations.

**Gateway**

- Reverse proxy implementation.
- Route matching and request forwarding.
- Header manipulation.
- API key authentication.
- Request and response filters.
- Connection pooling.
- Timeouts.
- Load balancing.
- Circuit breakers.
- Retry policies.
- Request correlation IDs.

**Policy Engine**

Support configurable policies based on:

- IP address.
- API key.
- User identity.
- Tenant.
- Route.
- HTTP method.

Example:

```yaml
policies:
  - name: public-api
    key: ip
    algorithm: sliding-window-counter
    limit: 100
    window: 60s

  - name: authenticated-api
    key: user-id
    algorithm: token-bucket
    capacity: 50
    refill-rate: 10
```

### Engineering Experiments

- Compare algorithm behavior under burst traffic.
- Benchmark throughput and latency with varying concurrency.
- Measure Redis round-trip latency.
- Compare local and distributed rate limiting.
- Simulate Redis outages.
- Analyze fail-open vs fail-closed behavior.
- Benchmark Lua scripts versus alternative atomic implementations.
- Measure gateway overhead compared to direct service calls.

### Observability

- Requests per second.
- Rejected request rate.
- Rate-limit decision latency.
- Redis operation latency.
- Gateway p50, p95, p99 latency.
- Active connections.
- Downstream service latency.
- Circuit breaker state.

### Deliverables

- Rate-limiting library.
- Gateway application.
- Policy configuration system.
- Load-testing suite.
- Benchmark results.
- Architecture Decision Records (ADRs).

---

# 02. Distributed Notification Engine

**Domain:** Distributed Systems · Event-Driven Architecture · Messaging Infrastructure

**Stack:** Java · Spring Boot · Apache Kafka · PostgreSQL · Redis · Docker

### Problem Statement

Build an asynchronous notification processing platform capable of ingesting, scheduling, routing, processing, and delivering high volumes of notifications across multiple providers.

The system should tolerate transient failures, duplicate messages, consumer crashes, provider outages, and traffic spikes without losing notification requests.

### Architecture

```text
                       API Clients
                           │
                           ▼
                 ┌───────────────────┐
                 │ Notification API  │
                 └─────────┬─────────┘
                           │
                 ┌─────────▼─────────┐
                 │ Notification DB   │
                 │    PostgreSQL     │
                 └─────────┬─────────┘
                           │
                 ┌─────────▼─────────┐
                 │  Outbox Relay     │
                 └─────────┬─────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │   Kafka Cluster  │
                  │                  │
                  │ notification.*   │
                  └────────┬─────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
    Email Consumer    SMS Consumer    Push Consumer
          │                │                │
          ▼                ▼                ▼
    Email Provider    SMS Provider    Push Provider
          │                │                │
          └────────────────┼────────────────┘
                           ▼
                  Delivery Status Store
```

### Implementation Scope

**Notification Ingestion**

- REST API for notification submission.
- Bulk notification ingestion.
- Validation and normalization.
- Template rendering.
- User preference evaluation.
- Idempotency keys.
- Priority classification.

**Event Pipeline**

Design Kafka topics and partitions for:

```text
notification.requested
notification.email
notification.sms
notification.push
notification.retry
notification.delivered
notification.failed
notification.dead-letter
```

Implement:

- Partition key selection.
- Consumer groups.
- Producer acknowledgments.
- Replication factor and ISR configuration.
- Consumer offset management.
- Manual vs automatic offset commits.
- Rebalancing behavior.
- Retry topics with delayed reprocessing.
- Dead-letter queue handling.

**Delivery Workers**

- Independent consumer pools per channel.
- Provider abstraction.
- Rate limiting per provider.
- Connection pooling.
- Bulkheads.
- Retry with exponential backoff and jitter.
- Circuit breakers.
- Timeout management.
- Graceful shutdown and in-flight processing.

**Reliability**

- Transactional Outbox Pattern.
- Idempotent consumers.
- Duplicate event detection.
- At-least-once delivery semantics.
- Delivery state machine.
- Poison message isolation.
- Retry exhaustion.
- Reconciliation jobs.
- Crash recovery.

**Scheduling**

- Delayed notifications.
- Scheduled notification execution.
- Time-zone aware delivery.
- Recurring notifications.
- Cancellation before dispatch.

### Data Model

```text
Notification
  ├── id
  ├── tenant_id
  ├── recipient
  ├── channel
  ├── template_id
  ├── payload
  ├── priority
  ├── status
  ├── scheduled_at
  ├── idempotency_key
  └── created_at

DeliveryAttempt
  ├── id
  ├── notification_id
  ├── provider
  ├── attempt_number
  ├── status
  ├── error_code
  ├── response_metadata
  └── attempted_at

OutboxEvent
  ├── id
  ├── aggregate_id
  ├── event_type
  ├── payload
  ├── published_at
  └── created_at
```

### Engineering Experiments

- Measure throughput with varying partition counts.
- Benchmark consumer scaling.
- Simulate consumer crashes before and after offset commits.
- Observe Kafka rebalancing.
- Simulate provider rate limits and outages.
- Measure queue buildup and recovery time.
- Analyze consumer lag.
- Test duplicate delivery.
- Evaluate ordering guarantees.
- Investigate backpressure and overload behavior.

### Observability

- Kafka consumer lag.
- Messages produced and consumed per second.
- Delivery success and failure rates.
- Retry distribution.
- Queue processing latency.
- Provider latency.
- Notification end-to-end latency.
- DLQ size.
- Worker utilization.

### Deliverables

- Notification ingestion API.
- Kafka event pipeline.
- Channel-specific workers.
- Delivery tracking subsystem.
- Retry and DLQ infrastructure.
- Load-testing suite.
- Failure-injection scenarios.
- Reliability and throughput report.

---

# 03. SC Alankar — Distributed E-commerce Management Platform

**Domain:** Product Engineering · Transactional Systems · Distributed Architecture

**Stack:** Java · Spring Boot · PostgreSQL · Redis · Kafka · Docker

### Problem Statement

Build a complete e-commerce management platform for SC Alankar, covering catalog management, inventory, pricing, customer management, checkout, payments, orders, and business operations.

The architecture should evolve from a modular monolith into selectively distributed services as scaling requirements and domain boundaries justify decomposition.

The emphasis is on transactional correctness, domain modeling, concurrency, and reliable workflows.

### Initial Architecture

```text
                    ┌──────────────────┐
                    │    Frontend      │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │    Backend API   │
                    │  Modular Monolith│
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
 ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
 │  Catalog     │    │  Inventory   │    │   Orders     │
 │  Module      │    │  Module      │    │   Module     │
 └──────────────┘    └──────────────┘    └──────────────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             ▼
                    ┌──────────────────┐
                    │   PostgreSQL     │
                    └──────────────────┘
```

### Implementation Scope

**Catalog & Pricing**

- Product and category management.
- Variants and SKU generation.
- Product attributes.
- Pricing rules.
- Discount policies.
- Search and filtering.
- Product media storage.

**Inventory**

- Inventory ledger.
- Stock reservation.
- Reservation expiration.
- Stock reconciliation.
- Inventory adjustments.
- Warehouse abstraction.
- Low-stock thresholds.

**Checkout & Order Processing**

Implement an order lifecycle:

```text
CREATED
   │
   ▼
PENDING_PAYMENT
   │
   ├──────────► PAYMENT_FAILED
   │
   ▼
PAYMENT_CONFIRMED
   │
   ▼
PROCESSING
   │
   ▼
SHIPPED
   │
   ▼
DELIVERED
```

Handle cancellation and refund transitions separately with explicit rules.

**Concurrency & Transactional Correctness**

Investigate and implement:

- Optimistic locking.
- Pessimistic locking.
- Transaction isolation levels.
- Deadlocks.
- Lost updates.
- Write skew.
- Atomic inventory reservation.
- Concurrent checkout.
- Idempotent order creation.

Test overselling under concurrent requests.

**Payment Subsystem**

- Payment gateway abstraction.
- Payment intent lifecycle.
- Webhook processing.
- Signature verification.
- Idempotent webhook handling.
- Refunds.
- Payment reconciliation.

**Event-Driven Workflows**

Publish domain events:

```text
OrderCreated
InventoryReserved
PaymentInitiated
PaymentConfirmed
OrderCancelled
InventoryReleased
RefundInitiated
OrderShipped
OrderDelivered
```

Implement:

- Transactional Outbox Pattern.
- Kafka event publishing.
- Consumer idempotency.
- Event versioning.
- Saga orchestration or choreography.
- Compensation workflows.

### Distributed Architecture Evolution

Once the modular monolith is functional, selectively extract components:

```text
                       ┌──────────────┐
                       │ API Gateway  │
                       └──────┬───────┘
                              │
           ┌──────────────────┼──────────────────┐
           ▼                  ▼                  ▼
     Catalog Service    Order Service    Customer Service
           │                  │                  │
           ▼                  ▼                  ▼
       Catalog DB         Order DB         Customer DB

           ┌──────────────────┼──────────────────┐
           ▼                  ▼                  ▼
    Inventory Service   Payment Service  Notification Engine
           │                  │                  │
           ▼                  ▼                  ▼
      Inventory DB        Payment DB         Kafka
```

Service decomposition should be driven by domain boundaries, scaling characteristics, deployment independence, and failure isolation—not merely by module count.

### Engineering Challenges

- Prevent overselling.
- Maintain consistency across order, inventory, and payment workflows.
- Handle partial failures.
- Implement compensating transactions.
- Prevent duplicate order creation.
- Handle webhook retries.
- Ensure event publication doesn't diverge from database state.
- Maintain auditability.
- Handle inventory reservation expiration.
- Design eventual consistency boundaries.

### Engineering Experiments

- Concurrent checkout load testing.
- PostgreSQL transaction isolation comparisons.
- Lock contention analysis.
- Index selection and query plan analysis.
- Redis caching strategies.
- Cache invalidation.
- Kafka event propagation latency.
- Modular monolith vs service decomposition.
- Failure injection across payment and inventory workflows.

### Observability

- Order conversion rate.
- Checkout latency.
- Payment success rate.
- Inventory reservation conflicts.
- DB connection pool utilization.
- Query latency.
- Kafka consumer lag.
- Event processing latency.
- Service error rates.
- Distributed traces across order workflows.

### Deliverables

- Complete e-commerce backend.
- Domain model and state machines.
- Database schema and migration history.
- API documentation.
- Transaction and concurrency test suite.
- Event-driven integration.
- Deployment infrastructure.
- Architecture Decision Records.
- Load and performance reports.

---

# Project Progression

| # | Project | Core Engineering Focus |
|---|---|---|
| 01 | Rate Limiter & API Gateway | Algorithms, networking, Redis atomicity, gateway infrastructure |
| 02 | Distributed Notification Engine | Kafka, messaging semantics, retries, idempotency, reliability |
| 03 | SC Alankar | Domain modeling, transactional correctness, distributed workflows, product engineering |

## Engineering Standards

Every project should have:

- Architecture diagrams.
- Documented API contracts.
- Database schema and migrations.
- Unit, integration, and concurrency tests.
- Docker-based local development.
- CI pipeline.
- Structured logging.
- Metrics and distributed tracing where applicable.
- Load-testing scripts.
- Failure-injection tests.
- Architecture Decision Records (ADRs).
- Documented benchmarks and trade-offs.

> Build the simplest architecture that satisfies the current requirements. Introduce complexity only when an experiment, bottleneck, or requirement justifies it.
