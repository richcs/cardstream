# Tech Stack — Real-Time TCG Card Market Intelligence Platform

Derived from `project-scope.md`. Choices favor a backend- and Kafka-Streams-first MVP: Pokémon only, USD only, simple UI, simulated marketplace feed.

## At a glance

| Layer | Choice |
|---|---|
| Language / runtime | Java 21 (LTS) |
| Backend framework | Spring Boot 3.x |
| Stream processing | Kafka Streams (via Spring Kafka) |
| Broker | Apache Kafka (KRaft) or Redpanda |
| Serialization | JSON now → Avro + Schema Registry later |
| Serving store | PostgreSQL 17 (alpine) |
| Hot reads | Kafka Streams Interactive Queries |
| Push to UI | WebSocket / SSE (Spring) |
| Frontend | React + TypeScript + Vite (thin) |
| Metadata source | Pokémon TCG API (MVP) |
| Live feed | In-house simulator (mimics the TCGplayer API) |
| Local orchestration | Docker Compose |
| Build | Gradle (backend), npm/Vite (frontend) |

---

## Backend

- **Java 21 (LTS)** — Kafka Streams is JVM-native; 21 gives records, pattern matching, and virtual threads.
- **Spring Boot 3.x** — REST controllers, dependency injection, config, actuator/health.
- **Spring Kafka / `spring-kafka`** — wires the Kafka Streams topology into the Spring lifecycle (`StreamsBuilderFactoryBean`), plus producers/consumers for the ingestion path.
- **Spring Web (MVC)** — REST API over materialized market state.
- **Spring WebSocket + SSE** — push alert/price feeds to the thin UI.
- **springdoc-openapi** — auto-generated OpenAPI/Swagger docs for the REST + ingestion contracts.

## Stream processing

- **Kafka Streams** — the core of the project. Exercises:
  - Windowed aggregation (tumbling & hopping) for hourly/daily price & volume.
  - Rolling moving averages & volatility off the sales stream.
  - **KStream–KTable join** of listings against the rolling market-average table for arbitrage detection.
  - Spike/anomaly detection vs. window baseline.
  - Suppression (one settled value per window).
  - Branching by game and by alert severity.
- **State stores (RocksDB)** — backing windowed aggregates and the market-average table; queried via Interactive Queries.
- **Metadata enrichment** — card metadata is held in a **GlobalKTable** (from a compacted `card-metadata` topic) and joined in-stream so alerts/aggregates carry card names. **Watchlist filtering** is done at the serving layer (user data), not in the topology.

### Stream semantics & thresholds
- **Event-time processing** — small grace period per window (e.g. 1 min for hourly windows); suppression emits one settled value on window close.
- **Spike threshold** — price/volume deviates > 3σ (or a configurable %) from the window baseline.
- **Arbitrage threshold** — listing priced below `(1 − margin) × rolling average`, margin default 15%. All thresholds configurable.
- **Windows** — tumbling & hopping for hourly/daily aggregates.

## Domain model & keys

- **Card identity / Kafka key**: `(cardId, finish, condition)` — e.g. a foil NM printing is a distinct "ticker." Language and graded/PSA grading are out of scope for MVP.
- **Currency**: USD only (MVP).
- **User identity**: no login; an anonymous client-generated `userId` (browser localStorage, sent as a header) scopes watchlists.

## Messaging / broker

- **Apache Kafka in KRaft mode** — no ZooKeeper. **Redpanda** is an acceptable drop-in (Kafka-API compatible, lighter for local dev).
- **Topics**: `listings`, `sales`, `price-updates`, a compacted `card-metadata` topic, derived aggregate topics, and an `alerts` topic. 6 partitions, keyed by card identity `(cardId, finish, condition)`.
- **Scale target**: ~2k Pokémon cards, ~100–500 events/sec (medium); 6 partitions is sufficient, revisit if throughput grows.
- **Serialization**: JSON (Jackson Serdes) for MVP velocity. **Upgrade path:** Avro + **Confluent Schema Registry** (or Redpanda's) — documented as the recommended next step for schema evolution and a strong learning payoff.

## Storage

- **PostgreSQL 17 (alpine)** — historical queries and the serving store behind the dashboard.
- **Sink path**: a **dedicated Spring Kafka consumer** reads the aggregate/alert topics and writes to Postgres — less infra and full control. (Kafka Connect JDBC sink is the noted later alternative.)
- **Flyway** — schema migrations for the Postgres serving store.

## Frontend (thin)

- **React + TypeScript** with **Vite** — fast dev server, minimal config; matches the existing TS toolchain.
- **Charts**: a lightweight library (e.g. Recharts or uPlot) for live price charts and top movers.
- **Live updates**: native `WebSocket` / `EventSource` (SSE) — no heavy state-management library in MVP.
- Scope is intentionally minimal: price chart, top movers, arbitrage feed, alert feed. Polish is post-MVP.

## Data sources

- **Pokémon TCG API** (pokemontcg.io) — MVP card metadata (names, sets, rarities, images). Free with API key, clean REST.
- **Per-game metadata adapter** — normalizes each provider into one common card-metadata model so the platform stays game-agnostic. Future: **Scryfall** (MTG), community dataset/scrape (One Piece).
- **Marketplace simulator** (in-house service) — for MVP, emits `listing` / `sale` / `price-update` events conforming to the **real TCGplayer API contract**. Realistic random-walk pricing with injectable spikes & arbitrage for deterministic demos. Because it matches the TCGplayer contract, the real TCGplayer feed swaps in later by replacing only the producer/poller — the ingestion layer and stream topology stay untouched.

## Infrastructure & dev tooling

- **Repo layout** — monorepo: a Gradle multi-module backend (`common`, `backend`) plus a **standalone `simulator`** service and a `/frontend` folder. The simulator runs as its own container, standing in for the external TCGplayer feed.
- **Docker Compose** — single-broker Kafka/Redpanda, Postgres, backend, simulator, thin frontend; optional Schema Registry + Kafka Connect when adopting Avro.
- **Gradle** — backend build (Kotlin DSL).
- **JUnit 5 + `kafka-streams-test-utils` (`TopologyTestDriver`)** — unit-test the topology without a live broker.
- **Testcontainers** — integration tests against ephemeral Kafka + Postgres.
- **Spring Boot Actuator + Micrometer** — health, metrics, and Kafka Streams state/lag visibility (optional Prometheus/Grafana later).

## Explicitly not in the stack (MVP)

- No auth/identity provider — anonymous `userId` in localStorage scopes watchlists.
- No email/notification service — alerts go to the WebSocket/SSE feed only.
- No ML/prediction stack.
- No real marketplace integration yet — simulator stands in.
- No Kubernetes/cloud deploy — local Docker Compose only.

---

_Open decisions to confirm during build: JSON vs. Avro timing, Kafka vs. Redpanda for local dev, chart library (Recharts vs. uPlot)._
