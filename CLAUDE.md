# CLAUDE.md

Guidance for working in this repo. Read alongside `project-scope.md` (what), `tech-stack.md` (how), `architecture.md` (how the pieces fit + the streams topology), and `implementation-plan.md` (build plan + event schemas, API surface, metric formulas).

## What this is

Real-time TCG card market intelligence platform. Ingests marketplace listings/sales as event streams and derives live market intelligence (prices, moving averages, volatility, arbitrage flags, spike alerts) with **Kafka Streams**. Cards are treated like exchange tickers. This is both a Kafka Streams learning vehicle and a usable tool; keep the MVP tight to the Core features in `project-scope.md`.

**MVP constraints:** Pokémon only, USD only, simple UI. The live feed is an in-house **simulator** that mimics the **TCGplayer API**; the backend polls it. Swapping in real TCGplayer later should mean repointing the poller only.

## Modules (Gradle multi-module monorepo)

| Module | Purpose |
|---|---|
| `common` | Shared event records, enums, `MarketKey` — **no framework deps** (plain Java + Jackson) |
| `backend` | One Spring Boot app: ingestion poller → Kafka, Streams topology, serving (REST + WS/SSE), Postgres sink consumer |
| `simulator` | Standalone Spring Boot service exposing a TCGplayer-shaped REST feed |
| `frontend` | Thin React/TS/Vite dashboard (Phase 6) |

The `backend` runs ingestion + streams + serving + sink in a single process (normal at this scale). The `simulator` is a separate process/container to keep the external-feed boundary honest.

## Commands

```bash
# Infra (Kafka KRaft + Postgres + topic creation)
docker compose up -d kafka postgres topic-init
docker compose logs topic-init        # confirm topics created
docker compose down                   # stop (add -v to wipe Postgres volume)

# Build / test (Windows: use gradlew.bat)
./gradlew build                       # compile + test all modules
./gradlew :backend:test               # one module's tests
./gradlew test --tests '*SomeTest*'   # a single test

# Run locally (host-exposed broker is localhost:29092)
./gradlew :backend:bootRun   --args='--spring.kafka.bootstrap-servers=localhost:29092'
./gradlew :simulator:bootRun

# Or run apps in containers
docker compose --profile apps up -d --build

# Health
curl http://localhost:8080/actuator/health   # backend
curl http://localhost:8081/actuator/health   # simulator
```

## Key facts & conventions

- **JDK:** target **Java 21 bytecode** (`sourceCompatibility`/`targetCompatibility` in root `build.gradle.kts`). The dev machine has JDK 23; that's fine — do **not** add a toolchain that forces a JDK 21 download. Docker build stages pin `gradle:8.12-jdk21`.
- **Gradle is not installed locally** — always use the wrapper (`./gradlew` / `gradlew.bat`, Gradle 8.12).
- **Package root:** `com.cardstream.*`.
- **Market key:** every market topic is keyed by `MarketKey` serialized as `"{cardId}|{finish}|{condition}"`. Use `MarketKey.asString()` / `MarketKey.parse()`, never hand-built strings.
- **Enums** (in `common`): `Finish` = NORMAL/HOLOFOIL/REVERSE_HOLOFOIL; `Condition` = NM/LP/MP/HP/DMG (TCGplayer grades); `Game` = POKEMON (MTG/ONE_PIECE later).
- **Topics:** `listings`, `sales`, `price-updates`, `agg-price-windowed`, `arbitrage`, `alerts` (6 partitions each), plus compacted `card-metadata`. Created by the `topic-init` compose service — add new topics there.
- **Serialization:** JSON (Jackson) for MVP. Avro + Schema Registry is the documented later upgrade — don't introduce it without discussing.
- **Money:** `BigDecimal`, never `double`.
- **Time:** `Instant` for event timestamps; the topology is **event-time** based.
- **Ports:** backend 8080, simulator 8081, Kafka host `localhost:29092` (in-network `kafka:9092`), Postgres 5432 (db/user/pass all `cardstream`).

## Architecture decisions (locked — see tech-stack.md)

- **Balanced topology:** card metadata via a **GlobalKTable** for in-stream enrichment; **watchlist filtering at the serving layer** (not in the topology).
- **Postgres sink** = a dedicated Spring Kafka **consumer** (not Kafka Connect).
- Arbitrage = **KStream–KTable** join of listings against the rolling market-average table (not stream-stream).
- **Kafka Streams wiring** (spring-kafka 3.2.x, context7-verified): `@EnableKafkaStreams` + a `KafkaStreamsConfiguration` bean; topology in `@Bean` methods taking an injected `StreamsBuilder`; event-time via a **custom `TimestampExtractor`** (not `WallclockTimestampExtractor`); serving uses **`KafkaStreamsInteractiveQueryService`** for IQ.
- Thresholds (spike 3σ, arbitrage 15% margin, `minSamples=20` cold-start gate) live in `application.yml` under `market.thresholds.*`.

## Testing

- Topology logic: **`TopologyTestDriver`** (no live broker).
- Integration: **Testcontainers** (Kafka + Postgres).
- Don't write tests that depend on a manually-running broker; the existing `BackendApplicationTests` points at an unused port and disables the Kafka health check so the context loads offline.

## Status

Phase 1 (metadata & catalog) complete and verified: recency-scoped Pokémon TCG API loader → Postgres (`card_set` + `card`, Flyway `V1__catalog.sql`) + compacted `card-metadata` topic; `GET /api/cards` (search/filter/paged) and `GET /api/cards/{cardId}`; on-demand `POST /api/admin/catalog/reload` (the loader is off at startup by default — `market.catalog.load-on-startup`). Verified end-to-end against live infra: a set's 122 cards landed in Postgres and the topic.

Phase 2 (simulator) complete and verified: standalone TCGplayer-shaped service (port 8081) that seeds its product universe from the backend's `/api/cards`, mints synthetic numeric `productId`s with a `productId↔cardId` map, and prices each SKU (product × finish × condition) via a geometric random walk. Endpoints: `/catalog/products`, `/pricing/{productId}`, pollable `/listings?since=` & `/sales?since=`, and `/admin/inject/{spike,arbitrage}` + `/admin/catalog/reload` (see implementation-plan.md Appendix E). Verified: seeded 122 products / 1510 SKUs, steady ~200 eps with `since=` polling, and injections produced observable spike/arbitrage anomalies.

Phase 3 (ingestion) complete and verified: multi-source poller in `backend` package `com.cardstream.backend.ingestion`. A `MarketDataSource` port + parameterized `TcgplayerRestSource` adapter (serves the `sim` feed now and real TCGplayer config-only later); a `SourceRegistry` builds enabled sources from `ingestion.sources.*`; a `SourcePoller` (programmatic `SchedulingConfigurer` fixed-delay loop, per-source poller-owned cursor, circuit breaker) dedupes (`RecentIdCache`), validates (`EventValidator` + `CatalogAllowlist` — the trust boundary), and produces to `listings`/`sales` keyed by `MarketKey`, tagged `source`. Kafka JSON values use the app `ObjectMapper` (ISO-8601, no type headers; `KafkaProducerConfig`) and an idempotent producer; `@EnableScheduling` is on the app. `GET /api/admin/ingestion/status` + `cardstream.ingestion.*` meters expose per-source counts. Verified end-to-end: ~200+ eps from `sim` into the topics (ISO-8601, `source=sim`), circuit breaker self-healed, and a tightened price bound forced rejects (counted, none ingested).

Phase 4 (Kafka Streams topology) — complete and verified (TopologyTestDriver + live end-to-end). In `backend` package `com.cardstream.backend.streams`: `MarketTopology` (a plain, test-driveable class built into the Spring-managed `StreamsBuilder` by `KafkaStreamsTopologyConfig` — `@EnableKafkaStreams` + a `KafkaStreamsConfiguration` bean, custom event-time `EventTimeExtractor`, no record caching). Operators: hourly/daily tumbling + 24h hopping windowed aggregates (`MarketStats` incremental mean/stddev) suppressed to one settled emit per window → `agg-price-windowed`; arbitrage via a KStream(`listings`)–KTable(rolling-avg) join → `arbitrage`; spike via a `SpikeDetector` Processor-API node (compare-then-update against prior stats); spike+arbitrage alerts merged, enriched against a `card-metadata` GlobalKTable, branched by severity → `alerts`. Thresholds in `market.thresholds.*` (`ThresholdProperties`); new topic records `WindowedAggregate`/`ArbitrageFlag`/`WindowType` in `common`. Live-verified end-to-end against real infra: injected spike (Ho-Oh, σ≈28, HIGH) and arbitrage (Mega Pyroar ex, 33% discount, MED) produced correctly-enriched `alerts`/`arbitrage` records, and the hourly `agg-price-windowed` window emitted exactly one settled aggregate per key (avg/volatility/volume/sampleCount). Operational note: the single stream thread is the throughput ceiling — at very high ingestion rates the streams consumer lags unboundedly and event-time stalls, so demo/verify at modest rates (the simulator's default ~200 eps or lower).

Phase 5 (serving & query) complete and verified. In `backend` packages `com.cardstream.backend.serving` (REST, Interactive Queries, WS/SSE, watchlist) and `com.cardstream.backend.sink` (Postgres sink consumer + repositories): `MarketQueryService` wraps a `KafkaStreamsInteractiveQueryService` bean reading the topology's `arb-ref-stats` KTable store directly for live "current state" (`GET /api/market/{marketKey}`, and `GET /api/cards/{cardId}` layered with a `markets` array across finish/condition); `MarketSinkConsumer` (two `@KafkaListener`s on distinct consumer-group ids) sinks `agg-price-windowed`/`alerts` into Flyway-migrated `price_window`/`alert` tables (`V2__serving.sql`, idempotent upserts) and fans the same records out live; `HistoryController` (`/api/cards/{cardId}/history`, `/api/top-movers`) and `AlertQueryController` (`/api/alerts`, `/api/arbitrage`) read those tables; `/ws/alerts` (watchlist-scoped via `?userId=`) and `/sse/prices` push live; watchlist CRUD is `X-User-Id`-scoped. Verified end-to-end against live infra: IQ snapshots, a forced window-close streaming out over SSE and landing in Postgres, synthetic spike + organic arbitrage alerts streaming over a real WebSocket client (and correctly withheld from a watchlist-scoped connection with nothing watched), top-movers/history reads, and full watchlist round-trips. A Testcontainers-backed `ServingRepositoriesIT` covers the same repository behavior for CI.

Phase 6 (thin UI) complete and verified. `frontend/`: React + TS + Vite, `react-router-dom` for
the five pages (card list/search, card detail, top movers, arbitrage, alerts), Recharts for the
price chart. `api/client.ts`/`api/types.ts` mirror the REST DTOs exactly; `hooks/useAlertsFeed.ts`
and `hooks/usePriceStream.ts` wrap `/ws/alerts` and `/sse/prices`; `hooks/useUserId.ts` is the
anon localStorage id that scopes the watchlist. The Vite dev server proxies `/api`, `/ws`, `/sse`
to the backend (`vite.config.ts`) — one origin in the browser, no CORS config on the backend.
Verified end-to-end against live infra: catalog search/detail, live IQ market snapshots, live
`/sse/prices` aggregates, a GlobalKTable-enriched spike alert and real arbitrage flags delivered
over `/ws/alerts` matching the UI's `Alert` shape, and a full watchlist add/list/remove round trip
— all through the dev-server proxy. `npm run build`/`npm run lint` pass clean. **Next:** **Phase 7**
— testing & docs. Follow the phase order and "done when" bars in `implementation-plan.md`.

## Workflow

- **Before implementing anything that touches a library, framework, or external API, consult context7 for up-to-date docs** (`resolve-library-id` → `query-docs`), then build against what it returns. Applies to new code and to changing how an existing library is used (Spring Boot, Spring Kafka / Kafka Streams, Flyway, the Pokémon TCG API, the frontend stack, etc.). Don't rely on memory for API surface — verify first. Pin the version when known (e.g. Spring Boot 3.3.x).
- Follow the phase order and "done when" bars in `implementation-plan.md`.

## Conventions for changes

- Match the existing code's style and altitude; keep comments sparse and purposeful.
- When you add a topic, an event field, an endpoint, or a threshold, update the matching appendix in `implementation-plan.md` so the docs stay the source of truth.
- Commit/push only when asked. The repo is git-tracked; work lands on `master`.
