# Implementation Plan — Real-Time TCG Card Market Intelligence Platform

Build plan derived from [`project-scope.md`](project-scope.md) and [`tech-stack.md`](tech-stack.md). MVP: **Pokémon only, USD only**, in-house simulator mimicking the **TCGplayer API**, balanced Kafka-Streams topology, thin UI.

Resolved architecture decisions feeding this plan:
- **Metadata** in a GlobalKTable for in-stream enrichment; **watchlist filtering** at the serving layer.
- **Postgres sink** = dedicated Spring Kafka consumer.
- **Monorepo**: multi-module backend + standalone simulator + `/frontend`.
- **Scale**: ~2k cards, ~100–500 events/sec, 6 partitions.

---

## Architecture overview

```
 Pokémon TCG API ──(metadata loader)──► Postgres catalog + compacted card-metadata topic
                                                                    │ (GlobalKTable)
 Simulator (TCGplayer-shaped REST API) ◄──poll── Ingestion poller ──► Kafka: listings / sales / price-updates
                                                                    │
                                                          Kafka Streams topology
                                            (windows · MA/volatility · arbitrage join ·
                                             spike · suppression · branching · enrichment)
                                                          │                    │
                                       state stores (Interactive Queries)   alerts + aggregate topics
                                                          │                    │
                                              Serving app: REST + WS/SSE   sink consumer ──► Postgres (history)
                                                          │
                                                   Thin React/TS UI
```

Data flow rationale: the simulator exposes a **TCGplayer-shaped REST API**; the backend's **ingestion poller** polls it and republishes to Kafka. Swapping in real TCGplayer later means repointing the poller — topology untouched.

---

## Repo layout

```
cardstream/
├── settings.gradle.kts          # multi-module
├── build.gradle.kts
├── docker-compose.yml           # kafka(KRaft)/redpanda, postgres, backend, simulator, frontend
├── common/                      # shared event models, key types, Serdes, enums
├── backend/                     # Spring Boot app: ingestion + streams + serving + sink consumer
│   ├── ingestion/               # poller → Kafka producers
│   ├── streams/                 # Kafka Streams topology
│   ├── serving/                 # REST controllers, WS/SSE, Interactive Queries, watchlist
│   ├── sink/                    # Kafka→Postgres consumer
│   └── metadata/                # Pokémon TCG API loader + catalog
├── simulator/                   # standalone Spring Boot: TCGplayer-shaped API + price engine
├── frontend/                    # React + TS + Vite
└── db/migration/                # Flyway SQL
```
> `backend` is one Spring Boot process (Kafka Streams + REST + consumers co-located — normal for this scale). `simulator` is a separate process/container to keep the external-feed boundary honest.

---

## Milestones

### Phase 0 — Scaffolding & infra
- Monorepo, Gradle multi-module, `common` module, code style/CI.
- `docker-compose.yml`: Kafka (KRaft) **or** Redpanda, Postgres 16, (placeholders for backend/simulator/frontend).
- Topic creation (auto-create off in prod-style; create via init script): `listings`, `sales`, `price-updates`, `card-metadata` (compacted), `agg-price-windowed`, `arbitrage`, `alerts`.
- Spring Boot skeletons for `backend` and `simulator`; Actuator health.
- **Done when:** `docker compose up` brings up Kafka + Postgres; both apps start and report healthy; topics exist.

### Phase 1 — Metadata & catalog
- `metadata` module: load Pokémon TCG API → Postgres `card` table; publish each card to compacted `card-metadata` topic.
- Flyway migration for `card` (+ `set`) tables.
- REST: `GET /api/cards` (search/filter), `GET /api/cards/{cardId}`.
- **Done when:** catalog seeded (~2k cards), search/filter works, `card-metadata` topic populated.

### Phase 2 — Simulator (TCGplayer-shaped feed)
- Standalone service seeded from the catalog (shares `common` + reads Pokémon TCG API or a snapshot).
- Price engine: geometric random walk per `(cardId, finish, condition)`; generates listings & sales at configurable rate (~100–500 eps).
- TCGplayer-shaped REST: `GET /pricing/{productId}`, `GET /listings?since=`, `GET /sales?since=` (and a catalog endpoint).
- Admin endpoints to **inject** a price spike or an arbitrage listing on demand (for deterministic demos).
- **Done when:** simulator emits a steady, pollable stream; injection endpoints produce observable anomalies.

### Phase 3 — Ingestion
- `ingestion` module: scheduled poller hits the simulator's REST endpoints, diffs/normalizes, and produces `listing` / `sale` / `price-update` events to Kafka keyed by `(cardId, finish, condition)`.
- Idempotency/dedup by `eventId`; map simulator fields → canonical event schema (Appendix A).
- **Done when:** events flow to Kafka at target rate; consumer-lag and counts visible in Actuator/metrics.

### Phase 4 — Kafka Streams topology (the core)
- Windowed aggregation: hourly **tumbling** + daily; **hopping** (24h advancing 1h) for moving average/volatility.
- Moving average & volatility (stddev) over the trailing window per card.
- **Arbitrage**: KStream(`listings`)–KTable(rolling avg) join → `arbitrage` topic when `price < (1−0.15)×avg` and `sampleCount ≥ minSamples`.
- **Spike**: emit to `alerts` when `|latest − mean| > 3σ` and `sampleCount ≥ minSamples`; severity by deviation magnitude.
- **Suppression**: one settled aggregate per window (`Suppressed.untilWindowCloses`).
- **Branching**: split alerts by severity; structure topology so per-game routing is ready.
- **Enrichment**: GlobalKTable(`card-metadata`) join so alerts/aggregates carry card name/set/image.
- **Spring integration** (context7-verified for spring-kafka 3.2.x): `@EnableKafkaStreams` + a `KafkaStreamsConfiguration` bean (name = `KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME`); define the topology in `@Bean` methods taking an injected `StreamsBuilder` (lifecycle managed by `StreamsBuilderFactoryBean`). Set `DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG` to a **custom event-time `TimestampExtractor`** that reads `soldAt`/`listedAt` (do *not* use `WallclockTimestampExtractor`).
- **Done when:** TopologyTestDriver tests pass for each operator; injected spike/arbitrage from Phase 2 produce correct alerts end-to-end.

### Phase 5 — Serving & query
- Interactive Queries via Spring Kafka's **`KafkaStreamsInteractiveQueryService`** (3.2+ facade — don't hand-roll store/host lookup): `GET /api/market/{marketKey}`, `GET /api/cards/{cardId}` current state across finishes/conditions.
- `sink` consumer → Postgres windowed-aggregate + alert tables (Flyway migrations).
- History/derived endpoints: `/api/cards/{cardId}/history`, `/api/top-movers`, `/api/arbitrage`, `/api/alerts` (Appendix B).
- WS/SSE feeds: `/ws/alerts`, `/sse/prices`; watchlist CRUD (`/api/watchlist`, `X-User-Id` header); serving-layer filter of alert feed by a user's watchlist.
- **Done when:** all endpoints return correct data; alert feed pushes live; watchlist scoping works.

### Phase 6 — Thin UI
- React + TS + Vite: card list/search, card detail with live price chart + history, top movers, arbitrage feed, alert feed with severity filter, watchlist toggle.
- Live updates via WebSocket/SSE; minimal styling.
- **Done when:** dashboard demos the streams live against the simulator.

### Phase 7 — Testing, observability, docs
- Unit (TopologyTestDriver), integration (Testcontainers: Kafka + Postgres), a thin e2e (inject anomaly → assert alert + UI/API).
- Micrometer metrics, Streams state/lag dashboards (optional Prometheus/Grafana).
- README + run instructions; revisit JSON→Avro upgrade.
- **Done when:** green test suite; `docker compose up` yields a working demo from a clean checkout.

---

## Appendix A — Event schemas (JSON, MVP)

Key (all market topics): `marketKey = "{cardId}|{finish}|{condition}"`.

```jsonc
// listings
{ "eventId": "uuid", "cardId": "base1-4", "finish": "HOLOFOIL", "condition": "NM",
  "price": 412.50, "quantity": 2, "sellerId": "s-123", "listedAt": "2026-05-30T12:00:00Z" }

// sales
{ "eventId": "uuid", "cardId": "base1-4", "finish": "HOLOFOIL", "condition": "NM",
  "price": 405.00, "quantity": 1, "soldAt": "2026-05-30T12:01:30Z" }

// price-updates
{ "eventId": "uuid", "cardId": "base1-4", "finish": "HOLOFOIL", "condition": "NM",
  "oldPrice": 420.00, "newPrice": 412.50, "listingId": "l-987", "updatedAt": "2026-05-30T12:00:05Z" }

// card-metadata (compacted; key = cardId)
{ "cardId": "base1-4", "name": "Charizard", "set": "Base", "rarity": "Rare Holo",
  "game": "POKEMON", "imageUrl": "https://..." }

// alerts
{ "alertId": "uuid", "type": "SPIKE | ARBITRAGE", "severity": "LOW|MED|HIGH",
  "cardId": "base1-4", "marketKey": "base1-4|HOLOFOIL|NM", "name": "Charizard",
  "detail": { "...": "..." }, "ts": "2026-05-30T12:01:31Z" }
```

**Enums** — `finish`: `NORMAL | HOLOFOIL | REVERSE_HOLOFOIL`; `condition`: `NM | LP | MP | HP | DMG` (TCGplayer grades); `game`: `POKEMON` (MTG, ONE_PIECE later).

---

## Appendix B — API surface (MVP)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/cards?game=&set=&rarity=&condition=&q=&page=` | Search/filter catalog |
| GET | `/api/cards/{cardId}` | Card detail + current market state (all finishes/conditions) |
| GET | `/api/cards/{cardId}/history?window=hourly\|daily&from=&to=` | Price/volume history |
| GET | `/api/market/{marketKey}` | Current state for one ticker (Interactive Query) |
| GET | `/api/top-movers?window=daily&dir=gainers\|losers&limit=` | Biggest movers |
| GET | `/api/arbitrage?limit=` | Recent arbitrage flags |
| GET | `/api/alerts?severity=&type=&limit=` | Recent alerts |
| GET/POST/DELETE | `/api/watchlist` (`X-User-Id`) | Manage watchlist |
| WS | `/ws/alerts` | Live alert feed (filtered by watchlist when `userId` set) |
| SSE | `/sse/prices` | Live price/aggregate updates |

---

## Appendix C — Metric definitions & thresholds (configurable)

| Metric | Definition (default) |
|---|---|
| Hourly/daily aggregate | Tumbling window avg price + volume per `marketKey` |
| Moving average | Mean sale price over trailing 24h (hopping window, 1h advance) |
| Volatility | Stddev of sale prices over the same trailing 24h window |
| Top movers | % change of latest window avg vs. previous window (hourly & daily) |
| Spike | `|latest − windowMean| > 3σ`, gated by `minSamples` |
| Arbitrage | `listing.price < (1 − 0.15) × rollingAvg`, gated by `minSamples` |
| Cold start | `minSamples = 20` before spike/arbitrage can fire |

All thresholds live in `application.yml` (`market.thresholds.*`).

---

## Appendix D — Postgres schema (serving store)

- `card(card_id PK, name, set, rarity, game, image_url, ...)`
- `price_window(market_key, window_type, window_start, avg_price, volume, ma, volatility, PK(market_key,window_type,window_start))`
- `alert(alert_id PK, type, severity, card_id, market_key, detail JSONB, ts)`
- `watchlist(user_id, card_id, created_at, PK(user_id, card_id))`

Migrations via Flyway in `db/migration/`.
