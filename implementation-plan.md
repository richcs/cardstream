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
- **Recency-scoped load (set-driven):** enumerate `/v2/sets?orderBy=-releaseDate`, keep sets with `releaseDate >= market.catalog.since-release-date` (default `2024-01-01`), then page `/v2/cards?q=set.id:<id>` per kept set. Set-driven so the `set` table is populated cleanly and older sets can be backfilled later by lowering the cutoff. Cutoff lives in `application.yml` under `market.catalog.*`.
- Flyway migration for `card` (+ `set`) tables.
- REST: `GET /api/cards` (search/filter), `GET /api/cards/{cardId}`.
- **Done when:** recent catalog seeded (sets since cutoff, low thousands of cards), search/filter works, `card-metadata` topic populated.

### Phase 2 — Simulator (TCGplayer-shaped feed)
- Standalone service, **seeded from the backend's `GET /api/cards`** at startup (retries while the backend warms up; reseed via `POST /admin/catalog/reload`). Shares `common` for the enums.
- **Product/SKU model:** each catalog card becomes a product with a **synthetic numeric `productId`** and a `productId ↔ cardId` map (exposed via `/catalog`); a **SKU** = product × finish × condition (TCGplayer's grain). Finishes are inferred from rarity (commons/uncommons → NORMAL + REVERSE_HOLOFOIL; rarer → + HOLOFOIL).
- Price engine: geometric (log-normal) random walk per **SKU**, seeded deterministically from rarity × finish × condition; money kept as `BigDecimal`. Generates listings & sales at a configurable rate (`simulator.feed.events-per-second`, default 200) into in-memory, retention-bounded buffers.
- TCGplayer-shaped REST: `GET /catalog/products[/{productId}]`, `GET /pricing/{productId}` (low/mid/high/market/directLow per SKU), `GET /listings?since=&limit=`, `GET /sales?since=&limit=` (ISO-8601 `since`). See Appendix E.
- Admin endpoints to **inject** a price spike (multiplicative jump + sale burst) or an arbitrage listing (single below-market listing) on demand: `POST /admin/inject/spike`, `POST /admin/inject/arbitrage`.
- **Done when:** simulator emits a steady, pollable stream; injection endpoints produce observable anomalies. ✅ Verified: seeded 122 products / 1510 SKUs from the backend, steady ~200+ eps with working `since=` polling, spike jumped a SKU 3× with a sale burst, arbitrage produced a 0.6× below-market listing in the feed.

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
| GET | `/api/cards?game=&set=&rarity=&q=&page=&pageSize=` | Search/filter catalog (`set` matches set id; returns `{items,page,pageSize,total}`) |
| GET | `/api/cards/{cardId}` | Card detail (current market state layered on in Phase 5); 404 if unknown |
| POST | `/api/admin/catalog/reload` | Seed/refresh the catalog from the Pokémon TCG API (sets since the configured cutoff) → Postgres + `card-metadata` topic |
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

All thresholds live in `application.yml` (`market.thresholds.*`). Catalog scope lives under `market.catalog.*` — `since-release-date` (default `2024-01-01`) bounds which sets the metadata loader ingests; lower it to backfill older cards.

---

## Appendix D — Postgres schema (serving store)

- `card_set(set_id PK, name, series, printed_total, total, release_date, logo_url, symbol_url, updated_at)` — `set` is reserved in SQL, so the table is `card_set`.
- `card(card_id PK, set_id FK→card_set, name, number, rarity, supertype, image_small, image_large, game, updated_at)`
- `price_window(market_key, window_type, window_start, avg_price, volume, ma, volatility, PK(market_key,window_type,window_start))`
- `alert(alert_id PK, type, severity, card_id, market_key, detail JSONB, ts)`
- `watchlist(user_id, card_id, created_at, PK(user_id, card_id))`

Migrations via Flyway on the backend classpath at `backend/src/main/resources/db/migration/` (default `classpath:db/migration`) — so they ship in the jar and run identically in the IDE, Docker, and tests. `V1__catalog.sql` creates `card_set` + `card`.

---

## Appendix E — Simulator (TCGplayer-shaped) contract

Standalone service on **port 8081**. Stands in for TCGplayer; the Phase 3 poller resolves `productId → cardId` via `/catalog`, maps `subTypeName → Finish`, and republishes to Kafka. Config under `simulator.*` (backend base-url, `feed.events-per-second`, `feed.sale-ratio`, `feed.retention`, `feed.default-limit`, `walk.sigma`/`drift`, `conditions`).

| Method | Path | Purpose |
|---|---|---|
| GET | `/catalog/products?page=&pageSize=` | Products (synthetic `productId`, `cardId`, name, set, rarity, SKUs); `{items,page,pageSize,total}` |
| GET | `/catalog/products/{productId}` | One product; 404 if unknown |
| GET | `/pricing/{productId}` | Per-SKU `lowPrice/midPrice/highPrice/marketPrice/directLowPrice`; 404 if unknown |
| GET | `/listings?since=&limit=` | Listing events newer than `since` (ISO-8601 instant; 400 if unparseable), capped at `limit` |
| GET | `/sales?since=&limit=` | Sale events, same semantics |
| POST | `/admin/inject/spike` | `{cardId, finish?, condition?, factor?=2.5, burst?=6}` → price jump + sale burst |
| POST | `/admin/inject/arbitrage` | `{cardId, finish?, condition?, factor?=0.7}` → one below-market listing |
| POST | `/admin/catalog/reload` | Reseed products from the backend catalog |

A **SKU** = product × finish × condition; `subTypeName` is the printing label (`Normal`/`Holofoil`/`Reverse Holofoil`). Feed event shapes (the poller maps these to the canonical `listings`/`sales` events in Appendix A):

```jsonc
// GET /listings item
{ "eventId": 16746, "productId": 500000, "skuId": 1000005, "subTypeName": "Reverse Holofoil",
  "condition": "NM", "price": 0.17, "quantity": 1, "sellerId": "s-0453", "listedAt": "2026-05-31T13:49:27Z" }

// GET /sales item
{ "eventId": 16802, "productId": 500000, "skuId": 1000000, "subTypeName": "Normal",
  "condition": "NM", "price": 0.84, "quantity": 1, "soldAt": "2026-05-31T13:49:05Z" }
```
