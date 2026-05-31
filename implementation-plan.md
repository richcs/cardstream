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
└── frontend/                    # React + TS + Vite
```
> `backend` is one Spring Boot process (Kafka Streams + REST + consumers co-located — normal for this scale). `simulator` is a separate process/container to keep the external-feed boundary honest. Flyway migrations live on the backend classpath at `backend/src/main/resources/db/migration/` (see Appendix D), not at the repo root.

---

## Milestones

### Phase 0 — Scaffolding & infra
- Monorepo, Gradle multi-module, `common` module, code style/CI.
- `docker-compose.yml`: Kafka (KRaft) **or** Redpanda, Postgres 17, (placeholders for backend/simulator/frontend).
- Topic creation (auto-create off in prod-style; create via init script): `listings`, `sales`, `price-updates`, `card-metadata` (compacted), `agg-price-windowed`, `arbitrage`, `alerts`.
- Spring Boot skeletons for `backend` and `simulator`; Actuator health.
- **Done when:** `docker compose up` brings up Kafka + Postgres; both apps start and report healthy; topics exist.

### Phase 1 — Metadata & catalog
- `metadata` package (in `backend`): load Pokémon TCG API → Postgres `card_set` + `card` tables; publish each card to the compacted `card-metadata` topic.
- **Recency-scoped load (set-driven):** enumerate `/v2/sets?orderBy=-releaseDate`, keep sets with `releaseDate >= market.catalog.since-release-date` (default `2024-01-01`), then page `/v2/cards?q=set.id:<id>` per kept set. Set-driven so the `card_set` table is populated cleanly and older sets can be backfilled later by lowering the cutoff. Cutoff lives in `application.yml` under `market.catalog.*`.
- Flyway migration (`V1__catalog.sql`) for `card_set` + `card`.
- REST: `GET /api/cards` (search/filter), `GET /api/cards/{cardId}`; on-demand `POST /api/admin/catalog/reload`.
- **Done when:** recent catalog seeded (sets since cutoff, low thousands of cards), search/filter works, `card-metadata` topic populated. ✅ Verified: a set's 122 cards landed in Postgres and the topic.

### Phase 2 — Simulator (TCGplayer-shaped feed)
- Standalone service, **seeded from the backend's `GET /api/cards`** at startup (retries while the backend warms up; reseed via `POST /admin/catalog/reload`). Shares `common` for the enums.
- **Product/SKU model:** each catalog card becomes a product with a **synthetic numeric `productId`** and a `productId ↔ cardId` map (exposed via `/catalog`); a **SKU** = product × finish × condition (TCGplayer's grain). Finishes are inferred from rarity (commons/uncommons → NORMAL + REVERSE_HOLOFOIL; rarer → + HOLOFOIL).
- Price engine: geometric (log-normal) random walk per **SKU**, seeded deterministically from rarity × finish × condition; money kept as `BigDecimal`. Generates listings & sales at a configurable rate (`simulator.feed.events-per-second`, default 200) into in-memory, retention-bounded buffers.
- TCGplayer-shaped REST: `GET /catalog/products[/{productId}]`, `GET /pricing/{productId}` (low/mid/high/market/directLow per SKU), `GET /listings?since=&limit=`, `GET /sales?since=&limit=` (ISO-8601 `since`). See Appendix E.
- Admin endpoints to **inject** a price spike (multiplicative jump + sale burst) or an arbitrage listing (single below-market listing) on demand: `POST /admin/inject/spike`, `POST /admin/inject/arbitrage`.
- **Done when:** simulator emits a steady, pollable stream; injection endpoints produce observable anomalies. ✅ Verified: seeded 122 products / 1510 SKUs from the backend, steady ~200+ eps with working `since=` polling, spike jumped a SKU 3× with a sale burst, arbitrage produced a 0.6× below-market listing in the feed.

### Phase 3 — Ingestion (multi-source) ✅
Designed around a **source port/adapter** so the topology never knows where events came from; sources are pluggable and can run **concurrently**.

Implemented in `backend` package `com.cardstream.backend.ingestion`: `MarketDataSource` port (`id()` + `poll(cursor)`); `TcgplayerRestSource` adapter (parameterized by base-url + auth, with per-source connect/read timeouts); `SourceRegistry` (builds enabled sources from config); `SourcePoller` (programmatic `SchedulingConfigurer` fixed-delay loop — simple-duration `@Scheduled` strings need Spring Framework 6.2, this app is on 6.1; per-source cursor owned poller-side, circuit breaker); `EventValidator` + `CatalogAllowlist` + `RecentIdCache` (the trust boundary); `MarketEventPublisher` (idempotent producer → `listings`/`sales` keyed by `MarketKey`).

- **Port:** a `MarketDataSource` interface — `id()` + `poll(cursor)` returning **already-normalized** canonical `Listing`/`Sale` events (Appendix A) plus the advanced cursor. Each adapter owns its own `productId → cardId` and `subTypeName → Finish` mapping, so normalization is the adapter's job, not the poller's.
- **Registry + poller:** a scheduled `SourcePoller` iterates the **enabled** sources from `ingestion.sources.*`, polls each with its own persisted **cursor** (per-feed max event timestamp), and produces to Kafka keyed by `MarketKey`. Each event is tagged with its `source`.
- **One adapter, two sources:** because the simulator is TCGplayer-shaped, a single `TcgplayerRestSource` adapter (parameterized by `base-url` + auth) serves both the simulator (`sim`) now and real TCGplayer later — adding TCGplayer is config-only. MVP enables `sim` only.
- **Dedup/idempotency:** keyed by `source + eventId` (a bounded per-source recent-id cache guards against `since=` overlap re-delivery); the Kafka producer is idempotent.
- **Source-agnostic downstream:** `MarketKey` (and thus windows, joins, aggregates) carry no source dimension — a card is one ticker across sources, which keeps cross-source aggregation/arbitrage open as a later option.
- Config sketch:
  ```yaml
  ingestion:
    poll-interval: 2s
    sources:
      sim:        { enabled: true,  type: tcgplayer-rest, base-url: http://localhost:8081 }
      tcgplayer:  { enabled: false, type: tcgplayer-rest, base-url: https://api.tcgplayer.com, api-key: ... }
  ```
- **Scaling / deployment:** MVP runs ingestion **in the backend process**, single instance, registry polling all enabled sources (add per-source timeouts + a circuit breaker so one bad source can't stall the loop). The same binary supports **one-instance-per-source** later by enabling a single source per instance — provided per-source cursor/dedup/metrics state stays source-keyed (no shared mutable state). The prerequisite for that split is **extracting ingestion into its own deployable** (separate app/profile), since the backend process also owns the single-logical Streams topology + serving. Safe to scale because ingestion are **producers** (not bound by the 6-partition limit) and every event is keyed by `MarketKey` (per-ticker ordering preserved); the only rule is **one owner per source** — never two instances polling the same source without sharding.
- **Trust boundary & input validation:** the ingestor is the trust boundary — every field a source returns is untrusted, and poisoned input becomes corrupted prices/alerts/arbitrage. The adapter validates (and quarantines on failure) **before** producing to Kafka:
  - **Catalog allowlist** — only ingest events whose resolved `cardId` exists in the catalog; drop unknown cards (kills key-forgery, junk tickers, and unbounded state-store cardinality). Optionally scope each source to a declared card universe — the main mitigation for the source-agnostic `MarketKey` (any trusted source can otherwise move *any* ticker).
  - **Value checks** — `price` positive/finite/scale-bounded within sane min–max; `quantity` positive/bounded; reject NaN/negative/zero. Sanitize/reject the `|` delimiter and control chars in identifiers; validate `finish`/`condition` against the enums.
  - **Timestamp clamping** — the custom `TimestampExtractor` rejects/clamps event times outside a plausible skew window (not far-future, not absurdly old) so a source can't prematurely advance stream time and close windows early; derive the poller cursor so a future-dated event can't skip later legitimate ones.
  - **Transport & payload limits** — HTTPS + cert verification for real sources; connect/read timeouts (anti slow-loris); max response size + Jackson `StreamReadConstraints` (nesting/array/string caps); bounded connection pool and max-events-per-poll.
  - **Dedup & isolation** — `source + eventId` dedup with a bounded cache; idempotent producer; cursor owned poller-side; per-source timeouts + circuit breaker; a quarantine/dead-letter path for rejected events.
  - **Defense in depth + detection** — the `minSamples=20` gate + window suppression already blunt single-event poisoning; consider clamping extreme outliers before aggregation. Emit per-source metrics (reject rate, parse errors, out-of-bounds counts, event rate) and alert when a feed's own behavior shifts. Note: channel auth (API key/mTLS) proves *who* sent data, not that it's *correct* — it complements, never replaces, validation.
- **Done when:** events flow to Kafka at target rate from the `sim` source (tagged `source=sim`); a second source can be enabled by config alone; out-of-bounds / unknown-card / malformed events are rejected (and counted) rather than ingested; consumer-lag and counts visible in Actuator/metrics. ✅ Verified end-to-end against live infra: ~200+ eps from `sim` into `listings`/`sales` keyed by `MarketKey` and tagged `source=sim` (ISO-8601 timestamps matching Appendix A); circuit breaker opened on a down feed and self-healed; `GET /api/admin/ingestion/status` and `cardstream.ingestion.*` meters show per-source ingested/rejected/duplicate counts; a tightened price bound forced every event to `price_out_of_bounds` (ingested 0, rejected counted) while the cursor still advanced. (Consumer-lag becomes relevant once the Phase 4 Streams topology consumes these topics.)

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

`source` identifies the ingestion source (e.g. `"sim"`, `"tcgplayer"`); dedup is keyed by `source + eventId`. `marketKey` stays source-agnostic so a card is one ticker across sources.

```jsonc
// listings
{ "eventId": "uuid", "source": "sim", "cardId": "base1-4", "finish": "HOLOFOIL", "condition": "NM",
  "price": 412.50, "quantity": 2, "sellerId": "s-123", "listedAt": "2026-05-30T12:00:00Z" }

// sales
{ "eventId": "uuid", "source": "sim", "cardId": "base1-4", "finish": "HOLOFOIL", "condition": "NM",
  "price": 405.00, "quantity": 1, "soldAt": "2026-05-30T12:01:30Z" }

// price-updates
{ "eventId": "uuid", "source": "sim", "cardId": "base1-4", "finish": "HOLOFOIL", "condition": "NM",
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

**Serialization** — Kafka JSON values are produced with the application `ObjectMapper` (`KafkaProducerConfig`): timestamps as **ISO-8601** instants (not epoch numbers) and **no `__TypeId__` headers** (consumers deserialize with explicit target types). The producer is **idempotent** (`acks=all`).

---

## Appendix B — API surface (MVP)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/cards?game=&set=&rarity=&q=&page=&pageSize=` | Search/filter catalog (`set` matches set id; returns `{items,page,pageSize,total}`) |
| GET | `/api/cards/{cardId}` | Card detail (current market state layered on in Phase 5); 404 if unknown |
| POST | `/api/admin/catalog/reload` | Seed/refresh the catalog from the Pokémon TCG API (sets since the configured cutoff) → Postgres + `card-metadata` topic |
| GET | `/api/admin/ingestion/status` | Per-source ingestion health: cursor positions, ingested/rejected/duplicate counts, circuit state |
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
