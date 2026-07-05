# Implementation Plan — Real-Time TCG Card Market Intelligence Platform

Build plan derived from [`project-scope.md`](project-scope.md) and [`tech-stack.md`](tech-stack.md). MVP: **Pokémon only, USD only**, in-house simulator mimicking the **TCGplayer API**, balanced Kafka-Streams topology, thin UI.

**All 7 phases below are complete** — this doc is now the historical build record (what was built,
in what order, and how each phase was verified). For the living, keep-updated reference material
that used to live in this file's appendices — event schemas, REST/WS/SSE API surface, metric
thresholds, Postgres schema, the simulator contract — see [`reference.md`](reference.md). For *why*
things are built this way, see [`architecture.md`](architecture.md).

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
├── docker-compose.yml           # kafka (KRaft), postgres, backend + simulator (behind an `apps` profile)
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
> `backend` is one Spring Boot process (Kafka Streams + REST + consumers co-located — normal for this scale). `simulator` is a separate process/container to keep the external-feed boundary honest. Flyway migrations live on the backend classpath at `backend/src/main/resources/db/migration/` (see `reference.md`'s Postgres schema section), not at the repo root.

---

## Milestones

### Phase 0 — Scaffolding & infra
- Monorepo, Gradle multi-module, `common` module, code style/CI.
- `docker-compose.yml`: Kafka (KRaft) **or** Redpanda, Postgres 17, (placeholders for backend/simulator/frontend).
- Topic creation (auto-create off in prod-style; create via init script): `listings`, `sales`, `price-updates`, `card-metadata` (compacted), `agg-price-windowed`, `arbitrage`, `alerts`.
- Spring Boot skeletons for `backend` and `simulator`; Actuator health.
- **Done when:** `docker compose up` brings up Kafka + Postgres; both apps start and report healthy; topics exist.

### Phase 1 — Metadata & catalog ✅
- `metadata` package (in `backend`): load Pokémon TCG API → Postgres `card_set` + `card` tables; publish each card to the compacted `card-metadata` topic.
- **Recency-scoped load (set-driven):** enumerate `/v2/sets?orderBy=-releaseDate`, keep sets with `releaseDate >= market.catalog.since-release-date` (default `2024-01-01`), then page `/v2/cards?q=set.id:<id>` per kept set. Set-driven so the `card_set` table is populated cleanly and older sets can be backfilled later by lowering the cutoff. Cutoff lives in `application.yml` under `market.catalog.*`.
- Flyway migration (`V1__catalog.sql`) for `card_set` + `card`.
- REST: `GET /api/cards` (search/filter), `GET /api/cards/{cardId}`; on-demand `POST /api/admin/catalog/reload`.
- **Done when:** recent catalog seeded (sets since cutoff, low thousands of cards), search/filter works, `card-metadata` topic populated. ✅ Verified: a set's 122 cards landed in Postgres and the topic.

### Phase 2 — Simulator (TCGplayer-shaped feed) ✅
- Standalone service, **seeded from the backend's `GET /api/cards`** at startup (retries while the backend warms up; reseed via `POST /admin/catalog/reload`). Shares `common` for the enums.
- **Product/SKU model:** each catalog card becomes a product with a **synthetic numeric `productId`** and a `productId ↔ cardId` map (exposed via `/catalog`); a **SKU** = product × finish × condition (TCGplayer's grain). Finishes are inferred from rarity (commons/uncommons → NORMAL + REVERSE_HOLOFOIL; rarer → + HOLOFOIL).
- Price engine: geometric (log-normal) random walk per **SKU**, seeded deterministically from rarity × finish × condition; money kept as `BigDecimal`. Generates listings & sales at a configurable rate (`simulator.feed.events-per-second`, default 200) into in-memory, retention-bounded buffers.
- TCGplayer-shaped REST: `GET /catalog/products[/{productId}]`, `GET /pricing/{productId}` (low/mid/high/market/directLow per SKU), `GET /listings?since=&limit=`, `GET /sales?since=&limit=` (ISO-8601 `since`). See `reference.md`'s simulator contract section.
- Admin endpoints to **inject** a price spike (multiplicative jump + sale burst) or an arbitrage listing (single below-market listing) on demand: `POST /admin/inject/spike`, `POST /admin/inject/arbitrage`.
- **Done when:** simulator emits a steady, pollable stream; injection endpoints produce observable anomalies. ✅ Verified: seeded 122 products / 1510 SKUs from the backend, steady ~200+ eps with working `since=` polling, spike jumped a SKU 3× with a sale burst, arbitrage produced a 0.6× below-market listing in the feed.

### Phase 3 — Ingestion (multi-source) ✅
Designed around a **source port/adapter** so the topology never knows where events came from; sources are pluggable and can run **concurrently**.

Implemented in `backend` package `com.cardstream.backend.ingestion`: `MarketDataSource` port (`id()` + `poll(cursor)`); `TcgplayerRestSource` adapter (parameterized by base-url + auth, with per-source connect/read timeouts); `SourceRegistry` (builds enabled sources from config); `SourcePoller` (programmatic `SchedulingConfigurer` fixed-delay loop — simple-duration `@Scheduled` strings need Spring Framework 6.2, this app is on 6.1; per-source cursor owned poller-side, circuit breaker); `EventValidator` + `CatalogAllowlist` + `RecentIdCache` (the trust boundary); `MarketEventPublisher` (idempotent producer → `listings`/`sales` keyed by `MarketKey`).

- **Port:** a `MarketDataSource` interface — `id()` + `poll(cursor)` returning **already-normalized** canonical `Listing`/`Sale` events (see `reference.md`'s event schemas) plus the advanced cursor. Each adapter owns its own `productId → cardId` and `subTypeName → Finish` mapping, so normalization is the adapter's job, not the poller's.
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
- **Done when:** events flow to Kafka at target rate from the `sim` source (tagged `source=sim`); a second source can be enabled by config alone; out-of-bounds / unknown-card / malformed events are rejected (and counted) rather than ingested; consumer-lag and counts visible in Actuator/metrics. ✅ Verified end-to-end against live infra: ~200+ eps from `sim` into `listings`/`sales` keyed by `MarketKey` and tagged `source=sim` (ISO-8601 timestamps matching `reference.md`'s event schemas); circuit breaker opened on a down feed and self-healed; `GET /api/admin/ingestion/status` and `cardstream.ingestion.*` meters show per-source ingested/rejected/duplicate counts; a tightened price bound forced every event to `price_out_of_bounds` (ingested 0, rejected counted) while the cursor still advanced. (Consumer-lag becomes relevant once the Phase 4 Streams topology consumes these topics.)

### Phase 4 — Kafka Streams topology (the core) ✅
Implemented in `backend` package `com.cardstream.backend.streams`: a plain, test-driveable `MarketTopology` (built into the Spring-managed `StreamsBuilder` by `KafkaStreamsTopologyConfig`, `@EnableKafkaStreams` + a `KafkaStreamsConfiguration` bean), a custom event-time `EventTimeExtractor` (reads `soldAt`/`listedAt`), `MarketStats` (incremental count/sum/sumSq accumulator → mean/stddev), a `SpikeDetector` Processor-API node, `JsonSerdes` (app `ObjectMapper`, no type headers), and `ThresholdProperties` (`market.thresholds.*`). New topic schema records live in `common`: `WindowedAggregate`, `ArbitrageFlag`, `WindowType` (see `reference.md`'s event schemas).

- Windowed aggregation: hourly **tumbling** + daily; **hopping** (24h advancing 1h) for moving average/volatility.
- Moving average & volatility (stddev) over the trailing window per card.
- **Arbitrage**: KStream(`listings`)–KTable(rolling avg) join → `arbitrage` topic when `price < (1−0.15)×avg` and `sampleCount ≥ minSamples`.
- **Spike**: emit to `alerts` when `|latest − mean| > 3σ` and `sampleCount ≥ minSamples`; severity by deviation magnitude.
- **Suppression**: one settled aggregate per window (`Suppressed.untilWindowCloses`).
- **Branching**: split alerts by severity; structure topology so per-game routing is ready.
- **Enrichment**: GlobalKTable(`card-metadata`) join so alerts/aggregates carry card name/set/image.
- **Spring integration** (context7-verified for spring-kafka 3.2.x): `@EnableKafkaStreams` + a `KafkaStreamsConfiguration` bean (name = `KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME`); define the topology in `@Bean` methods taking an injected `StreamsBuilder` (lifecycle managed by `StreamsBuilderFactoryBean`). Set `DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG` to a **custom event-time `TimestampExtractor`** that reads `soldAt`/`listedAt` (do *not* use `WallclockTimestampExtractor`).
- **Done when:** TopologyTestDriver tests pass for each operator; injected spike/arbitrage from Phase 2 produce correct alerts end-to-end. ✅ Both bars met. TopologyTestDriver: per-operator tests green (spike fires past `minSamples`/σ and is suppressed below them; arbitrage flags a below-margin listing and respects the margin; hourly windowed aggregate emits exactly one settled result on window close; GlobalKTable enrichment attaches the card name). Live end-to-end (against real Kafka/Postgres + the simulator): injected spike (Ho-Oh, σ≈28 vs a 230-sample baseline → HIGH) and arbitrage (Mega Pyroar ex, listing 2.84 vs ref-avg 4.23 → 33% discount, MED) produced correctly-enriched records on `alerts` **and** `arbitrage`; the hourly `agg-price-windowed` window emitted exactly one settled aggregate per key (4680 keys, avg/volatility/volume/sampleCount; e.g. me4-15 avg 4.23 matched its arbitrage ref-avg). Note: the single stream thread caps throughput — at very high ingestion rates the streams consumer lags unboundedly and event-time stalls (windows never close); verify at modest rates. To exercise window emission without waiting for the wall-clock hour boundary, produce sales timestamped at the window end directly to `sales` (advances event-time, triggers `Suppressed` flush).

### Phase 5 — Serving & query ✅
Implemented in `backend` packages `com.cardstream.backend.serving` (REST, IQ, WS/SSE, watchlist) and
`com.cardstream.backend.sink` (the Postgres sink consumer + its repositories).

- Interactive Queries via Spring Kafka's **`KafkaStreamsInteractiveQueryService`** (a bean in
  `KafkaStreamsTopologyConfig`, wrapping the `defaultKafkaStreamsBuilder`): `MarketQueryService` reads
  the topology's `arb-ref-stats` KTable store directly, so "current state" is a live, unbounded running
  mean/stddev/last-price per ticker (not a settled window). `GET /api/market/{marketKey}` and
  `GET /api/cards/{cardId}` (layered with a `markets` array enumerating all finish/condition combos)
  both read off it; a not-yet-ready state store maps to `503`, an unknown ticker to `404`, a malformed
  key to `400`.
- `sink` consumer (`MarketSinkConsumer`, two `@KafkaListener`s on **distinct consumer-group ids** —
  sharing one group id across topics with different subscriptions is a Kafka pitfall) parses
  `agg-price-windowed`/`alerts` JSON manually with the app `ObjectMapper` (no type headers, consistent
  with the rest of the wire format) and upserts into `price_window`/`alert` (Flyway `V2__serving.sql`;
  idempotent — `ON CONFLICT` — so consumer restarts/redelivery never duplicate). `cardId` is recovered
  from `marketKey` via Postgres `split_part` rather than a redundant column.
- History/derived endpoints (`PriceWindowRepository`/`AlertRepository`, both sink-owned since they read
  what the sink writes): `/api/cards/{cardId}/history` (hourly/daily, cardId-prefix scan), `/api/top-movers`
  (a `LAG()`-windowed-function query ranking % change of latest vs. previous settled window),
  `/api/arbitrage` and `/api/alerts` (the same `alert` table; arbitrage is just `type=ARBITRAGE`, since
  Postgres only sinks the unified `alerts` topic — not the raw `arbitrage` topic — per `reference.md`'s Postgres schema section).
- WS/SSE feeds: `/ws/alerts` (`AlertWebSocketHandler`, watchlist-scoped when the client connects with
  `?userId=`) and `/sse/prices` (`PriceSseController`), both fed directly from the same sink-consumer
  callbacks that write to Postgres — one Kafka read powers persistence and the live push. Watchlist CRUD
  (`WatchlistRepository`/`WatchlistController`, `X-User-Id` header, `POST` 404s on an unknown `cardId`).
- **Done when:** all endpoints return correct data; alert feed pushes live; watchlist scoping works.
  ✅ Verified end-to-end against live infra: `GET /api/cards/{cardId}` and `/api/market/{marketKey}`
  returned live IQ snapshots; a future-timestamped `sales` record (the Phase 4 technique — advances
  event-time to force window close) produced settled windows that landed in `price_window` **and**
  streamed out over `/sse/prices` in real time; synthetic spike/organic arbitrage alerts landed in
  `alert` and streamed over `/ws/alerts` to a real WebSocket client; a second client connected with
  `?userId=` on an empty watchlist received **no** push for the same alert (confirmed present via
  `/api/alerts`), proving watchlist scoping; `/api/top-movers` (gainers/losers) and `/api/cards/{cardId}/history`
  read back seeded windows correctly; watchlist `GET/POST/DELETE` round-tripped (404 on unknown card).
  A `ServingRepositoriesIT` (Testcontainers Postgres) covers the same repository behavior for CI/other
  machines; it couldn't run in this sandbox (local Docker Desktop API access blocker unrelated to the code).

### Phase 6 — Thin UI ✅
React + TS + Vite app in `frontend/`: `api/client.ts` + `api/types.ts` (typed fetch wrappers
matching `reference.md`'s API surface exactly), `hooks/useAlertsFeed.ts` (`/ws/alerts`, reconnecting, optionally
watchlist-scoped) and `hooks/usePriceStream.ts` (`/sse/prices`), `hooks/useUserId.ts` (anon
`crypto.randomUUID` in localStorage). Pages: card list/search (`CardListPage`, paged grid),
card detail (`CardDetailPage` — per-ticker market table, `PriceChart` via Recharts fed by
`/history` and live SSE updates, watchlist star), top movers, arbitrage feed, and an alert
feed with severity/type filters + a watchlist-only toggle (`AlertsPage`). The Vite dev server
proxies `/api`, `/ws`, `/sse` to the backend (`vite.config.ts`) so the browser only ever talks
to one origin — no backend CORS config needed.
- **Done when:** dashboard demos the streams live against the simulator. ✅ Verified end-to-end
  against live infra (Vite dev server on 5173 proxying to the backend on 8080): `/api/cards`
  search and card detail loaded real catalog + IQ market data through the proxy; `/sse/prices`
  streamed settled windowed aggregates live; `/ws/alerts` delivered a correctly GlobalKTable-
  enriched (`name` populated) synthetic spike alert and real arbitrage flags matching the
  `Alert` shape the UI renders; watchlist add/list/remove round-tripped through the proxy with
  the `X-User-Id` header. `npm run build` and `npm run lint` (oxlint) both pass clean.

### Phase 7 — Testing & docs ✅
`MarketPipelineE2EIT` (`backend/src/test/java/.../e2e/`) adds the thin e2e: real Kafka +
Postgres via Testcontainers (`org.testcontainers:kafka` added to `backend/build.gradle.kts`),
ingestion disabled, synthetic sales/listings produced straight onto `sales`/`listings` with the
app's own `ObjectMapper` (byte-identical wire format to the real producer/topology serdes), then
asserts a spike and an arbitrage alert land enriched in Postgres (`AlertRepository`) and are
readable back over `/api/alerts`, `/api/arbitrage`, and `/api/cards/{cardId}` (`TestRestTemplate`)
— the same path the simulator's `/admin/inject/*` exercises live. Root `README.md` now documents
the full local flow (infra → backend/simulator → catalog reload → frontend dev server → tests),
and reconfirms the JSON-over-Avro decision for the MVP.
- **Done when:** green test suite; `docker compose up` yields a working demo from a clean checkout.
  Unit tests (TopologyTestDriver + the ingestion/metadata/simulator suites) are green. The
  Testcontainers-backed tests (`ServingRepositoriesIT`, `MarketPipelineE2EIT`) compile and were
  reviewed line-by-line for correctness, but — like `ServingRepositoriesIT` before it —
  `MarketPipelineE2EIT` couldn't be executed in this sandbox (the same local Docker Desktop API
  access blocker, unrelated to the code); both need a real Docker socket to actually run.

---

## What's next

The build plan above is complete. Event schemas, the REST/WS/SSE API surface, metric thresholds,
the Postgres schema, and the simulator contract now live in [`reference.md`](reference.md) —
update that doc (not this one) as the code evolves. Further work is maintenance/enhancement
(e.g. a second real ingestion source, Avro migration, multi-game support) rather than a new
numbered phase; scope it against [`project-scope.md`](project-scope.md) and record decisions in
[`tech-stack.md`](tech-stack.md) or [`architecture.md`](architecture.md) as appropriate.
