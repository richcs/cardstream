# Architecture — Real-Time TCG Card Market Intelligence Platform

How Cardstream is put together and **how the Kafka Streams topology works**. Read alongside
[`project-scope.md`](project-scope.md) (what), [`tech-stack.md`](tech-stack.md) (choices), and
[`implementation-plan.md`](implementation-plan.md) (phased build + event schemas). This doc is the
"how the pieces fit and why" layer; the implementation plan's appendices remain the source of truth
for exact schemas, endpoints, and thresholds.

---

## 1. System overview

Cardstream treats each card printing as an exchange ticker and derives live market intelligence from
a stream of marketplace listings and sales. The pipeline is **event-driven**: ingest → stream-process
→ serve.

```
 Pokémon TCG API ──(metadata loader)──► Postgres catalog + compacted card-metadata topic
                                                                  │ (GlobalKTable)
 Simulator (TCGplayer-shaped REST) ◄─poll─ Ingestion poller ──► Kafka: listings / sales
   (separate process, the                   (trust boundary:        │
    external-feed boundary)                   validate + dedup)      ▼
                                                          ┌──────────────────────────┐
                                                          │   Kafka Streams topology  │
                                                          │  windows · MA/volatility  │
                                                          │  arbitrage join · spike   │
                                                          │  suppression · enrichment │
                                                          └──────────────────────────┘
                                                            │                      │
                                              state stores (RocksDB)      agg-price-windowed
                                              via Interactive Queries     arbitrage · alerts
                                                            │                      │
                                              Serving: REST + WS/SSE      sink consumer ─► Postgres
                                                            │
                                                     Thin React/TS UI
```

**Design throughline:** the simulator mimics the real TCGplayer API, so swapping in real TCGplayer
later means repointing the poller — the topology and serving layers never learn where events came
from. The `source` tag rides on each event, but the `MarketKey` stays source-agnostic, so a card is
one ticker across every feed.

---

## 2. Modules & processes

| Module | Process | Responsibility |
|---|---|---|
| `common` | (library) | Event records, enums, `MarketKey`, topic-schema records. **No framework deps** — plain Java + Jackson. |
| `backend` | one Spring Boot app | Ingestion poller → Kafka, the Streams topology, serving (REST + WS/SSE), and the Postgres sink consumer — co-located in a single process (normal at this scale). |
| `simulator` | standalone Spring Boot app | A TCGplayer-shaped REST feed with a random-walk price engine + anomaly injection. Separate process to keep the external-feed boundary honest. |
| `frontend` | Vite dev/static | Thin React/TS dashboard (Phase 6). |

The `backend` runs ingestion + streams + serving + sink in **one JVM**. That's deliberate: it's the
simplest thing that works for ~100–500 events/sec, and the boundaries are drawn so ingestion can be
split into its own deployable later (the only rule being *one owner per source*).

---

## 3. End-to-end data flow

1. **Catalog load** (`metadata/`) — a recency-scoped Pokémon TCG API loader writes `card_set` + `card`
   to Postgres and publishes each card to the **compacted `card-metadata`** topic (key = `cardId`).
2. **Simulation** (`simulator`) — seeds its product universe from the backend's `/api/cards`, prices
   each SKU (product × finish × condition) via a geometric random walk, and serves pollable
   `/listings?since=` / `/sales?since=` feeds plus spike/arbitrage injection endpoints.
3. **Ingestion** (`ingestion/`) — a scheduled poller pulls each enabled source, **normalizes** to the
   canonical `Listing`/`Sale` events, and at the **trust boundary** validates + dedups before
   producing to `listings`/`sales`, keyed by `MarketKey` and tagged with `source`.
4. **Stream processing** (`streams/`) — the topology consumes `listings`/`sales`, enriches against the
   `card-metadata` GlobalKTable, and emits to `agg-price-windowed`, `arbitrage`, and `alerts`.
5. **Serving + sink** (Phase 5) — Interactive Queries answer hot reads from the state stores; a
   dedicated consumer sinks aggregates/alerts to Postgres for history; WS/SSE push live feeds.

---

## 4. Topics

| Topic | Key | Produced by | Notes |
|---|---|---|---|
| `listings` | `marketKey` | ingestion | Canonical listing events (Appendix A). 6 partitions. |
| `sales` | `marketKey` | ingestion | Canonical sale events. 6 partitions. |
| `price-updates` | `marketKey` | ingestion | Reserved; not yet consumed by the topology. |
| `card-metadata` | `cardId` | metadata loader | **Compacted**; loaded as a GlobalKTable. |
| `agg-price-windowed` | `marketKey` | **streams** | One settled `WindowedAggregate` per closed window. |
| `arbitrage` | `marketKey` | **streams** | `ArbitrageFlag` per below-market listing. |
| `alerts` | `marketKey` | **streams** | `Alert` (SPIKE or ARBITRAGE), enriched + severity-branched. |

Everything market-related is keyed by `MarketKey.asString()` = `"{cardId}|{finish}|{condition}"`, so
all sales/listings for a ticker land on the same partition and per-ticker ordering is preserved.

---

## 5. The Kafka Streams topology

The core of the project. Implemented in `com.cardstream.backend.streams`:

| Class | Role |
|---|---|
| `KafkaStreamsTopologyConfig` | `@EnableKafkaStreams` + the `KafkaStreamsConfiguration` bean; injects the shared `StreamsBuilder` into the topology. |
| `MarketTopology` | A plain (Spring-free) class that wires every operator into a `StreamsBuilder`. Built this way so it's drivable directly by `TopologyTestDriver`. |
| `EventTimeExtractor` | Custom `TimestampExtractor`: stream time follows `soldAt`/`listedAt`, never wall clock. |
| `MarketStats` | Incremental count/sum/sumSq accumulator → O(1) mean & stddev; the value type for every aggregation and the spike store. |
| `SpikeDetector` | Processor-API node: compare-then-update spike detection. |
| `JsonSerdes` | Builds JSON serdes from the app `ObjectMapper` (ISO-8601, no type headers). |
| `ThresholdProperties` | `market.thresholds.*` — `spike-sigma`, `arbitrage-margin`, `min-samples`. |

### 5.1 Topology graph

```
                         ┌──────────────┐         ┌──────────────────┐
   card-metadata ───────►│ GlobalKTable │         │  sales (KStream) │◄─── sales topic
   (compacted)           └──────┬───────┘         └───┬───────┬──────┘
                                │                     │       │
                                │      groupByKey ────┘       │ process(SpikeDetector)
                                │           │                 │   [store: spike-stats]
                                │   ┌───────┴────────┐        │   compare vs prior stats,
                                │   │ windowedBy ×3  │        │   then fold in
                                │   │ HOURLY (1h)    │        ▼
                                │   │ DAILY  (1d)    │   spikeAlerts (KStream<_,Alert>)
                                │   │ MA_24H (24h/1h)│        │
                                │   └───────┬────────┘        │
                                │     aggregate→MarketStats   │
                                │     suppress(untilClose)    │
                                │           │                 │
                                │     WindowedAggregate        │
                                │           ▼                  │
                                │   agg-price-windowed         │
                                │                              │
                                │   groupByKey.aggregate       │
                                │        ▼                     │
                                │   referenceStats (KTable)    │
                                │        │ (table side)        │
   listings (KStream) ──────────┼────────┤                    │
       │                        │   leftJoin (KStream–KTable)  │
       │                        │        ▼                     │
       │                        │   ArbitrageFlag (filter≠null)│
       │                        │     ├──► arbitrage topic     │
       │                        │     └──► mapValues→Alert ────┤
       │                        │                              ▼
       │                        │                    spikeAlerts.merge(arbAlerts)
       │                        └──── leftJoin (cardId) ──────►│  enrich: attach card name
       │                                                       ▼
       │                                          split by severity (HIGH/MED/LOW)
       │                                                       ▼
       └──────────────────────────────────────────────►  alerts topic
```

### 5.2 Sources, keying, and time

- **Sources:** `sales` and `listings` are read with explicit `Consumed.with(stringSerde, …)`.
  `card-metadata` is a **GlobalKTable** (every instance holds the full table → no co-partitioning
  needed for the enrichment join).
- **Keying:** events arrive already keyed by `MarketKey` from ingestion, so `groupByKey` does **not**
  repartition — a quiet but important efficiency and ordering property.
- **Event-time** (`EventTimeExtractor`): windows reflect *when trades happened*, not when we processed
  them. The extractor reads `soldAt`/`listedAt`; non-market records (metadata, internal repartition
  topics) and any missing timestamp fall back to the broker record time. The ingestion trust boundary
  already clamps event times to a plausible skew window, so a misbehaving feed can't jump stream time
  forward and slam windows shut early.

### 5.3 Windowed aggregates → `agg-price-windowed`

One helper (`windowedAggregate`) is instantiated three times:

| `WindowType` | Window | Yields |
|---|---|---|
| `HOURLY` | tumbling 1h | avg price + volume |
| `DAILY` | tumbling 1d | avg price + volume |
| `MA_24H` | **hopping** 24h advancing 1h | trailing moving average + volatility (stddev) |

Each groups sales by ticker, folds them into a `MarketStats`, and **suppresses** with
`Suppressed.untilWindowCloses(unbounded())` so downstream sees exactly **one settled emit per window**
instead of a noisy stream of intermediate updates. On close, the `Windowed<String>` result maps to a
`WindowedAggregate` (window bounds + `avgPrice`, `volatility`, `volume`, `sampleCount`). For `MA_24H`,
`avgPrice` *is* the moving average and `volatility` is its stddev over the same trailing window.

> Why `MarketStats` carries `sumSq`: it lets mean **and** standard deviation be computed in O(1) per
> window without retaining individual samples — variance = `sumSq/n − mean²` (floored at 0 to absorb
> floating-point underflow on near-constant prices).

### 5.4 Arbitrage — KStream–KTable join

```
referenceStats = sales.groupByKey().aggregate(MarketStats::empty, add)   // running per-ticker stats
arbitrage      = listings.leftJoin(referenceStats, evaluateArbitrage).filter(≠ null)
```

The **table side** (`referenceStats`, store `arb-ref-stats`) is the rolling average for each ticker;
the **stream side** is incoming listings. For each listing we look up its ticker's current stats and
flag arbitrage when `listing.price < (1 − margin) × mean` **and** the ticker has cleared the
`minSamples` cold-start gate. A match produces an `ArbitrageFlag` (with `discountPct`) to the
`arbitrage` topic and is mapped to an `Alert(type=ARBITRAGE)` for the unified feed; non-matches return
`null` and are filtered out.

A KStream–KTable join (not stream-stream) is the right primitive here: a listing is a point-in-time
event evaluated against the *latest known* market average, with no windowing on the join itself.

### 5.5 Spike — Processor API (compare-then-update)

Spike detection lives in `SpikeDetector`, a `Processor<String, Sale, String, Alert>` backed by a keyed
`spike-stats` store. Per sale: load the ticker's **prior** `MarketStats`; if it has ≥ `minSamples` and
the deviation `|price − mean| / stddev` exceeds `spike-sigma`, forward an `Alert(type=SPIKE)` with
severity scaled by the deviation magnitude (≥5σ HIGH, ≥4σ MED, else LOW); **then** fold the sale into
the stats.

> **Why the Processor API rather than a DSL stream-table join?** The "compare against the prior
> distribution, then update" ordering must be explicit. If spike used a stream-table self-join over
> `sales`, the same record both updates the table and reads it, and the read-vs-update order is a
> fragile artifact of topology build order — a sale could contaminate its own baseline. A processor
> with one state store makes the sequence unambiguous and trivially testable. This is the one place we
> reach past the DSL on purpose.

The running stats are intentionally maintained **twice** — once as the `referenceStats` KTable (DSL,
joinable by listings) and once in the spike processor's store — because the two consumers need
genuinely different mechanics (a joinable table vs. ordered compare-then-update). Each is small and
independently correct; unifying them would couple two concerns for no real saving.

### 5.6 Enrichment, branching, output

Spike and arbitrage alerts are `merge`d, then **left-joined against the `card-metadata` GlobalKTable**
(keyed by `cardId`) to attach the card name — `leftJoin` so an alert still flows even if metadata is
momentarily missing. The merged stream is then **split by severity** (`HIGH`/`MED`/`LOW`), each branch
incrementing a per-severity meter before writing to `alerts`. Branching is wired so per-game routing
(or fanning HIGH alerts to a separate channel) is a one-line change later.

### 5.7 State stores

| Store | Type | Backs |
|---|---|---|
| `agg-hourly` / `agg-daily` / `agg-ma_24h` | windowed (RocksDB) | the three windowed aggregates (with retention ≥ window size) |
| `arb-ref-stats` | key-value (RocksDB) | rolling reference average for the arbitrage join |
| `spike-stats` | key-value (RocksDB) | per-ticker prior distribution for spike detection |

All are durable RocksDB stores (changelog-backed in a real cluster) and become queryable via
Interactive Queries in Phase 5. Caching is disabled (`STATESTORE_CACHE_MAX_BYTES = 0`) so windowed
and joined results forward promptly and deterministically.

### 5.8 Configuration knobs

`KafkaStreamsTopologyConfig` sets: `application-id` (namespaces the consumer group + internal
topics/state), the event-time extractor, zero record cache, a 1s commit interval, and
`LogAndContinueExceptionHandler` so a single poison record can't halt the topology. Market thresholds
live under `market.thresholds.*` (`ThresholdProperties`); see Appendix C of the implementation plan.

---

## 6. Cross-cutting conventions

- **Serialization** — JSON via the application `ObjectMapper`: ISO-8601 instants (not epoch numbers)
  and **no `__TypeId__` headers**. The ingestion producer (`KafkaProducerConfig`) and the streams
  serdes (`JsonSerdes`) share this contract, so every consumer deserializes with an explicit target
  type. Avro + Schema Registry is the documented later upgrade.
- **Money** — `BigDecimal` on the wire; folded to `double` only inside `MarketStats` for the
  statistics math, then surfaced back as scaled `BigDecimal`.
- **Time** — `Instant` everywhere; the topology is event-time based.
- **Keys** — always build/parse with `MarketKey.asString()` / `MarketKey.parse()`, never hand-rolled
  strings (the `|` delimiter is rejected inside identifiers at the trust boundary).
- **Trust boundary** — the ingestor is where untrusted input is gated: catalog allowlist, value/scale
  bounds, timestamp clamping, dedup. Channel auth proves *who* sent data, not that it's *correct* — so
  validation complements, never replaces, it.

## 7. Observability & testing

- **Metrics** — Micrometer meters: `cardstream.ingestion.*` (per-source ingested/rejected/duplicate)
  and `cardstream.streams.*` (spike count by severity, alert count by type/severity). Actuator exposes
  health/metrics; Kafka Streams state/lag dashboards are an optional later add.
- **Topology tests** — `TopologyTestDriver` exercises each operator offline (no broker): spike fires
  past the σ/`minSamples` gates and is suppressed below them, arbitrage flags a below-margin listing
  and respects the margin, the hourly window emits exactly one settled aggregate on close, and
  GlobalKTable enrichment attaches the card name.
- **Integration** — Testcontainers (Kafka + Postgres) for the wired path; the context-load smoke test
  runs fully offline (streams `auto-startup: false`, H2 for Postgres, Kafka health disabled).

---

## 8. Status & roadmap

Phases 1–3 (catalog, simulator, ingestion) are complete and verified end-to-end. Phase 4 (this
topology) is code-complete and TopologyTestDriver-verified; live end-to-end verification against
running infra is the next step. Phase 5 layers serving (Interactive Queries, the Postgres sink, history
and feed endpoints, WS/SSE); Phase 6 is the thin UI. See [`implementation-plan.md`](implementation-plan.md)
for the phase-by-phase "done when" bars.
