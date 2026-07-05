# Reference — Event Schemas, API Surface & Config

The living source of truth for the wire formats, REST/WS/SSE surface, tunable thresholds, and
Postgres schema. **When you add a topic, an event field, an endpoint, or a threshold, update the
matching section here** so this stays accurate — see [`../CLAUDE.md`](../CLAUDE.md)'s "Conventions for
changes". For *why* things are built this way, see [`architecture.md`](architecture.md); for the
build history, see [`implementation-plan.md`](implementation-plan.md).

---

## Event schemas (JSON, MVP)

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

// agg-price-windowed (key = marketKey) — one settled emit per closed window (suppressed)
{ "marketKey": "base1-4|HOLOFOIL|NM", "cardId": "base1-4",
  "windowType": "HOURLY | DAILY | MA_24H", "windowStart": "2026-05-30T12:00:00Z",
  "windowEnd": "2026-05-30T13:00:00Z", "avgPrice": 408.75, "volatility": 12.4,
  "volume": 37, "sampleCount": 31 }

// arbitrage (key = marketKey) — a listing below the rolling average
{ "marketKey": "base1-4|HOLOFOIL|NM", "cardId": "base1-4", "finish": "HOLOFOIL",
  "condition": "NM", "source": "sim", "sellerId": "s-123", "listingPrice": 300.00,
  "referenceAvg": 408.75, "discountPct": 0.2661, "sampleCount": 31,
  "detectedAt": "2026-05-30T12:00:00Z" }
```

`avgPrice` on `MA_24H` is the trailing moving average; `volatility` is the stddev over the same window. Spike/arbitrage are gated by `minSamples` (cold start) — see Metric thresholds below. The `alerts` feed carries both spike and arbitrage alerts (metadata-enriched, branched by severity); `arbitrage` is the raw flag the join produces.

**Enums** — `finish`: `NORMAL | HOLOFOIL | REVERSE_HOLOFOIL`; `condition`: `NM | LP | MP | HP | DMG` (TCGplayer grades); `game`: `POKEMON` (MTG, ONE_PIECE later).

**Serialization** — Kafka JSON values are produced with the application `ObjectMapper` (`KafkaProducerConfig`): timestamps as **ISO-8601** instants (not epoch numbers) and **no `__TypeId__` headers** (consumers deserialize with explicit target types). The producer is **idempotent** (`acks=all`).

---

## API surface (MVP)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/cards?game=&set=&rarity=&q=&page=&pageSize=` | Search/filter catalog (`set` matches set id; returns `{items,page,pageSize,total}`) |
| GET | `/api/cards/{cardId}` | Card detail, layered with current market state across finish/condition; 404 if unknown |
| POST | `/api/admin/catalog/reload` | Seed/refresh the catalog from the Pokémon TCG API (sets since the configured cutoff) → Postgres + `card-metadata` topic |
| GET | `/api/admin/ingestion/status` | Per-source ingestion health: cursor positions, ingested/rejected/duplicate counts, circuit state |
| GET | `/api/cards/{cardId}/history?window=hourly\|daily&from=&to=` | Price/volume history |
| GET | `/api/market/{marketKey}` | Current state for one ticker (Interactive Query) |
| GET | `/api/top-movers?window=daily&dir=gainers\|losers&limit=` | Biggest movers |
| GET | `/api/arbitrage?limit=` | Recent arbitrage flags |
| GET | `/api/alerts?severity=&type=&limit=` | Recent alerts |
| GET/POST | `/api/watchlist` (`X-User-Id`) | List / add (`{cardId}` body) |
| DELETE | `/api/watchlist/{cardId}` (`X-User-Id`) | Remove from watchlist |
| WS | `/ws/alerts` | Live alert feed (filtered by watchlist when `?userId=` is set) |
| SSE | `/sse/prices` | Live price/aggregate updates |

The `frontend/` dashboard (`api/client.ts`/`api/types.ts`) is a typed client over exactly this surface — keep them in sync when this table changes.

---

## Metric definitions & thresholds (configurable)

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

## Postgres schema (serving store)

- `card_set(set_id PK, name, series, printed_total, total, release_date, logo_url, symbol_url, updated_at)` — `set` is reserved in SQL, so the table is `card_set`.
- `card(card_id PK, set_id FK→card_set, name, number, rarity, supertype, image_small, image_large, game, updated_at)`
- `price_window(market_key, window_type, window_start, avg_price, volume, ma, volatility, PK(market_key,window_type,window_start))`
- `alert(alert_id PK, type, severity, card_id, market_key, detail JSONB, ts)`
- `watchlist(user_id, card_id, created_at, PK(user_id, card_id))`

Migrations via Flyway on the backend classpath at `backend/src/main/resources/db/migration/` (default `classpath:db/migration`) — so they ship in the jar and run identically in the IDE, Docker, and tests. `V1__catalog.sql` creates `card_set` + `card`; `V2__serving.sql` creates `price_window`, `alert`, `watchlist`.

---

## Simulator (TCGplayer-shaped) contract

Standalone service on **port 8081**. Stands in for TCGplayer; the ingestion poller resolves `productId → cardId` via `/catalog`, maps `subTypeName → Finish`, and republishes to Kafka. Config under `simulator.*` (backend base-url, `feed.events-per-second`, `feed.sale-ratio`, `feed.retention`, `feed.default-limit`, `walk.sigma`/`drift`, `conditions`).

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

A **SKU** = product × finish × condition; `subTypeName` is the printing label (`Normal`/`Holofoil`/`Reverse Holofoil`). Feed event shapes (the poller maps these to the canonical `listings`/`sales` events under Event schemas above):

```jsonc
// GET /listings item
{ "eventId": 16746, "productId": 500000, "skuId": 1000005, "subTypeName": "Reverse Holofoil",
  "condition": "NM", "price": 0.17, "quantity": 1, "sellerId": "s-0453", "listedAt": "2026-05-31T13:49:27Z" }

// GET /sales item
{ "eventId": 16802, "productId": 500000, "skuId": 1000000, "subTypeName": "Normal",
  "condition": "NM", "price": 0.84, "quantity": 1, "soldAt": "2026-05-31T13:49:05Z" }
```
