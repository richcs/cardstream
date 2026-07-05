# Cardstream — Real-Time TCG Card Market Intelligence Platform

Event-driven platform that ingests marketplace listings/sales and derives live market
intelligence (prices, moving averages, volatility, arbitrage, spike alerts) with Kafka Streams.

- **What** → [`project-scope.md`](project-scope.md)
- **How (stack)** → [`tech-stack.md`](tech-stack.md)
- **How (architecture + streams topology)** → [`architecture.md`](architecture.md)
- **Build plan** → [`implementation-plan.md`](implementation-plan.md)

## Modules

| Module | Purpose |
|---|---|
| `common` | Shared event models, enums, market key (no framework deps) |
| `backend` | Ingestion poller, Kafka Streams topology, serving (REST + WS/SSE), Postgres sink |
| `simulator` | Standalone service exposing a TCGplayer-shaped feed (stands in for real TCGplayer) |
| `frontend` | Thin React/TS/Vite dashboard |

## Quick start

Prereqs: Docker Desktop, JDK 21+ (23 works), Node 20+.

```bash
# 1. Bring up infrastructure (Kafka in KRaft mode, Postgres) and create topics
docker compose up -d kafka postgres topic-init

# 2. Verify topics were created
docker compose logs topic-init

# 3. Build everything
./gradlew build              # macOS/Linux
gradlew.bat build            # Windows

# 4. Run the backend (points at the host-exposed broker on localhost:29092) and simulator
./gradlew :backend:bootRun   --args='--spring.kafka.bootstrap-servers=localhost:29092'
./gradlew :simulator:bootRun

# 5. Seed the catalog from the Pokémon TCG API (first run only — the simulator seeds its
#    own product universe from this) and confirm ingestion is flowing
curl -X POST http://localhost:8080/api/admin/catalog/reload
curl http://localhost:8080/api/admin/ingestion/status

# 6. Run the dashboard (proxies /api, /ws, /sse to the backend — no CORS setup needed)
cd frontend && npm install && npm run dev   # http://localhost:5173

# health
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
```

Or run the backend/simulator in containers instead of steps 4–5:

```bash
docker compose --profile apps up -d --build
```

## Tests

```bash
./gradlew build                                   # unit tests (TopologyTestDriver, etc.) — no Docker needed
./gradlew :backend:test --tests '*IT'             # Testcontainers integration tests (needs a working
                                                   # Docker socket — ServingRepositoriesIT, MarketPipelineE2EIT)
```

`MarketPipelineE2EIT` boots real Kafka + Postgres containers, publishes synthetic sales/listings
straight onto the topics the topology reads, and asserts a spike + arbitrage alert come out the
other end enriched, sunk to Postgres, and queryable over `/api/alerts`, `/api/arbitrage`, and
`/api/cards/{cardId}` — the same path the simulator's `/admin/inject/*` endpoints exercise live.

## Topics

`listings`, `sales`, `price-updates`, `agg-price-windowed`, `arbitrage`, `alerts`
(all 6 partitions), plus compacted `card-metadata`.

## Ports

| Service | Port |
|---|---|
| Kafka (host) | `localhost:29092` |
| Kafka (in-network) | `kafka:9092` |
| Postgres | `localhost:5432` (db/user/pass: `cardstream`) |
| Backend | `localhost:8080` |
| Simulator | `localhost:8081` |
| Frontend (dev server) | `localhost:5173` |

## Serialization

JSON (Jackson) end to end for the MVP — chosen for velocity over a schema registry setup. The
documented upgrade path is Avro + Confluent Schema Registry once schema evolution across
producers/consumers becomes a real concern (see `tech-stack.md`); revisited and reconfirmed as
out of scope for the MVP.
