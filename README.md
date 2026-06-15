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
| `frontend` | Thin React/TS dashboard (Phase 6) |

## Quick start (Phase 0)

Prereqs: Docker Desktop, JDK 21+ (23 works).

```bash
# 1. Bring up infrastructure (Kafka in KRaft mode, Postgres) and create topics
docker compose up -d kafka postgres topic-init

# 2. Verify topics were created
docker compose logs topic-init

# 3. Build everything
./gradlew build              # macOS/Linux
gradlew.bat build            # Windows

# 4. Run an app locally (points at the host-exposed broker on localhost:29092)
./gradlew :backend:bootRun   --args='--spring.kafka.bootstrap-servers=localhost:29092'
./gradlew :simulator:bootRun

# health
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
```

Or run the apps in containers too:

```bash
docker compose --profile apps up -d --build
```

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
