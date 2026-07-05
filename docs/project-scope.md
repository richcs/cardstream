# Real-Time TCG Card Market Intelligence Platform

## Problem

Trading card game (TCG) markets — Magic: The Gathering, Pokémon, One Piece — are high-volume, fast-moving, and fragmented across many marketplaces. Prices shift constantly with reprints, tournament results, and hype. Collectors and resellers have no easy way to see live market state, spot underpriced listings, or get alerted to price spikes before the opportunity is gone.

## Solution

Build an event-driven platform that ingests live marketplace listings and sales, processes them as real-time streams, and continuously derives market intelligence — current prices, moving averages, volatility, arbitrage flags, and spike alerts. Cards are treated like tickers on an exchange: every listing, price change, and sale is an event that updates market state in real time.

## Users

- **Collectors** — track the value of cards they own and watch for buying opportunities
- **Analysts** — explore price history, trends, and volatility across games and sets

## Features

### Core Market Workflow
- Ingest marketplace data via REST endpoints and scheduled pollers, publishing listings / sales / price-updates as events
- Materialize current market state per card (latest price, volume, spread) as a queryable view
- Card list and detail views with price history and top movers
- Search and filter by game, set, rarity, and condition

### Stream Processing
- **Windowed price aggregation** — hourly / daily average price and volume per card
- **Moving averages & volatility** — rolling indicators derived from the sales stream
- **Arbitrage detection** — join incoming listings against the rolling market average to flag underpriced cards
- **Spike / anomaly detection** — emit alerts when price or volume deviates sharply from the window baseline
- **Suppression** — emit one settled price per window instead of noisy intermediate updates
- **Branching** — route events by game (MTG / Pokémon / One Piece) or by alert severity

### Alerts
- Real-time price-spike and arbitrage alerts
- Fan-out to a live dashboard feed
- User-defined watchlists — alert only on cards a user follows

### Serving & Query
- API over the materialized market state (current price, history, top movers)
- Low-latency reads of current market state
- Serving store for historical queries and the dashboard

### Dashboard (simple UI — MVP)
> Backend-first: a thin UI over the serving API and live feed. Enough to demo the streams live; polish is post-MVP.
- Live price charts per card
- Top movers (biggest gainers / losers by window)
- Arbitrage feed (listings priced below market average)
- Alert feed with severity filtering

## MVP Scope & Assumptions

- **Intent** — both a learning vehicle and a genuinely usable tool, but the MVP stays tight to the Core features above; no scope creep beyond what's needed to exercise the streaming pipeline end to end.
- **Games** — the platform targets multiple TCGs (MTG, Pokémon, One Piece); MVP ships **Pokémon only**, with the others able to slot in later.
- **Currency** — USD only.
- **Data** — MVP runs on an in-house simulator built to the real TCGplayer API contract; the live TCGplayer feed swaps in later.

## Out of Scope (MVP)

- Buying / selling or payment processing — read-only market intelligence, not a marketplace
- ML-based price prediction (a natural later extension, not in the MVP)
- Email / external notification fan-out — alerts go to the live feed only in MVP
- User accounts beyond simple watchlists (no full auth / roles in MVP)
- Mobile apps — simple web UI only; no polished/production dashboard in MVP
- Exhaustive marketplace coverage — MVP runs on the simulated feed; one real source comes after
- Multi-game and multi-currency — platform targets MTG / Pokémon / One Piece, but MVP runs Pokémon / USD only

---

> **Technical decisions, stack, data sources, stream semantics, and domain keys live in [`tech-stack.md`](tech-stack.md).**
