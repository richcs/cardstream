# Frontend (thin UI)

React + TypeScript + Vite dashboard over the backend's REST + WebSocket/SSE contracts
(see [`../implementation-plan.md`](../implementation-plan.md) Appendix B).

## Run

```bash
npm install
npm run dev      # http://localhost:5173
```

The dev server proxies `/api`, `/ws`, and `/sse` to `http://localhost:8080` (see
`vite.config.ts`), so the browser only ever talks to one origin — no CORS setup needed.
Start the backend (and the simulator, for a live feed) first.

```bash
npm run build     # type-check + production bundle
npm run lint       # oxlint
```

## Pages

- **Cards** (`/`) — searchable/filterable catalog grid.
- **Card detail** (`/cards/:cardId`) — per-finish/condition market table, price history chart
  (hourly/daily, live-updated via SSE), watchlist toggle.
- **Top Movers** (`/top-movers`) — biggest gainers/losers by settled window.
- **Arbitrage** (`/arbitrage`) — listings priced below the rolling average, live-appended.
- **Alerts** (`/alerts`) — severity/type-filterable feed, live over `/ws/alerts`, with a
  watchlist-only toggle.

An anonymous `userId` (crypto.randomUUID, persisted in localStorage) scopes the watchlist —
no auth, per MVP scope.
