-- Serving store: history (sunk from agg-price-windowed), alerts, and per-user watchlists.
-- market_key = "{cardId}|{finish}|{condition}" throughout (never contains '|' inside cardId —
-- rejected at the ingestion trust boundary), so cardId is recoverable via split_part() below.

CREATE TABLE price_window (
    market_key   VARCHAR(160) NOT NULL,
    window_type  VARCHAR(16)  NOT NULL,
    window_start TIMESTAMPTZ  NOT NULL,
    window_end   TIMESTAMPTZ  NOT NULL,
    avg_price    NUMERIC(12, 2) NOT NULL,
    volatility   DOUBLE PRECISION NOT NULL,
    volume       BIGINT NOT NULL,
    sample_count BIGINT NOT NULL,
    PRIMARY KEY (market_key, window_type, window_start)
);

-- Powers /api/cards/{cardId}/history (prefix match on market_key) and /api/top-movers.
CREATE INDEX idx_price_window_type_start ON price_window (window_type, window_start);

CREATE TABLE alert (
    alert_id   VARCHAR(64) PRIMARY KEY,
    type       VARCHAR(16) NOT NULL,
    severity   VARCHAR(8)  NOT NULL,
    card_id    VARCHAR(64) NOT NULL,
    market_key VARCHAR(160) NOT NULL,
    name       VARCHAR(255),
    detail     JSONB NOT NULL,
    ts         TIMESTAMPTZ NOT NULL
);

-- Powers /api/alerts (severity/type filter) and /api/arbitrage (type = 'ARBITRAGE'), newest first.
CREATE INDEX idx_alert_ts ON alert (ts DESC);
CREATE INDEX idx_alert_type_severity ON alert (type, severity);

CREATE TABLE watchlist (
    user_id    VARCHAR(64) NOT NULL,
    card_id    VARCHAR(64) NOT NULL REFERENCES card (card_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, card_id)
);
