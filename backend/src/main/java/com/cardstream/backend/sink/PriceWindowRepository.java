package com.cardstream.backend.sink;

import com.cardstream.common.model.WindowType;
import com.cardstream.common.model.WindowedAggregate;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/** Sinks settled {@code agg-price-windowed} records and serves history/top-movers reads off them. */
@Repository
public class PriceWindowRepository {

    private static final String UPSERT = """
            INSERT INTO price_window (market_key, window_type, window_start, window_end, avg_price, volatility, volume, sample_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (market_key, window_type, window_start) DO UPDATE SET
                window_end = EXCLUDED.window_end,
                avg_price = EXCLUDED.avg_price,
                volatility = EXCLUDED.volatility,
                volume = EXCLUDED.volume,
                sample_count = EXCLUDED.sample_count
            """;

    private static final RowMapper<WindowedAggregate> MAPPER = (rs, n) -> new WindowedAggregate(
            rs.getString("market_key"),
            rs.getString("card_id"),
            WindowType.valueOf(rs.getString("window_type")),
            rs.getTimestamp("window_start").toInstant(),
            rs.getTimestamp("window_end").toInstant(),
            rs.getBigDecimal("avg_price"),
            rs.getDouble("volatility"),
            rs.getLong("volume"),
            rs.getLong("sample_count"));

    private final JdbcTemplate jdbc;

    public PriceWindowRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void upsert(WindowedAggregate agg) {
        jdbc.update(UPSERT,
                agg.marketKey(), agg.windowType().name(), java.sql.Timestamp.from(agg.windowStart()),
                java.sql.Timestamp.from(agg.windowEnd()), agg.avgPrice(), agg.volatility(),
                agg.volume(), agg.sampleCount());
    }

    /** History for every ticker under a card (finish/condition recoverable via {@code MarketKey.parse}). */
    public List<WindowedAggregate> history(String cardId, WindowType windowType, Instant from, Instant to) {
        List<String> clauses = new ArrayList<>(List.of("window_type = ?", "market_key LIKE ? ESCAPE '\\'"));
        List<Object> args = new ArrayList<>(List.of(windowType.name(), likePrefix(cardId)));
        if (from != null) {
            clauses.add("window_start >= ?");
            args.add(java.sql.Timestamp.from(from));
        }
        if (to != null) {
            clauses.add("window_start <= ?");
            args.add(java.sql.Timestamp.from(to));
        }
        String sql = "SELECT market_key, split_part(market_key, '|', 1) AS card_id, window_type, "
                + "window_start, window_end, avg_price, volatility, volume, sample_count FROM price_window"
                + " WHERE " + String.join(" AND ", clauses) + " ORDER BY window_start";
        return jdbc.query(sql, MAPPER, args.toArray());
    }

    /** Biggest movers by % change of the latest settled window vs. the previous one, for the given window type. */
    public List<TopMover> topMovers(WindowType windowType, Direction direction, int limit) {
        String order = direction == Direction.GAINERS ? "DESC" : "ASC";
        String sql = """
                WITH ranked AS (
                    SELECT market_key, avg_price,
                           LAG(avg_price) OVER (PARTITION BY market_key ORDER BY window_start) AS prev_avg_price,
                           ROW_NUMBER() OVER (PARTITION BY market_key ORDER BY window_start DESC) AS rn
                    FROM price_window
                    WHERE window_type = ?
                )
                SELECT r.market_key, split_part(r.market_key, '|', 1) AS card_id, c.name AS card_name,
                       r.avg_price, r.prev_avg_price,
                       (r.avg_price - r.prev_avg_price) / r.prev_avg_price AS pct_change
                FROM ranked r
                JOIN card c ON c.card_id = split_part(r.market_key, '|', 1)
                WHERE r.rn = 1 AND r.prev_avg_price IS NOT NULL AND r.prev_avg_price <> 0
                """ + "ORDER BY pct_change " + order + " LIMIT ?";
        return jdbc.query(sql, (rs, n) -> new TopMover(
                rs.getString("market_key"), rs.getString("card_id"), rs.getString("card_name"),
                rs.getBigDecimal("avg_price"), rs.getBigDecimal("prev_avg_price"), rs.getDouble("pct_change")),
                windowType.name(), limit);
    }

    private static String likePrefix(String cardId) {
        return cardId.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "|%";
    }

    public enum Direction { GAINERS, LOSERS }

    public record TopMover(String marketKey, String cardId, String cardName,
            BigDecimal avgPrice, BigDecimal previousAvgPrice, double pctChange) {
    }
}
