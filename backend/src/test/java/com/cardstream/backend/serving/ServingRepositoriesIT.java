package com.cardstream.backend.serving;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardstream.backend.sink.AlertRepository;
import com.cardstream.backend.sink.PriceWindowRepository;
import com.cardstream.backend.sink.PriceWindowRepository.Direction;
import com.cardstream.backend.sink.PriceWindowRepository.TopMover;
import com.cardstream.common.model.Alert;
import com.cardstream.common.model.AlertType;
import com.cardstream.common.model.Severity;
import com.cardstream.common.model.WindowType;
import com.cardstream.common.model.WindowedAggregate;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real-Postgres coverage (Flyway-migrated {@code V2__serving.sql}) for the Phase 5 sink/serving
 * repositories: upsert idempotency, history/top-movers derivation, and watchlist scoping. The topology
 * tests already prove the streams side; this proves what lands in — and comes back out of — Postgres.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:59092",
        "spring.flyway.enabled=true",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.streams.auto-startup=false",
        "ingestion.enabled=false"
})
class ServingRepositoriesIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

    private static final String CARD_ID = "base1-4";

    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private PriceWindowRepository priceWindows;
    @Autowired
    private AlertRepository alerts;
    @Autowired
    private WatchlistRepository watchlist;

    @BeforeEach
    void seedCatalog() {
        jdbc.update("DELETE FROM watchlist");
        jdbc.update("DELETE FROM alert");
        jdbc.update("DELETE FROM price_window");
        jdbc.update("DELETE FROM card");
        jdbc.update("DELETE FROM card_set");
        jdbc.update("INSERT INTO card_set (set_id, name) VALUES ('base1', 'Base')");
        jdbc.update("INSERT INTO card (card_id, set_id, name, game) VALUES (?, 'base1', 'Charizard', 'POKEMON')",
                CARD_ID);
    }

    @Test
    void priceWindowUpsertIsIdempotentAndQueryableByHistory() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        WindowedAggregate first = new WindowedAggregate(CARD_ID + "|HOLOFOIL|NM", CARD_ID, WindowType.HOURLY,
                start, start.plus(1, ChronoUnit.HOURS), new BigDecimal("10.00"), 1.5, 5, 4);
        priceWindows.upsert(first);
        // Same window key redelivered (e.g. sink consumer restart) must update in place, not duplicate.
        WindowedAggregate redelivered = new WindowedAggregate(first.marketKey(), first.cardId(), first.windowType(),
                first.windowStart(), first.windowEnd(), new BigDecimal("11.00"), 2.0, 6, 5);
        priceWindows.upsert(redelivered);

        List<WindowedAggregate> history = priceWindows.history(CARD_ID, WindowType.HOURLY, null, null);
        assertThat(history).hasSize(1);
        assertThat(history.get(0).avgPrice()).isEqualByComparingTo("11.00");
        assertThat(history.get(0).sampleCount()).isEqualTo(5);
    }

    @Test
    void topMoversRanksByPercentChangeAndJoinsCardName() {
        String marketKey = CARD_ID + "|HOLOFOIL|NM";
        Instant t1 = Instant.parse("2026-01-01T00:00:00Z");
        Instant t2 = t1.plus(1, ChronoUnit.DAYS);
        priceWindows.upsert(new WindowedAggregate(marketKey, CARD_ID, WindowType.DAILY,
                t1, t1.plus(1, ChronoUnit.DAYS), new BigDecimal("10.00"), 0, 3, 3));
        priceWindows.upsert(new WindowedAggregate(marketKey, CARD_ID, WindowType.DAILY,
                t2, t2.plus(1, ChronoUnit.DAYS), new BigDecimal("15.00"), 0, 3, 3));

        List<TopMover> gainers = priceWindows.topMovers(WindowType.DAILY, Direction.GAINERS, 10);
        assertThat(gainers).hasSize(1);
        assertThat(gainers.get(0).cardId()).isEqualTo(CARD_ID);
        assertThat(gainers.get(0).cardName()).isEqualTo("Charizard");
        assertThat(gainers.get(0).pctChange()).isEqualTo(0.5, Offset.offset(1e-9));
    }

    @Test
    void alertInsertIsIdempotentAndFilterableByType() {
        String alertId = UUID.randomUUID().toString();
        Alert alert = new Alert(alertId, AlertType.ARBITRAGE, Severity.MED, CARD_ID,
                CARD_ID + "|HOLOFOIL|NM", "Charizard", Map.of("discountPct", 0.3), Instant.now());
        alerts.insert(alert);
        alerts.insert(alert); // duplicate delivery must not fail or double-insert

        List<Alert> arbitrage = alerts.recent(AlertType.ARBITRAGE, null, 10);
        assertThat(arbitrage).hasSize(1);
        assertThat(arbitrage.get(0).alertId()).isEqualTo(alertId);
        assertThat(alerts.recent(AlertType.SPIKE, null, 10)).isEmpty();
    }

    @Test
    void watchlistAddRemoveAndIsWatching() {
        String userId = "u-1";
        assertThat(watchlist.isWatching(userId, CARD_ID)).isFalse();

        watchlist.add(userId, CARD_ID);
        assertThat(watchlist.isWatching(userId, CARD_ID)).isTrue();
        assertThat(watchlist.list(userId)).extracting("cardId").containsExactly(CARD_ID);

        watchlist.remove(userId, CARD_ID);
        assertThat(watchlist.isWatching(userId, CARD_ID)).isFalse();
    }
}
