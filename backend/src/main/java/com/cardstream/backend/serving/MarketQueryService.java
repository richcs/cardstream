package com.cardstream.backend.serving;

import com.cardstream.backend.streams.MarketStats;
import com.cardstream.backend.streams.MarketTopology;
import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.common.model.MarketKey;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Hot reads off the topology's rolling reference-stats store ({@link MarketTopology#ARB_REFERENCE_STORE})
 * via {@link KafkaStreamsInteractiveQueryService} — the same table the arbitrage join reads.
 */
@Service
public class MarketQueryService {

    private final KafkaStreamsInteractiveQueryService interactiveQueryService;

    public MarketQueryService(KafkaStreamsInteractiveQueryService interactiveQueryService) {
        this.interactiveQueryService = interactiveQueryService;
    }

    public Optional<MarketSnapshot> snapshot(MarketKey key) {
        MarketStats stats = store().get(key.asString());
        if (stats == null || stats.count() == 0) {
            return Optional.empty();
        }
        return Optional.of(toSnapshot(key, stats));
    }

    /** Current state across every finish/condition combo a card could trade under. */
    public List<MarketSnapshot> snapshotsForCard(String cardId) {
        ReadOnlyKeyValueStore<String, MarketStats> store = store();
        List<MarketSnapshot> snapshots = new ArrayList<>();
        for (Finish finish : Finish.values()) {
            for (Condition condition : Condition.values()) {
                MarketKey key = new MarketKey(cardId, finish, condition);
                MarketStats stats = store.get(key.asString());
                if (stats != null && stats.count() > 0) {
                    snapshots.add(toSnapshot(key, stats));
                }
            }
        }
        return snapshots;
    }

    private ReadOnlyKeyValueStore<String, MarketStats> store() {
        try {
            return interactiveQueryService.retrieveQueryableStore(
                    MarketTopology.ARB_REFERENCE_STORE, QueryableStoreTypes.<String, MarketStats>keyValueStore());
        } catch (InvalidStateStoreException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Streams state store not ready", e);
        }
    }

    private static MarketSnapshot toSnapshot(MarketKey key, MarketStats stats) {
        return new MarketSnapshot(key.asString(), key.cardId(), key.finish(), key.condition(),
                money(stats.last()), stats.lastTs(), money(stats.mean()), stats.stddev(),
                stats.volume(), stats.count());
    }

    private static BigDecimal money(double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }
}
