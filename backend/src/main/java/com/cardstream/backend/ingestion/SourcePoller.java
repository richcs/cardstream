package com.cardstream.backend.ingestion;

import com.cardstream.backend.ingestion.IngestionProperties.SourceConfig;
import com.cardstream.backend.ingestion.MarketDataSource.PollBatch;
import com.cardstream.backend.ingestion.MarketDataSource.SourceCursor;
import com.cardstream.common.model.Listing;
import com.cardstream.common.model.Sale;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

/**
 * Scheduled poll loop over the enabled sources: each is polled with its own cursor, deduped, validated,
 * and produced to Kafka keyed by {@code MarketKey}.
 */
@Component
public class SourcePoller implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(SourcePoller.class);
    private static final int DEDUP_CAPACITY = 50_000;

    private final IngestionProperties props;
    private final SourceRegistry registry;
    private final EventValidator validator;
    private final MarketEventPublisher publisher;
    private final MeterRegistry metrics;

    private final Map<String, SourceState> state = new LinkedHashMap<>();

    public SourcePoller(IngestionProperties props, SourceRegistry registry, EventValidator validator,
            MarketEventPublisher publisher, MeterRegistry metrics) {
        this.props = props;
        this.registry = registry;
        this.validator = validator;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    @PostConstruct
    void init() {
        Map<String, SourceConfig> configs = props.sources();
        for (MarketDataSource source : registry.sources()) {
            SourceConfig cfg = configs.get(source.id());
            state.put(source.id(), new SourceState(cfg == null ? 5 : cfg.circuitBreakerThreshold(),
                    cfg == null ? Duration.ofSeconds(30) : cfg.circuitBreakerCooldown()));
        }
    }

    /**
     * Registered programmatically (not {@code @Scheduled(fixedDelayString=...)}) since simple duration
     * strings there need Spring Framework 6.2, and this app runs on 6.1.
     */
    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        if (!props.enabled() || registry.sources().isEmpty()) {
            log.info("Ingestion poller idle (enabled={}, sources={})", props.enabled(),
                    registry.sources().size());
            return;
        }
        registrar.addFixedDelayTask(this::poll, props.pollInterval());
        log.info("Ingestion poller scheduled every {}", props.pollInterval());
    }

    void poll() {
        for (MarketDataSource source : registry.sources()) {
            pollOne(source);
        }
    }

    private void pollOne(MarketDataSource source) {
        SourceState s = state.get(source.id());
        if (s.circuitOpen()) {
            return;
        }
        try {
            PollBatch batch = source.poll(s.cursor);
            Instant now = Instant.now();

            Instant maxListing = s.cursor.listingsSince();
            for (Listing l : batch.listings()) {
                maxListing = advance(maxListing, l.listedAt(), now);
                if (!s.dedup.addIfNew(l.eventId())) {
                    s.duplicates++;
                } else if (validator.accept(l)) {
                    publisher.publish(l);
                    s.ingested++;
                    metrics.counter("cardstream.ingestion.ingested", "source", source.id(), "type", "listing")
                            .increment();
                } else {
                    s.rejected++;
                }
            }

            Instant maxSale = s.cursor.salesSince();
            for (Sale sale : batch.sales()) {
                maxSale = advance(maxSale, sale.soldAt(), now);
                if (!s.dedup.addIfNew(sale.eventId())) {
                    s.duplicates++;
                } else if (validator.accept(sale)) {
                    publisher.publish(sale);
                    s.ingested++;
                    metrics.counter("cardstream.ingestion.ingested", "source", source.id(), "type", "sale")
                            .increment();
                } else {
                    s.rejected++;
                }
            }

            s.cursor = new SourceCursor(maxListing, maxSale);
            s.onSuccess();
        } catch (RuntimeException e) {
            s.onFailure();
            metrics.counter("cardstream.ingestion.poll.errors", "source", source.id()).increment();
            log.warn("[{}] poll failed ({} consecutive){}: {}", source.id(), s.consecutiveFailures,
                    s.circuitOpen() ? " — circuit open" : "", e.toString());
        }
    }

    /** New high-water mark: advance to {@code ts} only if it is newer and not in the future. */
    private static Instant advance(Instant current, Instant ts, Instant now) {
        if (ts == null || ts.isAfter(now)) {
            return current;
        }
        if (current == null || ts.isAfter(current)) {
            return ts;
        }
        return current;
    }

    /** Snapshot for the status endpoint. */
    public List<SourceStatus> status() {
        return registry.sources().stream()
                .map(src -> {
                    SourceState s = state.get(src.id());
                    return new SourceStatus(src.id(), !s.circuitOpen(), s.cursor.listingsSince(),
                            s.cursor.salesSince(), s.ingested, s.rejected, s.duplicates,
                            s.consecutiveFailures);
                })
                .toList();
    }

    public record SourceStatus(String id, boolean healthy, Instant listingsCursor, Instant salesCursor,
            long ingested, long rejected, long duplicates, int consecutiveFailures) {
    }

    /** Mutable per-source runtime: cursor, dedup window, counters, and circuit-breaker state. */
    private static final class SourceState {
        private final RecentIdCache dedup = new RecentIdCache(DEDUP_CAPACITY);
        private final int breakerThreshold;
        private final Duration breakerCooldown;

        private SourceCursor cursor = new SourceCursor(Instant.now(), Instant.now());
        private long ingested;
        private long rejected;
        private long duplicates;
        private int consecutiveFailures;
        private Instant openUntil = Instant.EPOCH;

        SourceState(int breakerThreshold, Duration breakerCooldown) {
            this.breakerThreshold = breakerThreshold;
            this.breakerCooldown = breakerCooldown;
        }

        boolean circuitOpen() {
            return Instant.now().isBefore(openUntil);
        }

        void onSuccess() {
            consecutiveFailures = 0;
            openUntil = Instant.EPOCH;
        }

        void onFailure() {
            consecutiveFailures++;
            if (consecutiveFailures >= breakerThreshold) {
                openUntil = Instant.now().plus(breakerCooldown);
            }
        }
    }
}
