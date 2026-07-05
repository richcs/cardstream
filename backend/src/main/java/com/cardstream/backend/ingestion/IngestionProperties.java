package com.cardstream.backend.ingestion;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Ingestion config. Sources are pluggable, keyed by a logical id under {@code ingestion.sources.*};
 * the poller runs only the enabled ones.
 */
@ConfigurationProperties(prefix = "ingestion")
public record IngestionProperties(
        boolean enabled,
        Duration pollInterval,
        Validation validation,
        Map<String, SourceConfig> sources) {

    public IngestionProperties {
        if (pollInterval == null) {
            pollInterval = Duration.ofSeconds(2);
        }
        if (validation == null) {
            validation = new Validation(null, null, 0, null, null, 0);
        }
        if (sources == null) {
            sources = Map.of();
        }
    }

    /** One pluggable feed. {@code type} selects the adapter (only {@code tcgplayer-rest} for MVP). */
    public record SourceConfig(
            boolean enabled,
            String type,
            String baseUrl,
            String apiKey,
            Duration connectTimeout,
            Duration readTimeout,
            int maxEventsPerPoll,
            int circuitBreakerThreshold,
            Duration circuitBreakerCooldown) {

        public SourceConfig {
            if (type == null || type.isBlank()) {
                type = "tcgplayer-rest";
            }
            if (connectTimeout == null) {
                connectTimeout = Duration.ofSeconds(2);
            }
            if (readTimeout == null) {
                readTimeout = Duration.ofSeconds(5);
            }
            if (maxEventsPerPoll <= 0) {
                maxEventsPerPoll = 1000;
            }
            if (circuitBreakerThreshold <= 0) {
                circuitBreakerThreshold = 5;
            }
            if (circuitBreakerCooldown == null) {
                circuitBreakerCooldown = Duration.ofSeconds(30);
            }
        }
    }

    /**
     * Trust-boundary bounds applied to every event before it reaches Kafka. Out-of-bounds events are
     * rejected (and counted), never ingested.
     */
    public record Validation(
            BigDecimal minPrice,
            BigDecimal maxPrice,
            int maxQuantity,
            Duration maxFutureSkew,
            Duration maxAge,
            int maxPriceScale) {

        public Validation {
            if (minPrice == null) {
                minPrice = new BigDecimal("0.01");
            }
            if (maxPrice == null) {
                maxPrice = new BigDecimal("1000000");
            }
            if (maxQuantity <= 0) {
                maxQuantity = 10000;
            }
            if (maxFutureSkew == null) {
                maxFutureSkew = Duration.ofMinutes(5);
            }
            if (maxAge == null) {
                maxAge = Duration.ofDays(365);
            }
            if (maxPriceScale <= 0) {
                maxPriceScale = 2;
            }
        }
    }
}
