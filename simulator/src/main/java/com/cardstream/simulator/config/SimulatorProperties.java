package com.cardstream.simulator.config;

import com.cardstream.common.model.Condition;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Tunables for the marketplace simulator: catalog seeding, feed rate, and the price walk. */
@ConfigurationProperties(prefix = "simulator")
public record SimulatorProperties(Backend backend, Feed feed, Walk walk, List<Condition> conditions) {

    public SimulatorProperties {
        if (backend == null) {
            backend = new Backend(null, 0, 0);
        }
        if (feed == null) {
            feed = new Feed(0, 0, null, 0);
        }
        if (walk == null) {
            walk = new Walk(0, 0);
        }
        if (conditions == null || conditions.isEmpty()) {
            conditions = List.of(Condition.NM, Condition.LP, Condition.MP, Condition.HP, Condition.DMG);
        }
    }

    /** Where the product universe is seeded from (the backend catalog), with startup retry. */
    public record Backend(String baseUrl, int seedRetryAttempts, long seedRetryDelayMs) {
        public Backend {
            if (baseUrl == null || baseUrl.isBlank()) {
                baseUrl = "http://localhost:8080";
            }
            if (seedRetryAttempts <= 0) {
                seedRetryAttempts = 30;
            }
            if (seedRetryDelayMs <= 0) {
                seedRetryDelayMs = 2000;
            }
        }
    }

    /** Event generation: target rate, sale/listing mix, and how long the pollable buffers retain. */
    public record Feed(int eventsPerSecond, double saleRatio, Duration retention, int defaultLimit) {
        public Feed {
            if (eventsPerSecond <= 0) {
                eventsPerSecond = 200;
            }
            if (saleRatio <= 0 || saleRatio >= 1) {
                saleRatio = 0.3; // ~30% sales, ~70% listings
            }
            if (retention == null || retention.isZero() || retention.isNegative()) {
                retention = Duration.ofMinutes(5);
            }
            if (defaultLimit <= 0) {
                defaultLimit = 1000;
            }
        }
    }

    /** Geometric random walk per SKU: multiplicative log-normal step. */
    public record Walk(double sigma, double drift) {
        public Walk {
            if (sigma <= 0) {
                sigma = 0.02;
            }
        }
    }
}
