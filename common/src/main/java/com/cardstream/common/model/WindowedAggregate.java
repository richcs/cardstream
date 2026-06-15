package com.cardstream.common.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A settled windowed aggregate per {@link MarketKey}, published to the {@code agg-price-windowed}
 * topic (key = {@code marketKey}). {@code avgPrice} is the window mean sale price (the moving average
 * for {@link WindowType#MA_24H}); {@code volatility} is its stddev over the same window.
 */
public record WindowedAggregate(
        String marketKey,
        String cardId,
        WindowType windowType,
        Instant windowStart,
        Instant windowEnd,
        BigDecimal avgPrice,
        double volatility,
        long volume,
        long sampleCount) {
}
