package com.cardstream.common.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A settled windowed aggregate per {@link MarketKey}, published to {@code agg-price-windowed}.
 * {@code avgPrice} is the window mean (the moving average for {@link WindowType#MA_24H}).
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
