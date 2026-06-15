package com.cardstream.common.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A listing priced below the rolling market average, published to the {@code arbitrage} topic
 * (key = {@code marketKey}). {@code discountPct} is {@code (referenceAvg - listingPrice) / referenceAvg}.
 */
public record ArbitrageFlag(
        String marketKey,
        String cardId,
        Finish finish,
        Condition condition,
        String source,
        String sellerId,
        BigDecimal listingPrice,
        BigDecimal referenceAvg,
        double discountPct,
        long sampleCount,
        Instant detectedAt) {
}
