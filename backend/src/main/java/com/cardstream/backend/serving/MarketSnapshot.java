package com.cardstream.backend.serving;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Current state of one ticker, read live off the topology's rolling reference-stats store (not a
 * closed window). {@code avgPrice}/{@code volatility} are all-time running mean/stddev.
 */
public record MarketSnapshot(
        String marketKey,
        String cardId,
        Finish finish,
        Condition condition,
        BigDecimal lastPrice,
        Instant lastTradeAt,
        BigDecimal avgPrice,
        double volatility,
        long volume,
        long sampleCount) {
}
