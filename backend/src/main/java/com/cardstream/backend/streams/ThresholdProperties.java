package com.cardstream.backend.streams;

import java.math.BigDecimal;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Market-intelligence thresholds (configurable under {@code market.thresholds.*}). {@code minSamples}
 * is the cold-start gate: spike/arbitrage cannot fire for a ticker until it has accumulated that many
 * samples, which blunts single-event poisoning.
 */
@ConfigurationProperties(prefix = "market.thresholds")
public record ThresholdProperties(
        double spikeSigma,
        BigDecimal arbitrageMargin,
        long minSamples) {

    public ThresholdProperties {
        if (spikeSigma <= 0) {
            spikeSigma = 3.0;
        }
        if (arbitrageMargin == null) {
            arbitrageMargin = new BigDecimal("0.15");
        }
        if (minSamples <= 0) {
            minSamples = 20;
        }
    }
}
