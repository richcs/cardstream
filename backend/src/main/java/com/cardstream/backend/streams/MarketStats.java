package com.cardstream.backend.streams;

import com.cardstream.common.model.Sale;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Incremental sale-price statistics per ticker — the accumulator for every aggregation and the spike
 * detector's state. Holds running sums so mean/stddev are O(1) without retaining individual samples.
 */
public record MarketStats(
        long count,
        double sum,
        double sumSq,
        double min,
        double max,
        double last,
        long volume,
        Instant lastTs) {

    public static MarketStats empty() {
        return new MarketStats(0, 0, 0, Double.NaN, Double.NaN, Double.NaN, 0, null);
    }

    public MarketStats add(Sale sale) {
        return add(sale.price(), sale.quantity(), sale.soldAt());
    }

    public MarketStats add(BigDecimal price, int quantity, Instant ts) {
        double p = price.doubleValue();
        int qty = Math.max(quantity, 0);
        double newMin = count == 0 ? p : Math.min(min, p);
        double newMax = count == 0 ? p : Math.max(max, p);
        return new MarketStats(count + 1, sum + p, sumSq + p * p, newMin, newMax, p, volume + qty, ts);
    }

    public double mean() {
        return count == 0 ? 0.0 : sum / count;
    }

    /** Population variance; 0 until at least two samples are seen. */
    public double variance() {
        if (count < 2) {
            return 0.0;
        }
        double m = mean();
        double v = (sumSq / count) - (m * m);
        return v < 0 ? 0.0 : v; // guard against floating-point underflow on near-constant prices
    }

    public double stddev() {
        return Math.sqrt(variance());
    }
}
