package com.cardstream.simulator.engine;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.simulator.catalog.Product;
import com.cardstream.simulator.catalog.Sku;
import com.cardstream.simulator.config.SimulatorProperties;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/**
 * Per-SKU price state driven by a geometric (log-normal) random walk, so prices stay positive and
 * move multiplicatively. Each SKU's starting price is seeded deterministically from rarity, finish,
 * and condition; spikes apply an instantaneous multiplicative jump.
 */
@Component
public class PriceEngine {

    private static final BigDecimal FLOOR = new BigDecimal("0.10");

    private final double sigma;
    private final double drift;
    private final Map<Long, BigDecimal> prices = new ConcurrentHashMap<>();

    public PriceEngine(SimulatorProperties props) {
        this.sigma = props.walk().sigma();
        this.drift = props.walk().drift();
    }

    /** Current price, seeding the SKU on first touch. */
    public BigDecimal current(Product product, Sku sku) {
        return prices.computeIfAbsent(sku.skuId(), id -> seed(product, sku));
    }

    /** Apply one walk step and return the new price. */
    public BigDecimal advance(Product product, Sku sku) {
        return prices.compute(sku.skuId(), (id, prev) -> {
            BigDecimal base = prev != null ? prev : seed(product, sku);
            double z = ThreadLocalRandom.current().nextGaussian();
            double factor = Math.exp(drift + sigma * z);
            return scale(base.multiply(BigDecimal.valueOf(factor)));
        });
    }

    /** Instantaneous multiplicative jump (spike injection); returns the new price. */
    public BigDecimal spike(Product product, Sku sku, double factor) {
        return prices.compute(sku.skuId(), (id, prev) -> {
            BigDecimal base = prev != null ? prev : seed(product, sku);
            return scale(base.multiply(BigDecimal.valueOf(factor)));
        });
    }

    /** Drop all price state (used after a catalog reload). */
    public void reset() {
        prices.clear();
    }

    private static BigDecimal scale(BigDecimal raw) {
        BigDecimal v = raw.setScale(2, RoundingMode.HALF_UP);
        return v.compareTo(FLOOR) < 0 ? FLOOR : v;
    }

    /** Deterministic starting price from rarity × finish × condition, with per-SKU jitter. */
    private BigDecimal seed(Product product, Sku sku) {
        double base = baseForRarity(product.rarity());
        double price = base * finishMultiplier(sku.finish()) * conditionMultiplier(sku.condition());
        // Stable jitter so the same SKU seeds identically across runs.
        double jitter = 0.85 + new Random(sku.skuId()).nextDouble() * 0.30; // 0.85–1.15
        return scale(BigDecimal.valueOf(price * jitter));
    }

    private static double baseForRarity(String rarity) {
        String r = rarity == null ? "" : rarity.toLowerCase();
        if (r.contains("special illustration")) return 30.0;
        if (r.contains("illustration")) return 12.0;
        if (r.contains("hyper") || r.contains("secret") || r.contains("rainbow") || r.contains("gold")) return 40.0;
        if (r.contains("ultra")) return 10.0;
        if (r.contains("double")) return 5.0;
        if (r.contains("holo")) return 3.0;
        if (r.contains("rare")) return 1.5;
        if (r.contains("uncommon")) return 0.5;
        if (r.contains("common")) return 0.25;
        return 2.0;
    }

    private static double finishMultiplier(Finish finish) {
        return switch (finish) {
            case NORMAL -> 1.0;
            case REVERSE_HOLOFOIL -> 1.15;
            case HOLOFOIL -> 1.4;
        };
    }

    private static double conditionMultiplier(Condition condition) {
        return switch (condition) {
            case NM -> 1.0;
            case LP -> 0.8;
            case MP -> 0.6;
            case HP -> 0.4;
            case DMG -> 0.25;
        };
    }
}
