package com.cardstream.simulator.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.simulator.catalog.Product;
import com.cardstream.simulator.catalog.Sku;
import com.cardstream.simulator.config.SimulatorProperties;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class PriceEngineTest {

    private static final Sku SKU = new Sku(1_000_000L, 500_000L, "sv5-1", Finish.HOLOFOIL, Condition.NM);
    private static final Product PRODUCT =
            new Product(500_000L, "sv5-1", "Charizard", "Temporal Forces", "Illustration Rare", List.of(SKU));

    private PriceEngine engine() {
        return new PriceEngine(new SimulatorProperties(null, null, null, null));
    }

    @Test
    void seedsDeterministicallyPerSku() {
        BigDecimal a = engine().current(PRODUCT, SKU);
        BigDecimal b = engine().current(PRODUCT, SKU);
        assertThat(a).isEqualByComparingTo(b);
        assertThat(a).isPositive();
    }

    @Test
    void spikeAppliesMultiplicativeJump() {
        PriceEngine engine = engine();
        BigDecimal before = engine.current(PRODUCT, SKU);
        BigDecimal after = engine.spike(PRODUCT, SKU, 2.0);
        assertThat(after).isEqualByComparingTo(before.multiply(BigDecimal.valueOf(2.0)).setScale(2, java.math.RoundingMode.HALF_UP));
    }

    @Test
    void advanceStaysPositiveAndAboveFloor() {
        PriceEngine engine = engine();
        BigDecimal floor = new BigDecimal("0.10");
        for (int i = 0; i < 1000; i++) {
            assertThat(engine.advance(PRODUCT, SKU)).isGreaterThanOrEqualTo(floor);
        }
    }

    @Test
    void resetClearsState() {
        PriceEngine engine = engine();
        engine.spike(PRODUCT, SKU, 5.0);
        BigDecimal spiked = engine.current(PRODUCT, SKU);
        engine.reset();
        // After reset the SKU reseeds to its deterministic base, not the spiked value.
        assertThat(engine.current(PRODUCT, SKU)).isLessThan(spiked);
    }
}
