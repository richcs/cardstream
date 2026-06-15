package com.cardstream.backend.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.common.model.Sale;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class MarketStatsTest {

    private static Sale sale(double price, int qty) {
        return new Sale("e", "sim", "base1-4", Finish.HOLOFOIL, Condition.NM,
                BigDecimal.valueOf(price), qty, Instant.parse("2026-06-01T10:00:00Z"));
    }

    @Test
    void emptyHasNoMeanOrDeviation() {
        MarketStats s = MarketStats.empty();
        assertThat(s.count()).isZero();
        assertThat(s.mean()).isZero();
        assertThat(s.stddev()).isZero();
    }

    @Test
    void accumulatesMeanVarianceAndVolume() {
        MarketStats s = MarketStats.empty().add(sale(10, 1)).add(sale(20, 2)).add(sale(30, 3));

        assertThat(s.count()).isEqualTo(3);
        assertThat(s.volume()).isEqualTo(6);
        assertThat(s.mean()).isEqualTo(20.0);
        assertThat(s.min()).isEqualTo(10.0);
        assertThat(s.max()).isEqualTo(30.0);
        // Population variance of {10,20,30} = 200/3; stddev ≈ 8.165.
        assertThat(s.variance()).isCloseTo(200.0 / 3.0, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(s.stddev()).isCloseTo(8.1650, org.assertj.core.data.Offset.offset(1e-3));
    }

    @Test
    void singleSampleHasZeroDeviation() {
        MarketStats s = MarketStats.empty().add(sale(42, 1));
        assertThat(s.mean()).isEqualTo(42.0);
        assertThat(s.stddev()).isZero();
    }
}
