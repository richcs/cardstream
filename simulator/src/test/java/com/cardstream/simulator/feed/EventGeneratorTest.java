package com.cardstream.simulator.feed;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.simulator.catalog.BackendCatalogClient.CatalogCard;
import com.cardstream.simulator.catalog.CatalogRegistry;
import com.cardstream.simulator.catalog.Product;
import com.cardstream.simulator.config.SimulatorProperties;
import com.cardstream.simulator.engine.PriceEngine;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EventGeneratorTest {

    private CatalogRegistry registry;
    private PriceEngine engine;
    private FeedBuffers buffers;
    private EventGenerator generator;

    @BeforeEach
    void setUp() {
        SimulatorProperties props = new SimulatorProperties(null, null, null, List.of(Condition.NM));
        registry = new CatalogRegistry(props);
        registry.rebuild(List.of(new CatalogCard("sv5-1", "Pidgey", "Temporal Forces", "Common")));
        engine = new PriceEngine(props);
        buffers = new FeedBuffers(props);
        generator = new EventGenerator(registry, engine, buffers, props);
    }

    @Test
    void injectSpikeRaisesPriceAndEmitsSaleBurst() {
        Product product = registry.byCardId("sv5-1").orElseThrow();
        BigDecimal before = engine.current(product, product.skus().get(0));

        Optional<EventGenerator.Injection> result =
                generator.injectSpike("sv5-1", Finish.NORMAL, Condition.NM, 3.0, 5);

        assertThat(result).isPresent();
        assertThat(result.get().type()).isEqualTo("SPIKE");
        assertThat(result.get().price()).isGreaterThan(before);
        assertThat(buffers.saleCount()).isEqualTo(5);
    }

    @Test
    void injectArbitrageEmitsBelowMarketListing() {
        Product product = registry.byCardId("sv5-1").orElseThrow();
        BigDecimal market = engine.current(product, product.skus().get(0));

        Optional<EventGenerator.Injection> result =
                generator.injectArbitrage("sv5-1", Finish.NORMAL, Condition.NM, 0.7);

        assertThat(result).isPresent();
        assertThat(result.get().type()).isEqualTo("ARBITRAGE");
        assertThat(result.get().price()).isLessThan(market);
        assertThat(buffers.listingCount()).isEqualTo(1);
    }

    @Test
    void injectingUnknownCardYieldsEmpty() {
        assertThat(generator.injectSpike("nope-1", Finish.NORMAL, Condition.NM, 2.0, 3)).isEmpty();
    }
}
