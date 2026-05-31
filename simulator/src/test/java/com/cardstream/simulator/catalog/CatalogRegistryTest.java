package com.cardstream.simulator.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.simulator.catalog.BackendCatalogClient.CatalogCard;
import com.cardstream.simulator.config.SimulatorProperties;
import java.util.List;
import org.junit.jupiter.api.Test;

class CatalogRegistryTest {

    private CatalogRegistry registry(Condition... conditions) {
        return new CatalogRegistry(new SimulatorProperties(null, null, null, List.of(conditions)));
    }

    @Test
    void mintsProductsAndSkusWithBidirectionalMapping() {
        CatalogRegistry registry = registry(Condition.NM, Condition.LP);
        registry.rebuild(List.of(
                new CatalogCard("sv5-2", "Charizard", "Temporal Forces", "Illustration Rare"),
                new CatalogCard("sv5-1", "Pidgey", "Temporal Forces", "Common")));

        assertThat(registry.productCount()).isEqualTo(2);

        // Sorted by cardId, so sv5-1 gets the first product id.
        Product pidgey = registry.byCardId("sv5-1").orElseThrow();
        Product charizard = registry.byCardId("sv5-2").orElseThrow();
        assertThat(pidgey.productId()).isEqualTo(500_000L);
        assertThat(charizard.productId()).isEqualTo(500_001L);
        assertThat(registry.byProductId(500_000L)).contains(pidgey);

        // Common -> {NORMAL, REVERSE_HOLOFOIL} x 2 conditions = 4 skus.
        assertThat(pidgey.skus()).hasSize(4);
        // Rare -> {NORMAL, HOLOFOIL, REVERSE_HOLOFOIL} x 2 conditions = 6 skus.
        assertThat(charizard.skus()).hasSize(6);

        Sku sku = pidgey.skus().get(0);
        assertThat(registry.bySkuId(sku.skuId())).contains(sku);
        assertThat(sku.cardId()).isEqualTo("sv5-1");
    }

    @Test
    void finishHeuristicByRarity() {
        assertThat(CatalogRegistry.finishesFor("Common"))
                .containsExactly(Finish.NORMAL, Finish.REVERSE_HOLOFOIL);
        assertThat(CatalogRegistry.finishesFor("Uncommon"))
                .containsExactly(Finish.NORMAL, Finish.REVERSE_HOLOFOIL);
        assertThat(CatalogRegistry.finishesFor(null))
                .containsExactly(Finish.NORMAL, Finish.REVERSE_HOLOFOIL);
        assertThat(CatalogRegistry.finishesFor("Illustration Rare"))
                .containsExactly(Finish.NORMAL, Finish.HOLOFOIL, Finish.REVERSE_HOLOFOIL);
    }

    @Test
    void startsEmpty() {
        CatalogRegistry registry = registry(Condition.NM);
        assertThat(registry.isEmpty()).isTrue();
        assertThat(registry.products()).isEmpty();
    }
}
