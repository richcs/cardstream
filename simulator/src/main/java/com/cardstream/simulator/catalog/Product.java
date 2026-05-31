package com.cardstream.simulator.catalog;

import java.util.List;

/**
 * A catalog product (a single card printing family), standing in for a TCGplayer product.
 * Carries the synthetic {@code productId} and its mapping back to the Pokémon {@code cardId},
 * plus the SKUs (finish × condition) the simulator prices and trades.
 */
public record Product(
        long productId,
        String cardId,
        String name,
        String setName,
        String rarity,
        List<Sku> skus) {
}
