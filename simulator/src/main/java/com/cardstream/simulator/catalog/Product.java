package com.cardstream.simulator.catalog;

import java.util.List;

/**
 * A catalog product (a card printing family), standing in for a TCGplayer product: synthetic
 * {@code productId}, its {@code cardId} mapping, and the SKUs (finish × condition) it trades.
 */
public record Product(
        long productId,
        String cardId,
        String name,
        String setName,
        String rarity,
        List<Sku> skus) {
}
