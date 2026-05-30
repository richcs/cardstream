package com.cardstream.common.model;

/** Card catalog metadata. Published (compacted, key = cardId) to the `card-metadata` topic. */
public record CardMetadata(
        String cardId,
        String name,
        String set,
        String rarity,
        Game game,
        String imageUrl) {
}
