package com.cardstream.backend.metadata;

import com.cardstream.common.model.Game;

/** Catalog row as served by the REST API (card joined to its set). */
public record CardView(
        String cardId,
        String name,
        String number,
        String rarity,
        String supertype,
        Game game,
        String setId,
        String setName,
        String imageSmall,
        String imageLarge) {
}
