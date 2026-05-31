package com.cardstream.common.model;

import java.math.BigDecimal;
import java.time.Instant;

/** A card offered for sale on the marketplace. Published to the `listings` topic. */
public record Listing(
        String eventId,
        String source,
        String cardId,
        Finish finish,
        Condition condition,
        BigDecimal price,
        int quantity,
        String sellerId,
        Instant listedAt) {

    public MarketKey marketKey() {
        return new MarketKey(cardId, finish, condition);
    }
}
