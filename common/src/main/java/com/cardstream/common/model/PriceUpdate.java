package com.cardstream.common.model;

import java.math.BigDecimal;
import java.time.Instant;

/** A price change on an existing listing. Published to the `price-updates` topic. */
public record PriceUpdate(
        String eventId,
        String source,
        String cardId,
        Finish finish,
        Condition condition,
        BigDecimal oldPrice,
        BigDecimal newPrice,
        String listingId,
        Instant updatedAt) {

    public MarketKey marketKey() {
        return new MarketKey(cardId, finish, condition);
    }
}
