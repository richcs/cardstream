package com.cardstream.common.model;

import java.math.BigDecimal;
import java.time.Instant;

/** A completed sale on the marketplace. Published to the `sales` topic. */
public record Sale(
        String eventId,
        String cardId,
        Finish finish,
        Condition condition,
        BigDecimal price,
        int quantity,
        Instant soldAt) {

    public MarketKey marketKey() {
        return new MarketKey(cardId, finish, condition);
    }
}
