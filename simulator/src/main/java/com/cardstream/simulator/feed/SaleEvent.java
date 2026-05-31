package com.cardstream.simulator.feed;

import com.cardstream.common.model.Condition;
import java.math.BigDecimal;
import java.time.Instant;

/** A TCGplayer-shaped completed sale as served by {@code GET /sales?since=}. */
public record SaleEvent(
        long eventId,
        long productId,
        long skuId,
        String subTypeName,
        Condition condition,
        BigDecimal price,
        int quantity,
        Instant soldAt) implements FeedEvent {

    @Override
    public Instant timestamp() {
        return soldAt;
    }
}
