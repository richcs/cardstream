package com.cardstream.simulator.feed;

import com.cardstream.common.model.Condition;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * A TCGplayer-shaped listing as served by {@code GET /listings?since=}. Keyed externally by the
 * synthetic {@code productId}/{@code skuId}; the ingestion poller maps it to a canonical event.
 */
public record ListingEvent(
        long eventId,
        long productId,
        long skuId,
        String subTypeName,
        Condition condition,
        BigDecimal price,
        int quantity,
        String sellerId,
        Instant listedAt) implements FeedEvent {

    @Override
    public Instant timestamp() {
        return listedAt;
    }
}
