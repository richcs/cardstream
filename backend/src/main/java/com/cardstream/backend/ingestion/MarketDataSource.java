package com.cardstream.backend.ingestion;

import com.cardstream.common.model.Listing;
import com.cardstream.common.model.Sale;
import java.time.Instant;
import java.util.List;

/**
 * A pluggable marketplace feed. An adapter polls its upstream and returns already-normalized canonical
 * events; the cursor is owned by the poller (passed in, advanced poller-side) so adapters stay stateless.
 */
public interface MarketDataSource {

    /** Logical source id (e.g. {@code "sim"}); tags every event and keys per-source cursor/dedup/metrics. */
    String id();

    /** Fetch everything newer than the cursor, normalized to canonical events. */
    PollBatch poll(SourceCursor cursor);

    /** Per-feed high-water marks. {@code null} means "from the beginning" (omit {@code since}). */
    record SourceCursor(Instant listingsSince, Instant salesSince) {
        public static final SourceCursor EMPTY = new SourceCursor(null, null);
    }

    /** Normalized events from one poll. */
    record PollBatch(List<Listing> listings, List<Sale> sales) {
        public boolean isEmpty() {
            return listings.isEmpty() && sales.isEmpty();
        }
    }
}
