package com.cardstream.backend.ingestion;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded LRU cache of recently-seen event ids for one source — guards against {@code since=} overlap
 * re-delivery (the same event returned on consecutive polls). Eldest ids are evicted past capacity;
 * dedup is best-effort within that window, complementing the idempotent producer.
 */
public class RecentIdCache {

    private static final Object PRESENT = new Object();

    private final Map<String, Object> seen;

    public RecentIdCache(int capacity) {
        int cap = Math.max(1, capacity);
        this.seen = new LinkedHashMap<>(cap * 2, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
                return size() > cap;
            }
        };
    }

    /** Record the id; returns {@code true} if it was new (not a duplicate). */
    public synchronized boolean addIfNew(String eventId) {
        return seen.put(eventId, PRESENT) == null;
    }

    public synchronized int size() {
        return seen.size();
    }
}
