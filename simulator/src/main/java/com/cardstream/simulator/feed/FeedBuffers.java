package com.cardstream.simulator.feed;

import com.cardstream.simulator.config.SimulatorProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * In-memory, time-ordered buffers backing the pollable {@code /listings} and {@code /sales} feeds.
 * Events are appended newest-last and pruned by age, so a poller catches up with {@code since=}.
 */
@Component
public class FeedBuffers {

    private final Ring<ListingEvent> listings;
    private final Ring<SaleEvent> sales;

    public FeedBuffers(SimulatorProperties props) {
        this.listings = new Ring<>(props.feed().retention());
        this.sales = new Ring<>(props.feed().retention());
    }

    public void add(ListingEvent e) {
        listings.add(e);
    }

    public void add(SaleEvent e) {
        sales.add(e);
    }

    /** Listings with timestamp strictly after {@code since} (null = from the start), up to {@code limit}. */
    public List<ListingEvent> listingsSince(Instant since, int limit) {
        return listings.since(since, limit);
    }

    public List<SaleEvent> salesSince(Instant since, int limit) {
        return sales.since(since, limit);
    }

    public int listingCount() {
        return listings.size();
    }

    public int saleCount() {
        return sales.size();
    }

    /** A retention-bounded, time-ordered buffer guarded by its own monitor. */
    private static final class Ring<T extends FeedEvent> {

        private final Duration retention;
        private final Deque<T> events = new ArrayDeque<>();

        Ring(Duration retention) {
            this.retention = retention;
        }

        synchronized void add(T event) {
            events.addLast(event);
            Instant cutoff = event.timestamp().minus(retention);
            while (!events.isEmpty() && events.peekFirst().timestamp().isBefore(cutoff)) {
                events.removeFirst();
            }
        }

        synchronized List<T> since(Instant since, int limit) {
            List<T> out = new ArrayList<>();
            for (T e : events) {
                if (since == null || e.timestamp().isAfter(since)) {
                    out.add(e);
                    if (out.size() >= limit) {
                        break;
                    }
                }
            }
            return out;
        }

        synchronized int size() {
            return events.size();
        }
    }
}
