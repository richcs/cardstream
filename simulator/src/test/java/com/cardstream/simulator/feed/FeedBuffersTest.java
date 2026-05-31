package com.cardstream.simulator.feed;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardstream.common.model.Condition;
import com.cardstream.simulator.config.SimulatorProperties;
import com.cardstream.simulator.config.SimulatorProperties.Feed;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class FeedBuffersTest {

    private static final Instant T0 = Instant.parse("2026-05-30T12:00:00Z");

    private FeedBuffers buffers(Duration retention) {
        return new FeedBuffers(new SimulatorProperties(null, new Feed(0, 0, retention, 0), null, null));
    }

    private static ListingEvent listing(long id, Instant at) {
        return new ListingEvent(id, 500_000L, 1_000_000L, "Normal", Condition.NM,
                new BigDecimal("1.00"), 1, "s-1", at);
    }

    @Test
    void sinceReturnsOnlyNewerEvents() {
        FeedBuffers buffers = buffers(Duration.ofMinutes(5));
        buffers.add(listing(1, T0));
        buffers.add(listing(2, T0.plusSeconds(1)));
        buffers.add(listing(3, T0.plusSeconds(2)));

        assertThat(buffers.listingsSince(T0, 100)).extracting(ListingEvent::eventId).containsExactly(2L, 3L);
        assertThat(buffers.listingsSince(null, 100)).hasSize(3);
    }

    @Test
    void respectsLimit() {
        FeedBuffers buffers = buffers(Duration.ofMinutes(5));
        for (int i = 1; i <= 5; i++) {
            buffers.add(listing(i, T0.plusSeconds(i)));
        }
        assertThat(buffers.listingsSince(null, 2)).hasSize(2);
    }

    @Test
    void prunesEventsBeyondRetention() {
        FeedBuffers buffers = buffers(Duration.ofSeconds(10));
        buffers.add(listing(1, T0));
        buffers.add(listing(2, T0.plusSeconds(20))); // pushes cutoff past T0

        assertThat(buffers.listingCount()).isEqualTo(1);
        assertThat(buffers.listingsSince(null, 100)).extracting(ListingEvent::eventId).containsExactly(2L);
    }
}
