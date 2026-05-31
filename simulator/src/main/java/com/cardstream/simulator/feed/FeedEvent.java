package com.cardstream.simulator.feed;

import java.time.Instant;

/** Common shape for pollable feed items: a monotonic id and an event timestamp. */
public interface FeedEvent {
    long eventId();

    Instant timestamp();
}
