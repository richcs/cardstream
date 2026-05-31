package com.cardstream.simulator.api;

import com.cardstream.simulator.config.SimulatorProperties;
import com.cardstream.simulator.feed.FeedBuffers;
import com.cardstream.simulator.feed.ListingEvent;
import com.cardstream.simulator.feed.SaleEvent;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Pollable listing/sale feeds. The ingestion poller calls these with the timestamp of the last
 * event it saw ({@code since}, ISO-8601 instant) and ingests everything newer.
 */
@RestController
public class FeedController {

    private final FeedBuffers buffers;
    private final int defaultLimit;

    public FeedController(FeedBuffers buffers, SimulatorProperties props) {
        this.buffers = buffers;
        this.defaultLimit = props.feed().defaultLimit();
    }

    @GetMapping("/listings")
    public List<ListingEvent> listings(
            @RequestParam(required = false) String since,
            @RequestParam(required = false) Integer limit) {
        return buffers.listingsSince(parseSince(since), limit(limit));
    }

    @GetMapping("/sales")
    public List<SaleEvent> sales(
            @RequestParam(required = false) String since,
            @RequestParam(required = false) Integer limit) {
        return buffers.salesSince(parseSince(since), limit(limit));
    }

    private int limit(Integer requested) {
        if (requested == null || requested <= 0) {
            return defaultLimit;
        }
        return Math.min(requested, defaultLimit);
    }

    private static Instant parseSince(String since) {
        if (since == null || since.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(since);
        } catch (DateTimeParseException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Invalid 'since' (expected ISO-8601 instant, e.g. 2026-05-30T12:00:00Z): " + since);
        }
    }
}
