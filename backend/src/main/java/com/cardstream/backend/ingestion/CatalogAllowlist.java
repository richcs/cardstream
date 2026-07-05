package com.cardstream.backend.ingestion;

import com.cardstream.backend.metadata.CardRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Catalogued card ids ingestion is allowed to emit — kills key-forgery, junk tickers, and unbounded
 * state-store cardinality from a misbehaving source. Backed by Postgres, refreshed on a short TTL.
 */
@Component
public class CatalogAllowlist {

    private static final Logger log = LoggerFactory.getLogger(CatalogAllowlist.class);
    private static final Duration TTL = Duration.ofSeconds(30);

    private final CardRepository cards;
    private volatile Set<String> ids = Set.of();
    private volatile Instant loadedAt = Instant.EPOCH;

    public CatalogAllowlist(CardRepository cards) {
        this.cards = cards;
    }

    public boolean contains(String cardId) {
        refreshIfStale();
        return ids.contains(cardId);
    }

    public int size() {
        refreshIfStale();
        return ids.size();
    }

    private void refreshIfStale() {
        if (Duration.between(loadedAt, Instant.now()).compareTo(TTL) <= 0 && !ids.isEmpty()) {
            return;
        }
        try {
            ids = cards.allIds();
            loadedAt = Instant.now();
        } catch (RuntimeException e) {
            // Catalog table not ready (e.g. offline test context) — fail closed with an empty set.
            log.warn("Catalog allowlist refresh failed: {}", e.toString());
            loadedAt = Instant.now();
        }
    }
}
