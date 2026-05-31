package com.cardstream.backend.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RecentIdCacheTest {

    @Test
    void firstSightIsNewSecondIsDuplicate() {
        RecentIdCache cache = new RecentIdCache(100);
        assertThat(cache.addIfNew("sim:1")).isTrue();
        assertThat(cache.addIfNew("sim:1")).isFalse();
        assertThat(cache.addIfNew("sim:2")).isTrue();
    }

    @Test
    void evictsEldestPastCapacity() {
        RecentIdCache cache = new RecentIdCache(2);
        cache.addIfNew("a");
        cache.addIfNew("b");
        cache.addIfNew("c"); // evicts "a"
        assertThat(cache.size()).isEqualTo(2);
        // "a" was evicted, so it reads as new again; "b"/"c" still remembered.
        assertThat(cache.addIfNew("a")).isTrue();
        assertThat(cache.addIfNew("c")).isFalse();
    }
}
