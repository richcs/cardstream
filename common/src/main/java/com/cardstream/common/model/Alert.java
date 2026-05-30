package com.cardstream.common.model;

import java.time.Instant;
import java.util.Map;

/** A market-intelligence alert emitted by the streams topology to the `alerts` topic. */
public record Alert(
        String alertId,
        AlertType type,
        Severity severity,
        String cardId,
        String marketKey,
        String name,
        Map<String, Object> detail,
        Instant ts) {
}
