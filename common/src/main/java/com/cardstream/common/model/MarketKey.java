package com.cardstream.common.model;

import java.util.Objects;

/**
 * Identity of a tradable "ticker": a specific card printing in a specific finish and condition.
 * Serialized form ("{cardId}|{finish}|{condition}") is used as the Kafka record key.
 */
public record MarketKey(String cardId, Finish finish, Condition condition) {

    public MarketKey {
        Objects.requireNonNull(cardId, "cardId");
        Objects.requireNonNull(finish, "finish");
        Objects.requireNonNull(condition, "condition");
    }

    public String asString() {
        return cardId + "|" + finish.name() + "|" + condition.name();
    }

    public static MarketKey parse(String key) {
        String[] parts = key.split("\\|", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid market key: " + key);
        }
        return new MarketKey(parts[0], Finish.valueOf(parts[1]), Condition.valueOf(parts[2]));
    }

    @Override
    public String toString() {
        return asString();
    }
}
