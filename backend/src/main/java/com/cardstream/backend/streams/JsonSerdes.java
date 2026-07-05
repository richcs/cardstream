package com.cardstream.backend.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Builds Kafka Streams JSON serdes from the app {@link ObjectMapper} (ISO-8601 instants, no
 * {@code __TypeId__} headers — same contract as the ingestion producer).
 */
final class JsonSerdes {

    private JsonSerdes() {
    }

    static <T> JsonSerde<T> serde(Class<T> type, ObjectMapper objectMapper) {
        return new JsonSerde<>(type, objectMapper).noTypeInfo().ignoreTypeHeaders();
    }
}
