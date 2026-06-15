package com.cardstream.backend.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Builds Kafka Streams JSON serdes from the application {@link ObjectMapper} so topology I/O matches
 * the documented wire format (ISO-8601 instants, no {@code __TypeId__} headers — same contract as the
 * ingestion producer in {@code KafkaProducerConfig}). Deserialization uses the explicit target type
 * and ignores any type headers, so it is robust to producer differences.
 */
final class JsonSerdes {

    private JsonSerdes() {
    }

    static <T> JsonSerde<T> serde(Class<T> type, ObjectMapper objectMapper) {
        return new JsonSerde<>(type, objectMapper).noTypeInfo().ignoreTypeHeaders();
    }
}
