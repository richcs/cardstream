package com.cardstream.backend.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Make Kafka JSON values use the application's {@link ObjectMapper} (ISO-8601 instants, not epoch
 * numbers) so the wire format matches the documented event schema, and drop type-info headers —
 * downstream consumers deserialize with explicit target types. Customizes the auto-configured
 * producer factory in place, so all yml producer settings (idempotence, acks, bootstrap) still apply.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    @SuppressWarnings("unchecked")
    DefaultKafkaProducerFactoryCustomizer jsonValueSerializerCustomizer(ObjectMapper objectMapper) {
        return factory -> ((org.springframework.kafka.core.DefaultKafkaProducerFactory<Object, Object>) factory)
                .setValueSerializer(new JsonSerializer<>(objectMapper).noTypeInfo());
    }
}
