package com.cardstream.backend.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Makes Kafka JSON values use the app {@link ObjectMapper} (ISO-8601, no type-info headers) by
 * customizing the auto-configured producer factory in place, so yml producer settings still apply.
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
