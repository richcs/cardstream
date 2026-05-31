package com.cardstream.backend.metadata;

import com.cardstream.common.model.CardMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Publishes catalog entries to the compacted {@code card-metadata} topic, keyed by cardId. */
@Component
public class CardMetadataPublisher {

    static final String TOPIC = "card-metadata";

    private final KafkaTemplate<String, CardMetadata> kafkaTemplate;

    public CardMetadataPublisher(KafkaTemplate<String, CardMetadata> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(CardMetadata card) {
        kafkaTemplate.send(TOPIC, card.cardId(), card);
    }
}
