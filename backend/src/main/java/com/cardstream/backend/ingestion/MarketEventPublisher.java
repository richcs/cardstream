package com.cardstream.backend.ingestion;

import com.cardstream.common.model.Listing;
import com.cardstream.common.model.Sale;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Produces normalized, validated events to Kafka, keyed by {@code MarketKey.asString()} so per-ticker
 * ordering is preserved across partitions. The producer is configured idempotent (see application.yml).
 */
@Component
public class MarketEventPublisher {

    static final String LISTINGS_TOPIC = "listings";
    static final String SALES_TOPIC = "sales";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public MarketEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(Listing listing) {
        kafkaTemplate.send(LISTINGS_TOPIC, listing.marketKey().asString(), listing);
    }

    public void publish(Sale sale) {
        kafkaTemplate.send(SALES_TOPIC, sale.marketKey().asString(), sale);
    }
}
