package com.cardstream.backend.sink;

import com.cardstream.backend.serving.ws.AlertWebSocketHandler;
import com.cardstream.backend.serving.ws.PriceSseController;
import com.cardstream.backend.streams.MarketTopology;
import com.cardstream.common.model.Alert;
import com.cardstream.common.model.WindowedAggregate;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Dedicated Spring Kafka consumer (not Kafka Connect) sinking the topology's settled output to
 * Postgres for history/derived reads, and fanning the same records out to the live WS/SSE feeds.
 * Values are parsed manually with the app {@link ObjectMapper} — the wire format has no type headers
 * (see {@code KafkaProducerConfig}), so a single String-valued consumer factory covers every topic.
 */
@Component
public class MarketSinkConsumer {

    private static final Logger log = LoggerFactory.getLogger(MarketSinkConsumer.class);

    private final ObjectMapper objectMapper;
    private final PriceWindowRepository priceWindows;
    private final AlertRepository alerts;
    private final PriceSseController priceFeed;
    private final AlertWebSocketHandler alertFeed;

    public MarketSinkConsumer(ObjectMapper objectMapper, PriceWindowRepository priceWindows,
            AlertRepository alerts, PriceSseController priceFeed, AlertWebSocketHandler alertFeed) {
        this.objectMapper = objectMapper;
        this.priceWindows = priceWindows;
        this.alerts = alerts;
        this.priceFeed = priceFeed;
        this.alertFeed = alertFeed;
    }

    // Distinct group ids per topic: a shared group id across heterogeneous topic subscriptions is a
    // known Kafka consumer-group pitfall (inconsistent member subscriptions across a rebalance).
    @KafkaListener(id = "sink-agg-price-windowed", topics = MarketTopology.AGG_WINDOWED_TOPIC,
            groupId = "cardstream-sink-price")
    public void onWindowedAggregate(ConsumerRecord<String, String> record) {
        try {
            WindowedAggregate aggregate = objectMapper.readValue(record.value(), WindowedAggregate.class);
            priceWindows.upsert(aggregate);
            priceFeed.broadcast(aggregate);
        } catch (Exception e) {
            log.warn("Skipping unreadable agg-price-windowed record at offset {}", record.offset(), e);
        }
    }

    @KafkaListener(id = "sink-alerts", topics = MarketTopology.ALERTS_TOPIC,
            groupId = "cardstream-sink-alerts")
    public void onAlert(ConsumerRecord<String, String> record) {
        try {
            Alert alert = objectMapper.readValue(record.value(), Alert.class);
            alerts.insert(alert);
            alertFeed.broadcast(alert);
        } catch (Exception e) {
            log.warn("Skipping unreadable alert record at offset {}", record.offset(), e);
        }
    }
}
