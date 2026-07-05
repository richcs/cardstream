package com.cardstream.backend.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.cardstream.backend.serving.CardDetailResponse;
import com.cardstream.backend.sink.AlertRepository;
import com.cardstream.common.model.Alert;
import com.cardstream.common.model.AlertType;
import com.cardstream.common.model.CardMetadata;
import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.common.model.Game;
import com.cardstream.common.model.Listing;
import com.cardstream.common.model.MarketKey;
import com.cardstream.common.model.Sale;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Full-pipeline test against real Kafka + Postgres: publish sales/listings straight onto the topics
 * the topology reads, and assert an anomaly comes out the other end enriched, sunk, and queryable
 * over the public API — the same path {@code /admin/inject/*} exercises against the live simulator.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ingestion.enabled=false",
        "market.thresholds.min-samples=5"
})
class MarketPipelineE2EIT {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withKraft();

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

    static {
        kafka.start();
        postgres.start();
        createTopics();
    }

    private static final String CARD_ID = "e2e-1";
    private static final String CARD_NAME = "E2E Test Card";
    private static final MarketKey MARKET_KEY = new MarketKey(CARD_ID, Finish.NORMAL, Condition.NM);

    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private AlertRepository alerts;
    @Autowired
    private TestRestTemplate rest;

    private static void createTopics() {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(config)) {
            admin.createTopics(List.of(
                    new NewTopic("listings", 1, (short) 1),
                    new NewTopic("sales", 1, (short) 1),
                    new NewTopic("card-metadata", 1, (short) 1),
                    new NewTopic("agg-price-windowed", 1, (short) 1),
                    new NewTopic("arbitrage", 1, (short) 1),
                    new NewTopic("alerts", 1, (short) 1))).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Failed to create test topics", e);
        }
    }

    private <T> void produce(String topic, String key, T value) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            String json = objectMapper.writeValueAsString(value);
            producer.send(new ProducerRecord<>(topic, key, json)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to produce to " + topic, e);
        }
    }

    @Test
    void injectedSpikeAndArbitrageFlowThroughToTheApi() throws InterruptedException {
        jdbc.update("INSERT INTO card_set (set_id, name) VALUES ('e2e-set', 'E2E Set')");
        jdbc.update("INSERT INTO card (card_id, set_id, name, game) VALUES (?, 'e2e-set', ?, 'POKEMON')",
                CARD_ID, CARD_NAME);

        // Published before any sales; a real GlobalKTable takes a moment to catch up on a fresh broker.
        produce("card-metadata", CARD_ID,
                new CardMetadata(CARD_ID, CARD_NAME, "E2E Set", "Common", Game.POKEMON, null));
        Thread.sleep(3000);

        // Baseline: enough samples (minSamples=5) with real spread so stddev > 0.
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        double[] basePrices = {9.5, 10.0, 10.5, 9.8, 10.2};
        for (int i = 0; i < basePrices.length; i++) {
            Sale sale = new Sale(UUID.randomUUID().toString(), "e2e", CARD_ID, Finish.NORMAL, Condition.NM,
                    BigDecimal.valueOf(basePrices[i]), 1, t0.plus(i, ChronoUnit.SECONDS));
            produce("sales", MARKET_KEY.asString(), sale);
        }

        // Way outside 3 sigma of the baseline -> SPIKE.
        Sale spikeSale = new Sale(UUID.randomUUID().toString(), "e2e", CARD_ID, Finish.NORMAL, Condition.NM,
                BigDecimal.valueOf(100.0), 1, t0.plus(basePrices.length, ChronoUnit.SECONDS));
        produce("sales", MARKET_KEY.asString(), spikeSale);

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).untilAsserted(() -> {
            List<Alert> spikes = alerts.recent(AlertType.SPIKE, null, 10);
            assertThat(spikes).isNotEmpty();
            assertThat(spikes.get(0).cardId()).isEqualTo(CARD_ID);
            assertThat(spikes.get(0).name()).isEqualTo(CARD_NAME);
            assertThat(spikes.get(0).detail().get("sigma").toString()).isNotBlank();
        });

        // Reference average is now skewed by the spike sale, but this listing is far below it either way.
        Listing arbListing = new Listing(UUID.randomUUID().toString(), "e2e", CARD_ID, Finish.NORMAL, Condition.NM,
                BigDecimal.valueOf(1.0), 1, "e2e-seller", t0.plus(basePrices.length + 1, ChronoUnit.SECONDS));
        produce("listings", MARKET_KEY.asString(), arbListing);

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).untilAsserted(() -> {
            List<Alert> arbitrage = alerts.recent(AlertType.ARBITRAGE, null, 10);
            assertThat(arbitrage).isNotEmpty();
            assertThat(arbitrage.get(0).cardId()).isEqualTo(CARD_ID);
            assertThat(arbitrage.get(0).name()).isEqualTo(CARD_NAME);
        });

        // Same data, read back over the public REST surface.
        List<Alert> apiSpikes = rest.exchange("/api/alerts?type=SPIKE",
                HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Alert>>() { }).getBody();
        assertThat(apiSpikes).isNotEmpty();
        assertThat(apiSpikes.get(0).cardId()).isEqualTo(CARD_ID);

        List<Alert> apiArbitrage = rest.exchange("/api/arbitrage",
                HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Alert>>() { }).getBody();
        assertThat(apiArbitrage).isNotEmpty();
        assertThat(apiArbitrage.get(0).cardId()).isEqualTo(CARD_ID);

        CardDetailResponse detail = rest.getForObject("/api/cards/" + CARD_ID, CardDetailResponse.class);
        assertThat(detail.card().name()).isEqualTo(CARD_NAME);
        assertThat(detail.markets()).anySatisfy(m -> {
            assertThat(m.marketKey()).isEqualTo(MARKET_KEY.asString());
            assertThat(m.sampleCount()).isGreaterThanOrEqualTo(basePrices.length + 1L);
        });
    }
}
