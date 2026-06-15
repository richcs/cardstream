package com.cardstream.backend.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardstream.common.model.Alert;
import com.cardstream.common.model.AlertType;
import com.cardstream.common.model.ArbitrageFlag;
import com.cardstream.common.model.CardMetadata;
import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.common.model.Game;
import com.cardstream.common.model.Listing;
import com.cardstream.common.model.MarketKey;
import com.cardstream.common.model.Sale;
import com.cardstream.common.model.Severity;
import com.cardstream.common.model.WindowType;
import com.cardstream.common.model.WindowedAggregate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Drives {@link MarketTopology} offline with {@link TopologyTestDriver} — one operator per test. */
class MarketTopologyTest {

    private static final String CARD = "base1-4";
    private static final Instant T0 = Instant.parse("2026-06-01T10:00:00Z");

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    // minSamples lowered to 5 so tests stay compact; sigma/margin at production defaults.
    private final ThresholdProperties thresholds = new ThresholdProperties(3.0, new BigDecimal("0.15"), 5);

    private TopologyTestDriver driver;
    private TestInputTopic<String, Sale> salesIn;
    private TestInputTopic<String, Listing> listingsIn;
    private TestInputTopic<String, CardMetadata> metadataIn;
    private TestOutputTopic<String, Alert> alertsOut;
    private TestOutputTopic<String, ArbitrageFlag> arbitrageOut;
    private TestOutputTopic<String, WindowedAggregate> aggregatesOut;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new MarketTopology(mapper, thresholds, new SimpleMeterRegistry()).build(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class.getName());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        driver = new TopologyTestDriver(builder.build(), props);

        Serde<Sale> saleSerde = JsonSerdes.serde(Sale.class, mapper);
        Serde<Listing> listingSerde = JsonSerdes.serde(Listing.class, mapper);
        Serde<CardMetadata> metaSerde = JsonSerdes.serde(CardMetadata.class, mapper);
        Serde<Alert> alertSerde = JsonSerdes.serde(Alert.class, mapper);
        Serde<ArbitrageFlag> arbSerde = JsonSerdes.serde(ArbitrageFlag.class, mapper);
        Serde<WindowedAggregate> aggSerde = JsonSerdes.serde(WindowedAggregate.class, mapper);
        StringSerializer ks = new StringSerializer();
        StringDeserializer kd = new StringDeserializer();

        salesIn = driver.createInputTopic(MarketTopology.SALES_TOPIC, ks, saleSerde.serializer());
        listingsIn = driver.createInputTopic(MarketTopology.LISTINGS_TOPIC, ks, listingSerde.serializer());
        metadataIn = driver.createInputTopic(MarketTopology.CARD_METADATA_TOPIC, ks, metaSerde.serializer());
        alertsOut = driver.createOutputTopic(MarketTopology.ALERTS_TOPIC, kd, alertSerde.deserializer());
        arbitrageOut = driver.createOutputTopic(MarketTopology.ARBITRAGE_TOPIC, kd, arbSerde.deserializer());
        aggregatesOut = driver.createOutputTopic(MarketTopology.AGG_WINDOWED_TOPIC, kd, aggSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static String key() {
        return new MarketKey(CARD, Finish.HOLOFOIL, Condition.NM).asString();
    }

    private void publishMetadata() {
        metadataIn.pipeInput(CARD, new CardMetadata(CARD, "Charizard", "Base", "Rare Holo",
                Game.POKEMON, "https://img/charizard.png"));
    }

    private void pipeSale(double price, int qty, Instant ts) {
        salesIn.pipeInput(key(), new Sale(UUID.randomUUID().toString(), "sim", CARD,
                Finish.HOLOFOIL, Condition.NM, BigDecimal.valueOf(price), qty, ts));
    }

    private void pipeListing(double price, Instant ts) {
        listingsIn.pipeInput(key(), new Listing(UUID.randomUUID().toString(), "sim", CARD,
                Finish.HOLOFOIL, Condition.NM, BigDecimal.valueOf(price), 1, "s-1", ts));
    }

    @Test
    void emitsSpikeAlertWhenSaleDeviatesBeyondSigma() {
        publishMetadata();
        // Establish a baseline (mean ≈ 10) past the minSamples gate; moderate spread keeps σ small.
        double[] baseline = {10, 11, 9, 10, 11, 9};
        for (int i = 0; i < baseline.length; i++) {
            pipeSale(baseline[i], 1, T0.plus(Duration.ofMinutes(i)));
        }
        // A 50.00 sale is ~40σ above the running mean.
        pipeSale(50, 1, T0.plus(Duration.ofMinutes(30)));

        List<Alert> alerts = alertsOut.readValuesToList();
        assertThat(alerts).hasSize(1);
        Alert spike = alerts.get(0);
        assertThat(spike.type()).isEqualTo(AlertType.SPIKE);
        assertThat(spike.severity()).isEqualTo(Severity.HIGH);
        assertThat(spike.cardId()).isEqualTo(CARD);
        assertThat(spike.name()).isEqualTo("Charizard"); // GlobalKTable enrichment applied
        assertThat(spike.marketKey()).isEqualTo(key());
    }

    @Test
    void doesNotEmitSpikeBeforeMinSamples() {
        publishMetadata();
        // Only 3 samples (< minSamples=5): even a big jump cannot fire.
        pipeSale(10, 1, T0);
        pipeSale(10, 1, T0.plusSeconds(60));
        pipeSale(100, 1, T0.plusSeconds(120));

        assertThat(alertsOut.isEmpty()).isTrue();
    }

    @Test
    void flagsArbitrageWhenListingUndercutsRollingAverage() {
        publishMetadata();
        for (int i = 0; i < 6; i++) {
            pipeSale(10, 1, T0.plus(Duration.ofMinutes(i)));
        }
        // 5.00 < (1 − 0.15) × 10 = 8.50 -> arbitrage.
        pipeListing(5, T0.plus(Duration.ofMinutes(10)));

        List<ArbitrageFlag> flags = arbitrageOut.readValuesToList();
        assertThat(flags).hasSize(1);
        ArbitrageFlag flag = flags.get(0);
        assertThat(flag.cardId()).isEqualTo(CARD);
        assertThat(flag.listingPrice()).isEqualByComparingTo("5");
        assertThat(flag.referenceAvg()).isEqualByComparingTo("10.00");
        assertThat(flag.discountPct()).isEqualTo(0.5);
        assertThat(flag.sampleCount()).isEqualTo(6);

        List<Alert> alerts = alertsOut.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).type()).isEqualTo(AlertType.ARBITRAGE);
        assertThat(alerts.get(0).severity()).isEqualTo(Severity.HIGH); // 50% discount
        assertThat(alerts.get(0).name()).isEqualTo("Charizard");
    }

    @Test
    void doesNotFlagArbitrageWithinMargin() {
        for (int i = 0; i < 6; i++) {
            pipeSale(10, 1, T0.plus(Duration.ofMinutes(i)));
        }
        // 9.00 > 8.50 threshold -> not arbitrage.
        pipeListing(9, T0.plus(Duration.ofMinutes(10)));

        assertThat(arbitrageOut.isEmpty()).isTrue();
    }

    @Test
    void emitsSuppressedHourlyAggregateWhenWindowCloses() {
        // Three sales inside the 10:00–11:00 window.
        pipeSale(10, 1, T0.plus(Duration.ofMinutes(0)));
        pipeSale(20, 1, T0.plus(Duration.ofMinutes(10)));
        pipeSale(30, 1, T0.plus(Duration.ofMinutes(20)));
        // A sale in the next window advances stream time past 11:00, closing the first window.
        pipeSale(15, 1, T0.plus(Duration.ofMinutes(65)));

        List<WindowedAggregate> hourly = aggregatesOut.readValuesToList().stream()
                .filter(a -> a.windowType() == WindowType.HOURLY)
                .toList();
        assertThat(hourly).hasSize(1); // suppression -> exactly one settled emit for the closed window
        WindowedAggregate agg = hourly.get(0);
        assertThat(agg.windowStart()).isEqualTo(T0);
        assertThat(agg.avgPrice()).isEqualByComparingTo("20.00");
        assertThat(agg.sampleCount()).isEqualTo(3);
        assertThat(agg.volume()).isEqualTo(3);
    }
}
