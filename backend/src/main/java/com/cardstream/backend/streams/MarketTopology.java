package com.cardstream.backend.streams;

import com.cardstream.common.model.Alert;
import com.cardstream.common.model.AlertType;
import com.cardstream.common.model.ArbitrageFlag;
import com.cardstream.common.model.CardMetadata;
import com.cardstream.common.model.Listing;
import com.cardstream.common.model.MarketKey;
import com.cardstream.common.model.Sale;
import com.cardstream.common.model.Severity;
import com.cardstream.common.model.WindowType;
import com.cardstream.common.model.WindowedAggregate;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

/**
 * The core Kafka Streams topology. Consumes the ingested {@code listings}/{@code sales} streams,
 * enriches against a {@code card-metadata} GlobalKTable, and derives live market intelligence:
 *
 * <ul>
 *   <li><b>Windowed aggregates</b> — hourly + daily tumbling (avg price + volume) and a 24h hopping
 *       window (advancing 1h) for moving average + volatility, each suppressed to one settled emit per
 *       window, published to {@code agg-price-windowed}.</li>
 *   <li><b>Arbitrage</b> — a KStream({@code listings})–KTable(rolling average) join flags listings
 *       priced below {@code (1 − margin) × avg} (gated by {@code minSamples}) to {@code arbitrage}.</li>
 *   <li><b>Spike</b> — a {@link SpikeDetector} processor emits when a sale deviates &gt;{@code σ} from
 *       its ticker's running mean (gated by {@code minSamples}).</li>
 *   <li><b>Alerts</b> — spike + arbitrage alerts are merged, metadata-enriched, branched by severity,
 *       and written to {@code alerts}.</li>
 * </ul>
 *
 * <p>Built as a plain class (not Spring-wired) so it can be exercised directly with
 * {@code TopologyTestDriver}; {@link KafkaStreamsTopologyConfig} injects the shared {@code StreamsBuilder}.
 */
public class MarketTopology {

    public static final String LISTINGS_TOPIC = "listings";
    public static final String SALES_TOPIC = "sales";
    public static final String CARD_METADATA_TOPIC = "card-metadata";
    public static final String AGG_WINDOWED_TOPIC = "agg-price-windowed";
    public static final String ARBITRAGE_TOPIC = "arbitrage";
    public static final String ALERTS_TOPIC = "alerts";

    /** Rolling per-ticker reference stats (KTable store) — also the Phase 5 Interactive Query source. */
    public static final String ARB_REFERENCE_STORE = "arb-ref-stats";

    private final ObjectMapper objectMapper;
    private final ThresholdProperties thresholds;
    private final MeterRegistry metrics;

    public MarketTopology(ObjectMapper objectMapper, ThresholdProperties thresholds, MeterRegistry metrics) {
        this.objectMapper = objectMapper;
        this.thresholds = thresholds;
        this.metrics = metrics;
    }

    public void build(StreamsBuilder builder) {
        Serde<String> keySerde = Serdes.String();
        Serde<Sale> saleSerde = JsonSerdes.serde(Sale.class, objectMapper);
        Serde<Listing> listingSerde = JsonSerdes.serde(Listing.class, objectMapper);
        Serde<CardMetadata> metaSerde = JsonSerdes.serde(CardMetadata.class, objectMapper);
        Serde<MarketStats> statsSerde = JsonSerdes.serde(MarketStats.class, objectMapper);
        Serde<WindowedAggregate> aggSerde = JsonSerdes.serde(WindowedAggregate.class, objectMapper);
        Serde<ArbitrageFlag> arbSerde = JsonSerdes.serde(ArbitrageFlag.class, objectMapper);
        Serde<Alert> alertSerde = JsonSerdes.serde(Alert.class, objectMapper);

        KStream<String, Sale> sales = builder.stream(SALES_TOPIC, Consumed.with(keySerde, saleSerde));
        KStream<String, Listing> listings = builder.stream(LISTINGS_TOPIC, Consumed.with(keySerde, listingSerde));
        GlobalKTable<String, CardMetadata> metadata =
                builder.globalTable(CARD_METADATA_TOPIC, Consumed.with(keySerde, metaSerde));

        // Sales are already keyed by MarketKey from ingestion, so grouping does not repartition.
        KGroupedStream<String, Sale> grouped = sales.groupByKey(Grouped.with(keySerde, saleSerde));

        // --- Windowed aggregates (one settled emit per window) -> agg-price-windowed ---
        windowedAggregate(grouped, WindowType.HOURLY,
                TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)), Duration.ofHours(2),
                keySerde, statsSerde, aggSerde);
        windowedAggregate(grouped, WindowType.DAILY,
                TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)), Duration.ofDays(2),
                keySerde, statsSerde, aggSerde);
        windowedAggregate(grouped, WindowType.MA_24H,
                TimeWindows.ofSizeWithNoGrace(Duration.ofHours(24)).advanceBy(Duration.ofHours(1)),
                Duration.ofDays(2), keySerde, statsSerde, aggSerde);

        // --- Rolling per-ticker reference average (the arbitrage join's table side) ---
        KTable<String, MarketStats> referenceStats = grouped.aggregate(
                MarketStats::empty,
                (key, sale, agg) -> agg.add(sale),
                Materialized.<String, MarketStats, KeyValueStore<Bytes, byte[]>>as(ARB_REFERENCE_STORE)
                        .withKeySerde(keySerde).withValueSerde(statsSerde));

        // --- Arbitrage: listings (stream) joined against the rolling average (table) ---
        KStream<String, ArbitrageFlag> arbitrage = listings
                .leftJoin(referenceStats, this::evaluateArbitrage, Joined.with(keySerde, listingSerde, statsSerde))
                .filter((key, flag) -> flag != null);
        arbitrage.to(ARBITRAGE_TOPIC, Produced.with(keySerde, arbSerde));
        KStream<String, Alert> arbitrageAlerts = arbitrage.mapValues(this::toAlert);

        // --- Spike: processor over sales with a per-ticker stats store ---
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(SpikeDetector.STORE), keySerde, statsSerde));
        KStream<String, Alert> spikeAlerts = sales.process(
                () -> new SpikeDetector(thresholds.spikeSigma(), thresholds.minSamples(), metrics),
                SpikeDetector.STORE);

        // --- Merge, enrich via GlobalKTable, branch by severity, emit to alerts ---
        KStream<String, Alert> alerts = spikeAlerts.merge(arbitrageAlerts)
                .leftJoin(metadata, (key, alert) -> alert.cardId(), MarketTopology::enrich);

        alerts.split(Named.as("severity-"))
                .branch((key, a) -> a.severity() == Severity.HIGH,
                        Branched.withConsumer(s -> routeAlerts(s, "HIGH", keySerde, alertSerde)))
                .branch((key, a) -> a.severity() == Severity.MED,
                        Branched.withConsumer(s -> routeAlerts(s, "MED", keySerde, alertSerde)))
                .defaultBranch(Branched.withConsumer(s -> routeAlerts(s, "LOW", keySerde, alertSerde)));
    }

    private void windowedAggregate(KGroupedStream<String, Sale> grouped, WindowType type,
            TimeWindows windows, Duration retention,
            Serde<String> keySerde, Serde<MarketStats> statsSerde, Serde<WindowedAggregate> aggSerde) {
        grouped.windowedBy(windows)
                .aggregate(MarketStats::empty, (key, sale, agg) -> agg.add(sale),
                        Materialized.<String, MarketStats, WindowStore<Bytes, byte[]>>as("agg-" + type.name().toLowerCase())
                                .withKeySerde(keySerde).withValueSerde(statsSerde).withRetention(retention))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((Windowed<String> wk, MarketStats stats) -> KeyValue.pair(wk.key(), toAggregate(wk, stats, type)))
                .to(AGG_WINDOWED_TOPIC, Produced.with(keySerde, aggSerde));
    }

    private void routeAlerts(KStream<String, Alert> stream, String severity,
            Serde<String> keySerde, Serde<Alert> alertSerde) {
        stream.peek((key, a) -> metrics.counter("cardstream.streams.alert",
                        "type", a.type().name(), "severity", severity).increment())
                .to(ALERTS_TOPIC, Produced.with(keySerde, alertSerde));
    }

    private static WindowedAggregate toAggregate(Windowed<String> wk, MarketStats stats, WindowType type) {
        return new WindowedAggregate(wk.key(), MarketKey.parse(wk.key()).cardId(), type,
                wk.window().startTime(), wk.window().endTime(),
                money(stats.mean()), stats.stddev(), stats.volume(), stats.count());
    }

    /** Join evaluator: returns an {@link ArbitrageFlag} when the listing undercuts the rolling avg, else null. */
    private ArbitrageFlag evaluateArbitrage(Listing listing, MarketStats stats) {
        if (stats == null || stats.count() < thresholds.minSamples()) {
            return null;
        }
        double mean = stats.mean();
        if (mean <= 0) {
            return null;
        }
        double threshold = mean * (1.0 - thresholds.arbitrageMargin().doubleValue());
        if (listing.price().doubleValue() >= threshold) {
            return null;
        }
        double discount = (mean - listing.price().doubleValue()) / mean;
        return new ArbitrageFlag(listing.marketKey().asString(), listing.cardId(),
                listing.finish(), listing.condition(), listing.source(), listing.sellerId(),
                listing.price(), money(mean), round4(discount), stats.count(), listing.listedAt());
    }

    private Alert toAlert(ArbitrageFlag flag) {
        Severity severity = flag.discountPct() >= 0.40 ? Severity.HIGH
                : flag.discountPct() >= 0.25 ? Severity.MED : Severity.LOW;
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("listingPrice", flag.listingPrice());
        detail.put("referenceAvg", flag.referenceAvg());
        detail.put("discountPct", flag.discountPct());
        detail.put("sampleCount", flag.sampleCount());
        detail.put("sellerId", flag.sellerId());
        return new Alert(UUID.randomUUID().toString(), AlertType.ARBITRAGE, severity,
                flag.cardId(), flag.marketKey(), null, detail, flag.detectedAt());
    }

    private static Alert enrich(Alert alert, CardMetadata meta) {
        if (meta == null) {
            return alert;
        }
        return new Alert(alert.alertId(), alert.type(), alert.severity(), alert.cardId(),
                alert.marketKey(), meta.name(), alert.detail(), alert.ts());
    }

    private static BigDecimal money(double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    private static double round4(double value) {
        return BigDecimal.valueOf(value).setScale(4, RoundingMode.HALF_UP).doubleValue();
    }
}
