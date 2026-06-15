package com.cardstream.backend.streams;

import com.cardstream.common.model.Alert;
import com.cardstream.common.model.AlertType;
import com.cardstream.common.model.Sale;
import com.cardstream.common.model.Severity;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Spike detection over the {@code sales} stream. Each sale is compared against the <em>prior</em>
 * distribution for its ticker (held in a keyed {@link KeyValueStore}); when the deviation exceeds
 * {@code spikeSigma} standard deviations — and the ticker has cleared the {@code minSamples} cold-start
 * gate — a {@link AlertType#SPIKE} alert is forwarded, then the sale folds into the stats.
 *
 * <p>Comparing before folding keeps a sale out of its own baseline. The Processor API (not a DSL
 * stream-table join) is used so the read-evaluate-update order is explicit and free of same-source
 * races.
 */
class SpikeDetector implements Processor<String, Sale, String, Alert> {

    static final String STORE = "spike-stats";

    private final double sigma;
    private final long minSamples;
    private final MeterRegistry metrics;

    private KeyValueStore<String, MarketStats> store;
    private ProcessorContext<String, Alert> context;

    SpikeDetector(double sigma, long minSamples, MeterRegistry metrics) {
        this.sigma = sigma;
        this.minSamples = minSamples;
        this.metrics = metrics;
    }

    @Override
    public void init(ProcessorContext<String, Alert> context) {
        this.context = context;
        this.store = context.getStateStore(STORE);
    }

    @Override
    public void process(Record<String, Sale> record) {
        Sale sale = record.value();
        if (sale == null) {
            return;
        }
        String key = record.key();
        MarketStats stats = store.get(key);
        if (stats == null) {
            stats = MarketStats.empty();
        }

        if (stats.count() >= minSamples) {
            double sd = stats.stddev();
            double mean = stats.mean();
            if (sd > 0) {
                double ratio = Math.abs(sale.price().doubleValue() - mean) / sd;
                if (ratio > sigma) {
                    Severity severity = severity(ratio);
                    context.forward(record.withValue(alert(sale, mean, sd, ratio, stats.count(), severity)));
                    metrics.counter("cardstream.streams.spike", "severity", severity.name()).increment();
                }
            }
        }

        store.put(key, stats.add(sale));
    }

    private static Alert alert(Sale sale, double mean, double sd, double ratio, long samples, Severity severity) {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("price", sale.price());
        detail.put("mean", round(mean));
        detail.put("stddev", round(sd));
        detail.put("sigma", round(ratio));
        detail.put("sampleCount", samples);
        return new Alert(UUID.randomUUID().toString(), AlertType.SPIKE, severity,
                sale.cardId(), sale.marketKey().asString(), null, detail, sale.soldAt());
    }

    private static Severity severity(double ratioSigma) {
        if (ratioSigma >= 5.0) {
            return Severity.HIGH;
        }
        if (ratioSigma >= 4.0) {
            return Severity.MED;
        }
        return Severity.LOW;
    }

    private static BigDecimal round(double d) {
        return BigDecimal.valueOf(d).setScale(4, RoundingMode.HALF_UP);
    }
}
