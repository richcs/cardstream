package com.cardstream.backend.ingestion;

import com.cardstream.backend.ingestion.IngestionProperties.SourceConfig;
import com.cardstream.backend.ingestion.source.TcgplayerRestSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Builds the enabled {@link MarketDataSource} adapters from {@code ingestion.sources.*}. Adding a
 * second feed is config-only: flip {@code enabled} and point {@code base-url}/{@code api-key} at it.
 */
@Component
public class SourceRegistry {

    private static final Logger log = LoggerFactory.getLogger(SourceRegistry.class);

    private final List<MarketDataSource> sources;

    public SourceRegistry(IngestionProperties props, RestClient.Builder restClientBuilder,
            MeterRegistry metrics) {
        this.sources = props.sources().entrySet().stream()
                .filter(e -> e.getValue().enabled())
                .map(e -> build(e.getKey(), e.getValue(), restClientBuilder, metrics))
                .filter(java.util.Objects::nonNull)
                .toList();
        log.info("Ingestion sources enabled: {}", sources.stream().map(MarketDataSource::id).toList());
    }

    public List<MarketDataSource> sources() {
        return sources;
    }

    private static MarketDataSource build(String id, SourceConfig config,
            RestClient.Builder builder, MeterRegistry metrics) {
        return switch (config.type()) {
            case "tcgplayer-rest" -> new TcgplayerRestSource(id, config, builder, metrics);
            default -> {
                log.warn("Unknown source type '{}' for source '{}' — skipping", config.type(), id);
                yield null;
            }
        };
    }
}
