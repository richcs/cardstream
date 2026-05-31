package com.cardstream.backend.ingestion.source;

import com.cardstream.backend.ingestion.IngestionProperties.SourceConfig;
import com.cardstream.backend.ingestion.MarketDataSource;
import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.common.model.Listing;
import com.cardstream.common.model.Sale;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Adapter for any TCGplayer-shaped REST feed. Parameterized by base-url + optional api-key, so the
 * same code serves the in-house simulator (id {@code "sim"}) now and real TCGplayer later — adding
 * the latter is config-only.
 *
 * <p>Normalization is the adapter's job: it resolves the upstream numeric {@code productId} to our
 * {@code cardId} via the upstream catalog, and maps {@code subTypeName} to {@link Finish}. Events it
 * can't resolve are dropped and counted (the poller-side validator separately enforces the catalog
 * allowlist + value bounds — defense in depth).
 */
public class TcgplayerRestSource implements MarketDataSource {

    private static final Logger log = LoggerFactory.getLogger(TcgplayerRestSource.class);
    private static final Duration CATALOG_TTL = Duration.ofSeconds(60);

    private static final Map<String, Finish> FINISH_BY_SUBTYPE = Map.of(
            "normal", Finish.NORMAL,
            "holofoil", Finish.HOLOFOIL,
            "reverse holofoil", Finish.REVERSE_HOLOFOIL);

    private final String id;
    private final RestClient http;
    private final int maxEventsPerPoll;
    private final MeterRegistry metrics;

    /** productId → cardId, refreshed from the upstream catalog. */
    private volatile Map<Long, String> productToCard = Map.of();
    private volatile Instant catalogLoadedAt = Instant.EPOCH;

    public TcgplayerRestSource(String id, SourceConfig config, RestClient.Builder builder,
            MeterRegistry metrics) {
        this.id = id;
        this.maxEventsPerPoll = config.maxEventsPerPoll();
        this.metrics = metrics;

        SimpleClientHttpRequestFactory rf = new SimpleClientHttpRequestFactory();
        rf.setConnectTimeout((int) config.connectTimeout().toMillis());
        rf.setReadTimeout((int) config.readTimeout().toMillis());

        // Clone the app's configured builder (shares its ObjectMapper / message converters), then
        // pin this source's base-url, timeouts, and auth.
        RestClient.Builder b = builder.clone().baseUrl(config.baseUrl()).requestFactory(rf);
        if (config.apiKey() != null && !config.apiKey().isBlank()) {
            b = b.defaultHeader("Authorization", "Bearer " + config.apiKey());
        }
        this.http = b.build();
    }

    /** Test/wiring constructor that takes a pre-built client (e.g. backed by MockRestServiceServer). */
    TcgplayerRestSource(String id, RestClient http, int maxEventsPerPoll, MeterRegistry metrics) {
        this.id = id;
        this.http = http;
        this.maxEventsPerPoll = maxEventsPerPoll;
        this.metrics = metrics;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public PollBatch poll(SourceCursor cursor) {
        ensureCatalog();
        Map<Long, String> map = productToCard;

        List<Listing> listings = new ArrayList<>();
        for (ListingDto dto : fetchListings(cursor.listingsSince())) {
            String cardId = map.get(dto.productId());
            Finish finish = finishFor(dto.subTypeName());
            if (cardId == null) {
                drop("unresolved_product");
            } else if (finish == null) {
                drop("bad_subtype");
            } else {
                listings.add(new Listing(eventId(dto.eventId()), id, cardId, finish, dto.condition(),
                        dto.price(), dto.quantity(), dto.sellerId(), dto.listedAt()));
            }
        }

        List<Sale> sales = new ArrayList<>();
        for (SaleDto dto : fetchSales(cursor.salesSince())) {
            String cardId = map.get(dto.productId());
            Finish finish = finishFor(dto.subTypeName());
            if (cardId == null) {
                drop("unresolved_product");
            } else if (finish == null) {
                drop("bad_subtype");
            } else {
                sales.add(new Sale(eventId(dto.eventId()), id, cardId, finish, dto.condition(),
                        dto.price(), dto.quantity(), dto.soldAt()));
            }
        }
        return new PollBatch(listings, sales);
    }

    private String eventId(long raw) {
        return id + ":" + raw;
    }

    private Finish finishFor(String subTypeName) {
        return subTypeName == null ? null : FINISH_BY_SUBTYPE.get(subTypeName.trim().toLowerCase());
    }

    private void drop(String reason) {
        metrics.counter("cardstream.ingestion.normalize.dropped", "source", id, "reason", reason).increment();
    }

    private List<ListingDto> fetchListings(Instant since) {
        List<ListingDto> body = http.get()
                .uri(uri -> uri.path("/listings")
                        .queryParam("limit", maxEventsPerPoll)
                        .queryParamIfPresent("since", java.util.Optional.ofNullable(since).map(Instant::toString))
                        .build())
                .retrieve()
                .body(LISTING_LIST);
        return body == null ? List.of() : body;
    }

    private List<SaleDto> fetchSales(Instant since) {
        List<SaleDto> body = http.get()
                .uri(uri -> uri.path("/sales")
                        .queryParam("limit", maxEventsPerPoll)
                        .queryParamIfPresent("since", java.util.Optional.ofNullable(since).map(Instant::toString))
                        .build())
                .retrieve()
                .body(SALE_LIST);
        return body == null ? List.of() : body;
    }

    /** Refresh productId→cardId when empty or stale; misses between refreshes are just dropped. */
    private void ensureCatalog() {
        boolean stale = Duration.between(catalogLoadedAt, Instant.now()).compareTo(CATALOG_TTL) > 0;
        if (!productToCard.isEmpty() && !stale) {
            return;
        }
        try {
            Map<Long, String> next = new HashMap<>();
            int page = 0;
            while (true) {
                final int p = page;
                ProductsPage resp = http.get()
                        .uri(uri -> uri.path("/catalog/products")
                                .queryParam("page", p)
                                .queryParam("pageSize", 500)
                                .build())
                        .retrieve()
                        .body(ProductsPage.class);
                if (resp == null || resp.items() == null || resp.items().isEmpty()) {
                    break;
                }
                for (ProductDto pr : resp.items()) {
                    next.put(pr.productId(), pr.cardId());
                }
                if (next.size() >= resp.total() || resp.items().size() < resp.pageSize()) {
                    break;
                }
                page++;
            }
            if (!next.isEmpty()) {
                productToCard = Map.copyOf(next);
                catalogLoadedAt = Instant.now();
                log.debug("[{}] catalog refreshed: {} products", id, next.size());
            }
        } catch (RuntimeException e) {
            // Upstream catalog unreachable — keep the last known map; the poller's circuit breaker
            // handles a source that stays down. Don't let one bad refresh wipe a working map.
            log.warn("[{}] catalog refresh failed: {}", id, e.toString());
        }
    }

    // --- Upstream DTOs (TCGplayer-shaped; only the fields we consume) -----------------------------

    private static final org.springframework.core.ParameterizedTypeReference<List<ListingDto>> LISTING_LIST =
            new org.springframework.core.ParameterizedTypeReference<>() {};
    private static final org.springframework.core.ParameterizedTypeReference<List<SaleDto>> SALE_LIST =
            new org.springframework.core.ParameterizedTypeReference<>() {};

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ListingDto(long eventId, long productId, String subTypeName, Condition condition,
            BigDecimal price, int quantity, String sellerId, Instant listedAt) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record SaleDto(long eventId, long productId, String subTypeName, Condition condition,
            BigDecimal price, int quantity, Instant soldAt) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ProductsPage(List<ProductDto> items, int page, int pageSize, int total) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ProductDto(long productId, String cardId) {
    }
}
