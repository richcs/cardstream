package com.cardstream.backend.ingestion;

import com.cardstream.backend.ingestion.IngestionProperties.Validation;
import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.common.model.Listing;
import com.cardstream.common.model.Sale;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.stereotype.Component;

/**
 * The trust boundary: checks each event against the catalog allowlist + configured value/time bounds
 * before it reaches Kafka; rejects are counted (tagged by source + reason) and dropped.
 */
@Component
public class EventValidator {

    private final Validation v;
    private final CatalogAllowlist allowlist;
    private final MeterRegistry metrics;

    public EventValidator(IngestionProperties props, CatalogAllowlist allowlist, MeterRegistry metrics) {
        this.v = props.validation();
        this.allowlist = allowlist;
        this.metrics = metrics;
    }

    public boolean accept(Listing l) {
        String reason = check(l.cardId(), l.finish(), l.condition(), l.price(), l.quantity(), l.listedAt());
        if (reason == null && !validIdentifier(l.sellerId())) {
            reason = "bad_identifier";
        }
        return record(l.source(), reason);
    }

    public boolean accept(Sale s) {
        String reason = check(s.cardId(), s.finish(), s.condition(), s.price(), s.quantity(), s.soldAt());
        return record(s.source(), reason);
    }

    /** Shared field checks. Returns a reject reason, or {@code null} if the event is valid. */
    private String check(String cardId, Finish finish, Condition condition,
            BigDecimal price, int quantity, Instant ts) {
        if (finish == null || condition == null) {
            return "missing_enum";
        }
        if (cardId == null || !validIdentifier(cardId)) {
            return "bad_identifier";
        }
        if (!allowlist.contains(cardId)) {
            return "unknown_card";
        }
        if (price == null || price.signum() <= 0
                || price.scale() > v.maxPriceScale()
                || price.compareTo(v.minPrice()) < 0
                || price.compareTo(v.maxPrice()) > 0) {
            return "price_out_of_bounds";
        }
        if (quantity < 1 || quantity > v.maxQuantity()) {
            return "quantity_out_of_bounds";
        }
        if (ts == null) {
            return "timestamp_out_of_window";
        }
        Instant now = Instant.now();
        if (ts.isAfter(now.plus(v.maxFutureSkew())) || ts.isBefore(now.minus(v.maxAge()))) {
            return "timestamp_out_of_window";
        }
        return null;
    }

    /** No pipe (the MarketKey delimiter) and no control characters. */
    private static boolean validIdentifier(String s) {
        if (s == null || s.isBlank() || s.indexOf('|') >= 0) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (Character.isISOControl(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean record(String source, String reason) {
        if (reason == null) {
            return true;
        }
        metrics.counter("cardstream.ingestion.rejected", "source", source, "reason", reason).increment();
        return false;
    }
}
