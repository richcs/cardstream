package com.cardstream.backend.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.cardstream.backend.metadata.CardRepository;
import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.common.model.Listing;
import com.cardstream.common.model.Sale;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EventValidatorTest {

    @Mock
    CardRepository cards;

    SimpleMeterRegistry metrics;
    EventValidator validator;

    @BeforeEach
    void setUp() {
        when(cards.allIds()).thenReturn(Set.of("sv1-1"));
        metrics = new SimpleMeterRegistry();
        IngestionProperties props = new IngestionProperties(true, null, null, null);
        validator = new EventValidator(props, new CatalogAllowlist(cards), metrics);
    }

    private Listing listing(String cardId, BigDecimal price, int qty, Instant ts, String seller) {
        return new Listing("sim:1", "sim", cardId, Finish.HOLOFOIL, Condition.NM, price, qty, seller, ts);
    }

    @Test
    void acceptsAValidListing() {
        assertThat(validator.accept(listing("sv1-1", new BigDecimal("12.50"), 2, Instant.now(), "s-1")))
                .isTrue();
    }

    @Test
    void rejectsUnknownCard() {
        assertThat(validator.accept(listing("ghost-9", new BigDecimal("12.50"), 1, Instant.now(), "s-1")))
                .isFalse();
        assertThat(rejectCount("unknown_card")).isEqualTo(1.0);
    }

    @Test
    void rejectsNonPositiveAndOutOfBoundsPrice() {
        assertThat(validator.accept(listing("sv1-1", new BigDecimal("0.00"), 1, Instant.now(), "s-1")))
                .isFalse();
        assertThat(validator.accept(listing("sv1-1", new BigDecimal("2000000"), 1, Instant.now(), "s-1")))
                .isFalse();
        assertThat(rejectCount("price_out_of_bounds")).isEqualTo(2.0);
    }

    @Test
    void rejectsExcessivePriceScale() {
        assertThat(validator.accept(listing("sv1-1", new BigDecimal("12.5000"), 1, Instant.now(), "s-1")))
                .isFalse();
        assertThat(rejectCount("price_out_of_bounds")).isEqualTo(1.0);
    }

    @Test
    void rejectsBadQuantity() {
        assertThat(validator.accept(listing("sv1-1", new BigDecimal("5.00"), 0, Instant.now(), "s-1")))
                .isFalse();
        assertThat(rejectCount("quantity_out_of_bounds")).isEqualTo(1.0);
    }

    @Test
    void rejectsFutureDatedEvent() {
        Instant future = Instant.now().plusSeconds(3600);
        assertThat(validator.accept(listing("sv1-1", new BigDecimal("5.00"), 1, future, "s-1")))
                .isFalse();
        assertThat(rejectCount("timestamp_out_of_window")).isEqualTo(1.0);
    }

    @Test
    void rejectsSellerIdWithPipeDelimiter() {
        assertThat(validator.accept(listing("sv1-1", new BigDecimal("5.00"), 1, Instant.now(), "s|evil")))
                .isFalse();
        assertThat(rejectCount("bad_identifier")).isEqualTo(1.0);
    }

    @Test
    void acceptsAValidSale() {
        Sale sale = new Sale("sim:9", "sim", "sv1-1", Finish.NORMAL, Condition.LP,
                new BigDecimal("3.25"), 1, Instant.now());
        assertThat(validator.accept(sale)).isTrue();
    }

    private double rejectCount(String reason) {
        return metrics.find("cardstream.ingestion.rejected").tag("reason", reason).counter().count();
    }
}
