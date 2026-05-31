package com.cardstream.backend.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import com.cardstream.backend.ingestion.MarketDataSource.PollBatch;
import com.cardstream.backend.ingestion.MarketDataSource.SourceCursor;
import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class TcgplayerRestSourceTest {

    @Test
    void normalizesAndResolvesProductsDroppingUnknownAndBadSubtype() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        RestClient client = builder.build();

        server.expect(requestTo(containsString("/catalog/products")))
                .andRespond(withSuccess("""
                        {"items":[{"productId":500001,"cardId":"sv1-1"},
                                  {"productId":500002,"cardId":"sv1-2"}],
                         "page":0,"pageSize":500,"total":2}
                        """, MediaType.APPLICATION_JSON));

        server.expect(requestTo(containsString("/listings")))
                .andRespond(withSuccess("""
                        [{"eventId":1,"productId":500001,"skuId":11,"subTypeName":"Holofoil",
                          "condition":"NM","price":12.50,"quantity":2,"sellerId":"s-1",
                          "listedAt":"2026-05-30T12:00:00Z"},
                         {"eventId":2,"productId":999999,"skuId":22,"subTypeName":"Normal",
                          "condition":"NM","price":1.00,"quantity":1,"sellerId":"s-2",
                          "listedAt":"2026-05-30T12:00:01Z"},
                         {"eventId":3,"productId":500002,"skuId":33,"subTypeName":"Foil Etched",
                          "condition":"NM","price":4.00,"quantity":1,"sellerId":"s-3",
                          "listedAt":"2026-05-30T12:00:02Z"}]
                        """, MediaType.APPLICATION_JSON));

        server.expect(requestTo(containsString("/sales")))
                .andRespond(withSuccess("""
                        [{"eventId":4,"productId":500002,"skuId":33,"subTypeName":"Reverse Holofoil",
                          "condition":"LP","price":3.25,"quantity":1,"soldAt":"2026-05-30T12:00:03Z"}]
                        """, MediaType.APPLICATION_JSON));

        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        TcgplayerRestSource source = new TcgplayerRestSource("sim", client, 1000, metrics);

        PollBatch batch = source.poll(SourceCursor.EMPTY);

        // Listing 1 normalizes; listing 2 (unknown product) and listing 3 (bad subtype) are dropped.
        assertThat(batch.listings()).hasSize(1);
        var listing = batch.listings().get(0);
        assertThat(listing.eventId()).isEqualTo("sim:1");
        assertThat(listing.source()).isEqualTo("sim");
        assertThat(listing.cardId()).isEqualTo("sv1-1");
        assertThat(listing.finish()).isEqualTo(Finish.HOLOFOIL);
        assertThat(listing.condition()).isEqualTo(Condition.NM);

        assertThat(batch.sales()).hasSize(1);
        var sale = batch.sales().get(0);
        assertThat(sale.eventId()).isEqualTo("sim:4");
        assertThat(sale.cardId()).isEqualTo("sv1-2");
        assertThat(sale.finish()).isEqualTo(Finish.REVERSE_HOLOFOIL);
        assertThat(sale.condition()).isEqualTo(Condition.LP);

        assertThat(droppedCount(metrics, "unresolved_product")).isEqualTo(1.0);
        assertThat(droppedCount(metrics, "bad_subtype")).isEqualTo(1.0);

        server.verify();
    }

    private double droppedCount(SimpleMeterRegistry metrics, String reason) {
        return metrics.find("cardstream.ingestion.normalize.dropped").tag("reason", reason).counter().count();
    }
}
