package com.cardstream.simulator.catalog;

import com.cardstream.simulator.config.SimulatorProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Pages the backend's {@code GET /api/cards} to seed the simulator's product universe. */
@Component
public class BackendCatalogClient {

    private static final Logger log = LoggerFactory.getLogger(BackendCatalogClient.class);
    private static final int PAGE_SIZE = 200;

    private final RestClient restClient;

    public BackendCatalogClient(RestClient.Builder builder, SimulatorProperties props) {
        this.restClient = builder.baseUrl(props.backend().baseUrl()).build();
    }

    /** Fetch all catalog cards, paging until drained. Throws if the backend is unreachable. */
    public List<CatalogCard> fetchAll() {
        List<CatalogCard> all = new ArrayList<>();
        int page = 0;
        while (true) {
            final int p = page;
            CardsPage resp = restClient.get()
                    .uri(uri -> uri.path("/api/cards")
                            .queryParam("page", p)
                            .queryParam("pageSize", PAGE_SIZE)
                            .build())
                    .retrieve()
                    .body(CardsPage.class);
            if (resp == null || resp.items() == null || resp.items().isEmpty()) {
                break;
            }
            all.addAll(resp.items());
            if (all.size() >= resp.total() || resp.items().size() < PAGE_SIZE) {
                break;
            }
            page++;
        }
        log.info("Fetched {} cards from backend catalog", all.size());
        return all;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CardsPage(List<CatalogCard> items, int page, int pageSize, long total) {
    }

    /** The subset of the backend's CardView we need to build products. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CatalogCard(String cardId, String name, String setName, String rarity) {
    }
}
