package com.cardstream.backend.metadata;

import com.cardstream.backend.metadata.dto.TcgApiDtos.Card;
import com.cardstream.backend.metadata.dto.TcgApiDtos.CardListResponse;
import com.cardstream.backend.metadata.dto.TcgApiDtos.Set;
import com.cardstream.backend.metadata.dto.TcgApiDtos.SetListResponse;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Thin client over the Pokémon TCG API. Handles paging and the optional API key. */
@Component
public class PokemonTcgClient {

    private static final Logger log = LoggerFactory.getLogger(PokemonTcgClient.class);

    private final RestClient restClient;
    private final int pageSize;

    public PokemonTcgClient(RestClient.Builder builder, CatalogProperties props) {
        RestClient.Builder b = builder
                .baseUrl(props.apiBaseUrl())
                .defaultHeader("Accept", MediaType.APPLICATION_JSON_VALUE);
        if (props.apiKey() != null && !props.apiKey().isBlank()) {
            b = b.defaultHeader("X-Api-Key", props.apiKey());
        }
        this.restClient = b.build();
        this.pageSize = props.pageSize();
    }

    /** All sets, newest first. The API returns a few hundred — paging through is cheap. */
    public List<Set> fetchAllSets() {
        List<Set> all = new ArrayList<>();
        int page = 1;
        while (true) {
            final int p = page;
            SetListResponse resp = restClient.get()
                    .uri(uri -> uri.path("/sets")
                            .queryParam("orderBy", "-releaseDate")
                            .queryParam("page", p)
                            .queryParam("pageSize", pageSize)
                            .build())
                    .retrieve()
                    .body(SetListResponse.class);
            if (resp == null || resp.data() == null || resp.data().isEmpty()) {
                break;
            }
            all.addAll(resp.data());
            if (all.size() >= resp.totalCount() || resp.data().size() < pageSize) {
                break;
            }
            page++;
        }
        log.info("Fetched {} sets from Pokémon TCG API", all.size());
        return all;
    }

    /** All cards in a set, paged. */
    public List<Card> fetchCardsForSet(String setId) {
        List<Card> all = new ArrayList<>();
        int page = 1;
        while (true) {
            final int p = page;
            CardListResponse resp = restClient.get()
                    .uri(uri -> uri.path("/cards")
                            .queryParam("q", "set.id:" + setId)
                            .queryParam("orderBy", "number")
                            .queryParam("page", p)
                            .queryParam("pageSize", pageSize)
                            .build())
                    .retrieve()
                    .body(CardListResponse.class);
            if (resp == null || resp.data() == null || resp.data().isEmpty()) {
                break;
            }
            all.addAll(resp.data());
            if (all.size() >= resp.totalCount() || resp.data().size() < pageSize) {
                break;
            }
            page++;
        }
        return all;
    }
}
