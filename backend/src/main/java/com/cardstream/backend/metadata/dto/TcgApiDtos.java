package com.cardstream.backend.metadata.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Subset of the Pokémon TCG API (api.pokemontcg.io/v2) response shapes we consume.
 * Unknown fields are ignored so the upstream schema can grow without breaking the loader.
 */
public final class TcgApiDtos {

    private TcgApiDtos() {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SetListResponse(List<Set> data, int page, int pageSize, int count, int totalCount) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Set(
            String id,
            String name,
            String series,
            Integer printedTotal,
            Integer total,
            String releaseDate, // "YYYY/MM/DD"
            Images images) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Images(String symbol, String logo) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CardListResponse(List<Card> data, int page, int pageSize, int count, int totalCount) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Card(
            String id,
            String name,
            String number,
            String rarity,
            String supertype,
            List<String> types,
            CardImages images) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CardImages(String small, String large) {
    }
}
