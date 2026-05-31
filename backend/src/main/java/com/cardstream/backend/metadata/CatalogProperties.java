package com.cardstream.backend.metadata;

import java.time.LocalDate;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Catalog scope + Pokémon TCG API settings. The loader ingests only sets whose
 * release date is on or after {@code sinceReleaseDate}; lower it to backfill older cards.
 */
@ConfigurationProperties(prefix = "market.catalog")
public record CatalogProperties(
        String apiBaseUrl,
        String apiKey,
        LocalDate sinceReleaseDate,
        int pageSize,
        boolean loadOnStartup) {

    public CatalogProperties {
        if (apiBaseUrl == null || apiBaseUrl.isBlank()) {
            apiBaseUrl = "https://api.pokemontcg.io/v2";
        }
        if (sinceReleaseDate == null) {
            sinceReleaseDate = LocalDate.of(2024, 1, 1);
        }
        if (pageSize <= 0 || pageSize > 250) {
            pageSize = 250; // Pokémon TCG API max page size.
        }
    }
}
