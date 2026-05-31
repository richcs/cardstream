package com.cardstream.backend.metadata;

import com.cardstream.backend.metadata.CatalogLoader.Result;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * On-demand catalog seeding. Run once Kafka + Postgres are up to populate the catalog and the
 * {@code card-metadata} topic from the Pokémon TCG API (sets since the configured cutoff).
 */
@RestController
@RequestMapping("/api/admin/catalog")
public class CatalogAdminController {

    private final CatalogLoader loader;

    public CatalogAdminController(CatalogLoader loader) {
        this.loader = loader;
    }

    @PostMapping("/reload")
    public Result reload() {
        return loader.load();
    }
}
