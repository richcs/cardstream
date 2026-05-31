package com.cardstream.backend.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Seeds the catalog at boot when {@code market.catalog.load-on-startup=true}.
 * Off by default — the load hits an external API; trigger it explicitly via the admin endpoint
 * (or set the flag) once Kafka + Postgres are up.
 */
@Component
@ConditionalOnProperty(prefix = "market.catalog", name = "load-on-startup", havingValue = "true")
public class CatalogStartupRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CatalogStartupRunner.class);

    private final CatalogLoader loader;

    public CatalogStartupRunner(CatalogLoader loader) {
        this.loader = loader;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            loader.load();
        } catch (RuntimeException e) {
            // Don't block startup if the API/broker is briefly unavailable.
            log.error("Catalog load on startup failed: {}", e.getMessage(), e);
        }
    }
}
