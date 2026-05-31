package com.cardstream.simulator.catalog;

import com.cardstream.simulator.config.SimulatorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Seeds the registry from the backend catalog at startup, retrying while the backend comes up.
 * If it never appears, the simulator starts empty — seed later via {@code POST /admin/catalog/reload}.
 */
@Component
public class CatalogSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CatalogSeeder.class);

    private final BackendCatalogClient client;
    private final CatalogRegistry registry;
    private final SimulatorProperties props;

    public CatalogSeeder(BackendCatalogClient client, CatalogRegistry registry, SimulatorProperties props) {
        this.client = client;
        this.registry = registry;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
        int attempts = props.backend().seedRetryAttempts();
        long delay = props.backend().seedRetryDelayMs();
        for (int i = 1; i <= attempts; i++) {
            try {
                int count = registry.rebuild(client.fetchAll());
                if (count > 0) {
                    return;
                }
                log.warn("Backend catalog empty (attempt {}/{}); retrying", i, attempts);
            } catch (RuntimeException e) {
                log.warn("Backend catalog not ready (attempt {}/{}): {}", i, attempts, e.getMessage());
            }
            Thread.sleep(delay);
        }
        log.error("Could not seed catalog after {} attempts; starting empty. "
                + "Seed the backend, then POST /admin/catalog/reload.", attempts);
    }
}
