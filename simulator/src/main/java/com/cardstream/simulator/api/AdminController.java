package com.cardstream.simulator.api;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.simulator.catalog.BackendCatalogClient;
import com.cardstream.simulator.catalog.CatalogRegistry;
import com.cardstream.simulator.engine.PriceEngine;
import com.cardstream.simulator.feed.EventGenerator;
import com.cardstream.simulator.feed.EventGenerator.Injection;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * On-demand controls for deterministic demos: inject a price spike or a below-market (arbitrage)
 * listing, and reseed the catalog from the backend.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private static final double DEFAULT_SPIKE_FACTOR = 2.5;
    private static final int DEFAULT_SPIKE_BURST = 6;
    private static final double DEFAULT_ARBITRAGE_FACTOR = 0.7;

    private final EventGenerator generator;
    private final BackendCatalogClient catalogClient;
    private final CatalogRegistry registry;
    private final PriceEngine engine;

    public AdminController(EventGenerator generator, BackendCatalogClient catalogClient,
            CatalogRegistry registry, PriceEngine engine) {
        this.generator = generator;
        this.catalogClient = catalogClient;
        this.registry = registry;
        this.engine = engine;
    }

    @PostMapping("/inject/spike")
    public Injection spike(@RequestBody InjectRequest req) {
        double factor = req.factor() != null ? req.factor() : DEFAULT_SPIKE_FACTOR;
        int burst = req.burst() != null ? req.burst() : DEFAULT_SPIKE_BURST;
        return generator.injectSpike(req.cardId(), req.finish(), req.condition(), factor, burst)
                .orElseThrow(() -> unknown(req));
    }

    @PostMapping("/inject/arbitrage")
    public Injection arbitrage(@RequestBody InjectRequest req) {
        double factor = req.factor() != null ? req.factor() : DEFAULT_ARBITRAGE_FACTOR;
        return generator.injectArbitrage(req.cardId(), req.finish(), req.condition(), factor)
                .orElseThrow(() -> unknown(req));
    }

    @PostMapping("/catalog/reload")
    public ReloadResult reload() {
        int products = registry.rebuild(catalogClient.fetchAll());
        engine.reset();
        return new ReloadResult(products);
    }

    private static ResponseStatusException unknown(InjectRequest req) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND,
                "No SKU for cardId=%s finish=%s condition=%s".formatted(req.cardId(), req.finish(), req.condition()));
    }

    /** Spike/arbitrage target. finish/condition optional (first matching SKU is used). */
    public record InjectRequest(String cardId, Finish finish, Condition condition, Double factor, Integer burst) {
    }

    public record ReloadResult(int products) {
    }
}
