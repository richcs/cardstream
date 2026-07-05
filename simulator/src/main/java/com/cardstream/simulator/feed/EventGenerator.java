package com.cardstream.simulator.feed;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.simulator.catalog.CatalogRegistry;
import com.cardstream.simulator.catalog.Product;
import com.cardstream.simulator.catalog.Sku;
import com.cardstream.simulator.config.SimulatorProperties;
import com.cardstream.simulator.engine.PriceEngine;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the steady feed: every 100ms emits a slice of the configured per-second rate, advancing a
 * random SKU's price and recording a listing or sale. Also serves on-demand spike/arbitrage injections.
 */
@Component
public class EventGenerator {

    private static final Logger log = LoggerFactory.getLogger(EventGenerator.class);
    private static final long TICK_MS = 100;
    private static final BigDecimal FLOOR = new BigDecimal("0.10");

    private final CatalogRegistry registry;
    private final PriceEngine engine;
    private final FeedBuffers buffers;
    private final AtomicLong seq = new AtomicLong();
    private final int eventsPerTick;
    private final double saleRatio;

    public EventGenerator(CatalogRegistry registry, PriceEngine engine, FeedBuffers buffers,
            SimulatorProperties props) {
        this.registry = registry;
        this.engine = engine;
        this.buffers = buffers;
        this.eventsPerTick = Math.max(1, (int) Math.round(props.feed().eventsPerSecond() * TICK_MS / 1000.0));
        this.saleRatio = props.feed().saleRatio();
    }

    @Scheduled(fixedRate = TICK_MS)
    void tick() {
        List<Product> products = registry.products();
        if (products.isEmpty()) {
            return;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < eventsPerTick; i++) {
            Product product = products.get(rnd.nextInt(products.size()));
            Sku sku = product.skus().get(rnd.nextInt(product.skus().size()));
            BigDecimal market = engine.advance(product, sku);
            if (rnd.nextDouble() < saleRatio) {
                emitSale(sku, apply(market, 0.95, 1.02), rnd.nextInt(1, 3));
            } else {
                emitListing(sku, apply(market, 0.97, 1.06), rnd.nextInt(1, 5), randomSeller(rnd));
            }
        }
    }

    /** Force a price jump on a SKU, then emit a burst of sales at the new level (spike anomaly). */
    public Optional<Injection> injectSpike(String cardId, Finish finish, Condition condition,
            double factor, int burst) {
        return resolve(cardId, finish, condition).map(rs -> {
            BigDecimal price = engine.spike(rs.product(), rs.sku(), factor);
            for (int i = 0; i < burst; i++) {
                emitSale(rs.sku(), price, 1);
            }
            log.info("Injected spike x{} on {} -> {}", factor, rs.sku().skuId(), price);
            return new Injection("SPIKE", rs.sku(), price);
        });
    }

    /** Emit a single listing well below market so the backend flags arbitrage. */
    public Optional<Injection> injectArbitrage(String cardId, Finish finish, Condition condition,
            double factor) {
        return resolve(cardId, finish, condition).map(rs -> {
            BigDecimal price = apply(engine.current(rs.product(), rs.sku()), factor, factor);
            emitListing(rs.sku(), price, 1, randomSeller(ThreadLocalRandom.current()));
            log.info("Injected arbitrage listing at {}x ({}) on {}", factor, price, rs.sku().skuId());
            return new Injection("ARBITRAGE", rs.sku(), price);
        });
    }

    private void emitListing(Sku sku, BigDecimal price, int qty, String sellerId) {
        buffers.add(new ListingEvent(seq.incrementAndGet(), sku.productId(), sku.skuId(),
                sku.subTypeName(), sku.condition(), price, qty, sellerId, Instant.now()));
    }

    private void emitSale(Sku sku, BigDecimal price, int qty) {
        buffers.add(new SaleEvent(seq.incrementAndGet(), sku.productId(), sku.skuId(),
                sku.subTypeName(), sku.condition(), price, qty, Instant.now()));
    }

    private Optional<Resolved> resolve(String cardId, Finish finish, Condition condition) {
        return registry.byCardId(cardId).flatMap(product -> product.skus().stream()
                .filter(s -> finish == null || s.finish() == finish)
                .filter(s -> condition == null || s.condition() == condition)
                .findFirst()
                .map(sku -> new Resolved(product, sku)));
    }

    private static BigDecimal apply(BigDecimal market, double lo, double hi) {
        double f = lo == hi ? lo : ThreadLocalRandom.current().nextDouble(lo, hi);
        BigDecimal v = market.multiply(BigDecimal.valueOf(f)).setScale(2, RoundingMode.HALF_UP);
        return v.compareTo(FLOOR) < 0 ? FLOOR : v;
    }

    private static String randomSeller(ThreadLocalRandom rnd) {
        return String.format("s-%04d", rnd.nextInt(1, 500));
    }

    private record Resolved(Product product, Sku sku) {
    }

    /** Result of an injection, returned to the admin caller. */
    public record Injection(String type, Sku sku, BigDecimal price) {
    }
}
