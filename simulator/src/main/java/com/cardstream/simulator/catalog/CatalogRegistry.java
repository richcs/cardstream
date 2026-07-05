package com.cardstream.simulator.catalog;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;
import com.cardstream.simulator.catalog.BackendCatalogClient.CatalogCard;
import com.cardstream.simulator.config.SimulatorProperties;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The simulator's product universe: products with synthetic numeric IDs, their SKUs (finish ×
 * condition), and the {@code productId ↔ cardId} mapping. Swapped atomically on reload.
 */
@Component
public class CatalogRegistry {

    private static final Logger log = LoggerFactory.getLogger(CatalogRegistry.class);
    private static final long FIRST_PRODUCT_ID = 500_000L;
    private static final long FIRST_SKU_ID = 1_000_000L;

    private final SimulatorProperties props;

    private volatile Snapshot snapshot = Snapshot.empty();

    public CatalogRegistry(SimulatorProperties props) {
        this.props = props;
    }

    /** Rebuild the universe from the supplied catalog cards (sorted for stable id assignment). */
    public synchronized int rebuild(List<CatalogCard> cards) {
        List<CatalogCard> sorted = new ArrayList<>(cards);
        sorted.sort(Comparator.comparing(CatalogCard::cardId));

        Map<Long, Product> byProductId = new LinkedHashMap<>();
        Map<String, Product> byCardId = new LinkedHashMap<>();
        Map<Long, Sku> bySkuId = new LinkedHashMap<>();
        long productId = FIRST_PRODUCT_ID;
        long skuId = FIRST_SKU_ID;

        for (CatalogCard card : sorted) {
            if (card.cardId() == null || byCardId.containsKey(card.cardId())) {
                continue;
            }
            List<Sku> skus = new ArrayList<>();
            for (Finish finish : finishesFor(card.rarity())) {
                for (Condition condition : props.conditions()) {
                    skus.add(new Sku(skuId++, productId, card.cardId(), finish, condition));
                }
            }
            Product product = new Product(productId, card.cardId(), card.name(),
                    card.setName(), card.rarity(), List.copyOf(skus));
            byProductId.put(productId, product);
            byCardId.put(card.cardId(), product);
            skus.forEach(s -> bySkuId.put(s.skuId(), s));
            productId++;
        }

        this.snapshot = new Snapshot(
                List.copyOf(byProductId.values()),
                Map.copyOf(byProductId),
                Map.copyOf(byCardId),
                Map.copyOf(bySkuId));
        log.info("Catalog registry built: {} products, {} skus",
                byProductId.size(), bySkuId.size());
        return byProductId.size();
    }

    public List<Product> products() {
        return snapshot.products();
    }

    public Optional<Product> byProductId(long productId) {
        return Optional.ofNullable(snapshot.byProductId().get(productId));
    }

    public Optional<Product> byCardId(String cardId) {
        return Optional.ofNullable(snapshot.byCardId().get(cardId));
    }

    public Optional<Sku> bySkuId(long skuId) {
        return Optional.ofNullable(snapshot.bySkuId().get(skuId));
    }

    public boolean isEmpty() {
        return snapshot.products().isEmpty();
    }

    public int productCount() {
        return snapshot.products().size();
    }

    /**
     * Which printings a card is sold in, inferred from rarity (the backend catalog doesn't carry
     * finishes). Commons/uncommons get a reverse-holo variant; rarer cards also get a holofoil.
     */
    static List<Finish> finishesFor(String rarity) {
        String r = rarity == null ? "" : rarity.toLowerCase();
        boolean basic = r.isBlank() || r.contains("common"); // matches "Common" and "Uncommon"
        if (basic) {
            return List.of(Finish.NORMAL, Finish.REVERSE_HOLOFOIL);
        }
        return List.of(Finish.NORMAL, Finish.HOLOFOIL, Finish.REVERSE_HOLOFOIL);
    }

    private record Snapshot(
            List<Product> products,
            Map<Long, Product> byProductId,
            Map<String, Product> byCardId,
            Map<Long, Sku> bySkuId) {

        static Snapshot empty() {
            return new Snapshot(List.of(), Map.of(), Map.of(), Map.of());
        }
    }
}
