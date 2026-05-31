package com.cardstream.simulator.api;

import com.cardstream.common.model.Condition;
import com.cardstream.simulator.catalog.CatalogRegistry;
import com.cardstream.simulator.catalog.Product;
import com.cardstream.simulator.engine.PriceEngine;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * TCGplayer-shaped pricing: current market price per SKU, with the usual low/mid/high/market/
 * directLow band derived around the engine's price.
 */
@RestController
public class PricingController {

    private final CatalogRegistry registry;
    private final PriceEngine engine;

    public PricingController(CatalogRegistry registry, PriceEngine engine) {
        this.registry = registry;
        this.engine = engine;
    }

    @GetMapping("/pricing/{productId}")
    public ProductPricing pricing(@PathVariable long productId) {
        Product product = registry.byProductId(productId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown product: " + productId));
        List<SkuPrice> prices = product.skus().stream().map(sku -> {
            BigDecimal market = engine.current(product, sku);
            return new SkuPrice(sku.skuId(), sku.subTypeName(), sku.condition(),
                    band(market, 0.85), market, band(market, 1.30), market, band(market, 0.80));
        }).toList();
        return new ProductPricing(product.productId(), product.cardId(), product.name(), prices);
    }

    private static BigDecimal band(BigDecimal market, double factor) {
        return market.multiply(BigDecimal.valueOf(factor)).setScale(2, RoundingMode.HALF_UP);
    }

    public record ProductPricing(long productId, String cardId, String name, List<SkuPrice> prices) {
    }

    public record SkuPrice(long skuId, String subTypeName, Condition condition,
            BigDecimal lowPrice, BigDecimal midPrice, BigDecimal highPrice,
            BigDecimal marketPrice, BigDecimal directLowPrice) {
    }
}
