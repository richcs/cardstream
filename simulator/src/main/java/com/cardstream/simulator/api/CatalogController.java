package com.cardstream.simulator.api;

import com.cardstream.common.model.Condition;
import com.cardstream.simulator.catalog.CatalogRegistry;
import com.cardstream.simulator.catalog.Product;
import com.cardstream.simulator.catalog.Sku;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * TCGplayer-shaped catalog. Exposes the synthetic {@code productId ↔ cardId} mapping and each
 * product's SKUs so the ingestion poller can resolve products back to cards.
 */
@RestController
@RequestMapping("/catalog")
public class CatalogController {

    private final CatalogRegistry registry;

    public CatalogController(CatalogRegistry registry) {
        this.registry = registry;
    }

    @GetMapping("/products")
    public ProductsPage products(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "100") int pageSize) {
        int size = Math.min(Math.max(pageSize, 1), 500);
        List<Product> all = registry.products();
        int from = Math.min(page * size, all.size());
        int to = Math.min(from + size, all.size());
        List<ProductDto> items = all.subList(from, to).stream().map(CatalogController::toDto).toList();
        return new ProductsPage(items, page, size, all.size());
    }

    @GetMapping("/products/{productId}")
    public ResponseEntity<ProductDto> product(@PathVariable long productId) {
        return registry.byProductId(productId)
                .map(p -> ResponseEntity.ok(toDto(p)))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown product: " + productId));
    }

    private static ProductDto toDto(Product p) {
        List<SkuDto> skus = p.skus().stream()
                .map(s -> new SkuDto(s.skuId(), s.subTypeName(), s.condition()))
                .toList();
        return new ProductDto(p.productId(), p.cardId(), p.name(), p.setName(), p.rarity(), skus);
    }

    public record ProductsPage(List<ProductDto> items, int page, int pageSize, int total) {
    }

    public record ProductDto(long productId, String cardId, String name, String setName,
            String rarity, List<SkuDto> skus) {
    }

    public record SkuDto(long skuId, String subTypeName, Condition condition) {
    }
}
