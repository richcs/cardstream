package com.cardstream.backend.serving;

import com.cardstream.common.model.MarketKey;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Interactive-Query-backed current state for one ticker. */
@RestController
@RequestMapping("/api/market")
public class MarketController {

    private final MarketQueryService marketQuery;

    public MarketController(MarketQueryService marketQuery) {
        this.marketQuery = marketQuery;
    }

    @GetMapping("/{marketKey}")
    public MarketSnapshot get(@PathVariable String marketKey) {
        MarketKey key;
        try {
            key = MarketKey.parse(marketKey);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid market key: " + marketKey);
        }
        return marketQuery.snapshot(key)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No market data for: " + marketKey));
    }
}
