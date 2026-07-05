package com.cardstream.backend.serving;

import com.cardstream.backend.sink.PriceWindowRepository;
import com.cardstream.backend.sink.PriceWindowRepository.Direction;
import com.cardstream.backend.sink.PriceWindowRepository.TopMover;
import com.cardstream.common.model.WindowType;
import com.cardstream.common.model.WindowedAggregate;
import java.time.Instant;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** History and top-movers, both derived from the sunk {@code agg-price-windowed} history. */
@RestController
public class HistoryController {

    private final PriceWindowRepository priceWindows;

    public HistoryController(PriceWindowRepository priceWindows) {
        this.priceWindows = priceWindows;
    }

    @GetMapping("/api/cards/{cardId}/history")
    public List<WindowedAggregate> history(
            @PathVariable String cardId,
            @RequestParam(defaultValue = "hourly") String window,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to) {
        return priceWindows.history(cardId, parseHistoryWindow(window), from, to);
    }

    @GetMapping("/api/top-movers")
    public List<TopMover> topMovers(
            @RequestParam(defaultValue = "daily") String window,
            @RequestParam(defaultValue = "gainers") String dir,
            @RequestParam(defaultValue = "20") int limit) {
        return priceWindows.topMovers(parseHistoryWindow(window), parseDirection(dir), limit);
    }

    /** Appendix B scopes history/top-movers to the settled tumbling windows, not the MA_24H hop. */
    private static WindowType parseHistoryWindow(String window) {
        return switch (window.toLowerCase()) {
            case "hourly" -> WindowType.HOURLY;
            case "daily" -> WindowType.DAILY;
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "window must be hourly or daily");
        };
    }

    private static Direction parseDirection(String dir) {
        return switch (dir.toLowerCase()) {
            case "gainers" -> Direction.GAINERS;
            case "losers" -> Direction.LOSERS;
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "dir must be gainers or losers");
        };
    }
}
