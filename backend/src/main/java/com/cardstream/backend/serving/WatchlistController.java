package com.cardstream.backend.serving;

import com.cardstream.backend.metadata.CardRepository;
import com.cardstream.backend.metadata.CardView;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Watchlist CRUD, scoped by the anonymous {@code X-User-Id} header (no auth in MVP). */
@RestController
@RequestMapping("/api/watchlist")
public class WatchlistController {

    private final WatchlistRepository watchlist;
    private final CardRepository cards;

    public WatchlistController(WatchlistRepository watchlist, CardRepository cards) {
        this.watchlist = watchlist;
        this.cards = cards;
    }

    @GetMapping
    public List<CardView> list(@RequestHeader("X-User-Id") String userId) {
        return watchlist.list(userId);
    }

    @PostMapping
    public void add(@RequestHeader("X-User-Id") String userId, @RequestBody Map<String, String> body) {
        String cardId = body.get("cardId");
        if (cardId == null || cardId.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "cardId is required");
        }
        cards.findById(cardId).orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown card: " + cardId));
        watchlist.add(userId, cardId);
    }

    @DeleteMapping("/{cardId}")
    public void remove(@RequestHeader("X-User-Id") String userId, @PathVariable String cardId) {
        watchlist.remove(userId, cardId);
    }
}
