package com.cardstream.backend.metadata;

import com.cardstream.backend.metadata.CardRepository.Page;
import com.cardstream.common.model.Game;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

/** Catalog search and detail. Current market state is layered on in Phase 5. */
@RestController
@RequestMapping("/api/cards")
public class CardController {

    private final CardRepository cards;

    public CardController(CardRepository cards) {
        this.cards = cards;
    }

    @GetMapping
    public Page list(
            @RequestParam(required = false) Game game,
            @RequestParam(required = false) String set,
            @RequestParam(required = false) String rarity,
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int pageSize) {
        return cards.search(new CardQuery(game, set, rarity, q, page, pageSize));
    }

    @GetMapping("/{cardId}")
    public ResponseEntity<CardView> detail(@PathVariable String cardId) {
        return cards.findById(cardId)
                .map(ResponseEntity::ok)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown card: " + cardId));
    }
}
