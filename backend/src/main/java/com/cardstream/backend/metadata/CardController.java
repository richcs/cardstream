package com.cardstream.backend.metadata;

import com.cardstream.backend.metadata.CardRepository.Page;
import com.cardstream.backend.serving.CardDetailResponse;
import com.cardstream.backend.serving.MarketQueryService;
import com.cardstream.common.model.Game;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

/** Catalog search and detail; detail is layered with current market state via Interactive Queries. */
@RestController
@RequestMapping("/api/cards")
public class CardController {

    private final CardRepository cards;
    private final MarketQueryService marketQuery;

    public CardController(CardRepository cards, MarketQueryService marketQuery) {
        this.cards = cards;
        this.marketQuery = marketQuery;
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
    public ResponseEntity<CardDetailResponse> detail(@PathVariable String cardId) {
        return cards.findById(cardId)
                .map(card -> ResponseEntity.ok(new CardDetailResponse(card, marketQuery.snapshotsForCard(cardId))))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown card: " + cardId));
    }
}
