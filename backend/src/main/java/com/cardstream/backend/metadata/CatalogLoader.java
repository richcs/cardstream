package com.cardstream.backend.metadata;

import com.cardstream.backend.metadata.dto.TcgApiDtos.Card;
import com.cardstream.backend.metadata.dto.TcgApiDtos.Set;
import com.cardstream.common.model.CardMetadata;
import com.cardstream.common.model.Game;
import java.time.LocalDate;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Set-driven, recency-scoped catalog load: enumerate sets newest-first, keep those released on or
 * after the configured cutoff, then page each kept set's cards into Postgres and the
 * compacted {@code card-metadata} topic. Lower {@code market.catalog.since-release-date} to backfill.
 */
@Service
public class CatalogLoader {

    private static final Logger log = LoggerFactory.getLogger(CatalogLoader.class);

    private final PokemonTcgClient client;
    private final SetRepository setRepository;
    private final CardRepository cardRepository;
    private final CardMetadataPublisher publisher;
    private final CatalogProperties props;

    public CatalogLoader(PokemonTcgClient client, SetRepository setRepository,
            CardRepository cardRepository, CardMetadataPublisher publisher, CatalogProperties props) {
        this.client = client;
        this.setRepository = setRepository;
        this.cardRepository = cardRepository;
        this.publisher = publisher;
        this.props = props;
    }

    @Transactional
    public Result load() {
        LocalDate cutoff = props.sinceReleaseDate();
        List<Set> kept = client.fetchAllSets().stream()
                .filter(s -> {
                    LocalDate d = SetRepository.parseReleaseDate(s.releaseDate());
                    return d != null && !d.isBefore(cutoff);
                })
                .toList();
        log.info("Loading {} sets released on or after {}", kept.size(), cutoff);

        int cardCount = 0;
        for (Set set : kept) {
            setRepository.upsert(set);
            List<Card> cards = client.fetchCardsForSet(set.id());
            for (Card card : cards) {
                cardRepository.upsert(card, set.id(), Game.POKEMON);
                publisher.publish(toMetadata(card, set));
            }
            cardCount += cards.size();
            log.info("Loaded set {} ({}) — {} cards", set.id(), set.name(), cards.size());
        }
        log.info("Catalog load complete: {} sets, {} cards", kept.size(), cardCount);
        return new Result(kept.size(), cardCount, cutoff);
    }

    private static CardMetadata toMetadata(Card card, Set set) {
        String imageUrl = card.images() == null ? null
                : (card.images().large() != null ? card.images().large() : card.images().small());
        return new CardMetadata(card.id(), card.name(), set.name(), card.rarity(), Game.POKEMON, imageUrl);
    }

    /** Summary of a catalog load run. */
    public record Result(int sets, int cards, LocalDate sinceReleaseDate) {
    }
}
