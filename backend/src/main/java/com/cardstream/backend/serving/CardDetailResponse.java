package com.cardstream.backend.serving;

import com.cardstream.backend.metadata.CardView;
import java.util.List;

/** Catalog detail plus current market state across every finish/condition the card trades under. */
public record CardDetailResponse(CardView card, List<MarketSnapshot> markets) {
}
