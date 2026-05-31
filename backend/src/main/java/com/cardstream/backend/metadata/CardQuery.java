package com.cardstream.backend.metadata;

import com.cardstream.common.model.Game;

/** Filters + paging for catalog search. Page is 0-based; size is clamped to a sane range. */
public record CardQuery(Game game, String set, String rarity, String q, int page, int pageSize) {

    public CardQuery {
        if (page < 0) {
            page = 0;
        }
        if (pageSize <= 0) {
            pageSize = 50;
        } else if (pageSize > 200) {
            pageSize = 200;
        }
    }

    public int offset() {
        return page * pageSize;
    }
}
