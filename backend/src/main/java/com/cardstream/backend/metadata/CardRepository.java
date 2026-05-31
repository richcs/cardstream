package com.cardstream.backend.metadata;

import com.cardstream.backend.metadata.dto.TcgApiDtos.Card;
import com.cardstream.common.model.Game;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class CardRepository {

    private static final String UPSERT = """
            INSERT INTO card (card_id, set_id, name, number, rarity, supertype, image_small, image_large, game, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (card_id) DO UPDATE SET
                set_id = EXCLUDED.set_id,
                name = EXCLUDED.name,
                number = EXCLUDED.number,
                rarity = EXCLUDED.rarity,
                supertype = EXCLUDED.supertype,
                image_small = EXCLUDED.image_small,
                image_large = EXCLUDED.image_large,
                game = EXCLUDED.game,
                updated_at = now()
            """;

    private static final String SELECT = """
            SELECT c.card_id, c.name, c.number, c.rarity, c.supertype, c.game,
                   c.set_id, s.name AS set_name, c.image_small, c.image_large
            FROM card c JOIN card_set s ON c.set_id = s.set_id
            """;

    private static final RowMapper<CardView> MAPPER = (rs, n) -> new CardView(
            rs.getString("card_id"),
            rs.getString("name"),
            rs.getString("number"),
            rs.getString("rarity"),
            rs.getString("supertype"),
            Game.valueOf(rs.getString("game")),
            rs.getString("set_id"),
            rs.getString("set_name"),
            rs.getString("image_small"),
            rs.getString("image_large"));

    private final JdbcTemplate jdbc;

    public CardRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void upsert(Card card, String setId, Game game) {
        jdbc.update(UPSERT,
                card.id(),
                setId,
                card.name(),
                card.number(),
                card.rarity(),
                card.supertype(),
                card.images() == null ? null : card.images().small(),
                card.images() == null ? null : card.images().large(),
                game.name());
    }

    public long count() {
        Long c = jdbc.queryForObject("SELECT count(*) FROM card", Long.class);
        return c == null ? 0L : c;
    }

    public Optional<CardView> findById(String cardId) {
        List<CardView> rows = jdbc.query(SELECT + " WHERE c.card_id = ?", MAPPER, cardId);
        return rows.stream().findFirst();
    }

    /** Paged search with optional filters. {@code set} matches set id (case-insensitive). */
    public Page search(CardQuery query) {
        List<String> clauses = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        if (query.game() != null) {
            clauses.add("c.game = ?");
            args.add(query.game().name());
        }
        if (query.set() != null && !query.set().isBlank()) {
            clauses.add("lower(c.set_id) = lower(?)");
            args.add(query.set());
        }
        if (query.rarity() != null && !query.rarity().isBlank()) {
            clauses.add("lower(c.rarity) = lower(?)");
            args.add(query.rarity());
        }
        if (query.q() != null && !query.q().isBlank()) {
            clauses.add("lower(c.name) LIKE ?");
            args.add("%" + query.q().toLowerCase() + "%");
        }
        String where = clauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", clauses);

        Long total = jdbc.queryForObject(
                "SELECT count(*) FROM card c" + where, Long.class, args.toArray());
        long totalCount = total == null ? 0L : total;

        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(query.pageSize());
        pageArgs.add(query.offset());
        List<CardView> items = jdbc.query(
                SELECT + where + " ORDER BY c.set_id, c.number, c.card_id LIMIT ? OFFSET ?",
                MAPPER, pageArgs.toArray());

        return new Page(items, query.page(), query.pageSize(), totalCount);
    }

    /** A page of catalog results. */
    public record Page(List<CardView> items, int page, int pageSize, long total) {
    }
}
