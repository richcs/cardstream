package com.cardstream.backend.serving;

import com.cardstream.backend.metadata.CardView;
import com.cardstream.common.model.Game;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/** Per-user watchlist (anonymous {@code userId} scoping, no auth — see tech-stack.md). */
@Repository
public class WatchlistRepository {

    private static final RowMapper<CardView> CARD_MAPPER = (rs, n) -> new CardView(
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

    public WatchlistRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<CardView> list(String userId) {
        return jdbc.query("""
                SELECT c.card_id, c.name, c.number, c.rarity, c.supertype, c.game,
                       c.set_id, s.name AS set_name, c.image_small, c.image_large
                FROM watchlist w
                JOIN card c ON c.card_id = w.card_id
                JOIN card_set s ON c.set_id = s.set_id
                WHERE w.user_id = ?
                ORDER BY w.created_at
                """, CARD_MAPPER, userId);
    }

    public void add(String userId, String cardId) {
        jdbc.update("""
                INSERT INTO watchlist (user_id, card_id) VALUES (?, ?)
                ON CONFLICT (user_id, card_id) DO NOTHING
                """, userId, cardId);
    }

    public void remove(String userId, String cardId) {
        jdbc.update("DELETE FROM watchlist WHERE user_id = ? AND card_id = ?", userId, cardId);
    }

    public boolean isWatching(String userId, String cardId) {
        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM watchlist WHERE user_id = ? AND card_id = ?", Long.class, userId, cardId);
        return count != null && count > 0;
    }
}
