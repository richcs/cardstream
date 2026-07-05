package com.cardstream.backend.sink;

import com.cardstream.common.model.Alert;
import com.cardstream.common.model.AlertType;
import com.cardstream.common.model.Severity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/** Sinks {@code alerts} (SPIKE + ARBITRAGE); serves {@code /api/alerts} and {@code /api/arbitrage} reads. */
@Repository
public class AlertRepository {

    private static final String INSERT = """
            INSERT INTO alert (alert_id, type, severity, card_id, market_key, name, detail, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?)
            ON CONFLICT (alert_id) DO NOTHING
            """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final RowMapper<Alert> mapper;

    public AlertRepository(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.mapper = (rs, n) -> {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> detail = objectMapper.readValue(rs.getString("detail"), Map.class);
                return new Alert(rs.getString("alert_id"), AlertType.valueOf(rs.getString("type")),
                        Severity.valueOf(rs.getString("severity")), rs.getString("card_id"),
                        rs.getString("market_key"), rs.getString("name"), detail,
                        rs.getTimestamp("ts").toInstant());
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Corrupt alert detail JSON for " + rs.getString("alert_id"), e);
            }
        };
    }

    public void insert(Alert alert) {
        String detailJson;
        try {
            detailJson = objectMapper.writeValueAsString(alert.detail());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize alert detail for " + alert.alertId(), e);
        }
        jdbc.update(INSERT, alert.alertId(), alert.type().name(), alert.severity().name(), alert.cardId(),
                alert.marketKey(), alert.name(), detailJson, Timestamp.from(alert.ts()));
    }

    public List<Alert> recent(AlertType type, Severity severity, int limit) {
        List<String> clauses = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        if (type != null) {
            clauses.add("type = ?");
            args.add(type.name());
        }
        if (severity != null) {
            clauses.add("severity = ?");
            args.add(severity.name());
        }
        String where = clauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", clauses);
        args.add(limit);
        return jdbc.query("SELECT * FROM alert" + where + " ORDER BY ts DESC LIMIT ?", mapper, args.toArray());
    }
}
