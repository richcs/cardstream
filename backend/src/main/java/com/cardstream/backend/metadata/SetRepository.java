package com.cardstream.backend.metadata;

import com.cardstream.backend.metadata.dto.TcgApiDtos.Set;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SetRepository {

    /** Pokémon TCG API release dates are formatted "YYYY/MM/DD". */
    static final DateTimeFormatter RELEASE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    private static final String UPSERT = """
            INSERT INTO card_set (set_id, name, series, printed_total, total, release_date, logo_url, symbol_url, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (set_id) DO UPDATE SET
                name = EXCLUDED.name,
                series = EXCLUDED.series,
                printed_total = EXCLUDED.printed_total,
                total = EXCLUDED.total,
                release_date = EXCLUDED.release_date,
                logo_url = EXCLUDED.logo_url,
                symbol_url = EXCLUDED.symbol_url,
                updated_at = now()
            """;

    private final JdbcTemplate jdbc;

    public SetRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void upsert(Set set) {
        LocalDate releaseDate = parseReleaseDate(set.releaseDate());
        jdbc.update(UPSERT,
                set.id(),
                set.name(),
                set.series(),
                set.printedTotal(),
                set.total(),
                releaseDate == null ? null : Date.valueOf(releaseDate),
                set.images() == null ? null : set.images().logo(),
                set.images() == null ? null : set.images().symbol());
    }

    /** Parse the API's "YYYY/MM/DD" release date; null/unparseable yields null. */
    static LocalDate parseReleaseDate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(raw, RELEASE_DATE_FORMAT);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
