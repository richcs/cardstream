package com.cardstream.backend.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cardstream.backend.metadata.dto.TcgApiDtos.Card;
import com.cardstream.backend.metadata.dto.TcgApiDtos.CardImages;
import com.cardstream.backend.metadata.dto.TcgApiDtos.Set;
import com.cardstream.common.model.CardMetadata;
import com.cardstream.common.model.Game;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CatalogLoaderTest {

    @Mock PokemonTcgClient client;
    @Mock SetRepository setRepository;
    @Mock CardRepository cardRepository;
    @Mock CardMetadataPublisher publisher;

    private CatalogLoader loader(LocalDate cutoff) {
        CatalogProperties props = new CatalogProperties(null, null, cutoff, 250, false);
        return new CatalogLoader(client, setRepository, cardRepository, publisher, props);
    }

    private static Set set(String id, String name, String releaseDate) {
        return new Set(id, name, "Scarlet & Violet", 100, 110, releaseDate, null);
    }

    private static Card card(String id, String name) {
        return new Card(id, name, "1", "Rare", "Pokémon", List.of("Fire"),
                new CardImages("small.png", "large.png"));
    }

    @Test
    void loadsOnlySetsOnOrAfterCutoff() {
        when(client.fetchAllSets()).thenReturn(List.of(
                set("sv5", "Recent", "2024/05/03"),
                set("sv1", "OnCutoff", "2024/01/01"),
                set("swsh12", "TooOld", "2022/03/11"),
                set("noDate", "NoDate", null)));
        when(client.fetchCardsForSet("sv5")).thenReturn(List.of(card("sv5-1", "A"), card("sv5-2", "B")));
        when(client.fetchCardsForSet("sv1")).thenReturn(List.of(card("sv1-1", "C")));

        CatalogLoader.Result result = loader(LocalDate.of(2024, 1, 1)).load();

        assertThat(result.sets()).isEqualTo(2);
        assertThat(result.cards()).isEqualTo(3);
        verify(setRepository).upsert(argThatId("sv5"));
        verify(setRepository).upsert(argThatId("sv1"));
        verify(client, never()).fetchCardsForSet("swsh12");
        verify(client, never()).fetchCardsForSet("noDate");
        verify(cardRepository, times(3)).upsert(any(), any(), eq(Game.POKEMON));
        verify(publisher, times(3)).publish(any(CardMetadata.class));
    }

    @Test
    void publishesMetadataKeyedByCardWithSetNameAndLargeImage() {
        when(client.fetchAllSets()).thenReturn(List.of(set("sv5", "Temporal Forces", "2024/03/22")));
        when(client.fetchCardsForSet("sv5")).thenReturn(List.of(card("sv5-7", "Iron Crown")));

        loader(LocalDate.of(2024, 1, 1)).load();

        ArgumentCaptor<CardMetadata> captor = ArgumentCaptor.forClass(CardMetadata.class);
        verify(publisher).publish(captor.capture());
        CardMetadata md = captor.getValue();
        assertThat(md.cardId()).isEqualTo("sv5-7");
        assertThat(md.name()).isEqualTo("Iron Crown");
        assertThat(md.set()).isEqualTo("Temporal Forces");
        assertThat(md.game()).isEqualTo(Game.POKEMON);
        assertThat(md.imageUrl()).isEqualTo("large.png");
    }

    @Test
    void parsesReleaseDateAndTreatsBadInputAsNull() {
        assertThat(SetRepository.parseReleaseDate("2024/03/22")).isEqualTo(LocalDate.of(2024, 3, 22));
        assertThat(SetRepository.parseReleaseDate(null)).isNull();
        assertThat(SetRepository.parseReleaseDate("not-a-date")).isNull();
    }

    private static Set argThatId(String id) {
        return org.mockito.ArgumentMatchers.argThat(s -> s != null && id.equals(s.id()));
    }
}
