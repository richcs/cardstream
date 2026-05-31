package com.cardstream.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Cardstream backend: ingestion poller, Kafka Streams topology, serving (REST + WS/SSE),
 * and the Postgres sink consumer. Phase 1 adds the metadata catalog (Pokémon TCG API → Postgres
 * + compacted card-metadata topic) and the /api/cards endpoints; Phase 3 adds the multi-source
 * ingestion poller (marketplace feeds → Kafka).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class BackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }
}
