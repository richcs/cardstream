package com.cardstream.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Cardstream backend: ingestion poller, Kafka Streams topology, serving (REST + WS/SSE),
 * and the Postgres sink consumer. Phase 0 boots a bare web + actuator app.
 */
@SpringBootApplication
public class BackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }
}
