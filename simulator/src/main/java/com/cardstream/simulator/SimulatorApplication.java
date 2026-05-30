package com.cardstream.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Marketplace simulator: stands in for the real TCGplayer feed by exposing a TCGplayer-shaped
 * REST API (catalog/pricing/listings/sales) backed by a price engine. Phase 0 boots a bare app;
 * the price engine and endpoints arrive in Phase 2.
 */
@SpringBootApplication
public class SimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimulatorApplication.class, args);
    }
}
