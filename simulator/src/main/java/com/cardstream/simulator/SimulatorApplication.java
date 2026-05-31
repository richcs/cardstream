package com.cardstream.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Marketplace simulator: stands in for the real TCGplayer feed by exposing a TCGplayer-shaped
 * REST API (catalog/pricing/listings/sales) backed by a price engine. Seeds its product universe
 * from the backend catalog and emits a steady, pollable stream of listing/sale events.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class SimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimulatorApplication.class, args);
    }
}
