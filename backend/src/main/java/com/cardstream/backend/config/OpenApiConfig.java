package com.cardstream.backend.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    OpenAPI cardstreamOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Cardstream API")
                .description("Real-time TCG card market intelligence: catalog, market state, history, top movers, arbitrage, alerts, and watchlist.")
                .version("v1"));
    }
}
