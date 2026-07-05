plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.kafka:spring-kafka")
    // Phase 5: /ws/alerts live feed.
    implementation("org.springframework.boot:spring-boot-starter-websocket")

    // Phase 4: Kafka Streams topology (version managed by the Spring Boot BOM).
    implementation("org.apache.kafka:kafka-streams")

    // Phase 1: catalog persistence + migrations (Flyway 10 needs the postgres module).
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    // Phase 4: drive the topology offline (no broker) via TopologyTestDriver.
    testImplementation("org.apache.kafka:kafka-streams-test-utils")
    // Context-load smoke test runs offline against in-memory H2 (Flyway disabled in tests).
    testRuntimeOnly("com.h2database:h2")
    // Phase 5: sink/serving repository tests run Flyway migrations against a real Postgres.
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
}
