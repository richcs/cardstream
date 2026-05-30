plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.kafka:spring-kafka")

    // Added in later phases (kept out of Phase 0 so the app boots without a DB):
    //   spring-boot-starter-jdbc, postgresql, flyway-core   (Phase 1/5)
    //   org.apache.kafka:kafka-streams                      (Phase 4)

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
}
