plugins {
    `java-library`
}

dependencies {
    // Jackson for (de)serialization of event records on Kafka topics and REST.
    api("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.2")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}
