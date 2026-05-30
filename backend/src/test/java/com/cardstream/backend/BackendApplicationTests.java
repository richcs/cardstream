package com.cardstream.backend;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=localhost:59092")
class BackendApplicationTests {

    @Test
    void contextLoads() {
    }
}
