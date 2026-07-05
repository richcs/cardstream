package com.cardstream.backend.serving.ws;

import com.cardstream.common.model.WindowedAggregate;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** Live {@code /sse/prices} feed — pushes every settled windowed aggregate as the sink consumes it. */
@RestController
public class PriceSseController {

    private static final Logger log = LoggerFactory.getLogger(PriceSseController.class);
    private static final long TIMEOUT_MS = 30 * 60 * 1000L;

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    @GetMapping(path = "/sse/prices", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(TIMEOUT_MS);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        emitters.add(emitter);
        return emitter;
    }

    public void broadcast(WindowedAggregate aggregate) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(aggregate, MediaType.APPLICATION_JSON);
            } catch (IOException e) {
                log.debug("Dropping SSE emitter after send failure", e);
                emitters.remove(emitter);
            }
        }
    }
}
