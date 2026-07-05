package com.cardstream.backend.serving.ws;

import com.cardstream.backend.serving.WatchlistRepository;
import com.cardstream.common.model.Alert;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Live {@code /ws/alerts} feed. A connection with {@code ?userId=} only receives alerts for cards on
 * that user's watchlist; without it, every alert is pushed.
 */
@Component
public class AlertWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(AlertWebSocketHandler.class);
    private static final int SEND_TIME_LIMIT_MS = 5_000;
    private static final int BUFFER_SIZE_LIMIT = 512 * 1024;

    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, String> sessionUserIds = new ConcurrentHashMap<>();
    private final WatchlistRepository watchlist;
    private final ObjectMapper objectMapper;

    public AlertWebSocketHandler(WatchlistRepository watchlist, ObjectMapper objectMapper) {
        this.watchlist = watchlist;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.put(session.getId(),
                new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MS, BUFFER_SIZE_LIMIT));
        String userId = UriComponentsBuilder.fromUri(session.getUri()).build()
                .getQueryParams().getFirst("userId");
        if (userId != null && !userId.isBlank()) {
            sessionUserIds.put(session.getId(), userId);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
        sessionUserIds.remove(session.getId());
    }

    public void broadcast(Alert alert) {
        String json;
        try {
            json = objectMapper.writeValueAsString(alert);
        } catch (Exception e) {
            log.warn("Failed to serialize alert {} for WS broadcast", alert.alertId(), e);
            return;
        }
        TextMessage message = new TextMessage(json);
        sessions.forEach((sessionId, session) -> {
            String userId = sessionUserIds.get(sessionId);
            if (userId != null && !watchlist.isWatching(userId, alert.cardId())) {
                return;
            }
            try {
                session.sendMessage(message);
            } catch (IOException e) {
                log.debug("Dropping WS session {} after send failure", sessionId, e);
            }
        });
    }
}
