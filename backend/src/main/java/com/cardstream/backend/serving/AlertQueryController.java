package com.cardstream.backend.serving;

import com.cardstream.backend.sink.AlertRepository;
import com.cardstream.common.model.Alert;
import com.cardstream.common.model.AlertType;
import com.cardstream.common.model.Severity;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Recent alerts, and the arbitrage-only slice of them. */
@RestController
public class AlertQueryController {

    private final AlertRepository alerts;

    public AlertQueryController(AlertRepository alerts) {
        this.alerts = alerts;
    }

    @GetMapping("/api/alerts")
    public List<Alert> alerts(
            @RequestParam(required = false) Severity severity,
            @RequestParam(required = false) AlertType type,
            @RequestParam(defaultValue = "50") int limit) {
        return alerts.recent(type, severity, limit);
    }

    @GetMapping("/api/arbitrage")
    public List<Alert> arbitrage(@RequestParam(defaultValue = "50") int limit) {
        return alerts.recent(AlertType.ARBITRAGE, null, limit);
    }
}
