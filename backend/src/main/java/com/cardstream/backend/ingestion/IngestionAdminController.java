package com.cardstream.backend.ingestion;

import com.cardstream.backend.ingestion.SourcePoller.SourceStatus;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Per-source ingestion health: cursor positions, ingested/rejected/duplicate counts, circuit state. */
@RestController
@RequestMapping("/api/admin/ingestion")
public class IngestionAdminController {

    private final SourcePoller poller;

    public IngestionAdminController(SourcePoller poller) {
        this.poller = poller;
    }

    @GetMapping("/status")
    public List<SourceStatus> status() {
        return poller.status();
    }
}
