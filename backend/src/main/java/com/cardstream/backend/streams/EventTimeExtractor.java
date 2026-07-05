package com.cardstream.backend.streams;

import com.cardstream.common.model.Listing;
import com.cardstream.common.model.Sale;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Event-time extractor: stream time is driven by {@code soldAt}/{@code listedAt}, not wall clock.
 * Non-market records and any record missing an event time fall back to the broker record timestamp.
 */
public class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Instant eventTime = switch (record.value()) {
            case Sale s -> s.soldAt();
            case Listing l -> l.listedAt();
            case null, default -> null;
        };
        if (eventTime != null) {
            return eventTime.toEpochMilli();
        }
        long recordTime = record.timestamp();
        if (recordTime >= 0) {
            return recordTime;
        }
        return Math.max(partitionTime, 0L);
    }
}
