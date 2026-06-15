package com.cardstream.common.model;

/** Aggregation window flavor carried on {@code agg-price-windowed} records. */
public enum WindowType {
    /** Tumbling 1-hour window: average price + volume. */
    HOURLY,
    /** Tumbling 1-day window: average price + volume. */
    DAILY,
    /** Hopping 24h window (advances 1h): trailing moving average + volatility. */
    MA_24H
}
