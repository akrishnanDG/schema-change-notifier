package io.confluent.schemachange.config;

/**
 * Processing modes for handling historical vs real-time data.
 */
public enum ProcessingMode {
    /**
     * Stream mode - Only process new events (auto.offset.reset=latest)
     */
    STREAM,
    
    /**
     * Backfill mode - Process all historical events from beginning
     */
    BACKFILL,
    
    /**
     * Timestamp mode - Start processing from a specific timestamp
     */
    TIMESTAMP,
    
    /**
     * Resume mode - Continue from last committed offset
     */
    RESUME
}

