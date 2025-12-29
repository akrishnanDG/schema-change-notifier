package io.confluent.schemachange.config;

/**
 * Constants for Kafka and Schema Registry configuration.
 * Centralizes magic strings and default values used throughout the application.
 */
public final class KafkaConstants {

    private KafkaConstants() {
        // Prevent instantiation
    }

    // ==================== Security Configuration ====================
    
    /**
     * Security protocol for Confluent Cloud connections.
     */
    public static final String SECURITY_PROTOCOL = "SASL_SSL";

    /**
     * SASL mechanism for Confluent Cloud authentication.
     */
    public static final String SASL_MECHANISM = "PLAIN";

    /**
     * JAAS configuration template for PLAIN authentication.
     * Use with String.format(JAAS_CONFIG_TEMPLATE, username, password)
     */
    public static final String JAAS_CONFIG_TEMPLATE = 
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';";

    // ==================== Schema Registry Configuration ====================

    /**
     * Content type for Schema Registry API requests.
     */
    public static final String SCHEMA_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    /**
     * Credentials source for basic authentication.
     */
    public static final String BASIC_AUTH_CREDENTIALS_SOURCE = "USER_INFO";

    /**
     * Default schema type when not specified.
     */
    public static final String DEFAULT_SCHEMA_TYPE = "AVRO";

    // ==================== Audit Log Configuration ====================

    /**
     * Default Confluent Cloud audit log topic.
     */
    public static final String AUDIT_LOG_TOPIC = "confluent-audit-log-events";

    /**
     * Event type for Schema Registry operations in audit logs.
     */
    public static final String SCHEMA_REGISTRY_EVENT_TYPE = "io.confluent.sg.server/request";

    // ==================== Consumer Configuration ====================

    /**
     * Default consumer group ID prefix.
     */
    public static final String DEFAULT_CONSUMER_GROUP = "schema-change-notifier";

    /**
     * Default poll timeout in milliseconds.
     */
    public static final int DEFAULT_POLL_TIMEOUT_MS = 1000;

    /**
     * Default batch size for consumer polling.
     */
    public static final int DEFAULT_BATCH_SIZE = 100;

    // ==================== Producer Configuration ====================

    /**
     * Acknowledgment setting for reliable delivery.
     */
    public static final String ACKS_ALL = "all";

    /**
     * Compression type for efficient message transmission.
     */
    public static final String COMPRESSION_TYPE = "snappy";

    /**
     * Default batch size in bytes for producer.
     */
    public static final int PRODUCER_BATCH_SIZE = 16384;

    /**
     * Default linger time in milliseconds.
     */
    public static final int PRODUCER_LINGER_MS = 10;

    /**
     * Default number of retries.
     */
    public static final int DEFAULT_RETRIES = 3;

    /**
     * Default retry backoff in milliseconds.
     */
    public static final int DEFAULT_RETRY_BACKOFF_MS = 1000;

    // ==================== Timeouts ====================

    /**
     * Default HTTP connection timeout in seconds.
     */
    public static final int HTTP_CONNECT_TIMEOUT_SECONDS = 10;

    /**
     * Default HTTP read timeout in seconds.
     */
    public static final int HTTP_READ_TIMEOUT_SECONDS = 30;

    /**
     * Default producer send timeout in seconds.
     */
    public static final int PRODUCER_SEND_TIMEOUT_SECONDS = 30;

    /**
     * Default close timeout in seconds.
     */
    public static final int CLOSE_TIMEOUT_SECONDS = 10;

    // ==================== Deduplication ====================

    /**
     * Maximum number of events to track for deduplication.
     */
    public static final int MAX_DEDUP_EVENTS = 100_000;

    /**
     * Default state file path for deduplication.
     */
    public static final String DEFAULT_STATE_FILE_PATH = "./schema-change-notifier-state.json";

    // ==================== Result Status ====================

    /**
     * Success status in audit log results.
     */
    public static final String STATUS_SUCCESS = "SUCCESS";
}

