package io.confluent.schemachange.consumer;

import io.confluent.schemachange.config.AppConfig;
import io.confluent.schemachange.config.ProcessingMode;
import io.confluent.schemachange.model.AuditLogEvent;
import io.confluent.schemachange.util.JsonUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.schemachange.config.KafkaConstants.*;

/**
 * Kafka consumer for reading Confluent Cloud audit log events.
 * <p>
 * Supports multiple processing modes:
 * </p>
 * <ul>
 *   <li>{@link ProcessingMode#STREAM} - Real-time processing from the latest offset</li>
 *   <li>{@link ProcessingMode#BACKFILL} - Process all historical events</li>
 *   <li>{@link ProcessingMode#TIMESTAMP} - Process events from a specific timestamp</li>
 *   <li>{@link ProcessingMode#RESUME} - Resume from last committed offset</li>
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 * This class is NOT thread-safe. All operations should be performed from a single thread.
 */
public class AuditLogConsumer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AuditLogConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final AppConfig config;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Set<TopicPartition> endOffsetsReached = new HashSet<>();
    private Map<TopicPartition, Long> endOffsets;

    /**
     * Creates a new AuditLogConsumer with the given configuration.
     *
     * @param config The application configuration
     */
    public AuditLogConsumer(@Nonnull AppConfig config) {
        this.config = config;
        this.consumer = createConsumer();
        subscribeAndSeek();
    }

    /**
     * Creates the Kafka consumer with appropriate configuration.
     *
     * @return The configured Kafka consumer
     */
    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        
        // Bootstrap servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getAuditLogBootstrapServers());
        
        // Security configuration
        props.put("security.protocol", config.getSecurityProtocol());
        if (config.getSecurityProtocol().contains("SASL")) {
            props.put("sasl.mechanism", config.getSaslMechanism());
            props.put("sasl.jaas.config", String.format(
                    JAAS_CONFIG_TEMPLATE,
                    config.getAuditLogApiKey(),
                    config.getAuditLogApiSecret()
            ));
        }
        
        // Consumer configuration
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getConsumerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(config.getBatchSize()));
        
        // Set auto offset reset based on processing mode
        String autoOffsetReset = switch (config.getProcessingMode()) {
            case STREAM -> "latest";
            case BACKFILL, TIMESTAMP, RESUME -> "earliest";
        };
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        return new KafkaConsumer<>(props);
    }

    /**
     * Subscribes to the audit log topic and sets up rebalance handling.
     */
    private void subscribeAndSeek() {
        String topic = config.getAuditLogTopic();
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // No action needed
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                handlePartitionAssignment(partitions);
            }
        });
    }

    /**
     * Handles partition assignment based on the processing mode.
     *
     * @param partitions The assigned partitions
     */
    private void handlePartitionAssignment(@Nonnull Collection<TopicPartition> partitions) {
        ProcessingMode mode = config.getProcessingMode();
        
        switch (mode) {
            case STREAM -> {
                consumer.seekToEnd(partitions);
                logger.info("Setup complete, listening for schema changes in audit logs...");
            }
            case BACKFILL -> {
                consumer.seekToBeginning(partitions);
                if (config.isStopAtCurrent()) {
                    endOffsets = consumer.endOffsets(partitions);
                }
                logger.info("Setup complete, processing historical schema changes...");
            }
            case TIMESTAMP -> {
                seekToTimestamp(partitions);
                logger.info("Setup complete, processing schema changes from {}...", config.getStartTimestamp());
            }
            case RESUME -> {
                logger.info("Setup complete, resuming from last position...");
            }
        }
    }

    /**
     * Seeks all partitions to the configured start timestamp.
     *
     * @param partitions The partitions to seek
     */
    private void seekToTimestamp(@Nonnull Collection<TopicPartition> partitions) {
        String startTimestamp = config.getStartTimestamp();
        Instant startInstant = Instant.parse(startTimestamp);
        long startMs = startInstant.toEpochMilli();

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition partition : partitions) {
            timestampsToSearch.put(partition, startMs);
        }

        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
        
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
            TopicPartition partition = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
            
            if (offsetAndTimestamp != null) {
                consumer.seek(partition, offsetAndTimestamp.offset());
            } else {
                consumer.seekToEnd(Collections.singleton(partition));
            }
        }

        if (config.getEndTimestamp() != null) {
            endOffsets = consumer.endOffsets(partitions);
        }
    }

    /**
     * Polls for audit log events.
     *
     * @return List of parsed audit log events
     */
    @Nonnull
    public List<AuditLogEvent> poll() {
        if (!running.get()) {
            return Collections.emptyList();
        }

        ConsumerRecords<String, String> records = consumer.poll(
                Duration.ofMillis(config.getPollTimeoutMs())
        );

        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        List<AuditLogEvent> events = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {
            try {
                if (shouldStopAtCurrent(record)) {
                    continue;
                }

                if (shouldStopAtEndTimestamp(record)) {
                    continue;
                }

                AuditLogEvent event = JsonUtils.fromJson(record.value(), AuditLogEvent.class);
                if (event != null) {
                    events.add(event);
                }
            } catch (Exception e) {
                logger.warn("Failed to parse audit log event at offset {}: {}", 
                        record.offset(), e.getMessage());
            }
        }

        return events;
    }

    /**
     * Checks if processing should stop at the current offset (for BACKFILL mode).
     *
     * @param record The current record
     * @return true if this record should be skipped
     */
    private boolean shouldStopAtCurrent(@Nonnull ConsumerRecord<String, String> record) {
        if (!config.isStopAtCurrent() || endOffsets == null) {
            return false;
        }

        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        Long endOffset = endOffsets.get(partition);
        
        if (endOffset != null && record.offset() >= endOffset - 1) {
            endOffsetsReached.add(partition);
            
            if (endOffsetsReached.size() == endOffsets.size()) {
                logger.info("Reached end of all partitions, stopping...");
                running.set(false);
            }
        }
        
        return false;
    }

    /**
     * Checks if processing should stop at the end timestamp (for TIMESTAMP mode).
     *
     * @param record The current record
     * @return true if this record should be skipped
     */
    private boolean shouldStopAtEndTimestamp(@Nonnull ConsumerRecord<String, String> record) {
        if (config.getEndTimestamp() == null) {
            return false;
        }

        Instant endInstant = Instant.parse(config.getEndTimestamp());
        long endMs = endInstant.toEpochMilli();

        if (record.timestamp() > endMs) {
            logger.info("Reached end timestamp, stopping...");
            running.set(false);
            return true;
        }

        return false;
    }

    /**
     * Commits offsets synchronously.
     */
    public void commitSync() {
        try {
            consumer.commitSync();
            logger.debug("Committed offsets");
        } catch (Exception e) {
            logger.error("Failed to commit offsets: {}", e.getMessage());
        }
    }

    /**
     * Checks if the consumer is still running.
     *
     * @return true if the consumer is running
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Stops the consumer gracefully.
     */
    public void stop() {
        running.set(false);
    }

    @Override
    public void close() {
        running.set(false);
        try {
            consumer.close(Duration.ofSeconds(CLOSE_TIMEOUT_SECONDS));
            logger.info("Audit log consumer closed");
        } catch (Exception e) {
            logger.error("Error closing consumer: {}", e.getMessage());
        }
    }
}
