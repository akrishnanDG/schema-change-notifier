package io.confluent.schemachange.producer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.schemachange.avro.*;
import io.confluent.schemachange.config.AppConfig;
import io.confluent.schemachange.exception.NotificationProducerException;
import io.confluent.schemachange.model.SchemaChangeNotification;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.confluent.schemachange.config.KafkaConstants.*;

/**
 * Kafka producer for sending schema change notifications to the target topic.
 * <p>
 * Uses Avro serialization with Schema Registry. The notification schema is
 * registered upfront at startup to fail fast if there are connectivity issues.
 * </p>
 *
 * <h2>Thread Safety:</h2>
 * This class is thread-safe. The underlying Kafka producer is thread-safe.
 *
 * @see SchemaChangeNotification
 */
public class NotificationProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(NotificationProducer.class);

    private final KafkaProducer<String, io.confluent.schemachange.avro.SchemaChangeNotification> producer;
    private final String targetTopic;
    private final boolean dryRun;

    /**
     * Creates a new NotificationProducer with the given configuration.
     * <p>
     * Registers the notification schema upfront and fails fast if Schema Registry
     * is unavailable or credentials are invalid.
     * </p>
     *
     * @param config The application configuration
     * @throws NotificationProducerException if schema registration fails
     */
    public NotificationProducer(@Nonnull AppConfig config) {
        this.targetTopic = config.getTargetTopic();
        this.dryRun = config.isDryRun();
        
        if (!dryRun) {
            registerSchemaUpfront(config);
            this.producer = createProducer(config);
            logger.info("Notification producer initialized with Avro serialization for topic: {}", targetTopic);
        } else {
            this.producer = null;
            logger.info("Notification producer in dry-run mode");
        }
    }

    /**
     * Registers the notification schema upfront at startup.
     * Uses TopicNameStrategy so subject will be: {@code <topic>-value}
     *
     * @param config The application configuration
     * @throws NotificationProducerException if registration fails
     */
    private void registerSchemaUpfront(@Nonnull AppConfig config) {
        String subjectName = targetTopic + "-value";
        
        logger.info("Registering notification schema for subject: {}", subjectName);
        
        Map<String, String> srConfig = Map.of(
                "basic.auth.credentials.source", BASIC_AUTH_CREDENTIALS_SOURCE,
                "basic.auth.user.info", config.getTargetSchemaRegistryApiKey() + ":" + config.getTargetSchemaRegistryApiSecret()
        );
        
        try (SchemaRegistryClient srClient = new CachedSchemaRegistryClient(
                Collections.singletonList(config.getTargetSchemaRegistryUrl()),
                100,
                srConfig)) {
            
            org.apache.avro.Schema avroSchema = io.confluent.schemachange.avro.SchemaChangeNotification.getClassSchema();
            AvroSchema schema = new AvroSchema(avroSchema);
            
            int schemaId = srClient.register(subjectName, schema);
            
            logger.info("Schema registered successfully: subject={}, schemaId={}", subjectName, schemaId);
            
        } catch (IOException | RestClientException e) {
            throw new NotificationProducerException(
                    "Failed to register notification schema for subject " + subjectName + ": " + e.getMessage(), 
                    subjectName, e);
        }
    }

    /**
     * Creates the Kafka producer with appropriate configuration.
     *
     * @param config The application configuration
     * @return The configured Kafka producer
     */
    private KafkaProducer<String, io.confluent.schemachange.avro.SchemaChangeNotification> createProducer(
            @Nonnull AppConfig config) {
        
        Properties props = new Properties();
        
        // Bootstrap servers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getTargetBootstrapServers());
        
        // Security configuration
        props.put("security.protocol", config.getSecurityProtocol());
        if (config.getSecurityProtocol().contains("SASL")) {
            props.put("sasl.mechanism", config.getSaslMechanism());
            props.put("sasl.jaas.config", String.format(
                    JAAS_CONFIG_TEMPLATE,
                    config.getTargetApiKey(),
                    config.getTargetApiSecret()
            ));
        }
        
        // Serializers - Avro for value
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        
        // Schema Registry configuration
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, config.getTargetSchemaRegistryUrl());
        props.put("basic.auth.credentials.source", BASIC_AUTH_CREDENTIALS_SOURCE);
        props.put("basic.auth.user.info", 
                config.getTargetSchemaRegistryApiKey() + ":" + config.getTargetSchemaRegistryApiSecret());
        
        // Use TopicNameStrategy (default) - schema registered as <topic>-value
        props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, 
                "io.confluent.kafka.serializers.subject.TopicNameStrategy");
        
        // Disable auto-register - we register upfront at startup
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        
        // Reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_ALL);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(DEFAULT_RETRIES));
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(DEFAULT_RETRY_BACKOFF_MS));
        
        // Performance settings
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(PRODUCER_BATCH_SIZE));
        props.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(PRODUCER_LINGER_MS));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);

        logger.info("Creating notification producer with Schema Registry: {}", 
                config.getTargetSchemaRegistryUrl());

        return new KafkaProducer<>(props);
    }

    /**
     * Sends a schema change notification to the target topic.
     *
     * @param notification The notification to send (Java model)
     * @return true if sent successfully (or dry run), false otherwise
     */
    public boolean send(@Nonnull SchemaChangeNotification notification) {
        io.confluent.schemachange.avro.SchemaChangeNotification avroRecord = convertToAvro(notification);
        
        if (avroRecord == null) {
            logger.error("Failed to convert notification to Avro for subject: {}", notification.getSubject());
            return false;
        }

        String key = createKey(notification);

        if (dryRun) {
            logger.info("[DRY RUN] Would produce Avro notification for subject: {}, schema_id: {}", 
                    notification.getSubject(), notification.getSchemaId());
            return true;
        }

        try {
            ProducerRecord<String, io.confluent.schemachange.avro.SchemaChangeNotification> record = 
                    new ProducerRecord<>(targetTopic, key, avroRecord);
            
            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send notification for subject {}: {}", 
                            notification.getSubject(), exception.getMessage());
                } else {
                    logger.debug("Sent notification to {}-{} at offset {}", 
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            RecordMetadata metadata = future.get(PRODUCER_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            logger.info("Produced {} notification for subject {} (schema_id={}) to partition {} offset {}",
                    notification.getEventType(),
                    notification.getSubject(),
                    notification.getSchemaId(),
                    metadata.partition(),
                    metadata.offset());
            
            return true;
        } catch (Exception e) {
            logger.error("Error sending notification: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Converts a Java model notification to an Avro record.
     *
     * @param notification The Java model notification
     * @return The Avro record, or null if conversion fails
     */
    @Nullable
    private io.confluent.schemachange.avro.SchemaChangeNotification convertToAvro(
            @Nonnull SchemaChangeNotification notification) {
        try {
            io.confluent.schemachange.avro.SchemaChangeNotification.Builder builder = 
                    io.confluent.schemachange.avro.SchemaChangeNotification.newBuilder();
            
            // Set event type
            builder.setEventType(EventType.valueOf(notification.getEventType().name()));
            
            // Set basic fields
            builder.setEnvironmentId(notification.getEnvironmentId());
            builder.setSubject(notification.getSubject());
            builder.setSchemaId(notification.getSchemaId());
            builder.setVersion(notification.getVersion());
            builder.setSchemaType(notification.getSchemaType());
            builder.setTimestamp(notification.getTimestamp());
            builder.setAuditLogEventId(notification.getAuditLogEventId());
            
            // Set nested objects based on event type
            setDataContractRegistered(builder, notification);
            setDataContractDeleted(builder, notification);
            setSubjectDeleted(builder, notification);
            setCompatibilityUpdated(builder, notification);
            setModeUpdated(builder, notification);
            
            return builder.build();
        } catch (Exception e) {
            logger.error("Error converting notification to Avro: {}", e.getMessage(), e);
            return null;
        }
    }

    private void setDataContractRegistered(
            io.confluent.schemachange.avro.SchemaChangeNotification.Builder builder,
            SchemaChangeNotification notification) {
        if (notification.getDataContractRegistered() != null) {
            DataContractRegistered dcr = DataContractRegistered.newBuilder()
                    .setSchema$(notification.getDataContractRegistered().getSchema())
                    .setReferences(notification.getDataContractRegistered().getReferences() != null 
                            ? notification.getDataContractRegistered().getReferences().toString() 
                            : null)
                    .build();
            builder.setDataContractRegistered(dcr);
        }
    }

    private void setDataContractDeleted(
            io.confluent.schemachange.avro.SchemaChangeNotification.Builder builder,
            SchemaChangeNotification notification) {
        if (notification.getDataContractDeleted() != null) {
            DataContractDeleted dcd = DataContractDeleted.newBuilder()
                    .setPermanent(notification.getDataContractDeleted().getPermanent())
                    .build();
            builder.setDataContractDeleted(dcd);
        }
    }

    private void setSubjectDeleted(
            io.confluent.schemachange.avro.SchemaChangeNotification.Builder builder,
            SchemaChangeNotification notification) {
        if (notification.getSubjectDeleted() != null) {
            SubjectDeleted sd = SubjectDeleted.newBuilder()
                    .setPermanent(notification.getSubjectDeleted().getPermanent())
                    .setVersionsDeleted(notification.getSubjectDeleted().getVersionsDeleted())
                    .build();
            builder.setSubjectDeleted(sd);
        }
    }

    private void setCompatibilityUpdated(
            io.confluent.schemachange.avro.SchemaChangeNotification.Builder builder,
            SchemaChangeNotification notification) {
        if (notification.getCompatibilityUpdated() != null) {
            CompatibilityUpdated cu = CompatibilityUpdated.newBuilder()
                    .setNewCompatibility(notification.getCompatibilityUpdated().getNewCompatibility())
                    .build();
            builder.setCompatibilityUpdated(cu);
        }
    }

    private void setModeUpdated(
            io.confluent.schemachange.avro.SchemaChangeNotification.Builder builder,
            SchemaChangeNotification notification) {
        if (notification.getModeUpdated() != null) {
            ModeUpdated mu = ModeUpdated.newBuilder()
                    .setNewMode(notification.getModeUpdated().getNewMode())
                    .build();
            builder.setModeUpdated(mu);
        }
    }

    /**
     * Creates a key for the notification record.
     * Uses subject as key to ensure ordering per subject.
     *
     * @param notification The notification
     * @return The record key
     */
    @Nonnull
    private String createKey(@Nonnull SchemaChangeNotification notification) {
        return notification.getSubject() != null ? notification.getSubject() : "unknown";
    }

    /**
     * Flushes any pending messages.
     */
    public void flush() {
        if (producer != null) {
            producer.flush();
            logger.debug("Producer flushed");
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close(Duration.ofSeconds(CLOSE_TIMEOUT_SECONDS));
                logger.info("Notification producer closed");
            } catch (Exception e) {
                logger.error("Error closing producer: {}", e.getMessage());
            }
        }
    }
}
