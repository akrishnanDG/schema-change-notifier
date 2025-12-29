package io.confluent.schemachange.processor;

import io.confluent.schemachange.config.AppConfig;
import io.confluent.schemachange.model.AuditLogEvent;
import io.confluent.schemachange.model.SchemaChangeNotification;
import io.confluent.schemachange.model.SchemaChangeNotification.*;
import io.confluent.schemachange.registry.SchemaRegistryClient;
import io.confluent.schemachange.registry.SchemaRegistryService.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.confluent.schemachange.config.KafkaConstants.*;

/**
 * Processes audit log events and transforms them into schema change notifications.
 * <p>
 * This processor filters relevant Schema Registry events from the audit log stream,
 * enriches them with schema details from Schema Registry, and creates notifications.
 * </p>
 *
 * <h2>Supported Operations:</h2>
 * <ul>
 *   <li>schema-registry.RegisterSchema - New schema version registered</li>
 *   <li>schema-registry.DeleteSchema - Specific schema version deleted</li>
 *   <li>schema-registry.DeleteSubject - Entire subject deleted</li>
 *   <li>schema-registry.UpdateCompatibility - Subject compatibility changed</li>
 *   <li>schema-registry.UpdateMode - Subject mode changed</li>
 * </ul>
 *
 * @see SchemaChangeNotification
 */
public class SchemaChangeProcessor {

    private static final Logger logger = LoggerFactory.getLogger(SchemaChangeProcessor.class);

    /**
     * Pattern to extract environment ID from CRN (Confluent Resource Name).
     * Example CRN: crn://confluent.cloud/organization=xxx/environment=env-abc123/schema-registry=lsrc-xyz
     */
    private static final Pattern ENVIRONMENT_PATTERN = Pattern.compile("environment=([^/]+)");

    private static final Set<String> SCHEMA_CHANGE_METHODS = Set.of(
            "schema-registry.RegisterSchema",
            "schema-registry.DeleteSchema",
            "schema-registry.DeleteSubject"
    );

    private static final Set<String> CONFIG_CHANGE_METHODS = Set.of(
            "schema-registry.UpdateCompatibility",
            "schema-registry.UpdateMode"
    );

    private final AppConfig config;
    private final SchemaRegistryClient schemaRegistryClient;

    /**
     * Creates a new SchemaChangeProcessor.
     *
     * @param config                The application configuration
     * @param schemaRegistryClient The Schema Registry client for fetching schema details
     */
    public SchemaChangeProcessor(
            @Nonnull AppConfig config, 
            @Nonnull SchemaRegistryClient schemaRegistryClient) {
        this.config = config;
        this.schemaRegistryClient = schemaRegistryClient;
        logger.info("Schema change processor initialized");
    }

    /**
     * Extracts the environment ID from the resourceName CRN in the audit log event.
     * <p>
     * The resourceName contains the full CRN with environment, e.g.:
     * {@code crn://confluent.cloud/organization=xxx/environment=env-abc123/schema-registry=lsrc-xyz}
     * </p>
     *
     * @param event The audit log event
     * @return The environment ID (e.g., "env-abc123"), or null if not found
     */
    @Nullable
    public String extractEnvironmentId(@Nonnull AuditLogEvent event) {
        // First try resourceName from data (most reliable)
        if (event.getData() != null && event.getData().getResourceName() != null) {
            Matcher matcher = ENVIRONMENT_PATTERN.matcher(event.getData().getResourceName());
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        
        // Fallback to subject field
        if (event.getSubject() != null) {
            Matcher matcher = ENVIRONMENT_PATTERN.matcher(event.getSubject());
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        
        return null;
    }

    /**
     * Checks if an audit log event is relevant for schema change processing.
     * <p>
     * An event is considered relevant if:
     * </p>
     * <ol>
     *   <li>It's a Schema Registry event ({@code io.confluent.sg.server/request})</li>
     *   <li>It has valid audit data</li>
     *   <li>The method is in the configured include list</li>
     *   <li>The environment is configured for monitoring</li>
     *   <li>The result status matches filters (if configured)</li>
     *   <li>The subject matches filters (if configured)</li>
     * </ol>
     *
     * @param event The audit log event
     * @return true if the event should be processed
     */
    public boolean isRelevantEvent(@Nonnull AuditLogEvent event) {
        // 1. Check event type
        if (!SCHEMA_REGISTRY_EVENT_TYPE.equals(event.getType())) {
            logger.trace("Skipping event with type: {}", event.getType());
            return false;
        }

        // 2. Check if data is present
        if (event.getData() == null) {
            logger.trace("Skipping event with null data");
            return false;
        }

        String methodName = event.getData().getMethodName();

        // 3. Check method name against configured filters
        if (!config.getIncludeMethods().contains(methodName)) {
            if (methodName != null && methodName.startsWith("schema-registry.")) {
                logger.debug("Skipping SR event with method: {}", methodName);
            }
            return false;
        }

        // 4. Check if the event is from a monitored environment
        String environmentId = extractEnvironmentId(event);
        if (environmentId == null) {
            logger.debug("Skipping {} event - no environment ID in source: {}", methodName, event.getSource());
            return false;
        }
        if (!config.hasEnvironment(environmentId)) {
            if ("schema-registry.RegisterSchema".equals(methodName)) {
                logNonMonitoredEvent(event, environmentId);
            }
            return false;
        }

        // 5. Check result status if only successful is required
        if (config.isOnlySuccessful()) {
            AuditLogEvent.ResultData result = event.getData().getResult();
            if (result == null || !STATUS_SUCCESS.equalsIgnoreCase(result.getStatus())) {
                logger.debug("Skipping failed event: {} - {}", 
                        methodName, result != null ? result.getStatus() : "null");
                return false;
            }
        }

        // 6. Check subject filter if configured
        if (config.hasSubjectFilters()) {
            String subject = getSubjectFromEvent(event);
            if (!matchesSubjectFilter(subject)) {
                logger.debug("Skipping event for non-matching subject: {}", subject);
                return false;
            }
        }

        logger.trace("Event is relevant: {} for subject {} in environment {}", 
                methodName, getSubjectFromEvent(event), environmentId);
        return true;
    }

    /**
     * Logs details of a RegisterSchema event from a non-monitored environment.
     */
    private void logNonMonitoredEvent(@Nonnull AuditLogEvent event, @Nonnull String environmentId) {
        logger.info("=== RegisterSchema Event from non-monitored environment ===");
        logger.info("Environment ID: {}", environmentId);
        logger.info("Resource Name: {}", event.getData().getResourceName());
        if (event.getData().getRequest() != null) {
            logger.info("Request Subject: {}", event.getData().getRequest().getSubject());
        }
        if (event.getData().getResult() != null) {
            logger.info("Result Status: {}", event.getData().getResult().getStatus());
            logger.info("Schema ID: {}", event.getData().getResult().getSchemaId());
        }
        logger.info("Configured environments: {}", config.getMonitoredEnvironmentIds());
        logger.info("=== End Event ===");
    }

    /**
     * Extracts the subject name from an audit log event.
     */
    @Nullable
    private String getSubjectFromEvent(@Nonnull AuditLogEvent event) {
        AuditLogEvent.RequestData request = event.getData().getRequest();
        if (request != null && request.getSubject() != null) {
            return request.getSubject();
        }
        return event.getData().getResourceName();
    }

    /**
     * Checks if a subject matches the configured filters.
     * Supports glob patterns like "orders-*" or exact matches.
     *
     * @param subject The subject name to check
     * @return true if the subject matches at least one filter
     */
    private boolean matchesSubjectFilter(@Nullable String subject) {
        if (subject == null) {
            return false;
        }

        for (String filter : config.getSubjectFilters()) {
            if (filter.contains("*")) {
                String regex = filter.replace(".", "\\.").replace("*", ".*");
                if (Pattern.matches(regex, subject)) {
                    return true;
                }
            } else if (filter.equals(subject)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Processes an audit log event and creates a schema change notification.
     *
     * @param event The audit log event
     * @return Optional containing the notification, or empty if the event is not relevant
     *         or processing fails
     */
    @Nonnull
    public Optional<SchemaChangeNotification> process(@Nonnull AuditLogEvent event) {
        if (!isRelevantEvent(event)) {
            return Optional.empty();
        }

        String methodName = event.getData().getMethodName();
        
        try {
            SchemaChangeNotification notification = switch (methodName) {
                case "schema-registry.RegisterSchema" -> processRegisterSchema(event);
                case "schema-registry.DeleteSchema" -> processDeleteSchema(event);
                case "schema-registry.DeleteSubject" -> processDeleteSubject(event);
                case "schema-registry.UpdateCompatibility" -> processUpdateCompatibility(event);
                case "schema-registry.UpdateMode" -> processUpdateMode(event);
                default -> {
                    logger.warn("Unknown method name: {}", methodName);
                    yield null;
                }
            };

            if (notification != null) {
                notification.setAuditLogEventId(event.getId());
                logger.info("Processed {} event for subject: {}", 
                        notification.getEventType(), notification.getSubject());
            }

            return Optional.ofNullable(notification);
        } catch (Exception e) {
            logger.error("Error processing event {}: {}", event.getId(), e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Processes a RegisterSchema event.
     */
    @Nullable
    private SchemaChangeNotification processRegisterSchema(@Nonnull AuditLogEvent event) {
        AuditLogEvent.AuditLogData data = event.getData();
        AuditLogEvent.ResultData result = data.getResult();
        AuditLogEvent.RequestData request = data.getRequest();
        
        String environmentId = extractEnvironmentId(event);
        Integer schemaId = result != null ? result.getSchemaId() : null;
        String subject = request != null ? request.getSubject() : data.getResourceName();

        // Fetch schema details from Schema Registry
        String schema = null;
        String schemaType = null;
        Object references = null;
        Integer version = null;

        if (schemaId != null && environmentId != null) {
            SchemaInfo schemaInfo = schemaRegistryClient.getSchemaById(environmentId, schemaId);
            if (schemaInfo != null) {
                schema = schemaInfo.schema();
                schemaType = schemaInfo.schemaType();
                references = schemaInfo.references();
                version = schemaInfo.version();
            } else {
                logger.warn("Could not fetch schema {} from Schema Registry for environment {}", 
                        schemaId, environmentId);
            }
        } else if (schemaId == null) {
            logger.warn("No schema ID in RegisterSchema event for subject: {}", subject);
        }

        if (schemaType == null && request != null) {
            schemaType = request.getSchemaType();
        }

        return SchemaChangeNotification.builder()
                .eventType(EventType.SCHEMA_REGISTERED)
                .schemaId(schemaId)
                .subject(subject)
                .version(version)
                .schemaType(schemaType != null ? schemaType : DEFAULT_SCHEMA_TYPE)
                .timestamp(event.getTime())
                .environmentId(environmentId)
                .dataContractRegistered(new DataContractRegistered(schema, references))
                .build();
    }

    /**
     * Processes a DeleteSchema event.
     */
    @Nullable
    private SchemaChangeNotification processDeleteSchema(@Nonnull AuditLogEvent event) {
        AuditLogEvent.AuditLogData data = event.getData();
        AuditLogEvent.RequestData request = data.getRequest();
        String environmentId = extractEnvironmentId(event);

        String subject = data.getResourceName();
        Integer version = request != null ? request.getVersion() : null;

        return SchemaChangeNotification.builder()
                .eventType(EventType.SCHEMA_DELETED)
                .subject(subject)
                .version(version)
                .timestamp(event.getTime())
                .environmentId(environmentId)
                .dataContractDeleted(new DataContractDeleted(false))
                .build();
    }

    /**
     * Processes a DeleteSubject event.
     */
    @Nullable
    private SchemaChangeNotification processDeleteSubject(@Nonnull AuditLogEvent event) {
        AuditLogEvent.AuditLogData data = event.getData();
        String environmentId = extractEnvironmentId(event);
        String subject = data.getResourceName();

        return SchemaChangeNotification.builder()
                .eventType(EventType.SUBJECT_DELETED)
                .subject(subject)
                .timestamp(event.getTime())
                .environmentId(environmentId)
                .subjectDeleted(new SubjectDeleted(false, null))
                .build();
    }

    /**
     * Processes an UpdateCompatibility event.
     */
    @Nullable
    private SchemaChangeNotification processUpdateCompatibility(@Nonnull AuditLogEvent event) {
        AuditLogEvent.AuditLogData data = event.getData();
        AuditLogEvent.RequestData request = data.getRequest();
        String environmentId = extractEnvironmentId(event);

        String subject = data.getResourceName();
        String compatibility = request != null ? request.getCompatibility() : null;

        return SchemaChangeNotification.builder()
                .eventType(EventType.COMPATIBILITY_UPDATED)
                .subject(subject)
                .timestamp(event.getTime())
                .environmentId(environmentId)
                .compatibilityUpdated(new CompatibilityUpdated(compatibility))
                .build();
    }

    /**
     * Processes an UpdateMode event.
     */
    @Nullable
    private SchemaChangeNotification processUpdateMode(@Nonnull AuditLogEvent event) {
        AuditLogEvent.AuditLogData data = event.getData();
        AuditLogEvent.RequestData request = data.getRequest();
        String environmentId = extractEnvironmentId(event);

        String subject = data.getResourceName();
        String mode = request != null ? request.getMode() : null;

        return SchemaChangeNotification.builder()
                .eventType(EventType.MODE_UPDATED)
                .subject(subject)
                .timestamp(event.getTime())
                .environmentId(environmentId)
                .modeUpdated(new ModeUpdated(mode))
                .build();
    }

    /**
     * Creates a unique key for deduplication.
     * <p>
     * Uses the strategy: {@code subject + methodName + schemaId}
     * </p>
     * <p>
     * This works because:
     * </p>
     * <ul>
     *   <li>New schema version = new schema ID = unique key = processed</li>
     *   <li>Duplicate audit events for same registration = same schema ID = deduplicated</li>
     * </ul>
     *
     * @param event The audit log event
     * @return A unique deduplication key
     */
    @Nonnull
    public String createDeduplicationKey(@Nonnull AuditLogEvent event) {
        AuditLogEvent.AuditLogData data = event.getData();
        String methodName = data != null ? data.getMethodName() : "unknown";
        
        String subject = "unknown";
        if (data != null) {
            AuditLogEvent.RequestData request = data.getRequest();
            if (request != null && request.getSubject() != null) {
                subject = request.getSubject();
            } else {
                subject = data.getResourceName();
            }
        }
        
        Integer schemaId = null;
        if (data != null && data.getResult() != null) {
            schemaId = data.getResult().getSchemaId();
        }
        
        return String.format("%s:%s:%s", subject, methodName, schemaId != null ? schemaId : "null");
    }
}
