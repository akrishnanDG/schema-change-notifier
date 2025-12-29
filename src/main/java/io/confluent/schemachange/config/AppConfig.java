package io.confluent.schemachange.config;

import io.confluent.schemachange.exception.ConfigurationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.confluent.schemachange.config.KafkaConstants.*;

/**
 * Application configuration holder for all connection and processing settings.
 * <p>
 * This class encapsulates all configuration required for the Schema Change Notifier,
 * including audit log cluster credentials, target cluster settings, and processing options.
 * </p>
 *
 * <h2>Configuration Categories:</h2>
 * <ul>
 *   <li><b>Audit Log Cluster</b> - Source cluster containing Confluent Cloud audit logs</li>
 *   <li><b>Schema Registries</b> - Per-environment Schema Registry configurations</li>
 *   <li><b>Target Cluster</b> - Destination for schema change notifications</li>
 *   <li><b>Processing</b> - Mode, filtering, and operational settings</li>
 * </ul>
 *
 * @see EnvironmentConfig
 * @see ProcessingMode
 */
public class AppConfig {

    // ==================== Audit Log Cluster Configuration ====================

    private String auditLogBootstrapServers;
    private String auditLogApiKey;
    private String auditLogApiSecret;
    private String auditLogTopic = AUDIT_LOG_TOPIC;

    // ==================== Environment-specific Schema Registry Configurations ====================

    /**
     * Map of environment ID to Schema Registry configuration.
     * Key is the environment ID (e.g., "env-abc123").
     */
    private Map<String, EnvironmentConfig> environments = new HashMap<>();

    // ==================== Target Cluster Configuration ====================

    private String targetBootstrapServers;
    private String targetApiKey;
    private String targetApiSecret;
    private String targetTopic;

    // ==================== Target Schema Registry Configuration ====================

    private String targetSchemaRegistryUrl;
    private String targetSchemaRegistryApiKey;
    private String targetSchemaRegistryApiSecret;

    // ==================== Processing Configuration ====================

    private ProcessingMode processingMode = ProcessingMode.STREAM;
    private String startTimestamp;
    private String endTimestamp;
    private boolean stopAtCurrent = false;

    // ==================== Consumer Configuration ====================

    private String consumerGroupId = DEFAULT_CONSUMER_GROUP;

    // ==================== Filtering Configuration ====================

    private Set<String> includeMethods = new HashSet<>(Set.of(
            "schema-registry.RegisterSchema",
            "schema-registry.DeleteSchema",
            "schema-registry.DeleteSubject"
    ));
    private boolean includeConfigChanges = false;
    private boolean onlySuccessful = true;
    private Set<String> subjectFilters = new HashSet<>();

    // ==================== Deduplication Configuration ====================

    private boolean enableDeduplication = true;
    private String stateFilePath = DEFAULT_STATE_FILE_PATH;

    // ==================== Operational Configuration ====================

    private boolean dryRun = false;
    private int pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
    private int batchSize = DEFAULT_BATCH_SIZE;

    // ==================== Audit Log Cluster Getters/Setters ====================

    /**
     * Gets the bootstrap servers for the audit log Kafka cluster.
     *
     * @return The bootstrap servers string (e.g., "pkc-xxx.region.aws.confluent.cloud:9092")
     */
    @Nullable
    public String getAuditLogBootstrapServers() {
        return auditLogBootstrapServers;
    }

    /**
     * Sets the bootstrap servers for the audit log Kafka cluster.
     *
     * @param auditLogBootstrapServers The bootstrap servers string
     */
    public void setAuditLogBootstrapServers(@Nonnull String auditLogBootstrapServers) {
        this.auditLogBootstrapServers = auditLogBootstrapServers;
    }

    /**
     * Gets the API key for authenticating to the audit log cluster.
     *
     * @return The API key
     */
    @Nullable
    public String getAuditLogApiKey() {
        return auditLogApiKey;
    }

    /**
     * Sets the API key for authenticating to the audit log cluster.
     *
     * @param auditLogApiKey The API key
     */
    public void setAuditLogApiKey(@Nonnull String auditLogApiKey) {
        this.auditLogApiKey = auditLogApiKey;
    }

    /**
     * Gets the API secret for authenticating to the audit log cluster.
     *
     * @return The API secret
     */
    @Nullable
    public String getAuditLogApiSecret() {
        return auditLogApiSecret;
    }

    /**
     * Sets the API secret for authenticating to the audit log cluster.
     *
     * @param auditLogApiSecret The API secret
     */
    public void setAuditLogApiSecret(@Nonnull String auditLogApiSecret) {
        this.auditLogApiSecret = auditLogApiSecret;
    }

    /**
     * Gets the audit log topic name.
     *
     * @return The topic name (default: "confluent-audit-log-events")
     */
    @Nonnull
    public String getAuditLogTopic() {
        return auditLogTopic;
    }

    /**
     * Sets the audit log topic name.
     *
     * @param auditLogTopic The topic name
     */
    public void setAuditLogTopic(@Nonnull String auditLogTopic) {
        this.auditLogTopic = auditLogTopic;
    }

    // ==================== Environment Configuration Methods ====================

    /**
     * Gets all configured environment configurations.
     *
     * @return Unmodifiable map of environment ID to configuration
     */
    @Nonnull
    public Map<String, EnvironmentConfig> getEnvironments() {
        return environments;
    }

    /**
     * Sets all environment configurations.
     *
     * @param environments Map of environment ID to configuration
     */
    public void setEnvironments(@Nonnull Map<String, EnvironmentConfig> environments) {
        this.environments = environments;
    }

    /**
     * Adds an environment configuration.
     *
     * @param envConfig The environment configuration to add
     * @throws ConfigurationException if the configuration is invalid
     */
    public void addEnvironment(@Nonnull EnvironmentConfig envConfig) {
        envConfig.validate();
        this.environments.put(envConfig.getEnvironmentId(), envConfig);
    }

    /**
     * Gets the configuration for a specific environment.
     *
     * @param environmentId The environment ID
     * @return The environment configuration, or null if not found
     */
    @Nullable
    public EnvironmentConfig getEnvironment(@Nonnull String environmentId) {
        return environments.get(environmentId);
    }

    /**
     * Checks if an environment is configured for monitoring.
     *
     * @param environmentId The environment ID to check
     * @return true if the environment is configured
     */
    public boolean hasEnvironment(@Nonnull String environmentId) {
        return environments.containsKey(environmentId);
    }

    /**
     * Gets the set of monitored environment IDs.
     *
     * @return Set of environment IDs
     */
    @Nonnull
    public Set<String> getMonitoredEnvironmentIds() {
        return environments.keySet();
    }

    // ==================== Target Cluster Getters/Setters ====================

    /**
     * Gets the bootstrap servers for the target Kafka cluster.
     *
     * @return The bootstrap servers string
     */
    @Nullable
    public String getTargetBootstrapServers() {
        return targetBootstrapServers;
    }

    /**
     * Sets the bootstrap servers for the target Kafka cluster.
     *
     * @param targetBootstrapServers The bootstrap servers string
     */
    public void setTargetBootstrapServers(@Nonnull String targetBootstrapServers) {
        this.targetBootstrapServers = targetBootstrapServers;
    }

    /**
     * Gets the API key for authenticating to the target cluster.
     *
     * @return The API key
     */
    @Nullable
    public String getTargetApiKey() {
        return targetApiKey;
    }

    /**
     * Sets the API key for authenticating to the target cluster.
     *
     * @param targetApiKey The API key
     */
    public void setTargetApiKey(@Nonnull String targetApiKey) {
        this.targetApiKey = targetApiKey;
    }

    /**
     * Gets the API secret for authenticating to the target cluster.
     *
     * @return The API secret
     */
    @Nullable
    public String getTargetApiSecret() {
        return targetApiSecret;
    }

    /**
     * Sets the API secret for authenticating to the target cluster.
     *
     * @param targetApiSecret The API secret
     */
    public void setTargetApiSecret(@Nonnull String targetApiSecret) {
        this.targetApiSecret = targetApiSecret;
    }

    /**
     * Gets the target topic for schema change notifications.
     *
     * @return The topic name
     */
    @Nullable
    public String getTargetTopic() {
        return targetTopic;
    }

    /**
     * Sets the target topic for schema change notifications.
     *
     * @param targetTopic The topic name
     */
    public void setTargetTopic(@Nonnull String targetTopic) {
        this.targetTopic = targetTopic;
    }

    // ==================== Target Schema Registry Getters/Setters ====================

    /**
     * Gets the URL for the target Schema Registry.
     *
     * @return The Schema Registry URL
     */
    @Nullable
    public String getTargetSchemaRegistryUrl() {
        return targetSchemaRegistryUrl;
    }

    /**
     * Sets the URL for the target Schema Registry.
     *
     * @param targetSchemaRegistryUrl The Schema Registry URL
     */
    public void setTargetSchemaRegistryUrl(@Nonnull String targetSchemaRegistryUrl) {
        this.targetSchemaRegistryUrl = targetSchemaRegistryUrl;
    }

    /**
     * Gets the API key for authenticating to the target Schema Registry.
     *
     * @return The API key
     */
    @Nullable
    public String getTargetSchemaRegistryApiKey() {
        return targetSchemaRegistryApiKey;
    }

    /**
     * Sets the API key for authenticating to the target Schema Registry.
     *
     * @param targetSchemaRegistryApiKey The API key
     */
    public void setTargetSchemaRegistryApiKey(@Nonnull String targetSchemaRegistryApiKey) {
        this.targetSchemaRegistryApiKey = targetSchemaRegistryApiKey;
    }

    /**
     * Gets the API secret for authenticating to the target Schema Registry.
     *
     * @return The API secret
     */
    @Nullable
    public String getTargetSchemaRegistryApiSecret() {
        return targetSchemaRegistryApiSecret;
    }

    /**
     * Sets the API secret for authenticating to the target Schema Registry.
     *
     * @param targetSchemaRegistryApiSecret The API secret
     */
    public void setTargetSchemaRegistryApiSecret(@Nonnull String targetSchemaRegistryApiSecret) {
        this.targetSchemaRegistryApiSecret = targetSchemaRegistryApiSecret;
    }

    // ==================== Processing Configuration Getters/Setters ====================

    /**
     * Gets the processing mode.
     *
     * @return The processing mode
     */
    @Nonnull
    public ProcessingMode getProcessingMode() {
        return processingMode;
    }

    /**
     * Sets the processing mode.
     *
     * @param processingMode The processing mode
     */
    public void setProcessingMode(@Nonnull ProcessingMode processingMode) {
        this.processingMode = processingMode;
    }

    /**
     * Gets the start timestamp for TIMESTAMP mode.
     *
     * @return The start timestamp in ISO-8601 format
     */
    @Nullable
    public String getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Sets the start timestamp for TIMESTAMP mode.
     *
     * @param startTimestamp The start timestamp in ISO-8601 format
     */
    public void setStartTimestamp(@Nullable String startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    /**
     * Gets the end timestamp for TIMESTAMP mode.
     *
     * @return The end timestamp in ISO-8601 format
     */
    @Nullable
    public String getEndTimestamp() {
        return endTimestamp;
    }

    /**
     * Sets the end timestamp for TIMESTAMP mode.
     *
     * @param endTimestamp The end timestamp in ISO-8601 format
     */
    public void setEndTimestamp(@Nullable String endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    /**
     * Checks if processing should stop at the current offset.
     *
     * @return true if should stop at current offset
     */
    public boolean isStopAtCurrent() {
        return stopAtCurrent;
    }

    /**
     * Sets whether processing should stop at the current offset.
     *
     * @param stopAtCurrent true to stop at current offset
     */
    public void setStopAtCurrent(boolean stopAtCurrent) {
        this.stopAtCurrent = stopAtCurrent;
    }

    // ==================== Consumer Configuration Getters/Setters ====================

    /**
     * Gets the consumer group ID.
     *
     * @return The consumer group ID
     */
    @Nonnull
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    /**
     * Sets the consumer group ID.
     *
     * @param consumerGroupId The consumer group ID
     */
    public void setConsumerGroupId(@Nonnull String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    // ==================== Filtering Configuration Getters/Setters ====================

    /**
     * Gets the set of Schema Registry methods to include.
     *
     * @return Set of method names
     */
    @Nonnull
    public Set<String> getIncludeMethods() {
        return includeMethods;
    }

    /**
     * Sets the set of Schema Registry methods to include.
     *
     * @param includeMethods Set of method names
     */
    public void setIncludeMethods(@Nonnull Set<String> includeMethods) {
        this.includeMethods = includeMethods;
    }

    /**
     * Checks if configuration change events should be included.
     *
     * @return true if config changes should be included
     */
    public boolean isIncludeConfigChanges() {
        return includeConfigChanges;
    }

    /**
     * Sets whether configuration change events should be included.
     *
     * @param includeConfigChanges true to include config changes
     */
    public void setIncludeConfigChanges(boolean includeConfigChanges) {
        this.includeConfigChanges = includeConfigChanges;
        if (includeConfigChanges) {
            this.includeMethods.add("schema-registry.UpdateCompatibility");
            this.includeMethods.add("schema-registry.UpdateMode");
        }
    }

    /**
     * Checks if only successful operations should be processed.
     *
     * @return true if only successful operations should be processed
     */
    public boolean isOnlySuccessful() {
        return onlySuccessful;
    }

    /**
     * Sets whether only successful operations should be processed.
     *
     * @param onlySuccessful true to process only successful operations
     */
    public void setOnlySuccessful(boolean onlySuccessful) {
        this.onlySuccessful = onlySuccessful;
    }

    /**
     * Gets the subject filter patterns.
     *
     * @return Set of subject filter patterns
     */
    @Nonnull
    public Set<String> getSubjectFilters() {
        return subjectFilters;
    }

    /**
     * Sets the subject filter patterns.
     *
     * @param subjectFilters Set of subject filter patterns (supports glob patterns)
     */
    public void setSubjectFilters(@Nonnull Set<String> subjectFilters) {
        this.subjectFilters = subjectFilters;
    }

    /**
     * Checks if subject filters are configured.
     *
     * @return true if subject filters are set
     */
    public boolean hasSubjectFilters() {
        return subjectFilters != null && !subjectFilters.isEmpty();
    }

    // ==================== Deduplication Configuration Getters/Setters ====================

    /**
     * Checks if deduplication is enabled.
     *
     * @return true if deduplication is enabled
     */
    public boolean isEnableDeduplication() {
        return enableDeduplication;
    }

    /**
     * Sets whether deduplication is enabled.
     *
     * @param enableDeduplication true to enable deduplication
     */
    public void setEnableDeduplication(boolean enableDeduplication) {
        this.enableDeduplication = enableDeduplication;
    }

    /**
     * Gets the path for the deduplication state file.
     *
     * @return The state file path
     */
    @Nonnull
    public String getStateFilePath() {
        return stateFilePath;
    }

    /**
     * Sets the path for the deduplication state file.
     *
     * @param stateFilePath The state file path
     */
    public void setStateFilePath(@Nonnull String stateFilePath) {
        this.stateFilePath = stateFilePath;
    }

    // ==================== Operational Configuration Getters/Setters ====================

    /**
     * Checks if dry run mode is enabled.
     *
     * @return true if dry run mode is enabled
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Sets whether dry run mode is enabled.
     *
     * @param dryRun true to enable dry run mode
     */
    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    /**
     * Gets the poll timeout in milliseconds.
     *
     * @return The poll timeout
     */
    public int getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    /**
     * Sets the poll timeout in milliseconds.
     *
     * @param pollTimeoutMs The poll timeout
     */
    public void setPollTimeoutMs(int pollTimeoutMs) {
        this.pollTimeoutMs = pollTimeoutMs;
    }

    /**
     * Gets the batch size for consumer polling.
     *
     * @return The batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size for consumer polling.
     *
     * @param batchSize The batch size
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    // ==================== Validation ====================

    /**
     * Validates that all required configuration is present.
     *
     * @throws ConfigurationException if required configuration is missing or invalid
     */
    public void validate() {
        StringBuilder errors = new StringBuilder();

        if (isNullOrEmpty(auditLogBootstrapServers)) {
            errors.append("audit.log.bootstrap.servers is required.\n");
        }
        if (isNullOrEmpty(auditLogApiKey)) {
            errors.append("audit.log.api.key is required.\n");
        }
        if (isNullOrEmpty(auditLogApiSecret)) {
            errors.append("audit.log.api.secret is required.\n");
        }
        if (environments == null || environments.isEmpty()) {
            errors.append("At least one environment must be configured.\n");
        } else {
            for (Map.Entry<String, EnvironmentConfig> entry : environments.entrySet()) {
                try {
                    entry.getValue().validate();
                } catch (ConfigurationException e) {
                    errors.append(e.getMessage()).append("\n");
                }
            }
        }
        if (isNullOrEmpty(targetBootstrapServers)) {
            errors.append("target.bootstrap.servers is required.\n");
        }
        if (isNullOrEmpty(targetApiKey)) {
            errors.append("target.api.key is required.\n");
        }
        if (isNullOrEmpty(targetApiSecret)) {
            errors.append("target.api.secret is required.\n");
        }
        if (isNullOrEmpty(targetTopic)) {
            errors.append("target.topic is required.\n");
        }
        if (processingMode == ProcessingMode.TIMESTAMP && isNullOrEmpty(startTimestamp)) {
            errors.append("start.timestamp is required for TIMESTAMP mode.\n");
        }

        if (!errors.isEmpty()) {
            throw new ConfigurationException("Configuration validation failed:\n" + errors);
        }
    }

    private boolean isNullOrEmpty(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
